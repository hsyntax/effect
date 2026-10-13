// Run from the repository root with a Node version that supports TypeScript:
// node packages/effect/examples/cluster-workflow-lost-wakeup.ts
//
// ClusterWorkflowEngine runs in one process with memory storage and no-op runners.
// Storage hooks control the callback timing; sleeps only observe whether the
// parent makes further progress.
import { Effect, Latch, Layer, Option, Schedule, Schema } from "effect"
import {
  ClusterWorkflowEngine,
  MessageStorage,
  RunnerHealth,
  Runners,
  RunnerStorage,
  Sharding,
  ShardingConfig
} from "effect/cluster"
import { Activity, DurableDeferred, Workflow } from "effect/workflow"
import * as NodeRuntime from "../../platform/node/src/NodeRuntime.ts"

const program = Effect.gen(function*() {
  const activitySuspended = yield* Latch.make()
  const callbackSaved = yield* Latch.make()
  const siblingStarted = yield* Latch.make()
  const releaseB = yield* Latch.make()
  const callback = DurableDeferred.make("LostWakeup/Callback", { success: Schema.String })
  const workflow = Workflow.make("LostWakeup", {
    payload: {},
    success: Schema.Array(Schema.String),
    idempotencyKey: () => "one"
  })

  const activityA = Activity.make({
    name: "A",
    success: Schema.String,
    // Ensure B is active before A can suspend.
    execute: siblingStarted.await.pipe(Effect.andThen(DurableDeferred.await(callback)))
  })
  const activityB = Activity.make({
    name: "B",
    success: Schema.String,
    execute: siblingStarted.open.pipe(Effect.andThen(releaseB.await), Effect.as("B done"))
  })

  // Observe actual commits, rather than guessing when A or the callback finishes.
  const storageLayer = Layer.effect(
    MessageStorage.MessageStorage,
    Effect.map(MessageStorage.MessageStorage, (storage) => ({
      ...storage,
      saveReply: (reply) =>
        storage.saveReply(reply).pipe(
          Effect.tap(() => {
            if (reply.rpc._tag === "deferred") return callbackSaved.open
            if (
              reply.rpc._tag === "activity" && reply.reply._tag === "WithExit" &&
              reply.reply.exit._tag === "Success" && Schema.is(Workflow.Suspended)(reply.reply.exit.value)
            ) {
              return activitySuspended.open
            }
            return Effect.void
          })
        )
    }))
  ).pipe(Layer.provide(MessageStorage.layerMemory))

  const engineLayer = ClusterWorkflowEngine.layer.pipe(
    Layer.provideMerge(Sharding.layer),
    Layer.provide(Runners.layerNoop),
    Layer.provideMerge(storageLayer),
    Layer.provide(RunnerStorage.layerMemory),
    Layer.provide(RunnerHealth.layerNoop),
    Layer.provide(ShardingConfig.layer({
      shardsPerGroup: 300,
      entityMailboxCapacity: 10,
      entityTerminationTimeout: 0,
      entityMessagePollInterval: 100,
      sendRetryInterval: 10
    }))
  )
  const workflowLayer = workflow.toLayer(() => Effect.all([activityA, activityB], { concurrency: "unbounded" })).pipe(
    Layer.provideMerge(engineLayer)
  )

  const readParent = Effect.fnUntraced(function*(executionId: string) {
    const sharding = yield* Sharding.Sharding
    yield* sharding.pollStorage
    return yield* workflow.poll(executionId)
  })

  const waitForParent = (executionId: string, tag?: "Complete") =>
    readParent(executionId).pipe(
      Effect.repeat({
        schedule: Schedule.spaced("20 millis"),
        until: (reply) => {
          if (Option.isNone(reply)) return false
          return tag === undefined || reply.value._tag === tag
        }
      }),
      Effect.timeout("5 seconds"),
      Effect.map(Option.getOrThrow)
    )

  const observeParent = Effect.fnUntraced(function*(executionId: string) {
    let reply = yield* waitForParent(executionId)
    for (let attempt = 1; attempt <= 3; attempt++) {
      yield* Effect.sleep("300 millis")
      reply = yield* waitForParent(executionId)
      console.log(`   Poll ${attempt}: ${reply._tag}`)
    }
    return reply
  })

  const demonstrate = Effect.gen(function*() {
    yield* Effect.addFinalizer(() => releaseB.open)

    // 1. Start without an execute waiter that could retry and hide the bug.
    console.log("1. Start the workflow with discard: true.")
    const executionId = yield* workflow.execute({}, { discard: true })

    // 2. Establish the race: A has suspended, but B keeps the parent active.
    yield* Effect.all([activitySuspended.await, siblingStarted.await]).pipe(Effect.timeout("5 seconds"))
    const beforeCallback = yield* readParent(executionId)
    if (Option.isSome(beforeCallback)) {
      return yield* Effect.fail(new Error("Expected the parent to still be active while B is blocked"))
    }
    console.log("2. A's Suspended reply is saved. B and the parent are still active.")

    // 3. Deliver and persist the callback before allowing B to finish.
    yield* DurableDeferred.succeed(callback, {
      token: DurableDeferred.tokenFromExecutionId(callback, { workflow, executionId }),
      value: "A done"
    })
    yield* callbackSaved.await.pipe(Effect.timeout("5 seconds"))
    console.log("3. Callback reply is persisted while B is still running.")

    // 4. Finish B and observe whether the parent can complete by itself.
    yield* releaseB.open
    console.log("4. B is released. Observing the parent:")
    const parent = yield* observeParent(executionId)
    if (parent._tag === "Complete") {
      console.log("Automatic completion succeeded; the lost wakeup did not reproduce.")
      console.log("Result:", yield* parent.exit)
      return
    }

    // 5. A manual resume recovers the same execution using the saved results.
    console.log("BUG: parent remains Suspended even though the callback is recorded and B has finished.")
    yield* workflow.resume(executionId)
    const recovered = yield* waitForParent(executionId, "Complete")
    if (recovered._tag === "Complete") {
      console.log("5. Manual resume completed the workflow.")
      console.log("Result:", yield* recovered.exit)
    }
  })

  yield* demonstrate.pipe(Effect.provide(workflowLayer))
})

NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.timeout("20 seconds")))
