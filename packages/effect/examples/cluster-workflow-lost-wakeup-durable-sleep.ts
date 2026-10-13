// Run from the repository root with a Node version that supports TypeScript:
// node packages/effect/examples/cluster-workflow-lost-wakeup-durable-sleep.ts
//
// The same lost wakeup as cluster-workflow-lost-wakeup.ts, without any external
// callback: a durable sleep inside one activity ends while a sibling activity
// is still running. ClusterWorkflowEngine runs in one process with memory
// storage and no-op runners. The two durations set the order of events; storage
// hooks only report what was committed.
import { Duration, Effect, Latch, Layer, Option, Schedule, Schema } from "effect"
import {
  ClusterWorkflowEngine,
  MessageStorage,
  RunnerHealth,
  Runners,
  RunnerStorage,
  Sharding,
  ShardingConfig
} from "effect/cluster"
import { Activity, DurableClock, Workflow } from "effect/workflow"
import * as NodeRuntime from "../../platform/node/src/NodeRuntime.ts"

const program = Effect.gen(function*() {
  const activitySuspended = yield* Latch.make()
  const clockFired = yield* Latch.make()
  const siblingStarted = yield* Latch.make()
  const siblingFinished = yield* Latch.make()
  const DurableSleepWorkflow = Workflow.make("DurableSleepWorkflow", {
    payload: {},
    success: Schema.Array(Schema.String),
    idempotencyKey: () => "one"
  })

  // Activity A sleeps durably for 1 second while activity B works for 3 seconds.
  const SleepDurably = Activity.make({
    name: "SleepDurably",
    success: Schema.String,
    execute: Effect.gen(function*() {
      yield* siblingStarted.await
      // Sleeps of up to 60 seconds stay in memory by default and are not
      // affected. A zero threshold puts this short one on the durable clock.
      yield* DurableClock.sleep({ name: "Nap", duration: "1 second", inMemoryThreshold: Duration.zero })
      return "A done"
    })
  })
  const WorkLonger = Activity.make({
    name: "WorkLonger",
    success: Schema.String,
    execute: Effect.gen(function*() {
      yield* siblingStarted.open
      yield* Effect.sleep("3 seconds")
      yield* siblingFinished.open
      return "B done"
    })
  })

  // Observe actual commits, rather than guessing when A suspends or the clock fires.
  const StorageLayer = Layer.effect(
    MessageStorage.MessageStorage,
    Effect.gen(function*() {
      const storage = yield* MessageStorage.MessageStorage
      return MessageStorage.MessageStorage.of({
        ...storage,
        // The clock wakes the workflow by completing the sleep's deferred.
        saveRequest: (request) =>
          storage.saveRequest(request).pipe(
            Effect.tap(() => request.rpc._tag === "deferred" ? clockFired.open : Effect.void)
          ),
        saveReply: (reply) =>
          storage.saveReply(reply).pipe(
            Effect.tap(() => {
              if (
                reply.rpc._tag === "activity" && reply.reply._tag === "WithExit" &&
                reply.reply.exit._tag === "Success" && Schema.is(Workflow.Suspended)(reply.reply.exit.value)
              ) {
                return activitySuspended.open
              }
              return Effect.void
            })
          )
      })
    })
  ).pipe(Layer.provide(MessageStorage.layerMemory))

  const WorkflowEngineLayer = ClusterWorkflowEngine.layer.pipe(
    Layer.provideMerge(Sharding.layer),
    Layer.provide(Runners.layerNoop),
    Layer.provideMerge(StorageLayer),
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
  const DurableSleepWorkflowLayer = DurableSleepWorkflow.toLayer(() =>
    Effect.all([SleepDurably, WorkLonger], { concurrency: "unbounded" })
  ).pipe(
    Layer.provideMerge(WorkflowEngineLayer)
  )

  const readParent = Effect.fnUntraced(function*(executionId: string) {
    const sharding = yield* Sharding.Sharding
    yield* sharding.pollStorage
    return yield* DurableSleepWorkflow.poll(executionId)
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
    // 1. Start without an execute waiter, so only the clock can wake the run.
    console.log("1. Start the workflow with discard: true.")
    const executionId = yield* DurableSleepWorkflow.execute({}, { discard: true })

    // 2. Establish the race: A has suspended on its sleep, but B keeps the parent active.
    yield* Effect.all([activitySuspended.await, siblingStarted.await]).pipe(Effect.timeout("5 seconds"))
    if (Option.isSome(yield* readParent(executionId))) {
      return yield* Effect.fail(new Error("Expected the parent to still be active while B is working"))
    }
    console.log("2. A's Suspended reply is saved. B and the parent are still active.")

    // 3. The sleep elapses on its own, two seconds before B finishes.
    yield* clockFired.await.pipe(Effect.timeout("5 seconds"))
    if (siblingFinished.isOpen() || Option.isSome(yield* readParent(executionId))) {
      return yield* Effect.fail(new Error("Expected the clock to fire while B is still working"))
    }
    console.log("3. The durable clock fired and recorded A's wake-up while B is still running.")

    // 4. Let B finish and observe whether the parent can complete by itself.
    yield* siblingFinished.await.pipe(Effect.timeout("5 seconds"))
    console.log("4. B finished. Observing the parent:")
    const parent = yield* observeParent(executionId)
    if (parent._tag === "Complete") {
      console.log("Automatic completion succeeded; the lost wakeup did not reproduce.")
      console.log("Result:", yield* parent.exit)
      return
    }

    // 5. A manual resume recovers the same execution using the saved results.
    console.log("BUG: parent remains Suspended even though the sleep has elapsed and B has finished.")
    yield* DurableSleepWorkflow.resume(executionId)
    const recovered = yield* waitForParent(executionId, "Complete")
    if (recovered._tag === "Complete") {
      console.log("5. Manual resume completed the workflow.")
      console.log("Result:", yield* recovered.exit)
    }
  })

  yield* demonstrate.pipe(Effect.provide(DurableSleepWorkflowLayer))
})

NodeRuntime.runMain(program.pipe(Effect.scoped, Effect.timeout("20 seconds")))
