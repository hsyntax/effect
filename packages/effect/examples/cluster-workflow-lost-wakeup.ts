// Run from the repository root with a Node version that supports TypeScript:
// node packages/effect/examples/cluster-workflow-lost-wakeup.ts
// node packages/effect/examples/cluster-workflow-lost-wakeup.ts single
// node packages/effect/examples/cluster-workflow-lost-wakeup.ts cluster
//
// The default runs both engines sequentially. "single" uses WorkflowEngine.layerMemory;
// "cluster" uses ClusterWorkflowEngine with memory storage and no-op runners.
// Both modes run in one process. Hooks control the callback timing; sleeps only
// observe whether the parent makes further progress.
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
import { Activity, DurableDeferred, Workflow, WorkflowEngine } from "effect/workflow"
import * as NodeRuntime from "../../platform/node/src/NodeRuntime.ts"

const runMode = Effect.fn("runMode")(function*(mode: "single" | "cluster") {
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
    // Ensure B is active before A can suspend, even when activities run inline.
    execute: siblingStarted.await.pipe(
      Effect.andThen(DurableDeferred.await(callback)),
      // The memory engine runs the body directly and has no persisted replies.
      Effect.onExit(() =>
        mode === "single"
          ? Effect.flatMap(WorkflowEngine.WorkflowInstance, (instance) =>
            instance.suspended ? activitySuspended.open : Effect.void)
          : Effect.void
      )
    )
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

  const clusterEngineLayer = ClusterWorkflowEngine.layer.pipe(
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
  const engineLayer = mode === "single" ? WorkflowEngine.layerMemory : clusterEngineLayer
  const workflowLayer = workflow.toLayer(() => Effect.all([activityA, activityB], { concurrency: "unbounded" })).pipe(
    Layer.provideMerge(engineLayer)
  )

  return yield* Effect.gen(function*() {
    yield* Effect.addFinalizer(() => releaseB.open)
    const sharding = yield* Effect.serviceOption(Sharding.Sharding)
    const pollStorage = Option.isSome(sharding) ? sharding.value.pollStorage : Effect.void
    // A normal execute waiter can retry Suspended and hide the lost wakeup.
    const executionId = yield* workflow.execute({}, { discard: true })
    const poll = pollStorage.pipe(Effect.andThen(workflow.poll(executionId)))
    const waitForReply = (tag?: "Complete") =>
      poll.pipe(
        Effect.repeat({
          schedule: Schedule.spaced("20 millis"),
          until: (result) => Option.isSome(result) && (tag === undefined || result.value._tag === tag)
        }),
        Effect.timeout("5 seconds"),
        Effect.map(Option.getOrThrow)
      )

    console.log(`\n[${mode}]`)
    console.log("1. Started the workflow with discard: true.")
    yield* Effect.all([activitySuspended.await, siblingStarted.await]).pipe(Effect.timeout("5 seconds"))
    console.log(
      mode === "cluster"
        ? "2. A's Suspended reply is saved; B is still running."
        : "2. A has suspended; B is still running."
    )
    const beforeCallback = yield* poll
    if (Option.isSome(beforeCallback)) {
      return yield* Effect.fail(new Error("Expected the parent to still be active while B is blocked"))
    }
    console.log("   Parent: active, with no completed or suspended result yet.")

    yield* DurableDeferred.succeed(callback, {
      token: DurableDeferred.tokenFromExecutionId(callback, { workflow, executionId }),
      value: "A done"
    })
    if (mode === "cluster") yield* callbackSaved.await.pipe(Effect.timeout("5 seconds"))
    console.log(
      mode === "cluster"
        ? "3. Callback reply is persisted while B is still running."
        : "3. Callback is recorded in memory while B is still running."
    )

    yield* releaseB.open
    console.log("4. Released B. Polling the parent without an execute waiter:")
    yield* waitForReply()
    let result = yield* poll
    for (let i = 1; i <= 3; i++) {
      yield* Effect.sleep("300 millis")
      result = yield* poll
      console.log(`   Poll ${i}: ${Option.isSome(result) ? result.value._tag : "still active"}`)
    }
    if (Option.isNone(result)) {
      return yield* Effect.fail(new Error("Parent is still active after the observation window"))
    }
    if (result.value._tag === "Complete") {
      console.log("Automatic completion succeeded; the lost wakeup did not reproduce.")
      console.log("Result:", yield* result.value.exit)
      return "completed automatically"
    }

    console.log("BUG: parent remains Suspended even though the callback is recorded and B has finished.")
    yield* workflow.resume(executionId)
    const recovered = yield* waitForReply("Complete")
    if (recovered._tag === "Complete") {
      console.log("5. Manual resume completed the workflow.")
      console.log("Result:", yield* recovered.exit)
    }
    return "stuck until manual resume"
  }).pipe(Effect.provide(workflowLayer))
})

const program = Effect.gen(function*() {
  const mode = yield* Schema.decodeUnknownEffect(Schema.Literals(["single", "cluster", "both"]))(
    process.argv[2] ?? "both"
  )
  const modes: ReadonlyArray<"single" | "cluster"> = mode === "both" ? ["single", "cluster"] : [mode]
  for (const selected of modes) {
    const outcome = yield* runMode(selected).pipe(Effect.scoped, Effect.timeout("20 seconds"))
    console.log(`[${selected}] ${outcome}`)
  }
})

NodeRuntime.runMain(program)
