---
"effect": patch
---

Fix lost wake-ups in `ClusterWorkflowEngine` when a `DurableDeferred` or `DurableClock.sleep` awaited inside an activity completes while a sibling activity is still running. The run now replays instead of suspending when a completion arrived during it, and a completion that lands while the suspension is being stored waits for the run reply before resuming.
