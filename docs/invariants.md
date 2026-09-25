# Live Pod Migration — Core Safety Invariants (`I1`–`I9`)

The Live Pod Migration (`LPM`) controller evaluates a stateless set of nine safety invariants (`I1`–`I9`) at the end of every successful reconciliation step across `PodMigrationJobReconciler`, `PodGateReconciler`, and `PodMigrationReconciler`.

Each invariant predicate inspects an in-memory `ReconcileSnapshot` built from the reconciled object and the controller-runtime local informer cache (zero additional Kubernetes API server reads).

## Execution Modes (`--invariant-check-mode`)

- `observe` (default): Logs structured errors, increments `pod_migration_invariant_violations_total{invariant="I1".."I9"}`, and emits a deduplicated Kubernetes `Warning` Event (`Reason=InvariantViolation`) on first observation without interrupting the active migration.
- `strict`: Performs all `observe` actions and immediately transitions any violating `PodMigrationJob` to `Phase=Failed` (`Reason=InvariantViolation`), cleans up its `PodSnapshotManualTrigger`, and suppresses further requeues so the replacement Pod falls back to a clean cold start.
- `off`: Disables invariant evaluation.

---

## The 9 Core Invariants

### `I1` — At-Most-Once Restore (`AtMostOnceRestore`)
A `PodSnapshot` represents a point-in-time checkpoint of a single process tree and its file descriptors, sequence numbers, and in-memory state. Restoring the same snapshot into two concurrent Pod instances creates a split-brain state where two processes believe they own the same session or lock. `I1` verifies that no two active replacement Pods in the namespace carry the same `podsnapshot.gke.io/ps-name` annotation, and that once a `PodMigrationJob` has marked `Status.Consumed=true` and `Status.GateReleased=true` for a specific `Status.RestoredPodUID`, no Pod with a different UID is ever bound to that snapshot.

### `I2` — No Silent Cold-Start (`NoSilentColdStart`)
A `PodMigrationJob` must never report `Phase=Succeeded` (which signals a verified warm checkpoint restore) if the replacement Pod actually started cold without restoring from the snapshot. `I2` verifies that any `PodMigrationJob` in `Phase=Succeeded` has a non-empty `Status.SnapshotRef` and that its replacement Pod carries `podsnapshot.gke.io/ps-name=<Status.SnapshotRef>` rather than the explicit cold-start bypass (`podsnapshot.gke.io/ps-name: ""`). Conversely, if a gated replacement Pod assigned to a `PodMigrationJob` has its `gke.io/pod-migration-gate` removed, it must carry either a valid snapshot name or the explicit empty-string bypass (`podsnapshot.gke.io/ps-name: ""`) so it cannot accidentally restore a stale snapshot.

### `I3` — Gate Liveness (`GateLiveness`)
Replacement Pods opted into migration receive the `gke.io/pod-migration-gate` scheduling gate at admission so they remain `Pending` (`SchedulingGated`) until checkpointing and volume detachment finish. If a `PodMigrationJob` reaches a terminal phase (`Succeeded`, `Failed`, or `SucceededWithoutRestore`) and is consumed, or if it has already set `Status.GateReleased=true` for the replacement Pod's UID, leaving the scheduling gate on the Pod permanently strands the workload unschedulable. `I3` verifies that no non-terminating Pod retains `gke.io/pod-migration-gate` after its assigned `PodMigrationJob` is terminal and consumed, or beyond the 30-second two-step release grace window (`GateReleaseGraceWindow`) after `Status.GateReleased=true`.

### `I4` — Terminal Progress (`TerminalProgress`)
Every active migration (`Pending`, `Snapshotting`, `Evicting`) and every `PodMigration` deletion deferral (`podmigration.gke.io/storage-cleanup` finalizer) must terminate within a bounded deadline rather than hanging indefinitely. `I4` computes the effective migration timeout from `pod-migration.gke.io/timeout` (or memory-scaled checkpoint budget at 50 MiB/s up to 2 hours, defaulting to 10 minutes) plus a 30-second reconciliation slack window, and verifies that no active `PodMigrationJob` or deleting `PodMigration` exceeds its deadline without transitioning to a terminal state.

### `I5` — Zero Resource Leak (`ZeroResourceLeak`)
When a `PodMigrationJob` concludes (`Succeeded`, `Failed`, or `SucceededWithoutRestore`), the temporary `PodSnapshotManualTrigger` (`psmt-<pod>-<uid>`) created to trigger the GKE checkpoint must be deleted so stale triggers do not accumulate in etcd or block subsequent migrations. `I5` inspects the local cache whenever a `PodMigrationJob` is in a terminal phase and flags any un-deleted `PodSnapshotManualTrigger` still owned by that job.

### `I6` — Revision & Identity Fidelity (`RevisionAndIdentityFidelity`)
During rolling updates (`Deployment` or `StatefulSet` rollout) or `Indexed Job` execution, a checkpoint taken from an older revision or different completion index must never be restored into a replacement Pod belonging to a newer revision or different index. `I6` verifies that when a `PodMigrationJob` is bound to a replacement Pod (`Status.RestoredPodName`), their `pod-template-hash` (`pod-migration.gke.io/pod-template-hash`), `controller-revision-hash`, `batch.kubernetes.io/job-completion-index`, and parent workload UID (`pod-migration.gke.io/parent-uid`) match.

### `I7` — Disruption Bounding & PDB Compliance (`DisruptionBoundingPDBCompliance`)
Terminating the origin Pod during `Phase=Evicting` must respect Kubernetes `PodDisruptionBudget` (`PDB`) constraints via the `policy/v1` `Eviction` subresource rather than issuing a raw `Delete` before the migration deadline expires. `I7` verifies that raw Pod deletion is never invoked before deadline expiry, and that a `PodMigrationJob` never advances to `Restoring` or `Succeeded` while `BlockedByPDB=True` / `EvictionMisconfigured=True` remains active or while the non-terminating origin Pod (`TargetPodUID`) is still running.

### `I8` — Platform & Control-Plane Isolation (`PlatformAndControlPlaneIsolation`)
System and control-plane components running in `kube-system`, `pod-migration-system`, or `gke-managed-*` namespaces must never be intercepted, gated, or checkpointed by the migration controller, as gating critical cluster infrastructure (including the migration webhook/controller itself) can deadlock the cluster. `I8` verifies that no Pod in an excluded system namespace carries `gke.io/pod-migration-gate` and no `PodMigrationJob` is created in those namespaces.

### `I9` — Deterministic Fallback over Crashloop (`DeterministicFallbackOverCrashloop`)
When the container runtime (`gVisor` / `runsc` or `runc`) fails to restore a container from a snapshot (surfacing as `StartError` / `RunContainerError` with exit code `128` and a known engine restore-crash signature classified by `restore.Classify`), the controller must deterministically fail the `PodMigrationJob`, delete the crashed replacement Pod, and allow a clean cold start rather than leaving the Pod in `CrashLoopBackOff` or marking the migration `Succeeded`. `I9` verifies via live `restore.Classify` inspection and `RestoreCrashed` conditions that no `PodMigrationJob` with a fatal restore crash remains in `Restoring` or transitions to `Succeeded`.
