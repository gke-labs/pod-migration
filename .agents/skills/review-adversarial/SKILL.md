---
name: review-adversarial
description: Fresh-context adversarial red-team review of a pod-migration branch diff hunting for state-machine wedges, split-brain windows, silent cold starts, privilege escalations, invariant regressions, CRIU engine lifecycle and cleanup gaps, node-daemon host boundary bugs, informer cache / RBAC / Cluster Autoscaler interactions, and tests that never reach the changed code.
---

# Skill: Adversarial Pre-PR Self-Review (`review-adversarial`)

## Purpose
This skill instructs a fresh-context subagent or reviewer session to perform an aggressive, adversarial red-team code review of changes proposed in `gke-labs/pod-migration`. The reviewer must approach the diff with skepticism, assuming that subtle races, edge cases, privilege escalations, or state leaks are present.

> **Execution Context:** Run this skill in a clean session or subagent that did **not** author the code changes, ensuring unbiased review.

---

## Review Scope & Input
The review operates directly on the git diff between the target base branch (`upstream/main` by default) and the current `HEAD`:
```bash
git diff upstream/main...HEAD
```

The repository has two Go modules. Review both when the diff touches them:
- `controller/`: the orchestrator (webhooks, reconcilers, invariants, the engine seam in `controller/internal/engine/`).
- `engines/criu/`: the CRIU snapshot engine (engine controller, privileged node agent, holder).

Each snapshot engine has its own API group and its own restore order annotation, `<engine-group>/ps-name`: `podsnapshot.gke.io/ps-name` for GKE Pod Snapshots, and `criu.podmigration.gke.io/ps-name` for CRIU by default. A check that names only one of them is usually a bug.

---

## Audit Focus Areas

Areas 1–5 apply to every change. Areas 6–9 cover bug classes that earlier self-reviews missed; apply each one whenever the diff touches its scope.

### 1. State-Machine Wedges & Orphan Resources
Trace the full migration lifecycle:
`Pending` -> `Snapshotting` -> `Evicting` -> `Restoring` -> `Succeeded` / `Failed` / `SucceededWithoutRestore`

- **Phase Transitions:** Does any failure, timeout, or unexpected Kubernetes API error leave a `PodMigrationJob` stuck in a non-terminal phase?
- **Scheduling Gate Release:** Is `gke.io/pod-migration-gate` guaranteed to be removed if a migration aborts, fails, or completes? Check compliance with invariant `I3` (Gate Liveness).
- **Resource Cleanup:** Are temporary objects (`PodSnapshotManualTrigger`, ephemeral volumes, holder containers) cleaned up on all exit paths (success, failure, cancellation), in every engine's API group? Check compliance with invariant `I5` (Zero Resource Leak).
- **Finalizer Deadlocks:** Are controller finalizers (e.g., storage cleanup finalizers) guaranteed to unblock even if backend storage is unreachable, pods are force-deleted, or a spec field was edited to an invalid value?

### 2. Split-Brain & Dual-Writer Windows
Live migration must never allow the origin pod and the replacement pod to concurrently execute and process traffic or write to shared persistent storage:
- **Source Fencing:** Is the origin pod reliably isolated before the replacement pod is unscheduled/unfenced and allowed to run?
- **Endpoint Fence Verification:** Does the source pod have `pod-migration.gke.io/fenced: "true"` and condition `Ready=False` (`Reason=PodMigrationFenced`) held until eviction?
- **Pre-Checkpoint Abort:** If a migration aborts before checkpoint completion, is the source pod unfenced safely without leaking invalid state?
- **Post-Restore Re-Migration:** Can an active replacement pod be evicted or migrated again while its own restore job is still `Restoring`? Check webhook guardrails against concurrent migration collisions.

### 3. Silent Cold-Start Masquerading as Verified Restore
A migration must never falsely claim success if the workload started cold:
- **Warm vs. Cold Detection:** Does the reconciler verify that the replacement pod restored from the designated checkpoint snapshot rather than starting from a clean image?
- **Annotation & Spec Fidelity:** Does the replacement pod carry a valid `<engine-group>/ps-name` for the engine that serves the job, pointing to `Status.SnapshotRef` rather than the cold-start bypass (`""`)? Is the key the job's own engine's, not another engine's? Check compliance with `I2`.
- **Readiness vs. Restore Outcome:** Does `Phase=Succeeded` require explicit confirmation of successful memory restore rather than merely observing container `Ready` state?

### 4. Container Runtime, OCI Spec & RBAC Privilege Escalations
Inspect security boundaries across webhooks and snapshot engines:
- **Engine Privileges:** Do snapshot engines or helper components require unnecessary Linux capabilities (`CAP_SYS_ADMIN`, `CAP_CHECKPOINT_RESTORE`) or mount paths? Are capabilities strictly scoped and dropped where possible?
- **Host Namespace Leaks:** Are host namespaces (`hostPID`, `hostIPC`, `hostNetwork`) avoided unless strictly mandated by the runtime contract?
- **Admission Webhooks:** Do mutating webhooks validate input before patching pod specs or statuses? Can an unprivileged user manipulate migration annotations to bypass admission gates or hijack snapshots?
- **RBAC Scoping:** Are newly added ClusterRole rules strictly bounded to necessary verbs and API groups?

### 5. Invariant Governance & Negative Unit Testing
- **Two-Lane Governance Compliance:** Verify that `controller/internal/invariants/**` and `.github/CODEOWNERS` are untouched unless the PR explicitly follows Lane 2 rules or includes a verified `Invariant-Cochange` trailer.
- **No Weakened Invariants:** Ensure existing invariant predicates (`I1`–`I12`) have not been relaxed, bypassed, or commented out.
- **Negative Test Coverage:** Does every new error condition, validation failure, or edge case have a corresponding negative unit test asserting expected failure behavior?

### 6. CRIU Engine Lifecycle After `Succeeded`, Orphan Sweeps & Error-Path Cleanup
Scope: `engines/criu/`, and `controller/internal/engine/criu.go`. Migration safety does not end when the `PodMigrationJob` reaches `Succeeded`: the restored process keeps running as a sibling container for the rest of the pod's life.
- **Teardown of a restored pod:** On pod deletion, are the restored sibling container, its mounts and its containerd lease torn down, in an order that lets the app shut down gracefully?
- **Orphan sweep never kills a live pod:** The node agent's sweep ties each restore artifact to a pod through durable markers (for example the containerd lease label `pod-migrate.io/pod-uid`). If the pod or snapshot lookup fails or is incomplete, does the sweep keep the artifact (fail closed)? Can it race a restore that started after the sweep listed pods?
- **Frozen source is always thawed:** After the source container is frozen (its containerd task is paused; see `recordSourceFrozen` and the unfreeze path), is it resumed on every pre-restore error, checkpoint abort, cancellation, and `NotFound` race?
- **Network lock is always removed:** Are CRIU's network-lock `iptables` / `ip6tables` rules cleaned (`cleanNetnsLock`) on the same error paths?
- **Work done while the source is frozen is bounded:** Anything that runs inline between freeze and unfreeze (for example multi-part archive cleanup or a retry loop) extends the app's downtime. Does it have a short timeout and bounded concurrency?
- **Eligibility parity:** Does the controller-side `Engine.Eligible` refuse what the engine itself refuses (`hostNetwork`, a pod already restored or wrapped by the holder, a container without an explicit `command`)? A mismatch costs an eviction `429` and a refused trigger, or a checkpoint whose replacement starts cold.

### 7. Node-Daemon Host Filesystem, Syscall, `/proc` & Admission Boundaries
Scope: privileged node code (`engines/criu/internal/node/`, anything with `hostPID` or host mounts), and admission configuration in `controller/deploy.yaml` and `engines/criu/deploy/deploy.yaml`.
- **File walkers:** Do bundle and directory walkers handle entries that are not regular files (subdirectories, symlinks, devices) instead of failing with `EISDIR` or following them?
- **Path traversal:** Are relative `..` segments and symlink targets clamped under the intended root (for example an injected `procRoot`), so a path read from a pod can't escape to the host?
- **Namespaces by file descriptor:** Is a pod's namespace opened once and used through that file descriptor, and compared against the host's own namespace (`os.SameFile`), so a pod that shares a host namespace is refused rather than modified?
- **No silent host fallback:** When a `procRoot` or a command executor is injected, does every read go through it? A fallback to a plain host `os.ReadFile` or `exec` reads the wrong namespace in production and hides the bug in tests.
- **Protection after creation:** Mutating webhooks that see only `CREATE` don't protect an existing pod. Is every restore annotation (`<engine-group>/ps-name`, `pod-migrate.io/psengine-restore`) also protected on `UPDATE` of `pods` **and** `pods/status` by a `ValidatingAdmissionPolicy`, so a caller with `pods/patch` can't spoof restore state? Prefer that to a `failurePolicy: Fail` webhook on every pod `UPDATE`, which blocks pod updates while the webhook is down.
- **Identity checks:** Does a policy compare the whole service account username (namespace included), and does a test try an impostor service account, not only a plain user?

### 8. Informer Cache, RBAC & Cluster Autoscaler Interactions
Scope: reconcilers, webhooks, the node agent's manager setup, and restore placement.
- **Scheduling-gated pods and the autoscaler:** Cluster Autoscaler and node auto-provisioning ignore a pod while it carries a scheduling gate. So "no Ready node matches right now" is not proof that none will: on an autoscaled pool, does the code wait for the gate release to trigger a scale-up before it gives up and starts cold?
- **Node affinity written to a pod:** Is every label value validated (`IsValidLabelValue`) before the pod is updated? A rejected update (`422`) strands the gate and violates `I3`. Are the node affinity rules from the user's own template preserved, including when the added requirements are removed again?
- **Cache vs. RBAC:** A cached client needs `list` and `watch`; with only `get` / `patch` the cache never syncs and the reconciler hangs. Does each read match the granted verbs (cached `Client`, or `APIReader` / `Cache.DisableFor` for uncached reads)?
- **Uncached reads in hot paths:** Does an uncached `GET` sit inside a loop over pods or jobs, or on the eviction webhook's deadline? That is quadratic API load at scale.
- **Commit state after the write:** Is in-memory state (node readiness, cached file identity, "already processed" markers) updated only after the Kubernetes API `PATCH` succeeds? Otherwise one failed write is never retried.
- **Bad local configuration:** Does a malformed local config record a clear not-ready state and back off, rather than retrying in a tight loop?

### 9. "False-Green" Tests: Wiring & Threshold Bypass
A test that passes without executing the changed branch is worse than no test. For each changed production branch, ask: **"If I invert this `if` or delete this line, which test fails?"** If the answer is none, say so. Where it's cheap, actually make the change and run the test.
- **Fixtures below a threshold:** A payload or object count shrunk for speed can drop below the threshold that selects the code path under test (for example a multi-stream byte threshold), unless the test also lowers the threshold on the object under test.
- **Early returns:** Does a guard, a skip, or a missing fixture return before the line under test is reached?
- **Pre-seeded success:** Does the test write `Ready=True` (or another success state) onto its own struct, instead of reading the reconciled object back from the API?
- **A baseline that already fails:** In negative or sabotage tests, does the unsabotaged fixture pass? If it already fails, every case is "detected" for the wrong reason.
- **Assertions on the wrong identity:** Are denials asserted to come from the policy or check under test, and not from RBAC or an earlier validation?
- **Environment tests:** Go's `t.Setenv` does not change `/proc/<pid>/environ`. A test of code that reads another process's environment needs a child process started with an explicit `cmd.Env`.
- **Test names in the PR:** Do the tests named in the PR description exist under those names?

---

## Review Output Format

Structure the review report into actionable sections:

```markdown
### Adversarial Review Summary
- **Base Ref:** upstream/main
- **Overall Verdict:** [PASS / CONCERNS / BLOCKING]
- **Focus areas applied:** 1–5, plus [6 / 7 / 8 / 9 or "none in scope"]

### Findings Matrix
| Severity | Category | File & Line | Description |
|:---|:---|:---|:---|
| BLOCKER / WARNING / NIT | Wedge / Split-Brain / Cold-Start / Security / Invariant / Engine-Lifecycle / Host-Boundary / Cache-RBAC-Autoscaler / False-Green | `path/to/file.go:L123` | Explanation of race/failure scenario |

### Detailed Findings & Proof-of-Concept Scenarios
- **Finding 1:** Detailed explanation of the edge case or race condition.
  - *Risk:* Why this causes split-brain, data corruption, or deadlock.
  - *Recommendation:* Specific code or logic fix.

### Test Coverage Assessment
- Verified positive cases: ...
- Missing negative / fault-injection cases: ...
- Changed branches with no test that fails when they are inverted or removed: ...
```
