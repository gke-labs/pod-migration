---
name: review-adversarial
description: Fresh-context adversarial red-team review of a pod-migration branch diff hunting for state-machine wedges, split-brain windows, silent cold starts, privilege escalations, and invariant regressions.
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

---

## 5 Critical Adversarial Audit Focus Areas

### 1. State-Machine Wedges & Orphan Resources
Trace the full migration lifecycle:
`Pending` -> `Snapshotting` -> `Evicting` -> `Restoring` -> `Succeeded` / `Failed` / `SucceededWithoutRestore`

- **Phase Transitions:** Does any failure, timeout, or unexpected Kubernetes API error leave a `PodMigrationJob` stuck in a non-terminal phase?
- **Scheduling Gate Release:** Is `gke.io/pod-migration-gate` guaranteed to be removed if a migration aborts, fails, or completes? Check compliance with invariant `I3` (Gate Liveness).
- **Resource Cleanup:** Are temporary objects (`PodSnapshotManualTrigger`, ephemeral volumes, holder containers) cleaned up on all exit paths (success, failure, cancellation)? Check compliance with invariant `I5` (Zero Resource Leak).
- **Finalizer Deadlocks:** Are controller finalizers (e.g., storage cleanup finalizers) guaranteed to unblock even if backend storage is unreachable or pods are force-deleted?

### 2. Split-Brain & Dual-Writer Windows
Live migration must never allow the origin pod and the replacement pod to concurrently execute and process traffic or write to shared persistent storage:
- **Source Fencing:** Is the origin pod reliably isolated before the replacement pod is unscheduled/unfenced and allowed to run?
- **Endpoint Fence Verification:** Does the source pod have `pod-migration.gke.io/fenced: "true"` and condition `Ready=False` (`Reason=PodMigrationFenced`) held until eviction?
- **Pre-Checkpoint Abort:** If a migration aborts before checkpoint completion, is the source pod unfenced safely without leaking invalid state?
- **Post-Restore Re-Migration:** Can an active replacement pod be evicted or migrated again while its own restore job is still `Restoring`? Check webhook guardrails against concurrent migration collisions.

### 3. Silent Cold-Start Masquerading as Verified Restore
A migration must never falsely claim success if the workload started cold:
- **Warm vs. Cold Detection:** Does the reconciler verify that the replacement pod restored from the designated checkpoint snapshot rather than starting from a clean image?
- **Annotation & Spec Fidelity:** Does the replacement pod carry the valid `podsnapshot.gke.io/ps-name` pointing to `Status.SnapshotRef` rather than the cold-start bypass (`""`)? Check compliance with `I2`.
- **Readiness vs. Restore Outcome:** Does `Phase=Succeeded` require explicit confirmation of successful memory restore rather than merely observing container `Ready` state?

### 4. Container Runtime, OCI Spec & RBAC Privilege Escalations
Inspect security boundaries across webhooks and snapshot engines:
- **Engine Privileges:** Do snapshot engines or helper components require unnecessary Linux capabilities (`CAP_SYS_ADMIN`, `CAP_CHECKPOINT_RESTORE`) or mount paths? Are capabilities strictly scoped and dropped where possible?
- **Host Namespace Leaks:** Are host namespaces (`hostPID`, `hostIPC`, `hostNetwork`) avoided unless strictly mandated by the runtime contract?
- **Admission Webhooks:** Do mutating webhooks validate input before patching pod specs or statuses? Can an unprivileged user manipulate migration annotations to bypass admission gates or hijack snapshots?
- **RBAC Scoping:** Are newly added ClusterRole rules strictly bounded to necessary verbs and API groups?

### 5. Invariant Governance & Negative Unit Testing
- **Two-Lane Governance Compliance:** Verify that `controller/internal/invariants/**` and `.github/CODEOWNERS` are untouched unless the PR explicitly follows Lane 2 rules or includes a verified `Invariant-Cochange` trailer.
- **No Weakened Invariants:** Ensure existing invariant predicates (`I1`–`I11`) have not been relaxed, bypassed, or commented out.
- **Negative Test Coverage:** Does every new error condition, validation failure, or edge case have a corresponding negative unit test asserting expected failure behavior?

---

## Review Output Format

Structure the review report into actionable sections:

```markdown
### Adversarial Review Summary
- **Base Ref:** upstream/main
- **Overall Verdict:** [PASS / CONCERNS / BLOCKING]

### Findings Matrix
| Severity | Category | File & Line | Description |
|:---|:---|:---|:---|
| BLOCKER / WARNING / NIT | Wedge / Split-Brain / Cold-Start / Security / Invariant | `path/to/file.go:L123` | Explanation of race/failure scenario |

### Detailed Findings & Proof-of-Concept Scenarios
- **Finding 1:** Detailed explanation of the edge case or race condition.
  - *Risk:* Why this causes split-brain, data corruption, or deadlock.
  - *Recommendation:* Specific code or logic fix.

### Test Coverage Assessment
- Verified positive cases: ...
- Missing negative / fault-injection cases: ...
```
