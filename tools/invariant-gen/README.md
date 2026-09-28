# `invariant-gen` — Blind-Spot Detector, Snapshot Extractor & Red/Green Replay Verifier

`invariant-gen` implements **Track 5 (`PR E` — [Issue #54](https://github.com/gke-labs/pod-migration/issues/54))** of the LPM Correctness Guard Rail ([Epic #49](https://github.com/gke-labs/pod-migration/issues/49)).

It bridges `tools/pmprofiler` trace classification and `controller/internal/invariants` (`I1`–`I9` -> `I10+`) under a strict **Two-Lane `CODEOWNERS` Governance** model.

---

## Scope & Architectural Honesty

`invariant-gen` is a **deterministic offline Go CLI** (zero external dependencies, no LLM calls, no arbitrary Go AST synthesis from free-form prose):

1. **Deterministic Blind-Spot Detection (`Trigger A`)**:
   - Inspects `<run-dir>/run.json` and `<run-dir>/records.ndjson` produced by `pmprofiler`.
   - Identifies any migration ending in an unhealthy outcome (`wedged`, `failed`, `no-replacement`, `restore_crash_unmatched`, or unintended `cold-start`) **while `invariantViolations == 0`** (meaning `I1`–`I9` did not fire).
   - Reconstructs the point-in-time Kubernetes state from `records.ndjson` at the migration's failure timestamp (`tEnd`) and writes a minimized `SnapshotFixture` (`testdata/i<N>_<slug>_snapshot.json`).
2. **Built-In Structural Predicate Templates vs. Author Scaffolds**:
   - Classifies each blind spot (or `Trigger B` bugfix PR / `Trigger C` `/extract-invariant` directive) against 3 built-in structural predicate templates covering the blind-spot classes observed outside `I1`–`I9`:
     - `premature-snapshot-failed` (`PrematureSnapshotFailureIsolation`): `PMJ` enters `PhaseFailed` (`Reason=SnapshotFailed`) while its `PodSnapshot` still has `Checkpoint` or `StorageReplicated` in `InProgress` / `AwaitingCheckpoint` / `Succeeded` (the live GKE race filed in **[Issue #75](https://github.com/gke-labs/pod-migration/issues/75)**).
     - `wedged-restoring-orphan` (`RestoringReplacementLiveness`): `PMJ` remains in `PhaseRestoring` after source pod eviction while its bound `restoredPodName` has been deleted or is missing past the progress window.
     - `unintended-cold-start-active-pmj` (`ActivePMJReplacementColdStartGuard`): Replacement pod becomes `Ready` without a snapshot restore annotation while its `PMJ` is still active (`Snapshotting`, `Evicting`, `Restoring`).
   - For novel `/extract-invariant` directives or bugfix PRs outside these 3 structural classes, `invariant-gen` emits `custom-invariant-scaffold` with `requiresAuthorBody: true` and **`redPassed: false` / `greenPassed: false`** (exiting non-zero when `--require-red-green=true` until a human or coding agent authors the domain predicate).
3. **Automated RED / GREEN Replay Verification**:
   - **RED Proof**: Evaluates the candidate predicate against the extracted offending `SnapshotFixture` and asserts `>= 1` violation.
   - **GREEN Proof**: Replays the candidate predicate across canonical healthy lifecycle snapshots (`Pending`, `Snapshotting`, `Evicting`, `Restoring`, `Succeeded`, plus genuine `Checkpoint=Failed`) **and** across every reconstructed PMJ event step in clean `records.ndjson` traces (`--green-records`), asserting `0` false-positive violations.

---

## Two-Lane `CODEOWNERS` Governance (`verification-suite/verify_invariant_governance.sh`)

| Lane | Branch Pattern | Rule Enforced in `T0` CI | Purpose |
| :--- | :--- | :--- | :--- |
| **Lane 1** | All non-`invariant/*` branches (`feature/*`, `fix/*`, etc.) | **Forbids** modifying any file under `controller/internal/invariants/**` or `.github/CODEOWNERS` | Prevents feature or bugfix PRs from weakening or deleting `I1`–`I9` to turn a failing test green |
| **Lane 2** | `invariant/*` (e.g. `invariant/i10-premature-snapshot-failed`) | **Requires** all modified files to be strictly inside `controller/internal/invariants/**` (with `@yaoluo` / `@bnaylor` `CODEOWNERS` approval) | Keeps invariant additions self-contained (`rules.go`, `invariants_test.go`, `testdata/`) for 60-second human review |

---

## Usage

```bash
# 1. Run full pipeline (detect blind spot -> extract fixture -> RED/GREEN replay -> emit rule, test & PR body)
go run . \
  --mode pipeline \
  --run /tmp/pmprofiler-blindspot-run \
  --green-records /tmp/pmprofiler-live-t3/t3_s1/records.ndjson,/tmp/pmprofiler-live-t3/t3_s2/records.ndjson \
  --invariant-id I10 \
  --out-dir /tmp/invariant-i10-out

# 2. Extract from a /extract-invariant review slash command (Trigger C)
go run . \
  --mode extract-comment \
  --comment "/extract-invariant I10 premature-snapshot-failed PMJ failed while PodSnapshot Checkpoint=InProgress" \
  --out-dir /tmp/invariant-i10-comment
```
