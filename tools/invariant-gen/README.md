# `invariant-gen` — Blind-Spot Detector, Snapshot Extractor & Compiled `go test` Verifier

`invariant-gen` implements the deterministic CLI and T0 CI governance foundation of **Track 5 (`PR E` — [Issue #54](https://github.com/gke-labs/pod-migration/issues/54))** of the LPM Correctness Guard Rail ([Epic #49](https://github.com/gke-labs/pod-migration/issues/49)).

It bridges `tools/pmprofiler` trace classification and `controller/internal/invariants` (`I1`–`I9` -> `I10+`) under a **Two-Lane CI Governance** model.

---

## Scope & Architectural Honesty

`invariant-gen` is a **deterministic offline Go CLI** (zero external dependencies, no LLM calls, no arbitrary Go AST synthesis from free-form prose):

1. **Deterministic Blind-Spot Detection (`Trigger A`)**:
   - Inspects `<run-dir>/run.json` and `<run-dir>/records.ndjson` produced by `pmprofiler` (supporting `pmprofiler`'s `map[string]float64` `invariantViolations`, `totalInvariantViolations`, and `pod` / `snapshotName` / `phase` / `tTerminal` fields).
   - Identifies any migration ending in an unhealthy outcome (`wedged`, `stalled`, `failed`, `no-replacement`, `restore_crash_unmatched`, or unintended `cold-start`) **while `invariantViolations == 0`** (meaning `I1`–`I9` did not fire).
   - Reconstructs the point-in-time Kubernetes state from `records.ndjson` into a JSON fixture wire-compatible with `invariants.ReconcileSnapshot` (`testdata/i<N>_<slug>_snapshot.json`).
2. **Built-In `ReconcileSnapshot`-Native Templates vs. Author Scaffolds**:
   - Classifies each blind spot (or `Trigger B` bugfix PR / `Trigger C` `/extract-invariant` directive) against 4 built-in structural predicate templates that evaluate strictly over fields present on `invariants.ReconcileSnapshot` (`PodListFailed`, `PMJListFailed`, `PrimarySnapshotConditions`), `pmv1alpha1.PodMigrationJob`, and `corev1.Pod`:
     - `wedged-restoring-orphan` (`RestoringReplacementLiveness`): `PMJ` remains in `PhaseRestoring` `>= 30s` (`s.Now.Sub(pmj.Status.RestoringStartTime.Time) >= 30s`) while `!s.PodListFailed` and its bound `restoredPodName` has been deleted or is missing from `NamespacePods` (including single-pod namespaces where `NamespacePods` is empty after source eviction).
     - `no-replacement-evicting-stall` (`EvictingNoReplacementLiveness`): `PMJ` remains in `PhaseEvicting` `>= 30s` (`s.Now.Sub(pmj.Status.EvictingStartTime.Time) >= 30s`) with no `restoredPodName` while `!s.PodListFailed` after the source pod (`pmj.Spec.PodRef.Name`) has disappeared from `NamespacePods`.
     - `unintended-cold-start-active-pmj` (`ActivePMJReplacementColdStartGuard`): Replacement pod (`pmj.Status.RestoredPodName`) becomes `Ready` with no scheduling gates and an empty `podsnapshot.gke.io/ps-name` annotation while its `PMJ` is in `PhaseRestoring`.
     - `premature-snapshot-failed` (`SnapshotSubconditionConsistency`): `PMJ` transitions to `PhaseFailed` with `Reason=SnapshotFailed` while `s.PrimarySnapshotConditions` on the underlying `PodSnapshot` reports an in-progress `Checkpoint` or `StorageReplicated` sub-condition (`Status=False` with `Reason` in `InProgress`, `Pending`, `Running`, `Uploading`, `Triggering`, or `Replicating`, **[Issue #75](https://github.com/gke-labs/pod-migration/issues/75)** / **[Issue #88](https://github.com/gke-labs/pod-migration/issues/88)**).
   - For novel blind spots or custom `/extract-invariant` directives not matching one of the 4 built-in templates, `invariant-gen` emits `custom-invariant-scaffold` with `requiresAuthorBody: true`. Its generated `_rule.go` compiles cleanly inside `controller/internal/invariants` and intentionally returns `nil` so the compiled `go test` RED proof fails (`redPassed: false`, `greenPassed: false`) until a human/agent supplies the predicate body.
3. **Compiled `go test` RED / GREEN Replay Verification**:
   - Rather than evaluating an internal proxy predicate, `SynthesizeAndVerify` stages the emitted `testdata/i<N>_<slug>_snapshot.json`, `testdata/i<N>_<slug>_green_snapshots.json`, `i<N>_<slug>_rule.go`, and `i<N>_<slug>_test.go` into a temporary copy of `controller/internal/invariants` and executes `go test`:
     - **RED Proof (`TestI<N>_<Name>_RedProof`)**: Unmarshals `testdata/i<N>_<slug>_snapshot.json` into `invariants.ReconcileSnapshot`, calls `RuleI<N>().Evaluate(&snap)`, and asserts `>= 1` violation with `InvariantID == "I<N>"` (and verifies `--- PASS: TestI<N>_<Name>_RedProof` in `go test` output).
     - **GREEN Proof (`TestI<N>_<Name>_GreenProof`)**: Unmarshals `testdata/i<N>_<slug>_green_snapshots.json` (containing canonical healthy lifecycle snapshots plus every reconstructed PMJ event step from `--green-records` `records.ndjson` traces) into `[]invariants.ReconcileSnapshot` and asserts `0` false-positive violations across all steps.

---

## Two-Lane CI Governance Gate (`verification-suite/verify_invariant_governance.sh`)

| Lane | Branch Pattern | Rule Enforced in `T0` CI | Purpose |
| :--- | :--- | :--- | :--- |
| **Lane 1** | All non-`invariant/*` branches (`feature/*`, `fix/*`, etc.) | **Forbids** modifying any file under `controller/internal/invariants/**`, `.github/CODEOWNERS`, or `verification-suite/verify_invariant_governance.sh` | Prevents feature or bugfix PRs from silently weakening `I1`–`I9` or disabling the governance gate |
| **Lane 2** | `invariant/*` (e.g. `invariant/i10-wedged-restoring-orphan`) | **Requires** all modified files to be strictly inside `controller/internal/invariants/**` | Keeps invariant additions self-contained (`rules.go`, `invariants_test.go`, `testdata/`) for fast human review |
| **Reconciler + Invariant Co-Change** | Non-`invariant/*` branch with commit trailer `Invariant-Cochange: <reason>`, PR label `allow-invariant-cochange` (`ALLOW_INVARIANT_COCHANGE=true`), or `--allow-cochange` | Permits co-dependent `ReconcileSnapshot` / reconciler + `controller/internal/invariants/**` changes when explicitly justified (does **not** authorize governance-file edits via commit trailer) | Unblocks legitimate `ReconcileSnapshot` struct extensions (#88) without bypassing CI visibility |
| **Governance Files** | Any branch modifying `.github/CODEOWNERS` or `verification-suite/verify_invariant_governance.sh` | Requires PR label `allow-invariant-cochange` (`ALLOW_INVARIANT_COCHANGE=true`) in CI or `--allow-cochange` locally; **cannot** be bypassed by an author-controlled branch name or `Invariant-Cochange:` commit trailer | Prevents a PR author from unilaterally weakening `.github/CODEOWNERS` or `verify_invariant_governance.sh` |

> **Governance Enforcement Note:** `.github/CODEOWNERS` and `verify_invariant_governance.sh` operate as a **T0 CI governance check** (plus GitHub auto-reviewer assignment). Server-side GitHub branch protection / required CODEOWNERS review rulesets on `main` are tracked separately at the repository settings level.

### Remaining Scope in Issue #54
- **Delivered (`Part of #54`, `Closes #88`)**:
  - `.github/CODEOWNERS` + `verification-suite/verify_invariant_governance.sh` (T0 CI gate, fail-closed base-ref resolution, label-gated governance-file protection, 9-case self-test).
  - `tools/invariant-gen` CLI (Trigger A blind-spot detector, Trigger B/C directive parser, `ReconcileSnapshot` fixture extractor, compiled `go test` RED/GREEN verifier, and PR body generator).
  - `PodListFailed`, `PMJListFailed`, and `PrimarySnapshotConditions` populated on `invariants.ReconcileSnapshot` + `premature-snapshot-failed` native template (**[Issue #88](https://github.com/gke-labs/pod-migration/issues/88)**).
  - `.github/workflows/invariant-evolution.yaml` (`workflow_dispatch` + `/extract-invariant` `issue_comment` trigger with `actions/upload-artifact` output bundle).
- **Tracked for follow-up in `#54`**:
  - Automated Trigger B `pull_request: types: [closed]` workflow hook on merged `bug`/`regression` PRs.
  - Automated bot branch push (`invariant/i<N>-<slug>`) and GitHub PR creation (`gh pr create`) once repository write-token / GitHub App permissions are configured.

---

## Usage

```bash
# 1. Run full pipeline (detect blind spot -> extract ReconcileSnapshot fixture -> compiled go test RED/GREEN -> emit rule, test & PR body)
go run . \
  --mode pipeline \
  --run /tmp/pmprofiler-blindspot-run \
  --green-records /tmp/pmprofiler-live-t3/t3_s1/records.ndjson,/tmp/pmprofiler-live-t3/t3_s2/records.ndjson \
  --invariant-id I10 \
  --out-dir /tmp/invariant-i10-out

# 2. Extract from a /extract-invariant review slash command (Trigger C)
go run . \
  --mode extract-comment \
  --comment "/extract-invariant I10 wedged-restoring-orphan PMJ stuck in Restoring after replacement pod deleted" \
  --out-dir /tmp/invariant-i10-comment
```
