# Live Pod Migration — Repository Agent Guidelines (`AGENTS.md`)

Welcome to `gke-labs/pod-migration`! This document guides autonomous agents, AI assistants, and human contributors working within this repository. Follow these operational rules, development workflows, and governance boundaries at all times.

---

## 1. Project Overview

Live Pod Migration (`pod-migration`) is a Kubernetes controller and pluggable snapshot engine framework designed for live stateful pod migration across nodes in GKE Standard and Kubernetes clusters with minimal disruption (a brief pause during checkpoint/restore while open connections reconnect) and state preservation.

Key capabilities:
- **Stateful live migration:** Coordinates pod freezing/checkpointing, storage detachment/reattachment, network endpoint fencing, and warm memory restore into replacement pods.
- **Pluggable snapshot engines:** Supports multiple runtime backends via a unified snapshot engine contract (`docs/design/engine-contract.md`), including `GKEPodSnapshots` for gVisor sandboxed containers.
- **Formal safety invariants (`I1`–`I11`):** Continuous in-memory runtime verification (`docs/invariants.md`) guarantees safety properties such as at-most-once restore, no silent cold-start fallback, gate liveness, and source fencing.

---

## 2. Repository Layout

- `controller/`: Core Go controller-runtime manager daemon and admission webhooks:
  - `controller/cmd/main.go`: Controller entry point and flag configuration.
  - `controller/api/v1alpha1/`: Custom Resource Definitions (`PodMigration`, `PodMigrationJob`).
  - `controller/internal/controller/`: Reconcilers for `PodMigration`, `PodMigrationJob`, and `PodGate`.
  - `controller/internal/webhook/`: Mutating admission webhooks (`mpodgate`, `mpodstatus`, `mpodfence`) and eviction validation.
  - `controller/internal/engine/`: Pluggable snapshot engine interface (`Engine`) and registry (`Registry`).
  - `controller/internal/snapshot/`: Snapshot provider implementations (e.g., GKE Pod Snapshots).
  - `controller/internal/util/`: Shared controller constants, annotations, and helpers.
  - `controller/internal/invariants/`: Stateless runtime invariant verification engine and rules (`I1`–`I11`).
- `docs/invariants.md`: Authoritative specification of the runtime safety invariants (`I1`–`I11`).
- `docs/design/engine-contract.md`: Contract specification defining pluggable snapshot engine requirements (`C1`–`C11`).
- `verification-suite/`: Automated verification suite:
  - `verification-suite/verify_invariant_governance.sh`: Tier-0 (T0) Two-Lane CODEOWNERS governance gate.
  - T1/T2/T3 guardrail verification harnesses and workload validation scripts (`run_all_validations.sh`).
- `tools/pmprofiler/`: Live migration profiler and execution trace collector.
- `tools/invariant-gen/`: Offline invariant synthesizer consuming `pmprofiler` trace artifacts for Lane 2 invariant evolution.
- `patches/`: Operational helper scripts (e.g., `setup-storage.sh` for GKE Pod Snapshots storage configuration).
- `.agents/skills/`: Repository-specific agent skills for automated pre-PR review and quality control.

---

## 3. Build, Test & Governance Commands

Always execute build and test commands in the appropriate module root.

### Controller Suite
```bash
cd controller
go vet ./...
go test -race ./...
```

To run individual controller tests:
```bash
go test -v -race ./internal/controller/... -run TestPodMigrationJobReconciler
```

### Two-Lane Invariant Governance Gate (T0)
Before opening a PR or pushing changes, verify that your branch complies with invariant governance:
```bash
./verification-suite/verify_invariant_governance.sh
```

### Generated Code & Code Hygiene
- **Generated deepcopy code:** Never manually edit or regenerate `zz_generated.deepcopy.go` unless API types under `controller/api/v1alpha1/` have been modified.
- **License headers:** All new `.go` and `.sh` files must carry the standard 13-line Apache-2.0 license header used across the repository.
- **Formatting:** Maintain standard Go formatting with `gofmt` / `goimports`.

---

## 4. Two-Lane CODEOWNERS Invariant Governance (`I1`–`I11`)

The runtime safety invariants defined in `docs/invariants.md` and implemented in `controller/internal/invariants/` protect against split-brain states, silent cold-start fallbacks, orphan resources, and controller deadlocks. Changes to invariants are strictly governed by `.github/CODEOWNERS` and `./verification-suite/verify_invariant_governance.sh`:

### Lane 1: Feature & Bugfix PRs (Default)
- **Branch naming:** Any branch name not matching `^invariant/` (e.g., `feature/*`, `fix/*`, `docs/*`, `chore/*`).
- **Invariant isolation rule:** Feature and bugfix PRs **must NOT modify** `controller/internal/invariants/**`, `.github/CODEOWNERS`, or `verification-suite/verify_invariant_governance.sh`.
- **Never weaken an invariant:** If an invariant fails during feature development or testing, do not weaken or delete the invariant predicate. Fix the underlying reconciler logic, webhook interceptor, or timing race condition.
- **Co-change exception:** In the rare event where a reconciler change requires an atomic invariant update, the commit message must include the git commit trailer:
  ```text
  Invariant-Cochange: <detailed rationale explaining why reconciler and invariant must change together>
  ```
  Modifications to `.github/CODEOWNERS` or `verification-suite/verify_invariant_governance.sh` cannot use this trailer and require explicit maintainer approval via the `allow-invariant-cochange` PR label.

### Lane 2: Invariant Evolution PRs
- **Branch naming:** Must match `^invariant/` (e.g., `invariant/i11-source-fenced-after-checkpoint`).
- **Scope limitation:** Dedicated invariant PRs must touch **only** `controller/internal/invariants/**` and documentation (`docs/invariants.md`).
- **Verification requirement:** Every new or updated invariant rule must include positive tests (passing valid reconcile traces) and negative fault-injection tests (verifying detection of violating traces).

---

## 5. Pull Request Hygiene & Pre-PR Review

### Worktree Isolation
Always create an isolated git worktree branched from a freshly fetched `upstream/main`:
```bash
git fetch upstream --prune
git worktree add -B <branch-name> ~/gke-pod-migration-wt/<worktree-name> upstream/main
```
Never edit the main working directory directly when working on multiple features or reviews.

### Pull Request Description
Every pull request must follow [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md):
- Explicitly link target issues using GitHub closing keywords (e.g., `Closes #13, Closes #14`) so project boards track progress automatically.
- Provide clear summaries of what changed and why.
- Include evidence of local verification and unit test execution.

### Mandatory Pre-PR Self-Review (Fresh Context)
Before creating or requesting review on a pull request, run a two-pass self-review in a fresh context / subagent session that did not author the code edits:
1. **Adversarial Red-Team Pass (`review-adversarial`):**
   Follow `.agents/skills/review-adversarial/SKILL.md` against `git diff upstream/main...HEAD`. Hunt specifically for:
   - State-machine deadlocks or orphaned resources across phases (`Pending -> Snapshotting -> Evicting -> Restoring -> Succeeded/Failed`).
   - Split-brain or dual-writer windows (source pod unfenced while replacement runs, or pre-checkpoint fence leaks).
   - Silent cold-start masquerading as a warm checkpoint restore (`I2`).
   - RBAC, container capability, or OCI specification privilege escalations in admission webhooks.
   - Weakened or bypassed `I1`–`I11` invariants or missing negative test cases.
2. **Docs-Drift Pass (`review-docs-drift`):**
   Follow `.agents/skills/review-docs-drift/SKILL.md` to ensure CLI flags, annotations, CRD fields, and engine contracts match canonical documentation (`README.md`, `docs/invariants.md`, `docs/design/engine-contract.md`).

Record both review passes under `## Self-Review` in the PR description, detailing what was checked and any fixes made.

### Dual Review Gate
All PRs require review from automated/peer agents and maintainers (`CODEOWNERS`). Maintainers follow the norm of never merging without explicit maintainer sign-off.

### AI Agent Attribution
When opening or updating an AI-assisted Pull Request, include the `## Self-Review` section and an AI attribution footer in the PR description:
```text
Generated with the help of Jetski (Gemini).
```
or
```text
Generated with the help of Claude Code.
```

---

## 6. Open-Source Hygiene & Operational Safety

- **Public Repository:** `gke-labs/pod-migration` is an open-source project licensed under Apache-2.0. Never commit internal corporate URLs, internal issue links, corporate usernames, or internal cluster/project identifiers. Always use public placeholders like `<YOUR_PROJECT_ID>` or `<YOUR_CONTROLLER_IMAGE>`.
- **Node Cordon Hygiene:** When testing migrations against live clusters, never leave worker nodes cordoned (`SchedulingDisabled`). Always run `kubectl uncordon <node>` once migration completes.
- **Snapshot Metadata Cleanup:** Before running fresh E2E or verification workloads, clear stale snapshot descriptors in the specific test namespace:
  ```bash
  kubectl delete podsnapshots,podsnapshotmanualtriggers,podmigrationjobs --all -n <test-namespace>
  ```
  Never run cleanup with `--all -A` on shared clusters, as it deletes PodSnapshots cluster-wide (including those belonging to other workloads or teams).
