---
name: review-docs-drift
description: Verifies documentation synchronization against code changes to CLI flags, CRD schemas, annotations/labels, invariant definitions, engine contracts, the CRIU engine (flags, node labels, eligibility, deploy manifests, threat model), metrics and Events, and release and license docs.
---

# Skill: Docs-Drift Pre-PR Self-Review (`review-docs-drift`)

## Purpose
This skill verifies that code modifications in `gke-labs/pod-migration` are faithfully mirrored in canonical documentation. It prevents silent documentation drift where CLI flags, CRD schemas, annotations, invariant definitions, engine contracts, deploy manifests, or security boundaries change in implementation without corresponding updates in user-facing and architectural documentation.

It covers both Go modules: the orchestrator in `controller/` and the CRIU snapshot engine in `engines/criu/`.

---

## Review Scope & Input
The review inspects the diff against `upstream/main`:
```bash
git diff upstream/main...HEAD
```

---

## Audit Checklist & Synchronization Matrix

Inspect every modified code path against its canonical documentation target:

| Code Modification Area | Key Source Files | Canonical Documentation to Check |
|:---|:---|:---|
| **CLI Flags & Startup Config** | `controller/cmd/main.go` | `README.md` (CLI flag reference, execution modes), `docs/invariants.md` (`--invariant-mode`) |
| **CRD Types & Status Fields** | `controller/api/v1alpha1/*_types.go` | `README.md` (CRD samples, architecture), `docs/design/` |
| **Annotations, Labels & Gates** | `controller/internal/util/`, `controller/internal/webhook/`, `controller/internal/engine/` | `README.md` (Pod annotations table), `docs/invariants.md` (gate names, fenced labels, bypass annotations) |
| **Safety Invariants (`I1`–`I12`)** | `controller/internal/invariants/` | `docs/invariants.md` (Authoritative invariant descriptions, triggers, grace periods, debouncing) |
| **Pluggable Engine Contract** | `controller/internal/engine/` | `docs/design/engine-contract.md` (`C1`–`C11` clauses, and the per-engine status table) |
| **Operational & Deploy Scripts** | `patches/setup-storage.sh`, `controller/deploy.yaml` | `README.md` (Installation and prerequisite sections) |
| **CRIU Engine Flags** | `engines/criu/cmd/engine/main.go`, `engines/criu/cmd/agent/main.go` | `engines/criu/README.md` (for example `--api-group`, which must match the controller's `--criu-api-group` in `README.md`) |
| **CRIU Annotations, Node Labels & Eligibility** | `engines/criu/internal/psengine/`, `engines/criu/internal/node/`, `controller/internal/engine/criu.go`, `controller/internal/criuplacement/` | `engines/criu/README.md` (architecture, workload scope, node requirements, restore placement), `docs/design/engine-contract.md` (`C2`, `C8`) |
| **CRIU Deploy Manifests** | `engines/criu/deploy/crds.yaml`, `engines/criu/deploy/deploy.yaml` | `engines/criu/README.md` (install, API group, what the engine changes on a node), and the code that relies on them in `engines/criu/cmd/` and `engines/criu/internal/psengine/` |
| **Security Boundaries** | RBAC, capabilities, host mounts, sockets, and admission policies in `engines/criu/deploy/deploy.yaml` and `controller/deploy.yaml`; bucket access in `patches/setup-storage.sh` | `docs/threat-model-criu.md`, `SECURITY.md`, the security warning at the top of `engines/criu/README.md` |
| **Metrics & Kubernetes Events** | `controller/internal/metrics/metrics.go`, `engines/criu/internal/psengine/metrics.go`, every `Recorder.Event` / `Eventf` call | `README.md` (monitoring section, Event reasons in troubleshooting), `engines/criu/README.md` |
| **Images, Dockerfiles & Dependencies** | `controller/Dockerfile`, `engines/criu/build/`, `go.mod` in both modules | `docs/third-party-licenses.md` (image inventory, license notices), `docs/release-process.md` (build and publish steps) |

---

## Detailed Drift Verification Checks

### 1. CLI Flags (`controller/cmd/main.go`, `engines/criu/cmd/`)
- Did any flag names, default values, or descriptions change?
- If new flags were introduced (e.g., timeout configurations, invariant modes, engine toggles), are they documented in `README.md` or `engines/criu/README.md` with defaults and example usage?
- Do flags that must agree across binaries still agree in the docs (the controller's `--criu-api-group` and the engine's and agent's `--api-group`)?

### 2. Annotations and Labels
- Check well-known annotation and label constants (e.g., `pod-migration.gke.io/*`, `<engine-group>/ps-name`, `gke.io/pod-migration-gate`, and the CRIU engine's `pod-migrate.io/*`).
- If an annotation was added, renamed, or deprecated, is the table of supported annotations in `README.md` updated?
- Are semantics like bypass markers (`<engine-group>/ps-name: ""`) or fenced markers (`pod-migration.gke.io/fenced: "true"`) clearly explained in both `README.md` and `docs/invariants.md`?
- Each engine has its own restore order annotation: `podsnapshot.gke.io/ps-name` for GKE Pod Snapshots, and `criu.podmigration.gke.io/ps-name` for CRIU by default. Does a doc that names only one of them still hold for the other engine?

### 3. Invariants (`I1`–`I12`)
- If any invariant rule in `controller/internal/invariants/` was updated (Lane 2):
  - Is the corresponding section in `docs/invariants.md` updated?
  - Do timing constants (e.g., grace windows, debounce sample counts) in `docs/invariants.md` match the implementation in Go code?
  - Does the header and table of contents reflect the current invariant range (`I1`–`I12` today; `DefaultRules` in `controller/internal/invariants/rules.go` is the source of truth)?

### 4. Pluggable Snapshot Engine Contract (`C1`–`C11`)
- If `controller/internal/engine/` changed its lifecycle hooks or interfaces:
  - Does `docs/design/engine-contract.md` accurately reflect interface signatures (`OrderRestore`, `OrderColdStartBypass`, `Eligible`, `ResolvePolicy`)?
  - Are runtime capability requirements and security constraints documented?
- If an engine's behavior for a clause changed, is its row in the contract's per-engine status table still true? Watch for "open" or "not yet" wording that the change made stale.

### 5. CRIU Engine (`engines/criu/`)
- **Annotations and node labels:** Are the engine's own markers documented where they are set and read: `pod-migrate.io/criu-ready` on nodes, `pod-migrate.io/psengine-*` on pods, and any label the agent publishes for restore placement?
- **Eligibility:** When a refusal is added or removed (for example an explicit `command`, `hostNetwork`, sidecar init containers, single container), is the workload scope in `engines/criu/README.md` updated, and `C2` in the engine contract? Does the controller-side `Eligible` in `controller/internal/engine/criu.go` still match what the docs say?
- **Manifests vs. code:** Do the CRDs, RBAC rules, webhook and admission policy objects in `engines/criu/deploy/` match what `engines/criu/cmd/` and `engines/criu/internal/psengine/` use? A new API call needs its RBAC rule, and the README's description of the agent's permissions must follow.
- **What changes on a node:** If the agent writes a new file, mount, label or annotation on the host or the Node object, is it listed in `engines/criu/README.md`, including whether it stays after uninstall?

### 6. Security Docs (`docs/threat-model-criu.md`, `SECURITY.md`)
- Did the diff change RBAC, Linux capabilities, host namespaces, host mounts, sockets, a `ValidatingAdmissionPolicy`, a webhook's scope or `failurePolicy`, or storage bucket permissions? Then the threat model must describe the new boundary.
- Does the threat model still list a gap that the change closed, or miss one that it opened? Check every "Remaining Gaps" entry and every issue reference near the changed area.
- **Verify before you write.** A sentence such as "X is not removed at creation" is a claim about code. Find the code that proves it, in both modules, before adding or keeping it.
- If operator steps changed (for example how to lift an admission policy in an emergency), are the README steps safe to run during an incident, and do they match the manifest comments?

### 7. Metrics & Kubernetes Events
- Is every metric that was added, renamed, or given a new label value listed in the monitoring section of `README.md` (controller) or in `engines/criu/README.md` (engine)?
- Is every new Event reason documented where users look for it, with the wording the code emits? If the README quotes a message, does a test pin that message?
- When a skip or failure path gains a reason (for example a new `reason` value on a "skipped" counter), are the sibling paths documented too? Backfill missing rows you notice.

### 8. Release & License Docs
- If a Dockerfile, a base image, or a bundled binary changed (`controller/Dockerfile`, `engines/criu/build/`), or a Go dependency (`go.mod` in either module), is the image inventory in `docs/third-party-licenses.md` still accurate?
- If the build, test, or publish steps changed, does `docs/release-process.md` still work as written?
- Does a user-visible behavior change (a pod that is no longer migrated, a new requirement) have a release-note line or a tracked item for one?

### 9. Cross-References
- Do issue numbers, file paths, function names and test names quoted in docs and in the PR description exist as written?
- Do links between `README.md`, `engines/criu/README.md` and `docs/` still resolve after headings were renamed?

---

## Review Output Format

Structure the review report into actionable sections:

```markdown
### Docs-Drift Review Summary
- **Base Ref:** upstream/main
- **Overall Verdict:** [IN-SYNC / DRIFT DETECTED]

### Synchronization Findings
| Source File Modified | Missing / Outdated Doc | Description of Drift |
|:---|:---|:---|
| `controller/cmd/main.go` | `README.md` | New flag `--xyz` missing from CLI flags table |
| `engines/criu/deploy/deploy.yaml` | `docs/threat-model-criu.md` | New RBAC rule for the node agent isn't described |

### Recommended Documentation Edits
- Precise Markdown snippets to add or update in canonical docs.
```
