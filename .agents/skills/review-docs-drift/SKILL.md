---
name: review-docs-drift
description: Verifies documentation synchronization against code changes to CLI flags, CRD schemas, annotations/labels, invariant definitions, and engine contracts.
---

# Skill: Docs-Drift Pre-PR Self-Review (`review-docs-drift`)

## Purpose
This skill verifies that code modifications in `gke-labs/pod-migration` are faithfully mirrored in canonical documentation. It prevents silent documentation drift where CLI flags, CRD schemas, annotations, invariant definitions, or engine contracts change in implementation without corresponding updates in user-facing and architectural documentation.

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
| **Annotations, Labels & Gates** | `controller/internal/util/`, `controller/internal/webhook/` | `README.md` (Pod annotations table), `docs/invariants.md` (gate names, fenced labels, bypass annotations) |
| **Safety Invariants (`I1`–`I11`)** | `controller/internal/invariants/` | `docs/invariants.md` (Authoritative invariant descriptions, triggers, grace periods, debouncing) |
| **Pluggable Engine Contract** | `controller/internal/engine/` | `docs/design/engine-contract.md` (`C1`–`C11` clauses) |
| **Operational & Deploy Scripts** | `patches/setup-storage.sh`, `controller/deploy.yaml` | `README.md` (Installation and prerequisite sections) |

---

## Detailed Drift Verification Checks

### 1. CLI Flags (`controller/cmd/main.go`)
- Did any flag names, default values, or descriptions change?
- If new flags were introduced (e.g., timeout configurations, invariant modes, engine toggles), are they documented in `README.md` with defaults and example usage?

### 2. Annotations and Labels
- Check well-known annotation and label constants (e.g., `pod-migration.gke.io/*`, `podsnapshot.gke.io/*`, `gke.io/pod-migration-gate`).
- If an annotation was added, renamed, or deprecated, is the table of supported annotations in `README.md` updated?
- Are semantics like bypass markers (`podsnapshot.gke.io/ps-name: ""`) or fenced markers (`pod-migration.gke.io/fenced: "true"`) clearly explained in both `README.md` and `docs/invariants.md`?

### 3. Invariants (`I1`–`I11`)
- If any invariant rule in `controller/internal/invariants/` was updated (Lane 2):
  - Is the corresponding section in `docs/invariants.md` updated?
  - Do timing constants (e.g., grace windows, debounce sample counts) in `docs/invariants.md` match the implementation in Go code?
  - Does the header and table of contents reflect the current invariant range (`I1`–`I11`)?

### 4. Pluggable Snapshot Engine Contract (`C1`–`C11`)
- If `controller/internal/engine/` changed its lifecycle hooks or interfaces:
  - Does `docs/design/engine-contract.md` accurately reflect interface signatures (`OrderRestore`, `OrderColdStartBypass`, `Eligible`, `ResolvePolicy`)?
  - Are runtime capability requirements and security constraints documented?

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

### Recommended Documentation Edits
- Precise Markdown snippets to add or update in canonical docs.
```
