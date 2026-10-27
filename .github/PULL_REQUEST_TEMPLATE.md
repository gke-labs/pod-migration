## Summary

<!-- Briefly describe the problem this PR solves and the solution approach. -->
<!-- Link any relevant issues using standard closing keywords. -->
Closes #

## What Changed

<!-- Detail the changes made across packages, controllers, engines, or documentation. -->
- 

## Testing

<!-- List unit tests, envtest targets, or script verification performed. -->
- `cd controller && go test -race ./...`
- `./verification-suite/verify_invariant_governance.sh`

### Live validation

<!-- If tested against a live Kubernetes or GKE cluster, record the commands, workloads, and outcomes here. -->
- [ ] Validated on cluster: `<cluster-name>` (or N/A)

## Self-Review

<!-- Mandatory fresh-context review passes (see AGENTS.md). -->
- **Adversarial pass (`review-adversarial`):** 
  - Checked for: state-machine wedges, split-brain windows, silent cold-start bypass, privilege escalations, and invariant regressions.
  - Findings: <!-- None / detail findings and how they were resolved -->
- **Docs-drift pass (`review-docs-drift`):** 
  - Checked for: CLI flag, annotation, CRD, and engine contract documentation drift across README.md, docs/invariants.md, and docs/design/engine-contract.md.
  - Findings: <!-- None / detail docs updated -->

---
<!-- If AI-assisted, add attribution here, e.g.: Generated with the help of Jetski (Gemini). / Generated with the help of Claude Code. -->
