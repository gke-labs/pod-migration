# `scalesim` — T3 Offline Scale & Queue Simulator

`scalesim` synthesizes large-scale pod migration burst waves (`N = 2,000` pods across `50` nodes by default) and models:

1. **Cluster & Per-Node Concurrency Caps**: Enforces `--per-cluster` (and `--pmj-workers`), `--per-node-out`, and `--per-node-in` admission invariants across waves.
2. **Spot Preemption Priority**: Verifies that high-priority spot/node-down migrations (`--spot-percent`) preempt normal drain waves and complete strictly ahead of low-priority tail waves.
3. **Controller Reconciler Queue Dynamics (`PR #33`)**: Models `PodMigrationJobReconciler` (`--pmj-workers=50`), serialized `PodGateReconciler` (`--podgate-workers=1`), and `client-go` token-bucket rate limiting (`--client-qps=500`, `--client-burst=1000`) to verify that serialized `I3` scheduling-gate release stays well within the `60s` `gateHoldS` SLO.

## Usage

```bash
cd tools/scalesim
go run . --n 2000 --nodes 50 --per-cluster 20 --per-node-out 2 --per-node-in 4 --spot-percent 5 --json-out /tmp/scalesim.json
```
