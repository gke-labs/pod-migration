# `scalesim` — T3 Offline Capacity & Queue Model

> **Note (`model, not a controller gate`)**: `scalesim` is a standalone analytical capacity-planning model that imports **no** `controller/` packages. Passing `scalesim` verifies the model's own wave-admission math and queue bounds, **not** that the controller currently enforces per-node/per-cluster migration caps or spot priority ordering.

## Real Controller Parameters vs. Hypothetical Governor Parameters

- **Real controller defaults (`PR #33`)**:
  - `--pmj-workers=50`: `PodMigrationJobReconciler` `MaxConcurrentReconciles: 50`
  - `--podgate-workers=1`: Serialized `PodGateReconciler` (`MaxConcurrentReconciles: 1`) for `I3` scheduling-gate removal
  - `--client-qps=500`, `--client-burst=1000`: `client-go` token-bucket rate limiter settings
- **Hypothetical admission-governor parameters (planned in `#65`, not yet implemented in the controller)**:
  - `--per-cluster=20`, `--per-node-out=2`, `--per-node-in=4`: Hypothetical cluster-wide and per-node outbound/inbound migration concurrency caps
  - `--spot-percent=5`: Hypothetical spot/node-down priority queue preemption over normal drain waves

## Usage

```bash
cd tools/scalesim
go run . --n 2000 --nodes 50 --per-cluster 20 --per-node-out 2 --per-node-in 4 --spot-percent 5 --json-out /tmp/scalesim.json
```
