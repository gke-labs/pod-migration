# pmprofiler

> Profiler, invariant analyzer, and HTML report generator for `gke-labs/pod-migration`.
> Watches the `podmigration.gke.io` and `podsnapshot.gke.io` API groups (plus
> optional `pod-migrate.io` for cross-system comparisons), scrapes controller
> Prometheus `/metrics` (`pod_migration_invariant_violations_total`), and
> reconstructs per-pod migration timelines and invariant verification checks.

`pmprofiler` profiles pod-migration activity on a cluster and renders the
results as machine-readable JSON and a self-contained HTML report with an
engineering view (phase timings, timelines, failures, controller usage) and a
value view (restore rates, service gaps, verified application checks).

It is a standalone Go module with no runtime dependencies beyond cluster
credentials. `gcloud` is only invoked when GCS snapshot-size sampling is
enabled.

## How it works

`collect` opens list+watch streams (with automatic re-list on expiry) against:

- `podmigrationjobs.podmigration.gke.io` and every other resource in that group
- `podsnapshots.podsnapshot.gke.io` and the rest of the snapshot group
- workload pods (label selector, default `pod-migration.gke.io/enabled=true`)
- controller pods, events, and nodes
- controller `:8080/metrics` via the Kubernetes API server pod proxy (capturing `pod_migration_invariant_violations_total` across all controller replicas)

Every observed transition is appended to `records.ndjson` with an exact
receipt timestamp, so nothing is lost to poll sampling. Periodic samplers add
`metrics.k8s.io` pod/node usage, controller Prometheus metrics (`prommetrics`),
GCS snapshot object sizes, and controller log streams (`controller-<pod>.log`).

`analyze` reconstructs one timeline per `PodMigrationJob` — intercept →
checkpoint ready → source evicted → replacement Ready — preferring
object-carried timestamps (condition `lastTransitionTime`, `deletionTimestamp`,
`evictingStartTime`, `completionTime`) and falling back to receipt times only
where the API server does not timestamp a transition. Each migration is
classified as `restored`, `cold-start`, `replaced`, `failed`, `wedged`, or
`no-replacement`, with warning events, invariant violations (`I1`–`I9`), and
snapshot sizes joined in. Output is `run.json`.

By default, `analyze` enforces `--assert-zero-invariants=true`, exiting non-zero
if `sum(pod_migration_invariant_violations_total) > 0` or any
`Reason=InvariantViolation` Kubernetes Warning event was observed. Pass
`--assert-clean-outcomes` (and optionally `--allow-cold-start` for intentional
`I9` fallback scenarios) to fail if any wedged, failed, no-replacement, or
failed driver check is present. Pass `--enforce-slo` to enforce wall-clock
latency SLO thresholds (`T`) across the measured migration population:
- `--slo-gate-hold-p95` (default `60s`): `I3` scheduling-gate hold duration (`gateHoldS = tGateReleased - tDstCreated`)
- `--slo-downtime-p95` (default `60s`): `I4` serving blackout window (`downtimeS = tDstReady - tSrcDeleted`)
- `--slo-e2e-p95` (default `180s`): `I4` end-to-end migration duration (`e2eS = tDstReady - t0`)

`report` renders one or more `run.json` files into a single offline HTML file
(inline SVG, no external assets, light and dark themes, hover tooltips, and a
table view per chart).

## Usage

```sh
# during a scenario
pmprofiler collect --run runs/s1 --scenario "S1 rolling upgrade during drain" \
    --gcs-bucket gs://my-snapshots --controller-ns pod-migration-system

# scenario drivers record application-level verifications; --value/--total
# carry the verified-of-attempted magnitude (the report's "State verified"
# headline is sum(value)/sum(total) over the state-survival checks)
pmprofiler check --run runs/s1 --name "state survived (token-verified)" \
    --group redis --pass --value 50000 --total 50000 \
    --detail "50000/50000 populated keys + per-cycle nonce preserved"

# ingest an external timeseries (e.g. memtier p99 latency)
pmprofiler check --run runs/s1 --name "p99 latency" --unit ms \
    --series-file latency.txt   # lines: "<unix-seconds> <value>"

# after the scenario (asserts 0 invariant violations, clean outcomes, and wall-clock SLOs)
pmprofiler analyze --run runs/s1 --assert-zero-invariants --assert-clean-outcomes --enforce-slo
pmprofiler report --out report.html --title "Guardrail T2/T3 Suite" runs/*/run.json
```

## Files in a run directory

| File | Written by | Content |
|---|---|---|
| `records.ndjson` | collect | every watch event, metrics sample, prommetrics sample, GCS sample |
| `meta.json` | collect | server version, node inventory, scenario tag |
| `controller-<pod>.log` | collect | streamed controller logs |
| `checks.ndjson` | check | app-level verifications and ingested series |
| `run.json` | analyze | reconstructed migrations, invariant counts, stats, failure taxonomy |

## Development

```sh
go build ./...
go test -v -race ./...
```

