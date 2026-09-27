package analyze

// Run is the analyzed output of one profiling run (run.json). It is the
// contract between `pmprofiler analyze` and `pmprofiler report`.
type Run struct {
	Scenario   string           `json:"scenario"`
	Meta       map[string]any   `json:"meta,omitempty"`
	Migrations []Migration      `json:"migrations"`
	Stats      map[string]Stats `json:"stats"` // metric name -> distribution
	// Outcomes counts only the measured population (Population == "");
	// pre-collection stubs and stale CRs are excluded from all headline
	// numbers and appear only in Populations + the migrations list.
	Outcomes                 map[string]int     `json:"outcomes"` // outcome -> count
	Populations              map[string]int     `json:"populations,omitempty"`
	Checks                   []Check            `json:"checks,omitempty"`
	Series                   []Series           `json:"series,omitempty"`
	Controller               ControllerUsage    `json:"controller"`
	Failures                 []Failure          `json:"failures,omitempty"`
	InvariantViolations      map[string]float64 `json:"invariantViolations,omitempty"`
	TotalInvariantViolations float64            `json:"totalInvariantViolations"`
}

// Population labels for migrations excluded from headline stats.
const (
	// PopPreCollection marks objects created before the collector started:
	// their pods were never watched, so nothing can be reconstructed.
	PopPreCollection = "pre-collection"
	// PopStaleCR marks CRs that never showed any status during the run
	// (leftovers from earlier activity).
	PopStaleCR = "stale-cr"
)

// Migration is one reconstructed pod migration.
type Migration struct {
	PMJ     string `json:"pmj"`
	Pod     string `json:"pod"`
	App     string `json:"app,omitempty"` // pod's app label
	DstPod  string `json:"dstPod,omitempty"`
	SrcNode string `json:"srcNode,omitempty"`
	DstNode string `json:"dstNode,omitempty"`

	// Absolute timestamps (RFC3339Nano). T0 is PMJ creation (intercept).
	T0            string `json:"t0"`
	TSnapshotting string `json:"tSnapshotting,omitempty"`
	TSnapReady    string `json:"tSnapReady,omitempty"`
	TEvicting     string `json:"tEvicting,omitempty"`
	TSrcDeleted   string `json:"tSrcDeleted,omitempty"`
	TDstCreated   string `json:"tDstCreated,omitempty"`
	TDstReady     string `json:"tDstReady,omitempty"`
	TTerminal     string `json:"tTerminal,omitempty"` // PMJ Succeeded/Failed

	// Durations in seconds from T0 (negative = unknown).
	SnapReadyS float64 `json:"snapReadyS"`
	EvictedS   float64 `json:"evictedS"`
	E2ES       float64 `json:"e2eS"`
	// DowntimeS is the serving gap: replacement Ready minus source deleted.
	DowntimeS float64 `json:"downtimeS"`

	Phase   string `json:"phase"` // last observed PMJ phase
	Outcome string `json:"outcome"`
	// Restored is true ONLY when an explicit engine signal proved the
	// restore (psengine-restore annotation or PodMigration
	// agent.warmStateVerified). It is never inferred from generic
	// snapshot-related annotations.
	Restored bool `json:"restored"`
	// RestoreSignal names the explicit signal Restored was derived from,
	// or "absent" when a replacement was observed but no engine stated an
	// outcome (the report must not claim restoration for those).
	RestoreSignal string   `json:"restoreSignal,omitempty"`
	Population    string   `json:"population,omitempty"` // "", pre-collection, stale-cr
	SnapshotName  string   `json:"snapshotName,omitempty"`
	SnapshotBytes int64    `json:"snapshotBytes,omitempty"`
	Warnings      []string `json:"warnings,omitempty"` // warning events joined to this migration

	// psengine-native fields (criu-snapshot-engine runs; HR-2 coverage).
	// Engine is the PodSnapshot's pod-migrate.io/engine label; absent for
	// foreign (addon) snapshots. CheckpointUploadS is PodSnapshot creation →
	// Ready-condition LTT, i.e. kubelet checkpoint + verified GCS upload
	// (-1 = not determinable — absent, never invented). RestoreOutcome is
	// the pod-migrate.io/psengine-restore annotation verbatim
	// (restored|failed), "" when the engine never stamped one.
	Engine            string  `json:"engine,omitempty"`
	CheckpointUploadS float64 `json:"checkpointUploadS,omitempty"`
	RestoreOutcome    string  `json:"restoreOutcome,omitempty"`
}

// Outcome values.
const (
	OutcomeRestored  = "restored"   // Succeeded and engine explicitly signaled the restore
	OutcomeColdStart = "cold-start" // Succeeded but engine signaled the restore failed
	// OutcomeReplaced is Succeeded with a replacement observed but no
	// explicit restore signal either way — NOT a verified restore.
	OutcomeReplaced = "replaced"
	OutcomeFailed   = "failed"         // PMJ reached Failed
	OutcomeWedged   = "wedged"         // no phase progress within threshold
	OutcomeNoDst    = "no-replacement" // terminal but no replacement pod observed
	OutcomeInFlight = "in-flight"      // run ended before threshold elapsed
)

// Stats is a summary distribution for one duration metric.
type Stats struct {
	N    int     `json:"n"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

// Check is an app-level verification recorded by a scenario driver
// (e.g. "nonce survived", "counter monotonic", "backoffLimit untouched").
// Value/Total carry the underlying magnitude (e.g. 22 of 42 migrations
// verified) so the report can weight checks instead of treating an
// aggregate row as a single pass/fail bit.
type Check struct {
	TS     string  `json:"ts"`
	Name   string  `json:"name"`
	Group  string  `json:"group,omitempty"` // e.g. app name, use-case id
	Pass   bool    `json:"pass"`
	Detail string  `json:"detail,omitempty"`
	Value  float64 `json:"value,omitempty"`
	Total  float64 `json:"total,omitempty"` // denominator for Value; 0 = binary check
	Unit   string  `json:"unit,omitempty"`
}

// Series is a named timeseries either sampled by the collector (controller
// CPU/mem, GCS bytes) or ingested from a scenario driver (latency curves).
type Series struct {
	Name   string       `json:"name"`
	Unit   string       `json:"unit,omitempty"`
	Points [][2]float64 `json:"points"` // [unix seconds, value]
}

// ControllerUsage summarizes controller resource consumption and log health.
type ControllerUsage struct {
	MaxCPUMilli int64 `json:"maxCpuMilli"`
	MaxMemBytes int64 `json:"maxMemBytes"`
	LogErrors   int   `json:"logErrors"`
	LogRetries  int   `json:"logRetries"`
}

// Failure is one taxonomized failure for the engineering report.
type Failure struct {
	PMJ    string   `json:"pmj"`
	Pod    string   `json:"pod,omitempty"`
	Kind   string   `json:"kind"` // failed | wedged | no-replacement | cold-start
	Phase  string   `json:"phase,omitempty"`
	Detail string   `json:"detail,omitempty"`
	Events []string `json:"events,omitempty"`
}
