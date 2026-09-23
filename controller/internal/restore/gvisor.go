package restore

import "strings"

// gvisorSignatures are the substrings (matched case-insensitively) that
// identify a failed gVisor sentry restore as wrapped by containerd.
//
// Provenance is recorded per entry on purpose: a signature that fires DELETES a
// running pod, so each one has to justify itself. The full observation table is
// reproduced in the PR that introduced this package.
//
//   - "oci runtime restore failed" — OBSERVED on a live cluster (scenario S2).
//     Full message: "failed to start containerd task ... OCI runtime restore
//     failed".
//   - "restore failed" — a deliberate generalisation of the above, hedging
//     against containerd wrapping the runtime error differently across
//     versions. Strictly wider than the observed string, so it cannot miss what
//     S2 matches. It is also runc+CRIU wording, and is expected to migrate to a
//     CRIU engine when one lands.
//
// Anything outside this set classifies as FailureUnrecognized: reported via
// metric, Warning event and condition, but never acted on. That is how a
// changed upstream message reaches us as monitoring signal instead of as a
// silent regression — so the cost of omitting a real signature is a delay,
// while the cost of inventing one is a destroyed pod. Add entries only with a
// captured message to justify them.
var gvisorSignatures = []string{
	"oci runtime restore failed",
	"restore failed",
}

// GVisor classifies restore failures from gVisor sentry save/restore, as
// surfaced through containerd and the kubelet.
type GVisor struct{}

// Name implements Engine.
func (GVisor) Name() string { return "gvisor" }

// MatchesRestoreFailure implements Engine.
func (GVisor) MatchesRestoreFailure(msg string) (string, bool) {
	lower := strings.ToLower(msg)
	for _, sig := range gvisorSignatures {
		if strings.Contains(lower, sig) {
			return sig, true
		}
	}
	return "", false
}
