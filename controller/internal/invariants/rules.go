package invariants

import (
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	// MigrationGateName is the scheduling gate injected by LPM onto replacement pods.
	MigrationGateName = "gke.io/pod-migration-gate"

	// AnnotationAssignedPMJ links a replacement Pod to its owning PodMigrationJob.
	AnnotationAssignedPMJ = util.AnnotationAssignedPMJ
	// AnnotationPSName is the GKE Pod Snapshots restore annotation on replacement Pods.
	// An explicit empty string value ("") is the cold-start bypass signal written by
	// releaseWithColdStartBypass and the replacement webhook.
	AnnotationPSName = "podsnapshot.gke.io/ps-name"
	// AnnotationMigrationTimeout is the workload-configurable migration timeout annotation (PR #57).
	AnnotationMigrationTimeout = "pod-migration.gke.io/timeout"
	// AnnotationEvictingSince records when the Evicting phase started waiting for deletion.
	AnnotationEvictingSince = "pod-migration.gke.io/evicting-since"

	// LabelPodTemplateHash is the upstream Deployment revision label key on Pods.
	LabelPodTemplateHash = appsv1.DefaultDeploymentUniqueLabelKey
	// LabelControllerRevisionHash is the upstream StatefulSet revision label key on Pods and PMJs.
	LabelControllerRevisionHash = util.LabelControllerRevisionHash
	// LabelJobCompletionIndex is the upstream Indexed Job completion index label key on Pods and PMJs.
	LabelJobCompletionIndex = util.LabelJobCompletionIndex
	// LabelPMJPodTemplateHash is the PMJ label storing Deployment pod-template-hash.
	LabelPMJPodTemplateHash = util.LabelPodTemplateHash
	// LabelPMJParentUID is the PMJ label storing the resolved parent workload UID.
	LabelPMJParentUID = util.LabelParentUID
	// LabelPMJParentKind is the PMJ label storing the resolved parent workload Kind.
	LabelPMJParentKind = util.LabelParentKind

	// StorageCleanupFinalizer is the finalizer on PodMigration CRs.
	StorageCleanupFinalizer = "podmigration.gke.io/storage-cleanup"
	// MaxMigrationDuration is the default 10-minute baseline upper bound for active PMJs and deletion deferral.
	MaxMigrationDuration = 10 * time.Minute
	// MinMigrationTimeout is the minimum allowable migration timeout when overridden via annotation.
	MinMigrationTimeout = 1 * time.Minute
	// MaxMigrationTimeout is the maximum allowable migration timeout ceiling (2h) for memory-scaled workloads.
	MaxMigrationTimeout = 2 * time.Hour
	// DefaultThroughputBytesPerSec is the baseline checkpoint upload throughput (50 MiB/s) for memory-scaled deadlines.
	DefaultThroughputBytesPerSec = int64(50 * 1024 * 1024)
	// GateReleaseGraceWindow is the grace window allowed between patching Status.GateReleased=true on the PMJ
	// and updating the replacement Pod to remove gke.io/pod-migration-gate.
	GateReleaseGraceWindow = 30 * time.Second
)

// DefaultRules is the canonical ordered registry of the 9 Core Invariants (I1-I9).
var DefaultRules = []Rule{
	funcRule{id: "I1", name: "AtMostOnceRestore", fn: EvaluateI1},
	funcRule{id: "I2", name: "NoSilentColdStart", fn: EvaluateI2},
	funcRule{id: "I3", name: "GateLiveness", fn: EvaluateI3},
	funcRule{id: "I4", name: "TerminalProgress", fn: EvaluateI4},
	funcRule{id: "I5", name: "ZeroResourceLeak", fn: EvaluateI5},
	funcRule{id: "I6", name: "RevisionAndIdentityFidelity", fn: EvaluateI6},
	funcRule{id: "I7", name: "DisruptionBoundingPDBCompliance", fn: EvaluateI7},
	funcRule{id: "I8", name: "PlatformAndControlPlaneIsolation", fn: EvaluateI8},
	funcRule{id: "I9", name: "DeterministicFallbackOverCrashloop", fn: EvaluateI9},
}

func allPMJs(s *ReconcileSnapshot) []pmv1alpha1.PodMigrationJob {
	if s == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(s.NamespacePMJs)+1)
	var out []pmv1alpha1.PodMigrationJob
	if s.PrimaryPMJ != nil && s.PrimaryPMJ.Name != "" {
		key := s.PrimaryPMJ.Namespace + "/" + s.PrimaryPMJ.Name
		seen[key] = struct{}{}
		out = append(out, *s.PrimaryPMJ)
	}
	for _, job := range s.NamespacePMJs {
		key := job.Namespace + "/" + job.Name
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			out = append(out, job)
		}
	}
	return out
}

func allPods(s *ReconcileSnapshot) []corev1.Pod {
	if s == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(s.NamespacePods)+1)
	var out []corev1.Pod
	if s.PrimaryPod != nil && s.PrimaryPod.Name != "" {
		key := s.PrimaryPod.Namespace + "/" + s.PrimaryPod.Name
		seen[key] = struct{}{}
		out = append(out, *s.PrimaryPod)
	}
	for _, p := range s.NamespacePods {
		key := p.Namespace + "/" + p.Name
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}

// IsTerminalPhase reports whether phase is a terminal PodMigrationJobPhase.
func IsTerminalPhase(phase pmv1alpha1.PodMigrationJobPhase) bool {
	return phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
		phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
		phase == pmv1alpha1.PodMigrationJobPhaseFailed
}

func isTerminalPhase(phase pmv1alpha1.PodMigrationJobPhase) bool {
	return IsTerminalPhase(phase)
}

func podHasGate(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == MigrationGateName {
			return true
		}
	}
	return false
}

func podSnapshotAnnotation(pod *corev1.Pod) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	return pod.Annotations[AnnotationPSName]
}

// podHasColdStartBypass reports whether pod carries the explicit empty-string
// bypass annotation (`podsnapshot.gke.io/ps-name: ""`) set by releaseWithColdStartBypass
// and the replacement webhook.
func podHasColdStartBypass(pod *corev1.Pod) bool {
	if pod == nil || pod.Annotations == nil {
		return false
	}
	val, hasKey := pod.Annotations[AnnotationPSName]
	return hasKey && val == ""
}

// EvaluateI1 enforces I1 (At-Most-Once Restore):
// A PodSnapshot may be bound to and consumed by at most one replacement Pod UID
// across the cluster's lifetime, and a consumed PMJ with GateReleased=true must
// never be re-bound to a different replacement Pod UID.
func EvaluateI1(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	jobs := allPMJs(s)
	pods := allPods(s)

	snapshotConsumers := make(map[string]string) // snapshotRef -> podUID (or pod key)
	for _, pod := range pods {
		snap := podSnapshotAnnotation(&pod)
		if snap == "" {
			continue
		}
		podID := string(pod.UID)
		if podID == "" {
			podID = pod.Namespace + "/" + pod.Name
		}
		if prevPodID, exists := snapshotConsumers[snap]; exists && prevPodID != podID {
			violations = append(violations, Violation{
				InvariantID:   "I1",
				InvariantName: "AtMostOnceRestore",
				Reason:        "DuplicateSnapshotConsumption",
				Message:       fmt.Sprintf("snapshot %q is simultaneously bound to multiple replacement pods (%s and %s)", snap, prevPodID, podID),
				Namespace:     pod.Namespace,
				PodName:       pod.Name,
			})
		} else {
			snapshotConsumers[snap] = podID
		}
	}

	for _, job := range jobs {
		if job.Status.Consumed && job.Status.GateReleased && job.Status.SnapshotRef != "" {
			for _, pod := range pods {
				if podSnapshotAnnotation(&pod) == job.Status.SnapshotRef &&
					job.Status.RestoredPodUID != "" &&
					string(pod.UID) != "" &&
					string(pod.UID) != job.Status.RestoredPodUID {
					violations = append(violations, Violation{
						InvariantID:   "I1",
						InvariantName: "AtMostOnceRestore",
						Reason:        "ConsumedSnapshotReboundAfterGateRelease",
						Message: fmt.Sprintf("PMJ %s/%s already released gate for consumer UID %q on snapshot %q, but pod %s (UID %q) is bound to the same snapshot",
							job.Namespace, job.Name, job.Status.RestoredPodUID, job.Status.SnapshotRef, pod.Name, pod.UID),
						Namespace: job.Namespace,
						PMJName:   job.Name,
						PodName:   pod.Name,
					})
				}
			}
		}
	}
	return violations
}

// EvaluateI2 enforces I2 (No Silent Cold-Start / No State Rollback):
// A PMJ may transition to Succeeded (warm restore) iff Status.SnapshotRef is non-empty,
// Status.RestoredPodName is recorded, and (when the replacement Pod is present in the
// snapshot) the replacement Pod carries the matching `podsnapshot.gke.io/ps-name` annotation
// and did not bypass restore via `podsnapshot.gke.io/ps-name: ""`.
func EvaluateI2(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	jobs := allPMJs(s)
	pods := allPods(s)

	podByKey := make(map[string]corev1.Pod, len(pods))
	for _, p := range pods {
		podByKey[p.Namespace+"/"+p.Name] = p
	}

	for _, job := range jobs {
		if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
			continue
		}
		if job.Status.SnapshotRef == "" {
			violations = append(violations, Violation{
				InvariantID:   "I2",
				InvariantName: "NoSilentColdStart",
				Reason:        "SucceededWithoutSnapshotRef",
				Message:       fmt.Sprintf("PMJ %s/%s transitioned to Succeeded (warm restore) with empty Status.SnapshotRef", job.Namespace, job.Name),
				Namespace:     job.Namespace,
				PMJName:       job.Name,
			})
			continue
		}
		if job.Status.RestoredPodName != "" {
			if pod, ok := podByKey[job.Namespace+"/"+job.Status.RestoredPodName]; ok {
				snapAnn := podSnapshotAnnotation(&pod)
				coldStartBypass := podHasColdStartBypass(&pod)
				if snapAnn != job.Status.SnapshotRef || coldStartBypass {
					violations = append(violations, Violation{
						InvariantID:   "I2",
						InvariantName: "NoSilentColdStart",
						Reason:        "SucceededPodMissingRestoreAnnotation",
						Message: fmt.Sprintf("PMJ %s/%s is Succeeded expecting snapshot %q on replacement pod %s, but pod has %s=%q (coldStartBypass=%t)",
							job.Namespace, job.Name, job.Status.SnapshotRef, pod.Name, AnnotationPSName, snapAnn, coldStartBypass),
						Namespace: job.Namespace,
						PMJName:   job.Name,
						PodName:   pod.Name,
					})
				}
			}
		}
	}

	// Also verify that any replacement pod still carrying assigned-pmj whose gate was removed
	// has either a non-empty snapshot name or the explicit empty-string cold-start bypass.
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || podHasGate(&pod) || pod.Annotations == nil {
			continue
		}
		assigned := pod.Annotations[AnnotationAssignedPMJ]
		if assigned == "" {
			continue
		}
		_, hasPSKey := pod.Annotations[AnnotationPSName]
		if !hasPSKey {
			violations = append(violations, Violation{
				InvariantID:   "I2",
				InvariantName: "NoSilentColdStart",
				Reason:        "GateReleasedWithoutSnapshotOrColdStartBypass",
				Message: fmt.Sprintf("pod %s/%s had scheduling gate %q removed while assigned to PMJ %s without %s set to a snapshot name or explicit empty-string bypass",
					pod.Namespace, pod.Name, MigrationGateName, assigned, AnnotationPSName),
				Namespace: pod.Namespace,
				PMJName:   assigned,
				PodName:   pod.Name,
			})
		}
	}
	return violations
}

// EvaluateI3 enforces I3 (Gate Liveness):
// Every non-terminating Pod carrying the LPM scheduling gate must have an assigned PMJ
// that is non-terminal and has not left the pod gated past GateReleaseGraceWindow (30s)
// after marking Status.GateReleased=true.
func EvaluateI3(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	now := time.Now()
	if s != nil && !s.Now.IsZero() {
		now = s.Now
	}

	jobs := allPMJs(s)
	pods := allPods(s)

	jobByKey := make(map[string]pmv1alpha1.PodMigrationJob, len(jobs))
	for _, j := range jobs {
		jobByKey[j.Namespace+"/"+j.Name] = j
	}

	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || !podHasGate(&pod) {
			continue
		}
		assigned := ""
		if pod.Annotations != nil {
			assigned = pod.Annotations[AnnotationAssignedPMJ]
		}
		if assigned == "" {
			continue
		}
		if job, ok := jobByKey[pod.Namespace+"/"+assigned]; ok {
			if isTerminalPhase(job.Status.Phase) && job.Status.Consumed {
				violations = append(violations, Violation{
					InvariantID:   "I3",
					InvariantName: "GateLiveness",
					Reason:        "PodGatedAfterPMJTerminalAndConsumed",
					Message: fmt.Sprintf("pod %s/%s still carries scheduling gate %q even though assigned PMJ %s is already terminal (%s) and consumed",
						pod.Namespace, pod.Name, MigrationGateName, job.Name, job.Status.Phase),
					Namespace: pod.Namespace,
					PMJName:   job.Name,
					PodName:   pod.Name,
				})
			} else if job.Status.GateReleased && job.Status.RestoredPodUID != "" && string(pod.UID) == job.Status.RestoredPodUID {
				// Allow GateReleaseGraceWindow (30s) for the two-step release sequence in PodGateReconciler
				// (Step 1: patch Status.GateReleased=true on PMJ; Step 2: Update Pod to remove gate)
				// so transient conflict retries on Step 2 do not trigger false alarms.
				inGraceWindow := false
				if !isTerminalPhase(job.Status.Phase) {
					var anchor time.Time
					if job.Status.RestoringStartTime != nil && !job.Status.RestoringStartTime.IsZero() {
						anchor = job.Status.RestoringStartTime.Time
					} else if !pod.CreationTimestamp.IsZero() {
						anchor = pod.CreationTimestamp.Time
					}
					if !anchor.IsZero() && now.Sub(anchor) <= GateReleaseGraceWindow {
						inGraceWindow = true
					}
				}
				if !inGraceWindow {
					violations = append(violations, Violation{
						InvariantID:   "I3",
						InvariantName: "GateLiveness",
						Reason:        "PodGatedAfterGateReleasedStatus",
						Message: fmt.Sprintf("pod %s/%s (UID %q) still carries scheduling gate %q after PMJ %s marked Status.GateReleased=true",
							pod.Namespace, pod.Name, pod.UID, MigrationGateName, job.Name),
						Namespace: pod.Namespace,
						PMJName:   job.Name,
						PodName:   pod.Name,
					})
				}
			}
		}
	}
	return violations
}

func calculatePodMemoryBytes(pod *corev1.Pod) int64 {
	if pod == nil {
		return 0
	}
	var total int64
	for _, c := range pod.Spec.Containers {
		if req := c.Resources.Requests.Memory(); req != nil {
			total += req.Value()
		}
	}
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			if req := c.Resources.Requests.Memory(); req != nil {
				total += req.Value()
			}
		}
	}
	return total
}

func parseClampedTimeout(raw string) (time.Duration, bool) {
	if raw == "" {
		return 0, false
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, false
	}
	if d < MinMigrationTimeout {
		return MinMigrationTimeout, true
	}
	if d > MaxMigrationTimeout {
		return MaxMigrationTimeout, true
	}
	return d, true
}

// EffectiveMigrationTimeout computes the effective migration deadline for job,
// aligning with PR #57's workload-configurable `pod-migration.gke.io/timeout` annotation
// and memory-scaled deadline calculation (50 MiB/s throughput, clamped to [1m, 2h]).
func EffectiveMigrationTimeout(job *pmv1alpha1.PodMigrationJob, s *ReconcileSnapshot) time.Duration {
	if job == nil {
		return MaxMigrationDuration
	}
	if job.Annotations != nil {
		if d, ok := parseClampedTimeout(job.Annotations[AnnotationMigrationTimeout]); ok {
			return d
		}
	}
	for _, pod := range allPods(s) {
		if pod.Namespace == job.Namespace && pod.Name == job.Spec.PodRef.Name {
			if pod.Annotations != nil {
				if d, ok := parseClampedTimeout(pod.Annotations[AnnotationMigrationTimeout]); ok {
					return d
				}
			}
			if memBytes := calculatePodMemoryBytes(&pod); memBytes > 0 {
				scaled := MaxMigrationDuration + time.Duration(memBytes/DefaultThroughputBytesPerSec)*time.Second
				if scaled > MaxMigrationTimeout {
					return MaxMigrationTimeout
				}
				return scaled
			}
			break
		}
	}
	return MaxMigrationDuration
}

// EvaluateI4 enforces I4 (Terminal Progress):
// Active PMJs in Pending/Snapshotting/Evicting must not exceed their effective migration
// timeout (from `pod-migration.gke.io/timeout`, memory-scaled budget, or 10m baseline)
// without transitioning to a terminal phase, and a deleting PodMigration CR with
// StorageCleanupFinalizer must not remain un-finalized past MaxMigrationDuration (10m).
func EvaluateI4(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	now := time.Now()
	if s != nil && !s.Now.IsZero() {
		now = s.Now
	}

	// Grace slack beyond the effective deadline to distinguish an overdue stuck object
	// from the reconcile step that is actively timing it out.
	const progressGraceSlack = 30 * time.Second

	for _, job := range allPMJs(s) {
		if job.Status.Phase == pmv1alpha1.PodMigrationJobPhasePending ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
			effectiveTimeout := EffectiveMigrationTimeout(&job, s)
			budget := effectiveTimeout + progressGraceSlack

			// During Evicting, if `pod-migration.gke.io/evicting-since` is set, PR #57
			// anchors the eviction phase budget on evicting-since.
			if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting && job.Annotations != nil {
				if evictingSinceStr := job.Annotations[AnnotationEvictingSince]; evictingSinceStr != "" {
					evictingSince, err := time.Parse(time.RFC3339Nano, evictingSinceStr)
					if err != nil {
						evictingSince, err = time.Parse(time.RFC3339, evictingSinceStr)
					}
					if err == nil && now.Sub(evictingSince) <= budget {
						continue
					}
				}
			}

			if !job.CreationTimestamp.IsZero() && now.Sub(job.CreationTimestamp.Time) > budget {
				// Static message string (without dynamic now.Sub elapsed duration) so
				// client-go EventRecorder and Engine deduplication aggregate cleanly.
				violations = append(violations, Violation{
					InvariantID:   "I4",
					InvariantName: "TerminalProgress",
					Reason:        "ActivePMJExceededDeadline",
					Message: fmt.Sprintf("PMJ %s/%s remained in non-terminal phase %q beyond its %s deadline (created at %s)",
						job.Namespace, job.Name, job.Status.Phase, effectiveTimeout, job.CreationTimestamp.Time.UTC().Format(time.RFC3339)),
					Namespace: job.Namespace,
					PMJName:   job.Name,
				})
			}
		}
	}

	if s != nil && s.PrimaryMigration != nil {
		mig := s.PrimaryMigration
		if mig.DeletionTimestamp != nil && !mig.DeletionTimestamp.IsZero() {
			hasFinalizer := false
			for _, f := range mig.Finalizers {
				if f == StorageCleanupFinalizer {
					hasFinalizer = true
					break
				}
			}
			if hasFinalizer && now.Sub(mig.DeletionTimestamp.Time) > MaxMigrationDuration+progressGraceSlack {
				violations = append(violations, Violation{
					InvariantID:   "I4",
					InvariantName: "TerminalProgress",
					Reason:        "PodMigrationFinalizerStuckPastDeferralCap",
					Message: fmt.Sprintf("PodMigration %s/%s has been deleting with finalizer %q beyond the %s max deferral deadline (deletion started at %s)",
						mig.Namespace, mig.Name, StorageCleanupFinalizer, MaxMigrationDuration, mig.DeletionTimestamp.Time.UTC().Format(time.RFC3339)),
					Namespace: mig.Namespace,
				})
			}
		}
	}
	return violations
}

// EvaluateI5 enforces I5 (Zero Resource Leak):
// Terminal PMJs must not retain orphaned PodSnapshotManualTrigger objects past cleanup.
func EvaluateI5(s *ReconcileSnapshot) []Violation {
	if s == nil || !s.HasOrphanedTrigger || s.PrimaryPMJ == nil {
		return nil
	}
	job := s.PrimaryPMJ
	if isTerminalPhase(job.Status.Phase) {
		return []Violation{{
			InvariantID:   "I5",
			InvariantName: "ZeroResourceLeak",
			Reason:        "OrphanedManualTriggerOnTerminalPMJ",
			Message:       fmt.Sprintf("terminal PMJ %s/%s (phase=%s) still owns an uncleaned PodSnapshotManualTrigger", job.Namespace, job.Name, job.Status.Phase),
			Namespace:     job.Namespace,
			PMJName:       job.Name,
		}}
	}
	return nil
}

// EvaluateI6 enforces I6 (Revision & Identity Fidelity):
// When a PMJ is bound to a replacement Pod, their controller revision hashes
// (`pod-migration.gke.io/pod-template-hash`, `controller-revision-hash`),
// Indexed Job completion indices (`batch.kubernetes.io/job-completion-index`),
// and parent workload UID labels (`pod-migration.gke.io/parent-uid`) must match.
func EvaluateI6(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	jobs := allPMJs(s)
	pods := allPods(s)

	podByKey := make(map[string]corev1.Pod, len(pods))
	for _, p := range pods {
		podByKey[p.Namespace+"/"+p.Name] = p
	}

	for _, job := range jobs {
		targetPodName := job.Status.RestoredPodName
		if targetPodName == "" {
			continue
		}
		pod, ok := podByKey[job.Namespace+"/"+targetPodName]
		if !ok {
			continue
		}
		// 1. Check Deployment pod-template-hash (util.LabelPodTemplateHash on PMJ vs pod-template-hash on Pod)
		if expectedHash := job.Labels[LabelPMJPodTemplateHash]; expectedHash != "" {
			actualHash := pod.Labels[LabelPodTemplateHash]
			if actualHash != "" && actualHash != expectedHash {
				violations = append(violations, Violation{
					InvariantID:   "I6",
					InvariantName: "RevisionAndIdentityFidelity",
					Reason:        "PodTemplateHashMismatch",
					Message: fmt.Sprintf("PMJ %s/%s expects pod-template-hash %q but replacement pod %s has %q",
						job.Namespace, job.Name, expectedHash, pod.Name, actualHash),
					Namespace: job.Namespace,
					PMJName:   job.Name,
					PodName:   pod.Name,
				})
			}
		}
		// 2. Check StatefulSet controller-revision-hash (util.LabelControllerRevisionHash on PMJ vs Pod)
		if expectedRev := job.Labels[LabelControllerRevisionHash]; expectedRev != "" {
			actualRev := pod.Labels[LabelControllerRevisionHash]
			if actualRev != "" && actualRev != expectedRev {
				violations = append(violations, Violation{
					InvariantID:   "I6",
					InvariantName: "RevisionAndIdentityFidelity",
					Reason:        "ControllerRevisionHashMismatch",
					Message: fmt.Sprintf("PMJ %s/%s expects controller-revision-hash %q but replacement pod %s has %q",
						job.Namespace, job.Name, expectedRev, pod.Name, actualRev),
					Namespace: job.Namespace,
					PMJName:   job.Name,
					PodName:   pod.Name,
				})
			}
		}
		// 3. Check Indexed Job completion index (util.LabelJobCompletionIndex = batch.kubernetes.io/job-completion-index)
		if expectedIdx := job.Labels[LabelJobCompletionIndex]; expectedIdx != "" {
			actualIdx := pod.Labels[LabelJobCompletionIndex]
			if actualIdx != "" && actualIdx != expectedIdx {
				violations = append(violations, Violation{
					InvariantID:   "I6",
					InvariantName: "RevisionAndIdentityFidelity",
					Reason:        "JobCompletionIndexMismatch",
					Message: fmt.Sprintf("PMJ %s/%s expects job-completion-index %q but replacement pod %s has %q",
						job.Namespace, job.Name, expectedIdx, pod.Name, actualIdx),
					Namespace: job.Namespace,
					PMJName:   job.Name,
					PodName:   pod.Name,
				})
			}
		}
		// 4. Check generational parent-uid label (`util.LabelParentUID` on job.Labels)
		expectedParentUID := job.Labels[LabelPMJParentUID]
		expectedParentKind := job.Labels[LabelPMJParentKind]
		if expectedParentUID != "" {
			for _, ref := range pod.OwnerReferences {
				if ref.Controller != nil && *ref.Controller && string(ref.UID) != "" {
					// Direct comparison applies when the Pod's immediate controller owner Kind
					// matches the PMJ's recorded parent Kind (e.g. StatefulSet or Job, or when
					// ParentKind is unspecified). For Deployments, Pod.OwnerReferences points to
					// the intermediate ReplicaSet rather than the Deployment.
					if (expectedParentKind == "" || ref.Kind == expectedParentKind) && string(ref.UID) != expectedParentUID {
						violations = append(violations, Violation{
							InvariantID:   "I6",
							InvariantName: "RevisionAndIdentityFidelity",
							Reason:        "ParentControllerUIDMismatch",
							Message: fmt.Sprintf("PMJ %s/%s expects parent controller UID %q but replacement pod %s is owned by UID %q",
								job.Namespace, job.Name, expectedParentUID, pod.Name, ref.UID),
							Namespace: job.Namespace,
							PMJName:   job.Name,
							PodName:   pod.Name,
						})
					}
				}
			}
		}
	}
	return violations
}

// EvaluateI7 enforces I7 (Disruption Bounding / PDB Compliance):
// Origin Pod termination during PhaseEvicting must invoke the policy/v1 Eviction subresource
// (never raw Delete before the migration timeout deadline expires), and a PMJ must never
// advance to Restoring or Succeeded while BlockedByPDB/EvictionMisconfigured is True or
// while the non-terminating origin Pod is still running.
func EvaluateI7(s *ReconcileSnapshot) []Violation {
	if s == nil {
		return nil
	}
	var violations []Violation
	if s.UsedBareDeleteBeforeDeadline && s.PrimaryPMJ != nil {
		job := s.PrimaryPMJ
		violations = append(violations, Violation{
			InvariantID:   "I7",
			InvariantName: "DisruptionBoundingPDBCompliance",
			Reason:        "RawDeleteBypassedEvictionSubresource",
			Message:       fmt.Sprintf("PMJ %s/%s invoked raw Pod Delete during Evicting phase before deadline expiry, bypassing PodDisruptionBudgets", job.Namespace, job.Name),
			Namespace:     job.Namespace,
			PMJName:       job.Name,
			PodName:       job.Spec.PodRef.Name,
		})
	}

	pods := allPods(s)
	for _, job := range allPMJs(s) {
		if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring &&
			job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
			continue
		}
		pdbCond := meta.FindStatusCondition(job.Status.Conditions, "BlockedByPDB")
		misconfigCond := meta.FindStatusCondition(job.Status.Conditions, "EvictionMisconfigured")
		if (pdbCond != nil && pdbCond.Status == metav1.ConditionTrue) ||
			(misconfigCond != nil && misconfigCond.Status == metav1.ConditionTrue) {
			violations = append(violations, Violation{
				InvariantID:   "I7",
				InvariantName: "DisruptionBoundingPDBCompliance",
				Reason:        "EvictionBypassedWhileBlockedByPDB",
				Message: fmt.Sprintf("PMJ %s/%s advanced to phase %q while eviction blockage condition remains True",
					job.Namespace, job.Name, job.Status.Phase),
				Namespace: job.Namespace,
				PMJName:   job.Name,
				PodName:   job.Spec.PodRef.Name,
			})
		}
		if job.Spec.TargetPodUID != "" {
			for _, pod := range pods {
				if pod.Namespace == job.Namespace && string(pod.UID) == job.Spec.TargetPodUID && pod.DeletionTimestamp == nil {
					violations = append(violations, Violation{
						InvariantID:   "I7",
						InvariantName: "DisruptionBoundingPDBCompliance",
						Reason:        "OriginPodStillRunningInRestoringPhase",
						Message: fmt.Sprintf("PMJ %s/%s advanced to phase %q while origin pod %s (UID %q) is still running without eviction",
							job.Namespace, job.Name, job.Status.Phase, pod.Name, pod.UID),
						Namespace: job.Namespace,
						PMJName:   job.Name,
						PodName:   pod.Name,
					})
				}
			}
		}
	}
	return violations
}

func isSystemExcludedNamespace(ns string) bool {
	return ns == "kube-system" || ns == "pod-migration-system" || strings.HasPrefix(ns, "gke-managed-")
}

// EvaluateI8 enforces I8 (Platform & Control-Plane Isolation):
// Pods in excluded system namespaces (kube-system, pod-migration-system, gke-managed-*)
// must never be gated or targeted by a PodMigrationJob.
func EvaluateI8(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	for _, pod := range allPods(s) {
		if isSystemExcludedNamespace(pod.Namespace) && podHasGate(&pod) {
			violations = append(violations, Violation{
				InvariantID:   "I8",
				InvariantName: "PlatformAndControlPlaneIsolation",
				Reason:        "SystemNamespacePodGated",
				Message:       fmt.Sprintf("system-namespace pod %s/%s carries LPM scheduling gate %q", pod.Namespace, pod.Name, MigrationGateName),
				Namespace:     pod.Namespace,
				PodName:       pod.Name,
			})
		}
	}
	for _, job := range allPMJs(s) {
		if isSystemExcludedNamespace(job.Namespace) {
			violations = append(violations, Violation{
				InvariantID:   "I8",
				InvariantName: "PlatformAndControlPlaneIsolation",
				Reason:        "SystemNamespacePMJCreated",
				Message:       fmt.Sprintf("PodMigrationJob %s/%s was created in excluded system namespace %q", job.Namespace, job.Name, job.Namespace),
				Namespace:     job.Namespace,
				PMJName:       job.Name,
			})
		}
	}
	return violations
}

// EvaluateI9 enforces I9 (Deterministic Fallback over Crashloop):
// When a replacement Pod crashes with a matched gVisor/OCI restore-failure signature
// (detected live via restore.Classify, ConditionRestoreCrashed=True, or RestoreCrashSignatureMatched=true),
// the PMJ must not remain in PhaseRestoring or transition to PhaseSucceeded.
func EvaluateI9(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	pods := allPods(s)
	podByKey := make(map[string]corev1.Pod, len(pods))
	for _, p := range pods {
		podByKey[p.Namespace+"/"+p.Name] = p
	}

	for _, job := range allPMJs(s) {
		crashCond := meta.FindStatusCondition(job.Status.Conditions, "RestoreCrashed")
		hasCrashCond := crashCond != nil && crashCond.Status == metav1.ConditionTrue
		matched := hasCrashCond || (s != nil && s.RestoreCrashSignatureMatched && s.PrimaryPMJ != nil && s.PrimaryPMJ.Name == job.Name)

		if !matched && job.Status.RestoredPodName != "" {
			if pod, ok := podByKey[job.Namespace+"/"+job.Status.RestoredPodName]; ok {
				if job.Status.RestoredPodUID == "" || string(pod.UID) == job.Status.RestoredPodUID {
					if verdict := restore.Classify(&pod, restore.DefaultEngines()...); verdict.Class == restore.FailureFatal {
						matched = true
					}
				}
			}
		}

		if !matched {
			continue
		}
		if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseRestoring ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded {
			violations = append(violations, Violation{
				InvariantID:   "I9",
				InvariantName: "DeterministicFallbackOverCrashloop",
				Reason:        "RestoreCrashDidNotTriggerFallback",
				Message: fmt.Sprintf("PMJ %s/%s matched a container restore-crash signature but remained in phase %q instead of failing over to cold start",
					job.Namespace, job.Name, job.Status.Phase),
				Namespace: job.Namespace,
				PMJName:   job.Name,
				PodName:   job.Status.RestoredPodName,
			})
		}
	}
	return violations
}
