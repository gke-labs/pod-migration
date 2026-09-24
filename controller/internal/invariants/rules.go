package invariants

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

const (
	// MigrationGateName is the scheduling gate injected by LPM onto replacement pods.
	MigrationGateName = "gke.io/pod-migration-gate"
	// LegacyRestoreGateName is the legacy scheduling gate name checked for defense in depth.
	LegacyRestoreGateName = "podmigration.gke.io/restore-gate"

	// AnnotationAssignedPMJ links a replacement Pod to its owning PodMigrationJob.
	AnnotationAssignedPMJ = "pod-migration.gke.io/assigned-pmj"
	// AnnotationPSName is the GKE Pod Snapshots restore annotation on replacement Pods.
	AnnotationPSName = "podsnapshot.gke.io/ps-name"
	// AnnotationLegacySnapshotName is an alternate snapshot restore annotation key.
	AnnotationLegacySnapshotName = "pod-snapshots.gke.io/snapshot-name"
	// AnnotationColdStartBypass marks a Pod that intentionally bypassed snapshot restore.
	AnnotationColdStartBypass = "pod-migration.gke.io/cold-start-bypass"
	// AnnotationParentUID records the parent controller UID on a PMJ.
	AnnotationParentUID = "pod-migration.gke.io/parent-uid"

	// LabelPodTemplateHash is the Deployment revision label key.
	LabelPodTemplateHash = "pod-template-hash"
	// LabelControllerRevisionHash is the StatefulSet revision label key.
	LabelControllerRevisionHash = "controller-revision-hash"
	// LabelJobCompletionIndex is the Indexed Job completion index label key.
	LabelJobCompletionIndex = "batch.kubernetes.io/job-completion-index"
	// LabelPMJPodTemplateHash is the PMJ label storing Deployment pod-template-hash.
	LabelPMJPodTemplateHash = "pod-migration.gke.io/pod-template-hash"
	// LabelPMJControllerRevisionHash is the PMJ label storing StatefulSet controller-revision-hash.
	LabelPMJControllerRevisionHash = "pod-migration.gke.io/controller-revision-hash"
	// LabelPMJJobCompletionIndex is the PMJ label storing Indexed Job completion index.
	LabelPMJJobCompletionIndex = "pod-migration.gke.io/job-completion-index"

	// StorageCleanupFinalizer is the finalizer on PodMigration CRs.
	StorageCleanupFinalizer = "podmigration.gke.io/storage-cleanup"
	// MaxMigrationDuration is the 10-minute upper bound for active PMJs and deletion deferral.
	MaxMigrationDuration = 10 * time.Minute
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

func isTerminalPhase(phase pmv1alpha1.PodMigrationJobPhase) bool {
	return phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
		phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
		phase == pmv1alpha1.PodMigrationJobPhaseFailed
}

func podHasGate(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == MigrationGateName || g.Name == LegacyRestoreGateName {
			return true
		}
	}
	return false
}

func podSnapshotAnnotation(pod *corev1.Pod) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	if v := pod.Annotations[AnnotationPSName]; v != "" {
		return v
	}
	return pod.Annotations[AnnotationLegacySnapshotName]
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
// snapshot) the replacement Pod carries the matching snapshot restore annotation and
// did not bypass restore via cold-start.
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
				bypass := ""
				if pod.Annotations != nil {
					bypass = pod.Annotations[AnnotationColdStartBypass]
				}
				if snapAnn != job.Status.SnapshotRef || bypass == "true" {
					violations = append(violations, Violation{
						InvariantID:   "I2",
						InvariantName: "NoSilentColdStart",
						Reason:        "SucceededPodMissingRestoreAnnotation",
						Message: fmt.Sprintf("PMJ %s/%s is Succeeded expecting snapshot %q on replacement pod %s, but pod has ps-name=%q and cold-start-bypass=%q",
							job.Namespace, job.Name, job.Status.SnapshotRef, pod.Name, snapAnn, bypass),
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

// EvaluateI3 enforces I3 (Gate Liveness):
// Every non-terminating Pod carrying the LPM scheduling gate must have an assigned PMJ
// that is non-terminal and has not already marked GateReleased=true for that pod.
func EvaluateI3(s *ReconcileSnapshot) []Violation {
	var violations []Violation
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
	return violations
}

// EvaluateI4 enforces I4 (Terminal Progress):
// Active PMJs in Pending/Snapshotting/Evicting must not exceed MaxMigrationDuration (10m)
// without transitioning to a terminal phase, and a deleting PodMigration CR with
// StorageCleanupFinalizer must not remain un-finalized past MaxMigrationDuration (10m).
func EvaluateI4(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	now := time.Now()
	if s != nil && !s.Now.IsZero() {
		now = s.Now
	}

	//Grace slack beyond 10m to distinguish an overdue stuck object from the reconcile
	// step that is actively timing it out.
	const progressGraceSlack = 30 * time.Second

	for _, job := range allPMJs(s) {
		if job.Status.Phase == pmv1alpha1.PodMigrationJobPhasePending ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
			if !job.CreationTimestamp.IsZero() && now.Sub(job.CreationTimestamp.Time) > MaxMigrationDuration+progressGraceSlack {
				violations = append(violations, Violation{
					InvariantID:   "I4",
					InvariantName: "TerminalProgress",
					Reason:        "ActivePMJExceededDeadline",
					Message: fmt.Sprintf("PMJ %s/%s remained in non-terminal phase %q for %s (exceeding %s deadline)",
						job.Namespace, job.Name, job.Status.Phase, now.Sub(job.CreationTimestamp.Time).Round(time.Second), MaxMigrationDuration),
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
					Message: fmt.Sprintf("PodMigration %s/%s has been deleting with finalizer %q for %s (exceeding %s max deferral)",
						mig.Namespace, mig.Name, StorageCleanupFinalizer, now.Sub(mig.DeletionTimestamp.Time).Round(time.Second), MaxMigrationDuration),
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
// When a PMJ is bound to a replacement Pod, their controller revision hashes,
// Indexed Job completion indices, and parent UID annotations must match.
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
		// 1. Check Deployment pod-template-hash
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
		// 2. Check StatefulSet controller-revision-hash
		if expectedRev := job.Labels[LabelPMJControllerRevisionHash]; expectedRev != "" {
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
		// 3. Check Indexed Job completion index
		if expectedIdx := job.Labels[LabelPMJJobCompletionIndex]; expectedIdx != "" {
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
		// 4. Check generational parent-uid if present on both
		if expectedParentUID := job.Annotations[AnnotationParentUID]; expectedParentUID != "" {
			for _, ref := range pod.OwnerReferences {
				if ref.Controller != nil && *ref.Controller && string(ref.UID) != "" && string(ref.UID) != expectedParentUID {
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
	return violations
}

// EvaluateI7 enforces I7 (Disruption Bounding / PDB Compliance):
// Origin Pod termination during PhaseEvicting must invoke the policy/v1 Eviction subresource,
// never raw Delete before the migration timeout deadline expires.
func EvaluateI7(s *ReconcileSnapshot) []Violation {
	if s == nil || !s.UsedBareDeleteBeforeDeadline || s.PrimaryPMJ == nil {
		return nil
	}
	job := s.PrimaryPMJ
	return []Violation{{
		InvariantID:   "I7",
		InvariantName: "DisruptionBoundingPDBCompliance",
		Reason:        "RawDeleteBypassedEvictionSubresource",
		Message:       fmt.Sprintf("PMJ %s/%s invoked raw Pod Delete during Evicting phase before deadline expiry, bypassing PodDisruptionBudgets", job.Namespace, job.Name),
		Namespace:     job.Namespace,
		PMJName:       job.Name,
		PodName:       job.Spec.PodRef.Name,
	}}
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
// (ConditionRestoreCrashed=True or RestoreCrashSignatureMatched=true), the PMJ must not
// remain in PhaseRestoring or transition to PhaseSucceeded.
func EvaluateI9(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	for _, job := range allPMJs(s) {
		crashCond := meta.FindStatusCondition(job.Status.Conditions, "RestoreCrashed")
		hasCrashCond := crashCond != nil && crashCond.Status == metav1.ConditionTrue
		matched := hasCrashCond || (s != nil && s.RestoreCrashSignatureMatched && s.PrimaryPMJ != nil && s.PrimaryPMJ.Name == job.Name)
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
