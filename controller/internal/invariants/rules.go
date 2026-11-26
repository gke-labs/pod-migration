package invariants

import (
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	// MigrationGateName is the scheduling gate injected by LPM onto replacement pods.
	MigrationGateName = "gke.io/pod-migration-gate"

	// AnnotationAssignedPMJ links a replacement Pod to its owning PodMigrationJob.
	AnnotationAssignedPMJ = util.AnnotationAssignedPMJ
	// AnnotationColdStartBypassJob records the PMJ that set the cold-start bypass on a source pod (#163).
	AnnotationColdStartBypassJob = util.AnnotationColdStartBypassJob
	// AnnotationRestoreCompleted records the snapshotRef on the replacement Pod after a verified restore (#152).
	AnnotationRestoreCompleted = util.AnnotationRestoreCompleted
	// AnnotationPSName is the GKE Pod Snapshots restore annotation on replacement Pods.
	// An explicit empty string value ("") is the cold-start bypass signal written by
	// releaseWithColdStartBypass and the replacement webhook.
	AnnotationPSName = "podsnapshot.gke.io/ps-name"
	// AnnotationMigrationTimeout is the effective migration timeout annotation stamped on PMJs (#57).
	AnnotationMigrationTimeout = "pod-migration.gke.io/timeout"
	// AnnotationEvictingSince records when the Evicting phase started waiting for deletion.
	AnnotationEvictingSince = "pod-migration.gke.io/evicting-since"
	// ConditionRestored is the PMJ condition that records the restore's outcome.
	ConditionRestored = "Restored"
	// ReasonRestoreVerified is the Restored reason of a verified warm restore.
	ReasonRestoreVerified = "RestoreVerified"
	// ReasonRestoreInferred is the Restored reason of an inferred warm restore (#178).
	ReasonRestoreInferred = "RestoreInferred"

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
	// GateReleaseGraceWindow is the grace window allowed between patching Status.GateReleased=true on the PMJ
	// and updating the replacement Pod to remove gke.io/pod-migration-gate.
	GateReleaseGraceWindow = 30 * time.Second

	// LabelPodFenced is the pod label stamped on fenced source pods (#131, #154).
	LabelPodFenced = util.LabelFenced
	// ConditionSourceFenced is the PodMigrationJob condition recording source-pod fencing state (#131, #154).
	ConditionSourceFenced = util.ConditionSourceFenced
	// ReasonPodMigrationFenced is the Pod Ready condition reason held while a source pod is fenced (#131, #154).
	ReasonPodMigrationFenced = util.ReasonPodMigrationFenced
	// SourceFencedGraceWindow is the grace window after SourceFenced=True transitions
	// on a PodMigrationJob during which I11 tolerates informer cache lag before the
	// source Pod's fence label and Ready=False status appear in cached Pod lists.
	SourceFencedGraceWindow = 10 * time.Second

	// AnnotationEngine records the snapshot engine serving a PodMigrationJob (C1).
	AnnotationEngine = "pod-migration.gke.io/engine"
	// DefaultEngineName is the default GKE Pod Snapshots engine name when AnnotationEngine is unset.
	DefaultEngineName = "gke-pod-snapshots"
)

// DefaultRules is the canonical ordered registry of the Core & Evolved Invariants (I1-I12).
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
	NewDebouncedRule(RuleI10(), 3, 30*time.Second),
	NewDebouncedRule(RuleI11(), 3, 30*time.Second),
	RuleI12(),
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

func jobEngineAnnotation(job *pmv1alpha1.PodMigrationJob) string {
	if job == nil || job.Annotations == nil {
		return ""
	}
	return strings.TrimSpace(job.Annotations[AnnotationEngine])
}

func effectiveJobEngineName(job *pmv1alpha1.PodMigrationJob) string {
	if name := jobEngineAnnotation(job); name != "" {
		return name
	}
	return DefaultEngineName
}

// isGVisorPod reports whether pod runs in a gVisor sandbox. The GKE Pod
// Snapshots runtime reads podsnapshot.gke.io/ps-name when it creates the
// sandbox of such a pod, whatever engine serves the pod's job (#151).
func isGVisorPod(pod *corev1.Pod) bool {
	return pod != nil && pod.Spec.RuntimeClassName != nil && *pod.Spec.RuntimeClassName == restore.GVisorRuntimeClass
}

// gkeRestoreOrder reads the GKE Pod Snapshots restore order on pod
// (podsnapshot.gke.io/ps-name): a snapshot name, or the cold-start bypass when
// the value is empty.
func gkeRestoreOrder(pod *corev1.Pod) (snapshotRef string, coldStartBypass bool, present bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false, false
	}
	val, exists := pod.Annotations[AnnotationPSName]
	if !exists {
		return "", false, false
	}
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return "", true, true
	}
	return trimmed, false, true
}

// inspectPodRestoreOrder reads the restore order of engineName on pod ("" for
// the first order of any registered engine) through s.RestoreOrders, or the
// GKE Pod Snapshots order when the snapshot has no RestoreOrders.
func inspectPodRestoreOrder(s *ReconcileSnapshot, engineName string, pod *corev1.Pod) (snapshotRef string, coldStartBypass bool, present bool) {
	if s != nil && s.RestoreOrders != nil {
		return s.RestoreOrders(engineName, pod)
	}
	return gkeRestoreOrder(pod)
}

// assignedJobEngine returns the engine of the job that pod is attached to (its
// assigned-pmj annotation), named as effectiveJobEngineName names it, and
// whether that job is in jobByKey. Without a job in the snapshot, it returns
// "" and false, and callers accept the restore order of any registered engine.
func assignedJobEngine(jobByKey map[string]pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (string, bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false
	}
	assigned := pod.Annotations[AnnotationAssignedPMJ]
	if assigned == "" {
		return "", false
	}
	job, ok := jobByKey[pod.Namespace+"/"+assigned]
	if !ok {
		return "", false
	}
	return effectiveJobEngineName(&job), true
}

// restoreKeyProbe is a snapshot name that no engine orders. readsRestoreKey
// sets it under one annotation key to learn whether an engine reads that key.
const restoreKeyProbe = "pod-migration-invariants-restore-key-probe"

// readsRestoreKey reports whether engineName ("" for any registered engine)
// reads its restore order from the annotation key. It asks RestoreOrders about
// a probe pod that carries only key, set to restoreKeyProbe, and otherwise has
// pod's namespace, name, UID and runtime class. RestoreOrders only reads the
// pod (C5), so the engine reads the probe back exactly when key is its restore
// key. Without RestoreOrders, every engine reads podsnapshot.gke.io/ps-name.
func readsRestoreKey(s *ReconcileSnapshot, engineName string, pod *corev1.Pod, key string) bool {
	if pod == nil {
		pod = &corev1.Pod{}
	}
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   pod.Namespace,
			Name:        pod.Name,
			UID:         pod.UID,
			Annotations: map[string]string{key: restoreKeyProbe},
		},
		Spec: corev1.PodSpec{RuntimeClassName: pod.Spec.RuntimeClassName},
	}
	snap, bypass, present := inspectPodRestoreOrder(s, engineName, probe)
	return present && !bypass && snap == restoreKeyProbe
}

// snapshotOrder is an order on a pod to restore the snapshot snap. key is the
// pod annotation that holds the order (<group>/ps-name), so it names the
// snapshot's API group; "" means the group is unknown.
type snapshotOrder struct {
	key  string
	snap string
}

// sameSnapshot reports whether the orders a and b can name the same snapshot:
// the same name under the same key, or under a key that is unknown.
func sameSnapshot(a, b snapshotOrder) bool {
	return a.snap == b.snap && (a.key == b.key || a.key == "" || b.key == "")
}

// orderKey returns the annotation of pod from which engineName ("" for any
// registered engine) read its restore order snap: of the annotations that hold
// snap, the first in sorted order whose removal changes what RestoreOrders
// reads. It returns "" (an unknown API group) when no annotation holds snap,
// or when the order doesn't rest on any single one of them, such as two
// engines' annotations holding the same name on a pod whose job isn't known.
func orderKey(s *ReconcileSnapshot, engineName string, pod *corev1.Pod, snap string) string {
	var keys []string
	for k, v := range pod.Annotations {
		if strings.TrimSpace(v) == snap {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	for _, k := range keys {
		without := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   pod.Namespace,
				Name:        pod.Name,
				UID:         pod.UID,
				Annotations: make(map[string]string, len(pod.Annotations)),
			},
			Spec: corev1.PodSpec{RuntimeClassName: pod.Spec.RuntimeClassName},
		}
		for ak, av := range pod.Annotations {
			if ak != k {
				without.Annotations[ak] = av
			}
		}
		if got, bypass, ok := inspectPodRestoreOrder(s, engineName, without); !ok || bypass || got != snap {
			return k
		}
	}
	return ""
}

// restoredSnapshots returns the snapshots that pod is ordered to restore: the
// restore order of engineName ("" for any registered engine) and, on a gVisor
// pod, the GKE Pod Snapshots order, which the GKE runtime acts on whatever
// engine serves the pod's job (#151). A cold-start bypass restores nothing.
func restoredSnapshots(s *ReconcileSnapshot, engineName string, pod *corev1.Pod) []snapshotOrder {
	var orders []snapshotOrder
	if snap, bypass, ok := inspectPodRestoreOrder(s, engineName, pod); ok && !bypass && snap != "" {
		orders = append(orders, snapshotOrder{key: orderKey(s, engineName, pod, snap), snap: snap})
	}
	if isGVisorPod(pod) {
		if snap, bypass, ok := gkeRestoreOrder(pod); ok && !bypass && snap != "" {
			if o := (snapshotOrder{key: AnnotationPSName, snap: snap}); !slices.Contains(orders, o) {
				orders = append(orders, o)
			}
		}
	}
	return orders
}

// restoresJobSnapshot reports whether pod is ordered to restore snapshotRef, a
// snapshot of a job that engineName serves. When pod's job is known, orders are
// the ones restoredSnapshots counts for pod, and one of them must name
// snapshotRef under a key that engineName reads (or under an unknown key).
// Otherwise pod's order for engineName must name snapshotRef.
func restoresJobSnapshot(s *ReconcileSnapshot, engineName, snapshotRef string, pod *corev1.Pod, jobKnown bool, orders []snapshotOrder) bool {
	if !jobKnown {
		snap, bypass, ok := inspectPodRestoreOrder(s, engineName, pod)
		return ok && !bypass && snap == snapshotRef
	}
	for _, o := range orders {
		if o.snap == snapshotRef && (o.key == "" || readsRestoreKey(s, engineName, pod, o.key)) {
			return true
		}
	}
	return false
}

// migratedAgain reports whether targeting, the jobs that target one pod, holds a
// job other than job that was created at or after it, so that the pod was
// migrated again. Creation timestamps have one-second resolution, so a job
// created in the same second counts.
func migratedAgain(job *pmv1alpha1.PodMigrationJob, targeting []pmv1alpha1.PodMigrationJob) bool {
	for i := range targeting {
		other := &targeting[i]
		if other.Name != job.Name && !other.CreationTimestamp.Before(&job.CreationTimestamp) {
			return true
		}
	}
	return false
}

// restoreVerified reports whether job recorded a verified or inferred warm restore:
// the Restored condition is True (with reason RestoreVerified or RestoreInferred).
func restoreVerified(job *pmv1alpha1.PodMigrationJob) bool {
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionRestored)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return false
	}
	return cond.Reason == ReasonRestoreVerified || cond.Reason == ReasonRestoreInferred
}

// EvaluateI1 enforces I1 (At-Most-Once Restore):
// A PodSnapshot may be bound to and consumed by at most one replacement Pod UID
// across the cluster's lifetime, and a consumed PMJ with GateReleased=true must
// never be re-bound to a different replacement Pod UID.
//
// To find duplicates, a pod's restore order is read for the engine of its job
// (GKE Pod Snapshots for a job that names no engine), never another engine's,
// and as the first order of any registered engine when its job isn't in the
// snapshot. A gVisor pod also restores its podsnapshot.gke.io/ps-name order,
// whatever its job's engine. A snapshot belongs to the API group of the
// annotation that orders it (<group>/ps-name): the same name under two groups'
// keys is two snapshots, while engines that share a group share its snapshots.
// The rebind check counts the same orders for a pod whose job is known, under a
// key the consumed job's engine reads, and otherwise the pod's order for the
// consumed job's engine.
func EvaluateI1(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	jobs := allPMJs(s)
	pods := allPods(s)

	jobByKey := make(map[string]pmv1alpha1.PodMigrationJob, len(jobs))
	for _, j := range jobs {
		jobByKey[j.Namespace+"/"+j.Name] = j
	}

	// consumers maps a snapshot name to the orders to restore it seen so far,
	// with the pod (UID, or namespace/name without one) that carries each.
	type consumer struct {
		order snapshotOrder
		podID string
	}
	consumers := make(map[string][]consumer)
	// podOrders[i] holds the orders counted for pods[i], and jobKnown[i] whether
	// its job is in the snapshot; the rebind check below reads them again.
	podOrders := make([][]snapshotOrder, len(pods))
	jobKnown := make([]bool, len(pods))
	for i := range pods {
		pod := &pods[i]
		engName, known := assignedJobEngine(jobByKey, pod)
		podOrders[i], jobKnown[i] = restoredSnapshots(s, engName, pod), known
		podID := string(pod.UID)
		if podID == "" {
			podID = pod.Namespace + "/" + pod.Name
		}
		for _, order := range podOrders[i] {
			for _, prev := range consumers[order.snap] {
				if prev.podID != podID && sameSnapshot(prev.order, order) {
					violations = append(violations, Violation{
						InvariantID:   "I1",
						InvariantName: "AtMostOnceRestore",
						Reason:        "DuplicateSnapshotConsumption",
						Message:       fmt.Sprintf("snapshot %q is simultaneously bound to multiple replacement pods (%s and %s)", order.snap, prev.podID, podID),
						Namespace:     pod.Namespace,
						PodName:       pod.Name,
					})
					break
				}
			}
			consumers[order.snap] = append(consumers[order.snap], consumer{order: order, podID: podID})
		}
	}

	for _, job := range jobs {
		if job.Status.Consumed && job.Status.GateReleased && job.Status.SnapshotRef != "" {
			engName := effectiveJobEngineName(&job)
			for i := range pods {
				pod := &pods[i]
				if job.Status.RestoredPodUID != "" &&
					string(pod.UID) != "" &&
					string(pod.UID) != job.Status.RestoredPodUID &&
					restoresJobSnapshot(s, engName, job.Status.SnapshotRef, pod, jobKnown[i], podOrders[i]) {
					// The rebound pod is the one about to restore the consumed
					// snapshot, so report against the job it is attached to when
					// it has one; strict mode fails only the job a violation names (#198).
					owner := job.Name
					if pod.Annotations != nil && pod.Annotations[AnnotationAssignedPMJ] != "" {
						owner = pod.Annotations[AnnotationAssignedPMJ]
					}
					violations = append(violations, Violation{
						InvariantID:   "I1",
						InvariantName: "AtMostOnceRestore",
						Reason:        "ConsumedSnapshotReboundAfterGateRelease",
						Message: fmt.Sprintf("PMJ %s/%s already released gate for consumer UID %q on snapshot %q, but pod %s (UID %q) is bound to the same snapshot",
							job.Namespace, job.Name, job.Status.RestoredPodUID, job.Status.SnapshotRef, pod.Name, pod.UID),
						Namespace: job.Namespace,
						PMJName:   owner,
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
// snapshot) the replacement Pod carries the matching restore order for the job's engine
// (GKE Pod Snapshots for a job that names no engine; another engine's order doesn't count)
// and did not bypass restore via a cold-start bypass order. The replacement check matches
// by RestoredPodUID when recorded, skipping pods replaced under the same name. The cold-start
// bypass is accepted when another PMJ set the bypass on the pod (stamping its name in
// AnnotationColdStartBypassJob matching FormatPMJName, #163) or targets the replacement's
// UID in the snapshot (#151); this exemption is strictly guarded by the empty cold-start
// bypass order so that a replacement carrying a different snapshot name still violates I2 (#163).
// The cold-start bypass is also accepted once the PMJ recorded Restored=True (RestoreVerified
// or RestoreInferred) AND the replacement carries `pod-migration.gke.io/restore-completed`
// matching Status.SnapshotRef: the controller sets both after the verified restore, so that later
// restarts of the replacement don't restore the migration snapshot again (#152).
// A released gVisor replacement whose job's engine orders restores under a key other than
// podsnapshot.gke.io/ps-name must also carry the GKE Pod Snapshots cold-start bypass
// (podsnapshot.gke.io/ps-name: ""): the GKE runtime reads that key when it creates the
// sandbox of every gVisor pod, and can restore a leftover snapshot without it (#151).
func EvaluateI2(s *ReconcileSnapshot) []Violation {
	var violations []Violation
	jobs := allPMJs(s)
	pods := allPods(s)

	podByKey := make(map[string]corev1.Pod, len(pods))
	for _, p := range pods {
		podByKey[p.Namespace+"/"+p.Name] = p
	}
	jobByKey := make(map[string]pmv1alpha1.PodMigrationJob, len(jobs))
	jobsByTargetUID := make(map[string][]pmv1alpha1.PodMigrationJob, len(jobs))
	for _, j := range jobs {
		jobByKey[j.Namespace+"/"+j.Name] = j
		if j.Spec.TargetPodUID != "" {
			jobsByTargetUID[j.Spec.TargetPodUID] = append(jobsByTargetUID[j.Spec.TargetPodUID], j)
		}
	}
	if s != nil {
		for _, j := range s.TargetingPMJs {
			if j.Spec.TargetPodUID != "" {
				jobsByTargetUID[j.Spec.TargetPodUID] = append(jobsByTargetUID[j.Spec.TargetPodUID], j)
			}
		}
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
				// If the replacement was replaced under the same name (such as a
				// recreated StatefulSet pod), it has a different UID from the one
				// this job restored and its annotations belong to a later lifecycle.
				if job.Status.RestoredPodUID != "" && pod.UID != "" && string(pod.UID) != job.Status.RestoredPodUID {
					continue
				}
				engName := effectiveJobEngineName(&job)
				snapAnn, coldStartBypass, present := inspectPodRestoreOrder(s, engName, &pod)
				restoreCompleted := pod.Annotations[AnnotationRestoreCompleted]
				if coldStartBypass && restoreVerified(&job) && restoreCompleted != "" && restoreCompleted == job.Status.SnapshotRef {
					continue
				}
				bypassingJob := pod.Annotations[AnnotationColdStartBypassJob]
				hasBypassJob := pod.UID != "" && bypassingJob != "" && bypassingJob != job.Name && bypassingJob == util.FormatPMJName(pod.Name, string(pod.UID))
				// The TargetingPMJs / migratedAgain lookup stays alongside the stamp
				// to support pods that were stamped by an older controller (#156, #163).
				if coldStartBypass && (hasBypassJob || (pod.UID != "" && migratedAgain(&job, jobsByTargetUID[string(pod.UID)]))) {
					continue
				}
				if !present || snapAnn != job.Status.SnapshotRef || coldStartBypass {
					violations = append(violations, Violation{
						InvariantID:   "I2",
						InvariantName: "NoSilentColdStart",
						Reason:        "SucceededPodMissingRestoreAnnotation",
						Message: fmt.Sprintf("PMJ %s/%s is Succeeded expecting snapshot %q on replacement pod %s, but the pod's restore order for engine %q is %q (present=%t, coldStartBypass=%t)",
							job.Namespace, job.Name, job.Status.SnapshotRef, pod.Name, engName, snapAnn, present, coldStartBypass),
						Namespace: job.Namespace,
						PMJName:   job.Name,
						PodName:   pod.Name,
					})
				}
			}
		}
	}

	// Also verify that any replacement pod still carrying assigned-pmj whose gate was removed
	// has either a non-empty snapshot name or the explicit cold-start bypass order of its
	// job's engine (of any registered engine when the job isn't in the snapshot).
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || podHasGate(&pod) || pod.Annotations == nil {
			continue
		}
		assigned := pod.Annotations[AnnotationAssignedPMJ]
		if assigned == "" {
			continue
		}
		engName, jobKnown := assignedJobEngine(jobByKey, &pod)
		if _, _, hasOrder := inspectPodRestoreOrder(s, engName, &pod); !hasOrder {
			wantOrder := "any registered engine"
			if jobKnown {
				wantOrder = fmt.Sprintf("engine %q", engName)
			}
			violations = append(violations, Violation{
				InvariantID:   "I2",
				InvariantName: "NoSilentColdStart",
				Reason:        "GateReleasedWithoutSnapshotOrColdStartBypass",
				Message: fmt.Sprintf("pod %s/%s had scheduling gate %q removed while assigned to PMJ %s without a restore order of %s (a snapshot name or the explicit empty-string cold-start bypass)",
					pod.Namespace, pod.Name, MigrationGateName, assigned, wantOrder),
				Namespace: pod.Namespace,
				PMJName:   assigned,
				PodName:   pod.Name,
			})
		}
		// Without its job in the snapshot, the pod's engine is unknown, so
		// only a pod whose job's engine is known is checked here.
		if jobKnown && isGVisorPod(&pod) && !readsRestoreKey(s, engName, &pod, AnnotationPSName) {
			if gkeSnap, gkeBypass, gkePresent := gkeRestoreOrder(&pod); !gkePresent || !gkeBypass {
				violations = append(violations, Violation{
					InvariantID:   "I2",
					InvariantName: "NoSilentColdStart",
					Reason:        "GVisorPodMissingGKEColdStartBypass",
					Message: fmt.Sprintf("gVisor pod %s/%s had scheduling gate %q removed while assigned to PMJ %s, whose engine %q orders restores under a key other than %s, without the GKE Pod Snapshots cold-start bypass %s: \"\" (present=%t, snapshot=%q), so GKE Pod Snapshots can restore a leftover snapshot into its sandbox",
						pod.Namespace, pod.Name, MigrationGateName, assigned, engName, AnnotationPSName, AnnotationPSName, gkePresent, gkeSnap),
					Namespace: pod.Namespace,
					PMJName:   assigned,
					PodName:   pod.Name,
				})
			}
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

// EffectiveMigrationTimeout reads the effective migration deadline stamped on the PMJ's
// `pod-migration.gke.io/timeout` annotation (which PR #57 stamps at creation / Pending),
// capped at util.MaxMigrationTimeout (2h) to match the reconciler, and falling back to
// MaxMigrationDuration (10m) when unset or invalid.
func EffectiveMigrationTimeout(job *pmv1alpha1.PodMigrationJob) time.Duration {
	if job != nil && job.Annotations != nil {
		if raw := job.Annotations[AnnotationMigrationTimeout]; raw != "" {
			if d, err := time.ParseDuration(raw); err == nil && d > 0 {
				if d > util.MaxMigrationTimeout {
					return util.MaxMigrationTimeout
				}
				return d
			}
		}
	}
	return MaxMigrationDuration
}

func snapshotProgressForJob(s *ReconcileSnapshot, job *pmv1alpha1.PodMigrationJob, now time.Time) (SnapshotProgress, bool) {
	if s == nil || job == nil {
		return SnapshotProgress{}, false
	}
	if s.SnapshotProgress != nil {
		if prog, ok := s.SnapshotProgress[job.Namespace+"/"+job.Name]; ok {
			return prog, true
		}
	}
	if s.PrimaryPMJ != nil &&
		job.Namespace == s.PrimaryPMJ.Namespace &&
		job.Name == s.PrimaryPMJ.Name &&
		len(s.PrimarySnapshotConditions) > 0 {
		st := snapshot.StatusFromConditions(job.Status.SnapshotRef, s.PrimarySnapshotConditions, time.Time{}, "", now)
		if st != nil {
			return SnapshotProgress{
				Ready:            st.Phase == snapshot.PhaseReady,
				Failed:           st.Phase == snapshot.PhaseFailed || st.Phase == snapshot.PhaseRefused,
				LastProgressTime: st.LastProgressTime,
			}, true
		}
	}
	return SnapshotProgress{}, false
}

// EvaluateI4 enforces I4 (Terminal Progress):
// Active PMJs in Pending/Snapshotting/Evicting must not exceed their effective migration
// timeout (`pod-migration.gke.io/timeout` annotation on the PMJ, or 10m default)
// without transitioning to a terminal phase, and a deleting PodMigration CR with
// StorageCleanupFinalizer must not remain un-finalized past MaxMigrationDuration (10m).
// The PMJ deadline runs from creation while Pending and from Status.SnapshottingStartTime after.
// Healthy progressing Snapshotting jobs (with a non-empty Status.SnapshotRef and Ready or recent
// LastProgressTime) and Evicting jobs within their phase budget are tolerated up to the 2h
// MaxMigrationTimeout hard cap.
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
			effectiveTimeout := EffectiveMigrationTimeout(&job)
			budget := effectiveTimeout + progressGraceSlack

			// The deadline and its MaxMigrationTimeout (2h) hard cap run from the PMJ's
			// creation while it is Pending, and from Status.SnapshottingStartTime once it
			// has left Pending, as in the reconciler: a wait in Pending for the
			// disruption budget (#42) doesn't use up the migration's own time.
			anchor, anchorLabel := job.CreationTimestamp.Time, "created"
			if job.Status.SnapshottingStartTime != nil && !job.Status.SnapshottingStartTime.IsZero() {
				anchor, anchorLabel = job.Status.SnapshottingStartTime.Time, "snapshotting started"
			}

			// During Snapshotting, the reconciler extends the deadline while the snapshot
			// is actively progressing (Ready, or non-zero LastProgressTime within the
			// effective timeout), bounded by the hard MaxMigrationTimeout (2h) wall-clock cap.
			if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting &&
				job.Status.SnapshotRef != "" &&
				(anchor.IsZero() || now.Sub(anchor) <= util.MaxMigrationTimeout+progressGraceSlack) {
				if prog, ok := snapshotProgressForJob(s, &job, now); ok && !prog.Failed {
					if prog.Ready || (!prog.LastProgressTime.IsZero() && now.Sub(prog.LastProgressTime) <= budget) {
						continue
					}
				}
			}

			// During Evicting, PR #57 anchors the eviction phase budget on Status.EvictingStartTime
			// (with fallback to the `pod-migration.gke.io/evicting-since` annotation for in-flight jobs),
			// bounded by the hard MaxMigrationTimeout (2h) wall-clock cap.
			if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting &&
				(anchor.IsZero() || now.Sub(anchor) <= util.MaxMigrationTimeout+progressGraceSlack) {
				if job.Status.EvictingStartTime != nil && !job.Status.EvictingStartTime.IsZero() {
					if now.Sub(job.Status.EvictingStartTime.Time) <= budget {
						continue
					}
				} else if job.Annotations != nil {
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
			}

			if !anchor.IsZero() && now.Sub(anchor) > budget {
				// Static message string (without dynamic now.Sub elapsed duration) so
				// client-go EventRecorder and Engine deduplication aggregate cleanly.
				violations = append(violations, Violation{
					InvariantID:   "I4",
					InvariantName: "TerminalProgress",
					Reason:        "ActivePMJExceededDeadline",
					Message: fmt.Sprintf("PMJ %s/%s remained in non-terminal phase %q beyond its %s deadline (%s at %s)",
						job.Namespace, job.Name, job.Status.Phase, effectiveTimeout, anchorLabel, anchor.UTC().Format(time.RFC3339)),
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
// Terminal PMJs in Succeeded or Failed must not retain orphaned PodSnapshotManualTrigger
// objects past cleanup (SucceededWithoutRestore deliberately preserves snapshot state per #32).
func EvaluateI5(s *ReconcileSnapshot) []Violation {
	if s == nil || !s.HasOrphanedTrigger || s.PrimaryPMJ == nil {
		return nil
	}
	job := s.PrimaryPMJ
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed {
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
// A PMJ must never advance to Restoring or Succeeded while BlockedByPDB/EvictionMisconfigured
// is True or while the non-terminating origin Pod (TargetPodUID) is still running without eviction.
func EvaluateI7(s *ReconcileSnapshot) []Violation {
	if s == nil {
		return nil
	}
	var violations []Violation
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

// RuleI11 returns the stateless Rule for I11 (SourceFencedAfterCheckpoint).
func RuleI11() Rule {
	return funcRule{
		id:   "I11",
		name: "SourceFencedAfterCheckpoint",
		fn:   EvaluateI11,
	}
}

func isSourcePodForPMJ(pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob) bool {
	if pod == nil || job == nil {
		return false
	}
	if pod.Namespace != "" && job.Namespace != "" && pod.Namespace != job.Namespace {
		return false
	}
	if job.Status.RestoredPodUID != "" && string(pod.UID) != "" && string(pod.UID) == job.Status.RestoredPodUID {
		return false
	}
	if job.Spec.TargetPodUID != "" && string(pod.UID) != "" {
		if string(pod.UID) != job.Spec.TargetPodUID {
			return false
		}
		if job.Spec.PodRef.Name != "" && pod.Name != "" && pod.Name != job.Spec.PodRef.Name {
			return false
		}
		return true
	}
	return job.Spec.PodRef.Name != "" && pod.Name == job.Spec.PodRef.Name
}

// EvaluateI11 enforces I11 (SourceFencedAfterCheckpoint, #154 / #131):
// While a non-terminal PodMigrationJob records condition SourceFenced=True (after
// SourceFencedGraceWindow has elapsed since LastTransitionTime), its source pod,
// while it exists, must carry the label `pod-migration.gke.io/fenced: "true"`
// and have pod status condition `Ready=False` with reason `PodMigrationFenced`.
//
// Required unfence ordering: when unfencing a source pod on an abort/cancel path,
// the controller must set `SourceFenced=False` (Reason=Unfenced) on the
// PodMigrationJob before removing `pod-migration.gke.io/fenced` from the Pod.
// Terminal PMJs (Succeeded, SucceededWithoutRestore, Failed) are skipped so an
// unfenced pod from a concluded job never flags active migrations in the namespace.
func EvaluateI11(s *ReconcileSnapshot) []Violation {
	if s == nil {
		return nil
	}
	now := time.Now()
	if !s.Now.IsZero() {
		now = s.Now
	}
	var violations []Violation
	pods := allPods(s)
	for _, job := range allPMJs(s) {
		if isTerminalPhase(job.Status.Phase) {
			continue
		}
		fencedCond := meta.FindStatusCondition(job.Status.Conditions, ConditionSourceFenced)
		if fencedCond == nil || fencedCond.Status != metav1.ConditionTrue {
			continue
		}
		if !fencedCond.LastTransitionTime.IsZero() && now.Sub(fencedCond.LastTransitionTime.Time) < SourceFencedGraceWindow {
			continue
		}
		for i := range pods {
			pod := &pods[i]
			if !isSourcePodForPMJ(pod, &job) {
				continue
			}
			fencedLabel := ""
			if pod.Labels != nil {
				fencedLabel = pod.Labels[LabelPodFenced]
			}
			if fencedLabel != "true" {
				violations = append(violations, Violation{
					InvariantID:   "I11",
					InvariantName: "SourceFencedAfterCheckpoint",
					Reason:        "SourcePodMissingFenceLabel",
					Message: fmt.Sprintf("PMJ %s/%s has %s=True, but source pod %s has label %s=%q (want \"true\")",
						job.Namespace, job.Name, ConditionSourceFenced, pod.Name, LabelPodFenced, fencedLabel),
					Namespace: job.Namespace,
					PMJName:   job.Name,
					PodName:   pod.Name,
				})
			}

			var readyCond *corev1.PodCondition
			for j := range pod.Status.Conditions {
				if pod.Status.Conditions[j].Type == corev1.PodReady {
					readyCond = &pod.Status.Conditions[j]
					break
				}
			}
			if readyCond == nil || readyCond.Status != corev1.ConditionFalse || readyCond.Reason != ReasonPodMigrationFenced {
				actualStatus := "absent"
				actualReason := ""
				if readyCond != nil {
					actualStatus = string(readyCond.Status)
					actualReason = readyCond.Reason
				}
				violations = append(violations, Violation{
					InvariantID:   "I11",
					InvariantName: "SourceFencedAfterCheckpoint",
					Reason:        "SourcePodReadyNotFenced",
					Message: fmt.Sprintf("PMJ %s/%s has %s=True, but source pod %s has Ready=%s (reason=%q, want False/%s)",
						job.Namespace, job.Name, ConditionSourceFenced, pod.Name, actualStatus, actualReason, ReasonPodMigrationFenced),
					Namespace: job.Namespace,
					PMJName:   job.Name,
					PodName:   pod.Name,
				})
			}
		}
	}
	return violations
}

// RuleI12 returns the stateless Rule for I12 (RestoreOutcomeConfirmed).
func RuleI12() Rule {
	return funcRule{
		id:   "I12",
		name: "RestoreOutcomeConfirmed",
		fn:   EvaluateI12,
	}
}

func hasConfirmedRestoreEvidence(engName, msg string) bool {
	if engName == "" || engName == DefaultEngineName {
		return false
	}
	marker := fmt.Sprintf("engine %s confirmed the restore: ", engName)
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return false
	}
	return strings.TrimSpace(msg[idx+len(marker):]) != ""
}

// EvaluateI12 enforces I12 (RestoreOutcomeConfirmed, C6 / #79 / #179):
//  1. A non-Succeeded PodMigrationJob must never carry Restored=True.
//  2. Reason=RestoreVerified on Restored or Ready requires positive engine
//     restore confirmation (which GKE Pod Snapshots never provides in GKE today)
//     and non-empty evidence recorded in the Restored condition message.
//  3. When a Succeeded PodMigrationJob carries the Restored condition, it must
//     be True with Reason=RestoreVerified or Reason=RestoreInferred, and any
//     Ready condition on that job must match Restored's status and reason.
func EvaluateI12(s *ReconcileSnapshot) []Violation {
	if s == nil {
		return nil
	}
	var violations []Violation
	for _, job := range allPMJs(s) {
		engName := effectiveJobEngineName(&job)
		restoredCond := meta.FindStatusCondition(job.Status.Conditions, ConditionRestored)
		readyCond := meta.FindStatusCondition(job.Status.Conditions, "Ready")

		// Grandfather pre-#178 legacy Succeeded jobs:
		// The pre-#178 controller marked warm restores Succeeded with
		// Restored=True, Reason=RestoreVerified, but without an engine annotation.
		if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded &&
			restoredCond != nil && restoredCond.Status == metav1.ConditionTrue &&
			restoredCond.Reason == ReasonRestoreVerified &&
			(job.Annotations == nil || strings.TrimSpace(job.Annotations[AnnotationEngine]) == "") &&
			!hasConfirmedRestoreEvidence(engName, restoredCond.Message) {
			continue
		}

		if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded &&
			restoredCond != nil && restoredCond.Status == metav1.ConditionTrue {
			violations = append(violations, Violation{
				InvariantID:   "I12",
				InvariantName: "RestoreOutcomeConfirmed",
				Reason:        "NonSucceededJobMarkedRestored",
				Message: fmt.Sprintf("NonSucceededJobMarkedRestored: PMJ %s/%s has phase %q (not Succeeded) but condition %s=True (reason=%q)",
					job.Namespace, job.Name, job.Status.Phase, ConditionRestored, restoredCond.Reason),
				Namespace: job.Namespace,
				PMJName:   job.Name,
				PodName:   job.Status.RestoredPodName,
			})
		}

		claimsVerified := (restoredCond != nil && restoredCond.Reason == ReasonRestoreVerified) ||
			(readyCond != nil && readyCond.Reason == ReasonRestoreVerified)
		if claimsVerified {
			confirmedValid := restoredCond != nil &&
				restoredCond.Status == metav1.ConditionTrue &&
				restoredCond.Reason == ReasonRestoreVerified &&
				hasConfirmedRestoreEvidence(engName, restoredCond.Message) &&
				(readyCond == nil || (readyCond.Status == metav1.ConditionTrue && readyCond.Reason == ReasonRestoreVerified))
			if !confirmedValid {
				violations = append(violations, Violation{
					InvariantID:   "I12",
					InvariantName: "RestoreOutcomeConfirmed",
					Reason:        "UnconfirmedRestoreMarkedVerified",
					Message: fmt.Sprintf("PMJ %s/%s (engine %q) claims reason=%s without positive engine restore confirmation",
						job.Namespace, job.Name, engName, ReasonRestoreVerified),
					Namespace: job.Namespace,
					PMJName:   job.Name,
					PodName:   job.Status.RestoredPodName,
				})
			}
			continue
		}

		if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded && restoredCond != nil {
			validVerdict := restoredCond.Status == metav1.ConditionTrue &&
				(restoredCond.Reason == ReasonRestoreVerified || restoredCond.Reason == ReasonRestoreInferred) &&
				(readyCond == nil || (readyCond.Status == metav1.ConditionTrue && readyCond.Reason == restoredCond.Reason))
			if !validVerdict {
				violations = append(violations, Violation{
					InvariantID:   "I12",
					InvariantName: "RestoreOutcomeConfirmed",
					Reason:        "SucceededWithInvalidRestoreVerdict",
					Message: fmt.Sprintf("PMJ %s/%s is Succeeded with invalid %s condition (status=%q, reason=%q)",
						job.Namespace, job.Name, ConditionRestored, restoredCond.Status, restoredCond.Reason),
					Namespace: job.Namespace,
					PMJName:   job.Name,
					PodName:   job.Status.RestoredPodName,
				})
			}
		}
	}
	return violations
}
