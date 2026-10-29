package util

import (
	"context"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

const (
	LabelParentName              = "pod-migration.gke.io/parent-name"
	LabelParentKind              = "pod-migration.gke.io/parent-kind"
	LabelParentUID               = "pod-migration.gke.io/parent-uid"
	LabelPodTemplateHash         = "pod-migration.gke.io/pod-template-hash"
	LabelControllerRevisionHash  = appsv1.ControllerRevisionHashLabelKey
	LabelJobCompletionIndex      = batchv1.JobCompletionIndexAnnotation
	LabelOriginPodName           = "pod-migration.gke.io/origin-pod-name"
	AnnotationAssignedPMJ        = "pod-migration.gke.io/assigned-pmj"
	AnnotationRestoreCompleted   = "pod-migration.gke.io/restore-completed"
	AnnotationMismatchSince      = "pod-migration.gke.io/mismatch-since"
	AnnotationPDBEvictionTimeout = "pod-migration.gke.io/pdb-eviction-timeout"
	AnnotationPodSnapshotPolicy  = "pod-migration.gke.io/pod-snapshot-policy"
	AnnotationReplicaIdentityKey = "pod-migration.gke.io/replica-identity"
	LabelReplicaKey              = "pod-migration.gke.io/replica-key"

	// PodAssignedPMJIndexKey is the cache index mapping pods to the PMJ named
	// in their assigned-pmj annotation.  Registered at manager startup via
	// controller.RegisterFieldIndexes.
	PodAssignedPMJIndexKey = "pod.podmigration.gke.io/assigned-pmj"

	LabelTriggerSource          = "pod-migration.gke.io/trigger-source"
	AnnotationTriggerSource     = "pod-migration.gke.io/trigger-source"
	TriggerSourceSpotPreemption = "spot-preemption"
	TriggerSourceEviction       = "eviction-webhook"

	// DefaultSpotPreemptionNodeBudget is the baseline memory request budget (15 GiB)
	// allocated per node during a ~30-second spot preemption window at ~0.5 GB/s checkpoint throughput.
	DefaultSpotPreemptionNodeBudget = int64(15 * 1024 * 1024 * 1024)

	// PMJParentKeyIndexKey is the cache index mapping PodMigrationJobs to their
	// parent workload key ("<parentName>/<parentKind>" or "<podName>/Pod").
	// Registered at manager startup via controller.RegisterFieldIndexes.
	PMJParentKeyIndexKey = "pmj.podmigration.gke.io/parent-key"
)

// ResolveParentWorkload finds the parent owner details (ReplicaSet -> Deployment, Job, StatefulSet,
// or any custom operator controller=true owner).
func ResolveParentWorkload(ctx context.Context, c client.Reader, pod *corev1.Pod) (string, string, string, error) {
	var fallbackControllerOwner *metav1.OwnerReference
	for i := range pod.OwnerReferences {
		ref := &pod.OwnerReferences[i]
		if ref.Kind == "ReplicaSet" {
			rs := &appsv1.ReplicaSet{}
			err := c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, rs)
			if err == nil {
				for _, rsRef := range rs.OwnerReferences {
					if rsRef.Kind == "Deployment" {
						return rsRef.Name, "Deployment", string(rsRef.UID), nil
					}
				}
				return rs.Name, "ReplicaSet", string(rs.UID), nil
			} else {
				return "", "", "", err
			}
		} else if ref.Kind == "Job" || ref.Kind == "StatefulSet" {
			return ref.Name, ref.Kind, string(ref.UID), nil
		} else if ref.Controller != nil && *ref.Controller {
			fallbackControllerOwner = ref
		}
	}
	if fallbackControllerOwner != nil {
		return fallbackControllerOwner.Name, fallbackControllerOwner.Kind, string(fallbackControllerOwner.UID), nil
	}
	return "", "", "", nil
}

// IsBuiltInWorkloadKind reports whether kind is one of the built-in Kubernetes workload kinds
// natively understood by the controller (Deployment, ReplicaSet, StatefulSet, Job) or bare pod ("").
func IsBuiltInWorkloadKind(kind string) bool {
	switch kind {
	case "Deployment", "ReplicaSet", "StatefulSet", "Job", "":
		return true
	default:
		return false
	}
}

// ResolveOperatorReplicaKey computes the replica identity key for an operator-owned pod.
// It checks explicit replica annotations/labels, pod-template-hash when present,
// and per-kind tables (e.g. for RayCluster: ray.io/group plus ray.io/node-type).
func ResolveOperatorReplicaKey(pod *corev1.Pod, parentKind string) (string, bool) {
	if pod == nil {
		return "", false
	}
	if pod.Labels != nil {
		if k := pod.Labels[LabelReplicaKey]; k != "" {
			return k, true
		}
	}
	if pod.Annotations != nil {
		if k := pod.Annotations[AnnotationReplicaIdentityKey]; k != "" {
			return k, true
		}
	}
	if pod.Labels != nil {
		if hash := pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey]; hash != "" {
			return "pod-template-hash=" + hash, true
		}
	}
	switch parentKind {
	case "RayCluster":
		if pod.Labels == nil {
			break
		}
		group := pod.Labels["ray.io/group"]
		if group == "" {
			break
		}
		nodeType := pod.Labels["ray.io/node-type"]
		if nodeType != "" {
			return fmt.Sprintf("ray.io/group=%s,ray.io/node-type=%s", group, nodeType), true
		}
		return fmt.Sprintf("ray.io/group=%s", group), true
	}
	if pod.GenerateName == "" && pod.Name != "" {
		return "pod-name=" + pod.Name, true
	}
	return "", false
}

// JobReplicaIdentityKey extracts the replica identity key recorded on a PodMigrationJob.
func JobReplicaIdentityKey(job *pmv1alpha1.PodMigrationJob) string {
	if job == nil {
		return ""
	}
	if job.Annotations != nil {
		if k := job.Annotations[AnnotationReplicaIdentityKey]; k != "" {
			return k
		}
	}
	if job.Labels != nil {
		if hash := job.Labels[LabelPodTemplateHash]; hash != "" {
			return "pod-template-hash=" + hash
		}
	}
	return ""
}

// HasAmbiguousOperatorSibling checks if there is any other alive, non-terminating pod
// under the same parent that shares the same replica identity key.
func HasAmbiguousOperatorSibling(
	ctx context.Context,
	c client.Reader,
	namespace, parentName, parentKind, parentUID, replicaKey string,
	excludePodUIDs ...string,
) (bool, string, error) {
	if replicaKey == "" || parentName == "" || parentKind == "" {
		return false, "", nil
	}

	podList := &corev1.PodList{}
	if err := c.List(ctx, podList, client.InNamespace(namespace)); err != nil {
		return false, "", err
	}

	excludeMap := make(map[string]bool)
	for _, uid := range excludePodUIDs {
		if uid != "" {
			excludeMap[uid] = true
		}
	}

	for i := range podList.Items {
		p := &podList.Items[i]
		if excludeMap[string(p.UID)] || excludeMap[p.Name] {
			continue
		}
		if !isSourcePodAlive(p) {
			continue
		}

		// Check if p belongs to the same parent
		isChild := false
		for _, ref := range p.OwnerReferences {
			if ref.Kind == parentKind && ref.Name == parentName {
				if parentUID == "" || ref.UID == "" || string(ref.UID) == parentUID {
					isChild = true
					break
				}
			}
		}
		if !isChild {
			continue
		}

		siblingKey, ok := ResolveOperatorReplicaKey(p, parentKind)
		if ok && siblingKey == replicaKey {
			return true, p.Name, nil
		}
	}

	return false, "", nil
}

// HasActivePMJWithReplicaKey checks if there is already an active (unconsumed) PMJ
// for the given parent with the specified replica identity key.
func HasActivePMJWithReplicaKey(
	ctx context.Context,
	c client.Reader,
	namespace, parentName, parentKind, replicaKey, excludePMJName string,
) (bool, string, error) {
	if replicaKey == "" || parentName == "" || parentKind == "" {
		return false, "", nil
	}

	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := c.List(ctx, pmjList, client.InNamespace(namespace)); err != nil {
		return false, "", err
	}

	for i := range pmjList.Items {
		job := &pmjList.Items[i]
		if job.Name == excludePMJName {
			continue
		}
		if job.Status.Consumed {
			continue
		}
		phase := job.Status.Phase
		if phase == pmv1alpha1.PodMigrationJobPhaseFailed ||
			phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			continue
		}
		if job.Labels[LabelParentName] != parentName || job.Labels[LabelParentKind] != parentKind {
			continue
		}
		if JobReplicaIdentityKey(job) == replicaKey {
			return true, job.Name, nil
		}
	}

	return false, "", nil
}

// ShortUID returns the first 8 characters of a UID, or the full UID if shorter.
func ShortUID(uid string) string {
	if len(uid) <= 8 {
		return uid
	}
	return uid[:8]
}

// FormatPMJName returns the standardized PodMigrationJob name.
func FormatPMJName(podName, uid string) string {
	return fmt.Sprintf("pmj-%s-%s", podName, ShortUID(uid))
}

// FormatPSMTName returns the standardized PodSnapshotManualTrigger name.
func FormatPSMTName(podName, uid string) string {
	return fmt.Sprintf("psmt-%s-%s", podName, ShortUID(uid))
}

// FormatParentKey constructs the index key for a workload or bare pod parent.
// Workload-owned PMJs index by "<parentName>/<parentKind>".
// Bare-pod PMJs index by "<podName>/Pod".
func FormatParentKey(parentName, parentKind, podName string) string {
	if parentName != "" && parentKind != "" {
		return parentName + "/" + parentKind
	}
	if podName != "" {
		return podName + "/Pod"
	}
	return ""
}

// PMJParentKeyIndexValue extracts the parent key for a PodMigrationJob.
func PMJParentKeyIndexValue(obj client.Object) []string {
	job, ok := obj.(*pmv1alpha1.PodMigrationJob)
	if !ok || job == nil {
		return nil
	}
	parentName := job.Labels[LabelParentName]
	parentKind := job.Labels[LabelParentKind]
	if parentName != "" && parentKind != "" {
		return []string{parentName + "/" + parentKind}
	}
	if parentName == "" && job.Spec.PodRef.Name != "" {
		return []string{job.Spec.PodRef.Name + "/Pod"}
	}
	return nil
}

// PodAssignedPMJIndexValue extracts the assigned-pmj index value for a pod.
func PodAssignedPMJIndexValue(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil
	}
	if v := pod.Annotations[AnnotationAssignedPMJ]; v != "" {
		return []string{v}
	}
	return nil
}

func isPMJAssignedToPod(ctx context.Context, c client.Reader, namespace, pmjName string) (bool, error) {
	podList := &corev1.PodList{}
	err := c.List(ctx, podList, client.InNamespace(namespace), client.MatchingFields{PodAssignedPMJIndexKey: pmjName})
	if err != nil {
		return false, err
	}
	return len(podList.Items) > 0, nil
}

func filterCandidates(jobs []pmv1alpha1.PodMigrationJob, parentName, parentKind, parentUID, podName, podTemplateHash, jobCompletionIndex, replicaKey string) []pmv1alpha1.PodMigrationJob {
	var candidates []pmv1alpha1.PodMigrationJob
	for _, job := range jobs {
		if job.Status.Consumed {
			continue
		}

		if parentName == "" {
			if job.Spec.PodRef.Name != podName || job.Labels[LabelParentName] != "" {
				continue
			}
		} else {
			if job.Labels[LabelParentName] != parentName ||
				job.Labels[LabelParentKind] != parentKind {
				continue
			}

			if parentUID != "" && job.Labels[LabelParentUID] != "" && job.Labels[LabelParentUID] != parentUID {
				continue
			}

			if parentKind == "Deployment" && job.Labels[LabelPodTemplateHash] != podTemplateHash {
				continue
			}

			if parentKind == "Job" && jobCompletionIndex != "" && job.Labels[LabelJobCompletionIndex] != jobCompletionIndex {
				continue
			}

			if parentKind == "StatefulSet" && job.Spec.PodRef.Name != podName {
				continue
			}

			if !IsBuiltInWorkloadKind(parentKind) {
				if replicaKey == "" {
					continue
				}
				jobKey := JobReplicaIdentityKey(&job)
				if jobKey == "" || jobKey != replicaKey {
					continue
				}
			}
		}

		// A replacement exists because its source was evicted, which happens only
		// after the job's checkpoint. So a Pending job never owns a new pod: it can
		// wait there for the disruption budget for minutes (#42), and handing it a
		// sibling's replacement would leave that pod gated under a job that may
		// never checkpoint, while the evicted job never gets its replacement. A pod
		// created while only Pending jobs exist takes the scale-up path.
		phase := job.Status.Phase
		if phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
			phase == pmv1alpha1.PodMigrationJobPhaseEvicting ||
			phase == pmv1alpha1.PodMigrationJobPhaseRestoring {
			candidates = append(candidates, job)
		}
	}

	// For operator-owned workloads, check ambiguity: if multiple candidates share the same replicaKey,
	// refuse rather than guess.
	if !IsBuiltInWorkloadKind(parentKind) && replicaKey != "" {
		matchingCount := 0
		for _, cand := range candidates {
			if JobReplicaIdentityKey(&cand) == replicaKey {
				matchingCount++
			}
		}
		if matchingCount > 1 {
			return nil
		}
	}

	// Prefer the jobs whose source is being evicted; Snapshotting stays a
	// candidate only for a cache that lags the job's move to Evicting, and
	// FindUnassignedActivePMJ skips it while its source is alive.
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidatePhaseRank(candidates[i].Status.Phase) < candidatePhaseRank(candidates[j].Status.Phase)
	})
	return candidates
}

// candidatePhaseRank orders replacement candidates, closest to the source's
// eviction first.
func candidatePhaseRank(phase pmv1alpha1.PodMigrationJobPhase) int {
	switch phase {
	case pmv1alpha1.PodMigrationJobPhaseEvicting:
		return 0
	case pmv1alpha1.PodMigrationJobPhaseRestoring:
		return 1
	default:
		return 2
	}
}

// requiresSourceGone reports whether a job in this phase can own a new pod only
// once its source is terminating, in a terminal phase (Failed or Succeeded), or gone.
// While the source is alive and running, a new pod under the same parent is a
// scale-up or another replica's replacement: an Evicting job waits there while
// its eviction is refused, and a Snapshotting job while its checkpoint runs (#168).
// A Restoring job's source is gone.
func requiresSourceGone(phase pmv1alpha1.PodMigrationJobPhase) bool {
	return phase == pmv1alpha1.PodMigrationJobPhaseEvicting || phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting
}

// isSourcePodAlive reports whether pod is an active source workload pod. A pod
// that is terminating (has DeletionTimestamp) or in a terminal phase (Failed or
// Succeeded, such as a batch/v1 Job pod terminated by checkpoint) is treated as
// gone.
func isSourcePodAlive(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	return pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded
}

// FindUnassignedActivePMJ searches for an active PMJ under the parent that hasn't been assigned to a pod yet.
// When indexed is true, it uses cache field indexes (PMJParentKeyIndexKey and PodAssignedPMJIndexKey) to perform
// O(result) lookups against an index-backed client (e.g. manager cache client).
// When indexed is false (live APIReader fallback), it performs unindexed namespace lists and in-memory filtering
// without sending unsupported field selectors to the real apiserver.
// If replicaKey is provided, it is used to match operator-owned pods.
func FindUnassignedActivePMJ(
	ctx context.Context,
	c client.Reader,
	namespace, podName, parentName, parentKind, parentUID, podTemplateHash, jobCompletionIndex string,
	indexed bool,
	replicaKey ...string,
) (string, error) {
	var opReplicaKey string
	if len(replicaKey) > 0 {
		opReplicaKey = replicaKey[0]
	}

	if indexed {
		parentKey := FormatParentKey(parentName, parentKind, podName)
		if parentKey == "" {
			return "", nil
		}
		jobList := &pmv1alpha1.PodMigrationJobList{}
		if err := c.List(ctx, jobList, client.InNamespace(namespace), client.MatchingFields{PMJParentKeyIndexKey: parentKey}); err != nil {
			return "", err
		}

		candidates := filterCandidates(jobList.Items, parentName, parentKind, parentUID, podName, podTemplateHash, jobCompletionIndex, opReplicaKey)
		if len(candidates) == 0 && !IsBuiltInWorkloadKind(parentKind) && strings.HasPrefix(opReplicaKey, "pod-name=") {
			// In-flight upgrade compatibility: PMJs created by a pre-upgrade controller for stable-name
			// operator pods were indexed as bare pods under "<podName>/Pod" with empty parent labels.
			bareKey := podName + "/Pod"
			bareJobList := &pmv1alpha1.PodMigrationJobList{}
			if err := c.List(ctx, bareJobList, client.InNamespace(namespace), client.MatchingFields{PMJParentKeyIndexKey: bareKey}); err == nil {
				candidates = filterCandidates(bareJobList.Items, "", "", "", podName, "", "", "")
			}
		}
		if len(candidates) == 0 {
			return "", nil
		}

		for _, job := range candidates {
			// For operator-owned pods, ensure no other active sibling pod shares this replicaKey
			if !IsBuiltInWorkloadKind(parentKind) && opReplicaKey != "" {
				ambiguous, _, err := HasAmbiguousOperatorSibling(ctx, c, namespace, parentName, parentKind, parentUID, opReplicaKey, job.Spec.TargetPodUID, job.Spec.PodRef.Name, podName)
				if err != nil {
					return "", err
				}
				if ambiguous {
					return "", nil
				}
			}

			// Narrowing for scale-up race: don't match Evicting- or Snapshotting-phase PMJs whose origin
			// pod still exists and is running. If the origin pod is still alive, any newly arriving candidate
			// pod is a concurrent scale-up (see requiresSourceGone).
			// A pod in a terminal phase (Failed or Succeeded) is no longer running (e.g. a batch/v1 Job pod
			// whose container stopped on checkpoint and was marked Failed by PodStatusMutator).
			if requiresSourceGone(job.Status.Phase) {
				targetName := job.Spec.PodRef.Name
				if targetName != "" {
					originPod := &corev1.Pod{}
					err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: targetName}, originPod)
					if err == nil && isSourcePodAlive(originPod) {
						if job.Spec.TargetPodUID == "" || string(originPod.UID) == job.Spec.TargetPodUID {
							continue
						}
					} else if err != nil && !apierrors.IsNotFound(err) {
						// Skip candidate on transient read error rather than rejecting admission.
						continue
					}
				}
			}

			assigned, err := isPMJAssignedToPod(ctx, c, namespace, job.Name)
			if err != nil {
				return "", err
			}
			if !assigned {
				return job.Name, nil
			}
		}

		return "", nil
	}

	// Unindexed live reader path (fallback): full namespace list without field selectors.
	jobList := &pmv1alpha1.PodMigrationJobList{}
	if err := c.List(ctx, jobList, client.InNamespace(namespace)); err != nil {
		return "", err
	}

	candidates := filterCandidates(jobList.Items, parentName, parentKind, parentUID, podName, podTemplateHash, jobCompletionIndex, opReplicaKey)
	if len(candidates) == 0 && !IsBuiltInWorkloadKind(parentKind) && strings.HasPrefix(opReplicaKey, "pod-name=") {
		// In-flight upgrade compatibility: PMJs created by a pre-upgrade controller for stable-name
		// operator pods were recorded as bare pods with empty parent labels.
		candidates = filterCandidates(jobList.Items, "", "", "", podName, "", "", "")
	}
	if len(candidates) == 0 {
		return "", nil
	}

	assignedPMJs := make(map[string]bool)
	existingPodUIDs := make(map[string]bool)
	existingPodNames := make(map[string]bool)
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{"pod-migration.gke.io/enabled": "true"}); err != nil {
		return "", err
	}

	for _, p := range podList.Items {
		if isSourcePodAlive(&p) {
			existingPodUIDs[string(p.UID)] = true
			existingPodNames[p.Name] = true
		}
		if p.Annotations != nil {
			if pmjName, ok := p.Annotations[AnnotationAssignedPMJ]; ok {
				assignedPMJs[pmjName] = true
			}
		}
	}

	for _, job := range candidates {
		if !IsBuiltInWorkloadKind(parentKind) && opReplicaKey != "" {
			ambiguous, _, err := HasAmbiguousOperatorSibling(ctx, c, namespace, parentName, parentKind, parentUID, opReplicaKey, job.Spec.TargetPodUID, job.Spec.PodRef.Name, podName)
			if err != nil {
				return "", err
			}
			if ambiguous {
				return "", nil
			}
		}

		if requiresSourceGone(job.Status.Phase) {
			originExists := false
			if job.Spec.TargetPodUID != "" {
				originExists = existingPodUIDs[job.Spec.TargetPodUID]
			} else if job.Spec.PodRef.Name != "" {
				originExists = existingPodNames[job.Spec.PodRef.Name]
			}
			if originExists {
				continue
			}
		}

		if !assignedPMJs[job.Name] {
			return job.Name, nil
		}
	}

	return "", nil
}

// ResolveCollision deterministically resolves PMJ assignment races.
// Returns: (correctedPMJ, changed, error)
func ResolveCollision(ctx context.Context, c client.Client, pod *corev1.Pod, assignedPMJ, parentName, parentKind, parentUID string) (string, bool, error) {
	// Fetch the assigned PMJ to check targetPodUID
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: assignedPMJ}, job); err != nil {
		if apierrors.IsNotFound(err) {
			// NotFound is ambiguous from the cache (deleted vs not yet synced),
			// so no collision verdict is possible: report "no change".  The
			// caller MUST then resolve the ambiguity itself with a direct API
			// server read, and on confirmed deletion release the pod with the
			// cold-start bypass (assigned-pmj removed, ps-name set to "").
			return assignedPMJ, false, nil
		}
		return "", false, err
	}

	// Contenders are exactly the pods indexed under this PMJ name; a
	// full-namespace List here runs on every reconcile of every assigned pod
	// and does not scale.  Callers must have PodAssignedPMJIndex registered.
	podList := &corev1.PodList{}
	err := c.List(ctx, podList,
		client.InNamespace(pod.Namespace),
		client.MatchingFields{PodAssignedPMJIndexKey: assignedPMJ})
	if err != nil {
		return "", false, err
	}

	var contenders []*corev1.Pod
	for i := range podList.Items {
		p := &podList.Items[i]
		// Exclude the origin pod being migrated
		if string(p.UID) == job.Spec.TargetPodUID {
			continue
		}
		contenders = append(contenders, p)
	}

	if len(contenders) <= 1 {
		return assignedPMJ, false, nil // No collision
	}

	// Sort contenders deterministically by creation timestamp, then alphabetically by name
	sort.Slice(contenders, func(i, j int) bool {
		if contenders[i].CreationTimestamp.Equal(&contenders[j].CreationTimestamp) {
			return contenders[i].Name < contenders[j].Name
		}
		return contenders[i].CreationTimestamp.Before(&contenders[j].CreationTimestamp)
	})

	winner := contenders[0]
	if winner.UID == pod.UID {
		return assignedPMJ, false, nil // We are the winner, no change
	}

	var podTemplateHash string
	var jobCompletionIndex string
	var replicaKey string
	if pod.Labels != nil {
		podTemplateHash = pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey]
		jobCompletionIndex = pod.Labels[LabelJobCompletionIndex]
	}
	if !IsBuiltInWorkloadKind(parentKind) {
		replicaKey, _ = ResolveOperatorReplicaKey(pod, parentKind)
	}

	// We are the loser! Try to find an alternative active PMJ
	altPMJ, err := FindUnassignedActivePMJ(ctx, c, pod.Namespace, pod.Name, parentName, parentKind, parentUID, podTemplateHash, jobCompletionIndex, true, replicaKey)
	if err != nil {
		return "", false, err
	}

	if altPMJ != "" {
		return altPMJ, true, nil // Re-assigned to alternative PMJ
	}

	return "", true, nil // No alternative, we are a scale-up pod (clear assignment)
}
