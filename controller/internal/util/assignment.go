package util

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	AnnotationMismatchSince      = "pod-migration.gke.io/mismatch-since"
	AnnotationPDBEvictionTimeout = "pod-migration.gke.io/pdb-eviction-timeout"

	// PodAssignedPMJIndexKey is the cache index mapping pods to the PMJ named
	// in their assigned-pmj annotation.  Registered at manager startup via
	// controller.RegisterFieldIndexes.
	PodAssignedPMJIndexKey = "pod.podmigration.gke.io/assigned-pmj"

	// PMJParentKeyIndexKey is the cache index mapping PodMigrationJobs to their
	// parent workload key ("<parentName>/<parentKind>" or "<podName>/Pod").
	// Registered at manager startup via controller.RegisterFieldIndexes.
	PMJParentKeyIndexKey = "pmj.podmigration.gke.io/parent-key"
)

// ResolveParentWorkload finds the parent owner details (ReplicaSet -> Deployment, Job, or StatefulSet).
func ResolveParentWorkload(ctx context.Context, c client.Reader, pod *corev1.Pod) (string, string, string, error) {
	for _, ref := range pod.OwnerReferences {
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
		}
	}
	return "", "", "", nil
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

func filterCandidates(jobs []pmv1alpha1.PodMigrationJob, parentName, parentKind, parentUID, podName, podTemplateHash, jobCompletionIndex string) []pmv1alpha1.PodMigrationJob {
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
		}

		phase := job.Status.Phase
		if phase == pmv1alpha1.PodMigrationJobPhasePending ||
			phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
			phase == pmv1alpha1.PodMigrationJobPhaseEvicting ||
			phase == pmv1alpha1.PodMigrationJobPhaseRestoring {
			candidates = append(candidates, job)
		}
	}
	return candidates
}

// FindUnassignedActivePMJ searches for an active PMJ under the parent that hasn't been assigned to a pod yet.
// When indexed is true, it uses cache field indexes (PMJParentKeyIndexKey and PodAssignedPMJIndexKey) to perform
// O(result) lookups against an index-backed client (e.g. manager cache client).
// When indexed is false (live APIReader fallback), it performs unindexed namespace lists and in-memory filtering
// without sending unsupported field selectors to the real apiserver.
func FindUnassignedActivePMJ(ctx context.Context, c client.Reader, namespace, podName, parentName, parentKind, parentUID, podTemplateHash, jobCompletionIndex string, indexed bool) (string, error) {
	if indexed {
		parentKey := FormatParentKey(parentName, parentKind, podName)
		if parentKey == "" {
			return "", nil
		}
		jobList := &pmv1alpha1.PodMigrationJobList{}
		if err := c.List(ctx, jobList, client.InNamespace(namespace), client.MatchingFields{PMJParentKeyIndexKey: parentKey}); err != nil {
			return "", err
		}

		candidates := filterCandidates(jobList.Items, parentName, parentKind, parentUID, podName, podTemplateHash, jobCompletionIndex)
		if len(candidates) == 0 {
			return "", nil
		}

		for _, job := range candidates {
			// Narrowing for scale-up race: don't match Evicting-phase PMJs whose origin pod still exists.
			// If the origin pod is still alive, any newly arriving candidate pod is a concurrent scale-up.
			if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
				targetName := job.Spec.PodRef.Name
				if targetName != "" {
					originPod := &corev1.Pod{}
					err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: targetName}, originPod)
					if err == nil && originPod.DeletionTimestamp == nil {
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

	candidates := filterCandidates(jobList.Items, parentName, parentKind, parentUID, podName, podTemplateHash, jobCompletionIndex)
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
		if p.DeletionTimestamp == nil {
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
		if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
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
	if pod.Labels != nil {
		podTemplateHash = pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey]
		jobCompletionIndex = pod.Labels[LabelJobCompletionIndex]
	}

	// We are the loser! Try to find an alternative active PMJ
	altPMJ, err := FindUnassignedActivePMJ(ctx, c, pod.Namespace, pod.Name, parentName, parentKind, parentUID, podTemplateHash, jobCompletionIndex, true)
	if err != nil {
		return "", false, err
	}

	if altPMJ != "" {
		return altPMJ, true, nil // Re-assigned to alternative PMJ
	}

	return "", true, nil // No alternative, we are a scale-up pod (clear assignment)
}
