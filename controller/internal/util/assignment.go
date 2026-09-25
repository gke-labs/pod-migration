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
	LabelJobCompletionIndex      = batchv1.JobCompletionIndexAnnotation
	LabelOriginPodName           = "pod-migration.gke.io/origin-pod-name"
	AnnotationAssignedPMJ        = "pod-migration.gke.io/assigned-pmj"
	AnnotationMismatchSince      = "pod-migration.gke.io/mismatch-since"
	AnnotationPDBEvictionTimeout = "pod-migration.gke.io/pdb-eviction-timeout"
	AnnotationPodSnapshotPolicy  = "pod-migration.gke.io/pod-snapshot-policy"

	// PodAssignedPMJIndexKey is the cache index mapping pods to the PMJ named
	// in their assigned-pmj annotation.  Registered at manager startup via
	// controller.RegisterFieldIndexes.
	PodAssignedPMJIndexKey = "pod.podmigration.gke.io/assigned-pmj"
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

// FindUnassignedActivePMJ searches for an active PMJ under the parent that hasn't been assigned to a pod yet.
func FindUnassignedActivePMJ(ctx context.Context, c client.Reader, namespace, podName, parentName, parentKind, parentUID, podTemplateHash, jobCompletionIndex string) (string, error) {
	jobList := &pmv1alpha1.PodMigrationJobList{}
	err := c.List(ctx, jobList, client.InNamespace(namespace))
	if err != nil {
		return "", err
	}

	var candidates []pmv1alpha1.PodMigrationJob
	for _, job := range jobList.Items {
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

	if len(candidates) == 0 {
		return "", nil // Overwhelmingly common path: no matching active PMJ
	}

	// Scan opted-in pods to find which PMJs are already assigned and track existing pod UIDs and names.
	// We use typed PodList with MatchingLabels so the cached client performs an in-memory
	// filter on the already-running pod informer rather than lazily starting a 2nd metadata watch.
	assignedPMJs := make(map[string]bool)
	existingPodUIDs := make(map[string]bool)
	existingPodNames := make(map[string]bool)
	podList := &corev1.PodList{}
	err = c.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{"pod-migration.gke.io/enabled": "true"})
	if err != nil {
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
		// Narrowing for scale-up race: don't match Evicting-phase PMJs whose origin pod still exists.
		// If the origin pod is still alive, any newly arriving candidate pod is a concurrent scale-up.
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
	altPMJ, err := FindUnassignedActivePMJ(ctx, c, pod.Namespace, pod.Name, parentName, parentKind, parentUID, podTemplateHash, jobCompletionIndex)
	if err != nil {
		return "", false, err
	}

	if altPMJ != "" {
		return altPMJ, true, nil // Re-assigned to alternative PMJ
	}

	return "", true, nil // No alternative, we are a scale-up pod (clear assignment)
}
