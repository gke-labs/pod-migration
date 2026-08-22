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
	LabelParentName         = "pod-migration.gke.io/parent-name"
	LabelParentKind         = "pod-migration.gke.io/parent-kind"
	LabelPodTemplateHash    = "pod-migration.gke.io/pod-template-hash"
	LabelJobCompletionIndex = batchv1.JobCompletionIndexAnnotation
	LabelOriginPodName      = "pod-migration.gke.io/origin-pod-name"
	AnnotationAssignedPMJ   = "pod-migration.gke.io/assigned-pmj"
)

// ResolveParentWorkload finds the parent owner details (ReplicaSet -> Deployment, Job, or StatefulSet).
func ResolveParentWorkload(ctx context.Context, c client.Client, pod *corev1.Pod) (string, string, error) {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "ReplicaSet" {
			rs := &appsv1.ReplicaSet{}
			err := c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: ref.Name}, rs)
			if err == nil {
				for _, rsRef := range rs.OwnerReferences {
					if rsRef.Kind == "Deployment" {
						return rsRef.Name, "Deployment", nil
					}
				}
			} else {
				return "", "", err
			}
		} else if ref.Kind == "Job" || ref.Kind == "StatefulSet" {
			return ref.Name, ref.Kind, nil
		}
	}
	return "", "", nil
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
func FindUnassignedActivePMJ(ctx context.Context, c client.Client, namespace, podName, parentName, parentKind, podTemplateHash, jobCompletionIndex string) (string, error) {
	// Scan pods to find which PMJs are already assigned
	assignedPMJs := make(map[string]bool)
	podList := &corev1.PodList{}
	err := c.List(ctx, podList, client.InNamespace(namespace))
	if err != nil {
		return "", err
	}

	for _, p := range podList.Items {
		if p.Annotations != nil {
			if pmjName, ok := p.Annotations[AnnotationAssignedPMJ]; ok {
				assignedPMJs[pmjName] = true
			}
		}
	}

	jobList := &pmv1alpha1.PodMigrationJobList{}
	err = c.List(ctx, jobList, client.InNamespace(namespace))
	if err != nil {
		return "", err
	}

	// Pick the first active PMJ that is not yet assigned
	for _, job := range jobList.Items {
		if parentName == "" {
			// Bare Pod: match by origin pod name and absence of parent label
			if job.Spec.PodRef.Name != podName || job.Labels[LabelParentName] != "" {
				continue
			}
		} else {
			if job.Labels[LabelParentName] != parentName ||
				job.Labels[LabelParentKind] != parentKind {
				continue
			}

			// For Deployments, enforce pod-template-hash revision match
			if parentKind == "Deployment" && job.Labels[LabelPodTemplateHash] != podTemplateHash {
				continue
			}

			// For Indexed Jobs, enforce job-completion-index ordinal match
			if parentKind == "Job" && jobCompletionIndex != "" && job.Labels[LabelJobCompletionIndex] != jobCompletionIndex {
				continue
			}

			// For StatefulSets, strictly match by exact name
			if parentKind == "StatefulSet" && job.Spec.PodRef.Name != podName {
				continue
			}
		}

		phase := job.Status.Phase
		if phase == pmv1alpha1.PodMigrationJobPhasePending ||
			phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
			phase == pmv1alpha1.PodMigrationJobPhaseEvicting ||
			phase == pmv1alpha1.PodMigrationJobPhaseSucceeded {
			if !assignedPMJs[job.Name] {
				return job.Name, nil
			}
		}
	}
	return "", nil
}

// ResolveCollision deterministically resolves PMJ assignment races.
// Returns: (correctedPMJ, changed, error)
func ResolveCollision(ctx context.Context, c client.Client, pod *corev1.Pod, assignedPMJ, parentName, parentKind string) (string, bool, error) {
	// Fetch the assigned PMJ to check targetPodUID
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: assignedPMJ}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return "", true, nil // PMJ was deleted out-of-band, clear assignment
		}
		return "", false, err
	}

	podList := &corev1.PodList{}
	err := c.List(ctx, podList, client.InNamespace(pod.Namespace))
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
		if p.Annotations != nil && p.Annotations[AnnotationAssignedPMJ] == assignedPMJ {
			contenders = append(contenders, p)
		}
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
	altPMJ, err := FindUnassignedActivePMJ(ctx, c, pod.Namespace, pod.Name, parentName, parentKind, podTemplateHash, jobCompletionIndex)
	if err != nil {
		return "", false, err
	}

	if altPMJ != "" {
		return altPMJ, true, nil // Re-assigned to alternative PMJ
	}

	return "", true, nil // No alternative, we are a scale-up pod (clear assignment)
}
