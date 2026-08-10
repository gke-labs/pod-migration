package util

import (
	"context"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
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

// FindUnassignedActivePMJ searches for an active PMJ under the parent that hasn't been assigned to a pod yet.
func FindUnassignedActivePMJ(ctx context.Context, c client.Client, namespace, parentName, parentKind string) (string, error) {
	if parentName == "" {
		return "", nil
	}

	jobList := &pmv1alpha1.PodMigrationJobList{}
	err := c.List(ctx, jobList, client.InNamespace(namespace))
	if err != nil {
		return "", err
	}

	// Scan pods to find which PMJs are already assigned
	assignedPMJs := make(map[string]bool)
	podList := &corev1.PodList{}
	err = c.List(ctx, podList, client.InNamespace(namespace))
	if err != nil {
		return "", err
	}

	for _, p := range podList.Items {
		if p.Annotations != nil {
			if pmjName, ok := p.Annotations["pod-migration.gke.io/assigned-pmj"]; ok {
				assignedPMJs[pmjName] = true
			}
		}
	}

	// Pick the first active PMJ that is not yet assigned
	for _, job := range jobList.Items {
		if job.Labels["pod-migration.gke.io/parent-name"] == parentName &&
			job.Labels["pod-migration.gke.io/parent-kind"] == parentKind {
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
	}
	return "", nil
}

// ResolveCollision deterministically resolves PMJ assignment races.
// Returns: (correctedPMJ, changed, error)
func ResolveCollision(ctx context.Context, c client.Client, pod *corev1.Pod, assignedPMJ, parentName, parentKind string) (string, bool, error) {
	podList := &corev1.PodList{}
	err := c.List(ctx, podList, client.InNamespace(pod.Namespace))
	if err != nil {
		return "", false, err
	}

	var contenders []*corev1.Pod
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Annotations != nil && p.Annotations["pod-migration.gke.io/assigned-pmj"] == assignedPMJ {
			// Only consider contenders that are still scheduling (have the gate)
			gated := false
			for _, g := range p.Spec.SchedulingGates {
				if g.Name == "gke.io/pod-migration-gate" {
					gated = true
					break
				}
			}
			if gated {
				contenders = append(contenders, p)
			}
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

	// We are the loser! Try to find an alternative active PMJ
	altPMJ, err := FindUnassignedActivePMJ(ctx, c, pod.Namespace, parentName, parentKind)
	if err != nil {
		return "", false, err
	}

	if altPMJ != "" {
		return altPMJ, true, nil // Re-assigned to alternative PMJ
	}

	return "", true, nil // No alternative, we are a scale-up pod (clear assignment)
}
