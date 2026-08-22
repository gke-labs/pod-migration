package snapshot

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

type GKEProvider struct {
	client client.Client
	scheme *runtime.Scheme
}

func NewGKEProvider(c client.Client, s *runtime.Scheme) *GKEProvider {
	return &GKEProvider{client: c, scheme: s}
}

func (p *GKEProvider) EnsureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (ctrl.Result, error) {
	triggerName := util.FormatPSMTName(podName, job.Spec.TargetPodUID)
	logger := log.FromContext(ctx).WithValues("trigger", triggerName)

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	trigger.SetName(triggerName)
	trigger.SetNamespace(job.Namespace)
	trigger.Object["spec"] = map[string]interface{}{
		"targetPod": podName,
	}

	// Set owner reference to allow cascade deletion and ownership verification
	if err := controllerutil.SetControllerReference(job, trigger, p.scheme); err != nil {
		logger.Error(err, "Failed to set owner reference on trigger")
		return ctrl.Result{}, err
	}

	logger.Info("Creating PodSnapshotManualTrigger")
	err := p.client.Create(ctx, trigger)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Fetch existing trigger to verify ownership
			existingTrigger := &unstructured.Unstructured{}
			existingTrigger.SetGroupVersionKind(trigger.GroupVersionKind())
			getErr := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: triggerName}, existingTrigger)
			if getErr == nil {
				isOwned := false
				for _, ref := range existingTrigger.GetOwnerReferences() {
					if ref.UID == job.UID {
						isOwned = true
						break
					}
				}
				if isOwned {
					logger.Info("Trigger already exists and is owned by this job, proceeding")
					return ctrl.Result{}, nil
				} else {
					logger.Info("Stale trigger found (owned by another job), deleting it first")
					_ = p.client.Delete(ctx, existingTrigger)
					return ctrl.Result{Requeue: true}, nil
				}
			} else {
				logger.Error(getErr, "Failed to fetch existing trigger on AlreadyExists")
				return ctrl.Result{}, getErr
			}
		} else {
			logger.Error(err, "Failed to create PodSnapshotManualTrigger")
			return ctrl.Result{}, err
		}
	}

	logger.Info("Successfully created PodSnapshotManualTrigger")
	return ctrl.Result{}, nil
}

func (p *GKEProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*Status, error) {
	triggerName := util.FormatPSMTName(podName, job.Spec.TargetPodUID)
	logger := log.FromContext(ctx).WithValues("trigger", triggerName)

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	err := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: triggerName}, trigger)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Waiting for PodSnapshotManualTrigger to be created...", "trigger", triggerName)
			return &Status{
				Phase:   PhaseInProgress,
				Reason:  "Snapshotting",
				Message: "Waiting for GKE PodSnapshotManualTrigger to be created",
			}, nil
		}
		logger.Error(err, "Failed to get PodSnapshotManualTrigger")
		return nil, err
	}

	// 1. Fast-fail if GKE Snapshot Trigger (PSMT) reported a terminal failure
	psmtStatus, _ := trigger.Object["status"].(map[string]interface{})
	if psmtConditions, ok := psmtStatus["conditions"].([]interface{}); ok {
		for _, c := range psmtConditions {
			if cond, ok := c.(map[string]interface{}); ok {
				if cond["type"] == "Triggered" && cond["status"] == "False" && cond["reason"] == "Failed" {
					errMsg, _ := cond["message"].(string)
					logger.Info("PSMT reported terminal failure", "trigger", triggerName, "reason", errMsg)
					return &Status{
						Phase:   PhaseFailed,
						Reason:  "SnapshotTriggerFailed",
						Message: fmt.Sprintf("GKE PodSnapshotManualTrigger failed: %s", errMsg),
					}, nil
				}
			}
		}
	}

	snapshotName, found, err := unstructured.NestedString(trigger.Object, "status", "snapshotCreated", "name")
	if err != nil {
		logger.Error(err, "Failed to read snapshotCreated.name from trigger status")
		return nil, err
	}
	if !found || snapshotName == "" {
		logger.Info("Waiting for snapshot name to be populated in trigger status...", "trigger", triggerName)
		return &Status{
			Phase:   PhaseInProgress,
			Reason:  "Snapshotting",
			Message: "Waiting for GKE to populate snapshot name in trigger status",
		}, nil
	}

	targetSnapshot := &unstructured.Unstructured{}
	targetSnapshot.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshot",
	})
	err = p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: snapshotName}, targetSnapshot)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Waiting for PodSnapshot to be created...", "snapshot", snapshotName)
			return &Status{
				Phase:   PhaseInProgress,
				Reason:  "Snapshotting",
				Message: fmt.Sprintf("Waiting for GKE PodSnapshot object %q to be created", snapshotName),
			}, nil
		}
		logger.Error(err, "Failed to get PodSnapshot")
		return nil, err
	}

	snapStatus, ok := targetSnapshot.Object["status"].(map[string]interface{})
	if !ok {
		logger.Info("Snapshot status subresource not found, waiting...")
		return &Status{
			Phase:   PhaseInProgress,
			Reason:  "Snapshotting",
			Message: fmt.Sprintf("Waiting for GKE PodSnapshot %q status to be populated", snapshotName),
		}, nil
	}

	conditions, _ := snapStatus["conditions"].([]interface{})
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if ok {
			if cond["type"] == "Ready" && cond["status"] == "True" {
				return &Status{
					Phase:       PhaseReady,
					SnapshotRef: snapshotName,
				}, nil
			}
			if cond["type"] == "Checkpoint" && cond["status"] == "True" && cond["reason"] == "Succeeded" {
				return &Status{
					Phase:       PhaseReady,
					SnapshotRef: snapshotName,
				}, nil
			}
			// 2. Fast-fail if PodSnapshot reported terminal failure in Checkpoint, StorageReplicated, or Ready
			cType, _ := cond["type"].(string)
			cStatus, _ := cond["status"].(string)
			cReason, _ := cond["reason"].(string)
			cMsg, _ := cond["message"].(string)
			if cStatus == "False" && cReason == "Failed" && (cType == "Checkpoint" || cType == "StorageReplicated" || cType == "Ready") {
				logger.Info("PodSnapshot reported terminal failure", "snapshot", snapshotName, "type", cType, "reason", cMsg)
				return &Status{
					Phase:   PhaseFailed,
					Reason:  "SnapshotFailed",
					Message: fmt.Sprintf("GKE PodSnapshot %s failed: %s", cType, cMsg),
				}, nil
			}
		}
	}

	logger.Info("Snapshot is not ready yet, waiting...")
	return &Status{
		Phase:   PhaseInProgress,
		Reason:  "Snapshotting",
		Message: fmt.Sprintf("Waiting for GKE PodSnapshot %q checkpoint to complete", snapshotName),
	}, nil
}

func (p *GKEProvider) Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error {
	triggerName := util.FormatPSMTName(podName, job.Spec.TargetPodUID)
	logger := log.FromContext(ctx).WithValues("trigger", triggerName)

	logger.Info("Deleting manual trigger", "trigger", triggerName)
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	trigger.SetName(triggerName)
	trigger.SetNamespace(job.Namespace)

	err := p.client.Delete(ctx, trigger)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to delete trigger")
		return err
	}
	logger.Info("Successfully deleted trigger")
	return nil
}
