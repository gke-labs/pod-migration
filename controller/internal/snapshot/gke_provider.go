package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

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

	// Fetch existing trigger first to verify ownership and avoid write amplification on 1s polling loops
	existingTrigger := &unstructured.Unstructured{}
	existingTrigger.SetGroupVersionKind(trigger.GroupVersionKind())
	err := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: triggerName}, existingTrigger)
	if err == nil {
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
		}
		logger.Info("Stale trigger found (owned by another job), deleting it first")
		_ = p.client.Delete(ctx, existingTrigger)
		return ctrl.Result{Requeue: true}, nil
	} else if !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to get PodSnapshotManualTrigger")
		return ctrl.Result{}, err
	}

	logger.Info("Creating PodSnapshotManualTrigger")
	err = p.client.Create(ctx, trigger)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			logger.Info("Trigger already exists after create race, proceeding")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to create PodSnapshotManualTrigger")
		return ctrl.Result{}, err
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
				cType, _ := cond["type"].(string)
				cStatus, _ := cond["status"].(string)
				cReason, _ := cond["reason"].(string)
				if cType == "Triggered" && cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
					errMsg, _ := cond["message"].(string)
					logger.Info("PSMT reported terminal failure", "trigger", triggerName, "reason", cReason, "message", errMsg)
					return &Status{
						Phase:   PhaseFailed,
						Reason:  "SnapshotTriggerFailed",
						Message: fmt.Sprintf("GKE PodSnapshotManualTrigger failed (%s): %s", cReason, errMsg),
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
				Phase:       PhaseInProgress,
				SnapshotRef: snapshotName,
				Reason:      "Snapshotting",
				Message:     fmt.Sprintf("Waiting for GKE PodSnapshot object %q to be created", snapshotName),
			}, nil
		}
		logger.Error(err, "Failed to get PodSnapshot")
		return nil, err
	}

	snapStatus, ok := targetSnapshot.Object["status"].(map[string]interface{})
	if !ok {
		logger.Info("Snapshot status subresource not found, waiting...")
		return &Status{
			Phase:       PhaseInProgress,
			SnapshotRef: snapshotName,
			Reason:      "Snapshotting",
			Message:     fmt.Sprintf("Waiting for GKE PodSnapshot %q status to be populated", snapshotName),
		}, nil
	}

	conditions, _ := snapStatus["conditions"].([]interface{})
	var lastProgress time.Time
	for _, c := range conditions {
		if cond, ok := c.(map[string]interface{}); ok {
			if lttStr, ok := cond["lastTransitionTime"].(string); ok {
				if t, err := time.Parse(time.RFC3339, lttStr); err == nil && t.After(lastProgress) {
					lastProgress = t
				}
			}
		}
	}
	if lastProgress.IsZero() {
		lastProgress = targetSnapshot.GetCreationTimestamp().Time
	}

	// Pass 1: Scan all conditions for terminal failures
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if ok {
			cType, _ := cond["type"].(string)
			cStatus, _ := cond["status"].(string)
			cReason, _ := cond["reason"].(string)
			cMsg, _ := cond["message"].(string)
			if cStatus == "False" && isTerminalSnapshotFailureReason(cReason) && (cType == "Checkpoint" || cType == "StorageReplicated" || cType == "Ready") {
				logger.Info("PodSnapshot reported terminal failure", "snapshot", snapshotName, "type", cType, "reason", cReason, "message", cMsg)
				return &Status{
					Phase:       PhaseFailed,
					SnapshotRef: snapshotName,
					Reason:      "SnapshotFailed",
					Message:     fmt.Sprintf("GKE PodSnapshot %s failed (%s): %s", cType, cReason, cMsg),
				}, nil
			}
		}
	}

	// Check top-level phase if present on PodSnapshot
	if topPhase, ok := snapStatus["phase"].(string); ok {
		if isTerminalSnapshotFailureReason(topPhase) {
			logger.Info("PodSnapshot reported terminal failure via phase", "snapshot", snapshotName, "phase", topPhase)
			return &Status{
				Phase:       PhaseFailed,
				SnapshotRef: snapshotName,
				Reason:      "SnapshotFailed",
				Message:     fmt.Sprintf("GKE PodSnapshot failed with phase=%s", topPhase),
			}, nil
		}
	}

	// Pass 2: Check for readiness
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if ok {
			cType, _ := cond["type"].(string)
			cStatus, _ := cond["status"].(string)
			cReason, _ := cond["reason"].(string)
			if cType == "Ready" && cStatus == "True" {
				return &Status{
					Phase:       PhaseReady,
					SnapshotRef: snapshotName,
				}, nil
			}
			if cType == "Checkpoint" && cStatus == "True" && cReason == "Succeeded" {
				return &Status{
					Phase:       PhaseReady,
					SnapshotRef: snapshotName,
				}, nil
			}
		}
	}

	logger.Info("Snapshot is not ready yet, waiting...")
	return &Status{
		Phase:            PhaseInProgress,
		SnapshotRef:      snapshotName,
		Reason:           "Snapshotting",
		Message:          fmt.Sprintf("Waiting for GKE PodSnapshot %q checkpoint to complete", snapshotName),
		LastProgressTime: lastProgress,
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

// isTerminalSnapshotFailureReason checks if a condition reason indicates that the snapshot operation
// has terminally failed and will not recover or complete.
// Note on retries (#21): When a snapshot provider emits a terminal failure reason (such as Checkpoint failed or
// DeadlineExceeded), retrying trigger creation within the same PMJ prolongs migration disruption and risks
// double-checkpointing an actively degrading pod; failing fast allows the controller or workload controller
// to cleanly initiate fallback or reschedule rather than burning the migration deadline on known terminal states.
func isTerminalSnapshotFailureReason(reason string) bool {
	r := strings.ToLower(reason)
	if r == "" || r == "noerror" || strings.HasPrefix(r, "not") || strings.HasPrefix(r, "non") {
		return false
	}
	switch r {
	case "failed", "error", "deadlineexceeded":
		return true
	}
	return false
}
