package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// psNameAnnotation is the GKE Pod Snapshots restore annotation. When the agent
// creates a pod's sandbox, a snapshot name in it means "restore from that
// snapshot", and an empty value means "start cold".
const psNameAnnotation = "podsnapshot.gke.io/ps-name"

// transientReadyFailureSuppressionWindow bounds how long a Ready=False (Failed)
// condition is suppressed while a constituent sub-condition (Checkpoint or
// StorageReplicated) still reports an in-progress reason (#75, #95).
// The transient race observed in #75 lasted ~337ms; 60s covers that race by two
// orders of magnitude while ensuring a hung or crashed node snapshot agent still
// fast-fails instead of waiting out the memory-scaled migration deadline.
const transientReadyFailureSuppressionWindow = 60 * time.Second

type GKEProvider struct {
	client client.Client
	scheme *runtime.Scheme
	now    func() time.Time
}

func NewGKEProvider(c client.Client, s *runtime.Scheme) *GKEProvider {
	return &GKEProvider{client: c, scheme: s}
}

func (p *GKEProvider) nowFunc() time.Time {
	if p != nil && p.now != nil {
		return p.now()
	}
	return time.Now()
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

	// Never trigger without the cold-start bypass on the source (#151).
	sourceGone, err := p.ensureSourceColdStartBypass(ctx, job, podName)
	if err != nil {
		logger.Error(err, "Failed to set the cold-start bypass on the source pod; not creating the trigger", "pod", podName)
		return ctrl.Result{}, err
	}
	if sourceGone {
		logger.Info("Source pod not found, so nothing can restart from the snapshot; creating the trigger as before", "pod", podName)
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

// ensureSourceColdStartBypass sets the cold-start bypass (ps-name: "") on the
// source pod right before its checkpoint is triggered (#151).
//
// After the checkpoint, GKE Pod Snapshots stops the source (postCheckpoint:
// stop), and the kubelet restarts it in a new sandbox. Without the bypass, the
// agent can restore that sandbox from the snapshot that was just taken, so two
// copies run from one checkpoint. With an empty ps-name the restart is cold.
// The agent reads the annotation when it creates a sandbox, so setting it on a
// running pod takes effect at the next restart and changes nothing before.
//
// The merge patch is idempotent, and it also replaces a stale snapshot name
// left on a pod that an earlier migration restored. It carries the source's
// UID as a precondition, so a pod that was replaced under the same name is never
// patched. sourceGone reports that the source no longer exists, so nothing can
// restart from the snapshot.
func (p *GKEProvider) ensureSourceColdStartBypass(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (sourceGone bool, err error) {
	meta := map[string]interface{}{
		"annotations": map[string]interface{}{psNameAnnotation: ""},
	}
	if job.Spec.TargetPodUID != "" {
		meta["uid"] = job.Spec.TargetPodUID
	}
	data, err := json.Marshal(map[string]interface{}{"metadata": meta})
	if err != nil {
		return false, fmt.Errorf("encoding the cold-start bypass patch: %w", err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: job.Namespace}}
	if err := p.client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, data)); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("patching %s on pod %s/%s: %w", psNameAnnotation, job.Namespace, podName, err)
	}
	return false, nil
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

	// Scan conditions for terminal failures, progress, transition timestamps, and readiness in a single pass.
	var (
		lastProgress            time.Time
		terminalSubcond         map[string]interface{}
		readyTerminalCond       map[string]interface{}
		readyTerminalTransition time.Time
		subconditionInProgress  bool
		isReady                 bool
	)

	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		var condTransitionTime time.Time
		if lttStr, ok := cond["lastTransitionTime"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, lttStr); err == nil {
				condTransitionTime = t
				if t.After(lastProgress) {
					lastProgress = t
				}
			}
		}

		cType, _ := cond["type"].(string)
		cStatus, _ := cond["status"].(string)
		cReason, _ := cond["reason"].(string)

		switch cType {
		case "Checkpoint", "StorageReplicated":
			if cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
				terminalSubcond = cond
			} else if cStatus == "False" && isSnapshotSubconditionInProgressReason(cReason) {
				subconditionInProgress = true
			}
			if cType == "Checkpoint" && cStatus == "True" && cReason == "Succeeded" {
				isReady = true
			}
		case "Ready":
			if cStatus == "True" {
				isReady = true
			} else if cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
				readyTerminalCond = cond
				readyTerminalTransition = condTransitionTime
			}
		}
	}
	if lastProgress.IsZero() {
		lastProgress = targetSnapshot.GetCreationTimestamp().Time
	}

	// 1. If an underlying constituent sub-condition has terminally failed, fast-fail immediately.
	if terminalSubcond != nil {
		cType, _ := terminalSubcond["type"].(string)
		cReason, _ := terminalSubcond["reason"].(string)
		cMsg, _ := terminalSubcond["message"].(string)
		logger.Info("PodSnapshot reported terminal failure in sub-condition", "snapshot", snapshotName, "type", cType, "reason", cReason, "message", cMsg)
		return &Status{
			Phase:       PhaseFailed,
			SnapshotRef: snapshotName,
			Reason:      "SnapshotFailed",
			Message:     fmt.Sprintf("GKE PodSnapshot %s failed (%s): %s", cType, cReason, cMsg),
		}, nil
	}

	// 2. Check Ready condition for terminal failure (#75, #95).
	// In high-concurrency waves (Issue #75), GKE PodSnapshot may briefly exhibit a transient
	// Ready=False (Failed) condition (~337ms) while Checkpoint is still InProgress or
	// StorageReplicated is AwaitingCheckpoint. To avoid prematurely aborting the PMJ on that
	// transient race while still fast-failing if the node snapshot agent genuinely hangs or
	// crashes (#95), we suppress Ready=False only while a sub-condition is in progress AND
	// Ready's own lastTransitionTime is valid and within transientReadyFailureSuppressionWindow (60s).
	// Anchoring on Ready's lastTransitionTime (rather than the sub-condition's) ensures a transient
	// Ready blip midway through a multi-minute large-pod checkpoint still gets a full 60s window.
	// Note: lastTransitionTime is stamped by the node agent's clock while nowFunc() is ours
	// (cross-process comparison); 60s dwarfs normal NTP skew, so do not shrink this to a few seconds.
	if readyTerminalCond != nil {
		cReason, _ := readyTerminalCond["reason"].(string)
		cMsg, _ := readyTerminalCond["message"].(string)
		now := p.nowFunc()
		inSuppressionWindow := !readyTerminalTransition.IsZero() &&
			now.Sub(readyTerminalTransition) <= transientReadyFailureSuppressionWindow
		if subconditionInProgress && inSuppressionWindow {
			logger.Info("PodSnapshot reported Ready=False failure while sub-conditions are still in progress within suppression window; ignoring transient failure",
				"snapshot", snapshotName,
				"reason", cReason,
				"message", cMsg,
				"readyLastTransitionTime", readyTerminalTransition,
				"window", transientReadyFailureSuppressionWindow,
			)
		} else {
			if subconditionInProgress {
				logger.Info("PodSnapshot reported Ready=False failure and Ready transition exceeded suppression window; treating as terminal failure",
					"snapshot", snapshotName,
					"reason", cReason,
					"message", cMsg,
					"readyLastTransitionTime", readyTerminalTransition,
					"window", transientReadyFailureSuppressionWindow,
				)
			} else {
				logger.Info("PodSnapshot reported terminal failure", "snapshot", snapshotName, "type", "Ready", "reason", cReason, "message", cMsg)
			}
			return &Status{
				Phase:       PhaseFailed,
				SnapshotRef: snapshotName,
				Reason:      "SnapshotFailed",
				Message:     fmt.Sprintf("GKE PodSnapshot %s failed (%s): %s", "Ready", cReason, cMsg),
			}, nil
		}
	}

	// 3. Top-level status.phase terminal failure check (unconditional).
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

	// 4. Check for readiness
	if isReady {
		return &Status{
			Phase:       PhaseReady,
			SnapshotRef: snapshotName,
		}, nil
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
// Refused means the snapshot engine declined to checkpoint this pod (for example, a pod shape it does not
// support). That is a policy decision that will not change within the same PMJ, so it is terminal as well.
func isTerminalSnapshotFailureReason(reason string) bool {
	r := strings.ToLower(reason)
	if r == "" || r == "noerror" || strings.HasPrefix(r, "not") || strings.HasPrefix(r, "non") {
		return false
	}
	switch r {
	case "failed", "error", "deadlineexceeded", "refused":
		return true
	}
	return false
}

// isSnapshotSubconditionInProgressReason checks if a sub-condition reason indicates that the
// underlying checkpoint or storage replication operation is actively in progress (#75).
func isSnapshotSubconditionInProgressReason(reason string) bool {
	r := strings.ToLower(strings.TrimSpace(reason))
	switch r {
	case "inprogress", "awaitingcheckpoint", "pending":
		return true
	}
	return false
}
