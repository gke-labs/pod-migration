package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// The source fence (#131). After GKE Pod Snapshots checkpoints the source, it
// stops it, and the kubelet restarts it in a new sandbox. That instance starts
// cold, so if its Services still sent it traffic, it would serve without the
// checkpointed state, and the writes it acknowledged would be lost. So before
// the checkpoint is triggered, the controller fences the source: it labels it
// pod-migration.gke.io/fenced and holds its Ready condition at False through
// pods/status. The fence webhook keeps Ready at False against the kubelet's
// status writes until the pod is deleted, so the source leaves its Services'
// endpoints and doesn't come back.
//
// A successful migration deletes the source, and the label goes with it. When
// a migration fails, the source goes back into service only if the engine
// never stopped it and can't stop it later: its containers still match the
// ones recorded when it was fenced, and the engine's checkpoint has settled
// without completing. Otherwise the controller keeps it fenced and evicts it,
// so that its workload controller replaces it. After an unfence, the
// controller watches the source for a while, and fences and evicts it again if
// it stops after all.
const (
	// ConditionSourceFenced records the source's fence on the job.
	ConditionSourceFenced = util.ConditionSourceFenced

	// ReasonTriggerCreateFailed fails a job whose checkpoint trigger the API
	// server rejected (forbidden, invalid, or denied by admission). Retrying
	// won't create it, and nothing was checkpointed.
	ReasonTriggerCreateFailed = "TriggerCreateFailed"
	// ReasonFenceCapExceeded fails a job whose source stayed fenced for the
	// pre-checkpoint cap while its trigger couldn't be created.
	ReasonFenceCapExceeded = "FenceCapExceeded"

	// DefaultFencePreCheckpointCap bounds how long a fenced source waits for
	// its checkpoint trigger while creating it fails with retryable errors.
	DefaultFencePreCheckpointCap = 30 * time.Second
	// DefaultFenceLateStopWatch is how long the controller watches a source it
	// returned to service after a failed migration.
	DefaultFenceLateStopWatch = 5 * time.Minute

	// fenceSettleTimeout bounds how long, after a failure, the controller
	// waits for the engine's checkpoint to finish or go away. If it doesn't,
	// the source stays fenced.
	fenceSettleTimeout  = 30 * time.Second
	fenceSettleInterval = 2 * time.Second
	// lateStopPollInterval is how often an unfenced source is checked during
	// the late-stop watch.
	lateStopPollInterval = 5 * time.Second
	// keptSourceEvictionRetryInterval is how often the eviction of a source
	// that stays fenced is retried while the budget refuses it.
	keptSourceEvictionRetryInterval = 10 * time.Second
)

func (r *PodMigrationJobReconciler) fencePreCheckpointCap() time.Duration {
	if r.FencePreCheckpointCap > 0 {
		return r.FencePreCheckpointCap
	}
	return DefaultFencePreCheckpointCap
}

func (r *PodMigrationJobReconciler) fenceLateStopWatch() time.Duration {
	if r.FenceLateStopWatch > 0 {
		return r.FenceLateStopWatch
	}
	return DefaultFenceLateStopWatch
}

// fenceBaseline records the source's containers when it was fenced, before its
// checkpoint was triggered.
type fenceBaseline struct {
	Containers map[string]fencedContainer `json:"containers"`
}

type fencedContainer struct {
	ID       string `json:"id"`
	Restarts int32  `json:"restarts"`
}

func baselineOf(pod *corev1.Pod) fenceBaseline {
	b := fenceBaseline{Containers: map[string]fencedContainer{}}
	for _, cs := range pod.Status.ContainerStatuses {
		b.Containers[cs.Name] = fencedContainer{ID: cs.ContainerID, Restarts: cs.RestartCount}
	}
	return b
}

// jobFenceBaseline returns the baseline recorded on job, and whether one was
// recorded at all. The fence records it last, right before the trigger can be
// created, so a job without one never triggered a checkpoint. A baseline that
// doesn't parse has no containers, so no source matches it.
func jobFenceBaseline(job *pmv1alpha1.PodMigrationJob) (fenceBaseline, bool) {
	raw := job.Annotations[util.AnnotationFenceBaseline]
	if raw == "" {
		return fenceBaseline{}, false
	}
	var b fenceBaseline
	if err := json.Unmarshal([]byte(raw), &b); err != nil {
		return fenceBaseline{}, true
	}
	return b, true
}

// unchanged reports whether pod still runs the containers in the baseline:
// the same container IDs and restart counts, each running, in a running pod
// that isn't terminating. Otherwise it describes the first difference.
func (b fenceBaseline) unchanged(pod *corev1.Pod) (bool, string) {
	switch {
	case pod.DeletionTimestamp != nil:
		return false, "the source is terminating"
	case pod.Status.Phase != corev1.PodRunning:
		return false, fmt.Sprintf("the source's phase is %q", pod.Status.Phase)
	case len(b.Containers) == 0:
		return false, "no containers were recorded when the source was fenced"
	}
	statuses := make(map[string]corev1.ContainerStatus, len(pod.Status.ContainerStatuses))
	for _, cs := range pod.Status.ContainerStatuses {
		statuses[cs.Name] = cs
	}
	names := make([]string, 0, len(b.Containers))
	for name := range b.Containers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		want := b.Containers[name]
		got, ok := statuses[name]
		switch {
		case !ok:
			return false, fmt.Sprintf("container %s has no status", name)
		case want.ID == "" || got.ContainerID != want.ID:
			return false, fmt.Sprintf("container %s changed from %q to %q", name, want.ID, got.ContainerID)
		case got.RestartCount != want.Restarts:
			return false, fmt.Sprintf("container %s restarted (%d restarts, %d when fenced)", name, got.RestartCount, want.Restarts)
		case got.State.Running == nil:
			return false, fmt.Sprintf("container %s isn't running", name)
		}
	}
	return true, ""
}

// readyHeldByFence reports whether pod's Ready condition already says what the
// fence holds it at.
func readyHeldByFence(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionFalse && cond.Reason == util.ReasonPodMigrationFenced
		}
	}
	return false
}

// sourcePod reads the job's source pod through reader. It returns nil when the
// pod is gone or another pod took its name.
func (r *PodMigrationJobReconciler) sourcePod(ctx context.Context, job *pmv1alpha1.PodMigrationJob, reader client.Reader) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Spec.PodRef.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading source pod %s: %w", job.Spec.PodRef.Name, err)
	}
	if job.Spec.TargetPodUID != "" && string(pod.UID) != job.Spec.TargetPodUID {
		return nil, nil
	}
	return pod, nil
}

// patchSourceMetadata merge-patches the source's metadata. The patch carries
// the pod's UID, so a pod that replaced it under the same name is never
// patched.
func (r *PodMigrationJobReconciler) patchSourceMetadata(ctx context.Context, pod *corev1.Pod, fields map[string]interface{}) error {
	metadata := map[string]interface{}{}
	for k, v := range fields {
		metadata[k] = v
	}
	if pod.UID != "" {
		metadata["uid"] = pod.UID
	}
	data, err := json.Marshal(map[string]interface{}{"metadata": metadata})
	if err != nil {
		return err
	}
	target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
	return r.Patch(ctx, target, client.RawPatch(types.MergePatchType, data))
}

// applyFence labels the source fenced and holds its Ready condition at False.
// The label goes first: from then on the fence webhook holds Ready at False
// against every status write, the kubelet's included (#161). The kubelet only
// writes when the pod's status changes, so the controller sets Ready to False
// itself. Both writes carry the pod's UID and are skipped when already done.
func (r *PodMigrationJobReconciler) applyFence(ctx context.Context, pod *corev1.Pod) error {
	if pod.Labels[util.LabelFenced] != "true" {
		if err := r.patchSourceMetadata(ctx, pod, map[string]interface{}{
			"labels": map[string]interface{}{util.LabelFenced: "true"},
		}); err != nil {
			return fmt.Errorf("labeling source pod %s %s: %w", pod.Name, util.LabelFenced, err)
		}
	}
	if readyHeldByFence(pod) {
		return nil
	}
	metadata := map[string]interface{}{}
	if pod.UID != "" {
		metadata["uid"] = pod.UID
	}
	data, err := json.Marshal(map[string]interface{}{
		"metadata": metadata,
		"status": map[string]interface{}{
			"conditions": []map[string]interface{}{{
				"type":               string(corev1.PodReady),
				"status":             string(corev1.ConditionFalse),
				"reason":             util.ReasonPodMigrationFenced,
				"message":            util.FenceReadyMessage,
				"lastTransitionTime": metav1.Now(),
			}},
		},
	})
	if err != nil {
		return err
	}
	target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
	if err := r.Status().Patch(ctx, target, client.RawPatch(types.StrategicMergePatchType, data)); err != nil {
		return fmt.Errorf("holding source pod %s NotReady: %w", pod.Name, err)
	}
	return nil
}

// fenceSource fences the source before its checkpoint is triggered. It records
// SourceFenced=True on the job first, then labels the source and holds it
// NotReady, and records the source's containers on the job last. The caller
// creates the trigger only after fenceSource returns nil. Each step is skipped
// when it's done already, so a fence interrupted by a failed write resumes on
// the next pass. A source that's gone, replaced or finished has nothing to
// fence.
func (r *PodMigrationJobReconciler) fenceSource(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob) error {
	if _, done := jobFenceBaseline(job); done {
		return nil
	}
	pod, err := r.sourcePod(ctx, job, r.liveReader())
	if err != nil || pod == nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return err
	}

	if !meta.IsStatusConditionTrue(job.Status.Conditions, ConditionSourceFenced) {
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenced,
			Message:            "The source is fenced before its checkpoint: it's labeled " + util.LabelFenced + " and held NotReady, so its Services don't send it traffic",
			ObservedGeneration: job.Generation,
		})
		if err := r.patchStatus(ctx, job, origJob); err != nil {
			return err
		}
		*origJob = *job.DeepCopy()
	}

	if err := r.applyFence(ctx, pod); err != nil {
		return err
	}

	data, err := json.Marshal(baselineOf(pod))
	if err != nil {
		return err
	}
	before := job.DeepCopy()
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[util.AnnotationFenceBaseline] = string(data)
	if err := r.Patch(ctx, job, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("recording the fence baseline: %w", err)
	}
	*origJob = *job.DeepCopy()

	log.FromContext(ctx).Info("Fenced the source before its checkpoint", "pod", pod.Name)
	r.recordPodEvent(ctx, job, corev1.EventTypeNormal, "SourceFenced",
		"Fenced the source before its checkpoint: it's out of its Services' endpoints until it's deleted")
	metrics.RecordSourceFence("fenced")
	return nil
}

// triggerFailure decides whether a failure to create the checkpoint trigger
// fails the job. The API server's rejection (forbidden, invalid, or denied by
// admission) won't change on a retry, and nothing was checkpointed. Other
// errors are retried, but a fenced source isn't kept out of service for longer
// than the pre-checkpoint cap waiting for its trigger.
func (r *PodMigrationJobReconciler) triggerFailure(job *pmv1alpha1.PodMigrationJob, err error) (reason, message string, failed bool) {
	if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
		return ReasonTriggerCreateFailed, fmt.Sprintf("The checkpoint trigger was rejected, so nothing was checkpointed: %v", err), true
	}
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionSourceFenced)
	if cond == nil || cond.Status != metav1.ConditionTrue || job.Status.SnapshotRef != "" {
		return "", "", false
	}
	limit := r.fencePreCheckpointCap()
	if time.Since(cond.LastTransitionTime.Time) < limit {
		return "", "", false
	}
	return ReasonFenceCapExceeded, fmt.Sprintf("The source stayed fenced for %v while its checkpoint trigger couldn't be created: %v", limit, err), true
}

// failSnapshotting fails a Snapshotting job: it deletes the trigger and
// records Failed. resolveSourceFence then settles the source's fence.
func (r *PodMigrationJobReconciler) failSnapshotting(ctx context.Context, req ctrl.Request, job, origJob *pmv1alpha1.PodMigrationJob, reason, message string) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Failing migration job before its checkpoint completed", "reason", reason, "message", message)
	_ = r.getSnapshotProvider().Cleanup(ctx, job, job.Spec.PodRef.Name)
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
	now := metav1.Now()
	job.Status.CompletionTime = &now
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return r.handleStatusError(ctx, err, "Failed to update job status to Failed")
	}
	metrics.MarkPMJInactive(req.NamespacedName.String())
	metrics.RecordOutcome("failed")
	r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
	r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "%s", message)
	return ctrl.Result{Requeue: true}, nil
}

// resolveSourceFence settles the fence of a job that fenced its source and
// then concluded without a restore. It returns handled=true while there's work
// left, so the caller requeues instead of garbage-collecting the job.
func (r *PodMigrationJobReconciler) resolveSourceFence(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob) (ctrl.Result, bool, error) {
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed &&
		job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		return ctrl.Result{}, false, nil
	}
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionSourceFenced)
	if cond == nil {
		return ctrl.Result{}, false, nil
	}
	pod, err := r.sourcePod(ctx, job, r.Client)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if pod == nil || pod.DeletionTimestamp != nil {
		// The source is gone or going, and its label goes with it.
		return ctrl.Result{}, false, nil
	}
	switch {
	case cond.Status == metav1.ConditionTrue && cond.Reason == util.ReasonFenced:
		return r.decideSourceFence(ctx, job, origJob, cond)
	case cond.Status == metav1.ConditionTrue && cond.Reason == util.ReasonFenceKept:
		return r.evictKeptSource(ctx, job, pod)
	case cond.Status == metav1.ConditionFalse && cond.Reason == util.ReasonUnfenced:
		return r.watchUnfencedSource(ctx, job, origJob, cond, pod)
	}
	return ctrl.Result{}, false, nil
}

// decideSourceFence returns the source of a failed migration to service, or
// keeps it fenced.
func (r *PodMigrationJobReconciler) decideSourceFence(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob, cond *metav1.Condition) (ctrl.Result, bool, error) {
	baseline, triggered := jobFenceBaseline(job)
	if !triggered {
		pod, err := r.sourcePod(ctx, job, r.liveReader())
		if err != nil || pod == nil {
			return ctrl.Result{}, err != nil, err
		}
		return r.unfenceSource(ctx, job, origJob, pod, cond, "The migration failed before its checkpoint was triggered", false)
	}

	completed, settled, why, err := r.checkpointOutcome(ctx, job)
	if err != nil {
		if r.withinFenceSettleTimeout(job, cond) {
			log.FromContext(ctx).Info("Couldn't read the checkpoint's state; retrying", "error", err.Error())
			return ctrl.Result{RequeueAfter: fenceSettleInterval}, true, nil
		}
		why = fmt.Sprintf("the engine's checkpoint state couldn't be read: %v", err)
		completed = true
	}
	if !completed && !settled {
		if r.withinFenceSettleTimeout(job, cond) {
			return ctrl.Result{RequeueAfter: fenceSettleInterval}, true, nil
		}
		why = fmt.Sprintf("the checkpoint didn't finish within %v of the failure", fenceSettleTimeout)
		completed = true
	}

	pod, err := r.sourcePod(ctx, job, r.liveReader())
	if err != nil || pod == nil || pod.DeletionTimestamp != nil {
		return ctrl.Result{}, err != nil, err
	}
	if completed {
		return r.keepSourceFenced(ctx, job, origJob, pod, why)
	}
	if ok, diff := baseline.unchanged(pod); !ok {
		return r.keepSourceFenced(ctx, job, origJob, pod, diff)
	}
	return r.unfenceSource(ctx, job, origJob, pod, cond, "The source never stopped, and its checkpoint never completed", true)
}

// checkpointOutcome reports whether the job's checkpoint of the source
// completed and whether it has settled, with why the source must stay fenced
// when it completed.
func (r *PodMigrationJobReconciler) checkpointOutcome(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (completed, settled bool, why string, err error) {
	if job.Status.EvictingStartTime != nil || job.Annotations[util.AnnotationEvictingSince] != "" {
		return true, true, "the snapshot was durable, so the engine stopped the source", nil
	}
	provider := r.getSnapshotProvider()
	reporter, ok := provider.(snapshot.CheckpointReporter)
	if !ok {
		return true, true, "the snapshot engine doesn't report whether its checkpoint completed", nil
	}
	state, err := reporter.CheckpointState(ctx, job, job.Spec.PodRef.Name)
	if err != nil {
		return false, false, "", err
	}
	if !state.Settled && !state.Completed {
		// A trigger that's still there can start a checkpoint; delete it again.
		_ = provider.Cleanup(ctx, job, job.Spec.PodRef.Name)
	}
	return state.Completed, state.Settled, "the checkpoint completed, so the engine may stop the source", nil
}

// withinFenceSettleTimeout reports whether the job concluded less than
// fenceSettleTimeout ago.
func (r *PodMigrationJobReconciler) withinFenceSettleTimeout(job *pmv1alpha1.PodMigrationJob, cond *metav1.Condition) bool {
	concluded := cond.LastTransitionTime.Time
	if job.Status.CompletionTime != nil {
		concluded = job.Status.CompletionTime.Time
	}
	return time.Since(concluded) < fenceSettleTimeout
}

// keepSourceFenced records that the source stays fenced, and evicts it.
func (r *PodMigrationJobReconciler) keepSourceFenced(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob, pod *corev1.Pod, why string) (ctrl.Result, bool, error) {
	message := fmt.Sprintf("The source stays fenced: %s. ", why)
	if metav1.GetControllerOf(pod) == nil {
		message += "Nothing replaces a pod without a controller, so it isn't evicted; delete it to release it"
	} else {
		message += "It's evicted so that its workload controller replaces it"
	}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionSourceFenced,
		Status:             metav1.ConditionTrue,
		Reason:             util.ReasonFenceKept,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return ctrl.Result{}, true, err
	}
	*origJob = *job.DeepCopy()
	log.FromContext(ctx).Info("Keeping the source of a failed migration fenced", "pod", pod.Name, "why", why)
	r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "SourceKeptFenced", "%s", message)
	metrics.RecordSourceFence("kept")
	return r.evictKeptSource(ctx, job, pod)
}

// unfenceSource returns the source to service. It records SourceFenced=False
// before it removes the label, so a job never claims a fence that's gone. The
// kubelet's next status write, within about 10 s, sets Ready again. With watch,
// the late-stop watch follows.
func (r *PodMigrationJobReconciler) unfenceSource(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob, pod *corev1.Pod, cond *metav1.Condition, why string, watch bool) (ctrl.Result, bool, error) {
	fencedFor := time.Since(cond.LastTransitionTime.Time)
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionSourceFenced,
		Status:             metav1.ConditionFalse,
		Reason:             util.ReasonUnfenced,
		Message:            why + ", so the source is back in service",
		ObservedGeneration: job.Generation,
	})
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return ctrl.Result{}, true, err
	}
	*origJob = *job.DeepCopy()
	if err := r.removeFenceLabel(ctx, pod); err != nil {
		return ctrl.Result{}, true, err
	}
	log.FromContext(ctx).Info("Returned the source of a failed migration to service", "pod", pod.Name, "why", why)
	r.recordPodEvent(ctx, job, corev1.EventTypeNormal, "SourceUnfenced", "%s, so the source is back in service", why)
	metrics.RecordSourceFence("unfenced")
	metrics.RecordSourceFencedDuration("unfenced", fencedFor.Seconds())
	if watch {
		return ctrl.Result{RequeueAfter: lateStopPollInterval}, true, nil
	}
	return ctrl.Result{}, false, nil
}

func (r *PodMigrationJobReconciler) removeFenceLabel(ctx context.Context, pod *corev1.Pod) error {
	if _, ok := pod.Labels[util.LabelFenced]; !ok {
		return nil
	}
	if err := r.patchSourceMetadata(ctx, pod, map[string]interface{}{
		"labels": map[string]interface{}{util.LabelFenced: nil},
	}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("removing %s from source pod %s: %w", util.LabelFenced, pod.Name, err)
	}
	return nil
}

// watchUnfencedSource is the late-stop watch: for a while after an unfence,
// it checks that the source still runs the containers it was fenced with. If
// they changed, the engine stopped the source after all, and the restarted
// instance has none of its state, so the source is fenced again and evicted.
func (r *PodMigrationJobReconciler) watchUnfencedSource(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob, cond *metav1.Condition, pod *corev1.Pod) (ctrl.Result, bool, error) {
	if err := r.removeFenceLabel(ctx, pod); err != nil {
		return ctrl.Result{}, true, err
	}
	baseline, triggered := jobFenceBaseline(job)
	remaining := time.Until(cond.LastTransitionTime.Add(r.fenceLateStopWatch()))
	if !triggered || remaining <= 0 {
		return ctrl.Result{}, false, nil
	}
	next := ctrl.Result{RequeueAfter: min(lateStopPollInterval, remaining)}
	if ok, _ := baseline.unchanged(pod); ok {
		return next, true, nil
	}
	// The cache may lag; decide on what the API server says.
	live, err := r.sourcePod(ctx, job, r.liveReader())
	if err != nil {
		return ctrl.Result{}, true, err
	}
	if live == nil || live.DeletionTimestamp != nil {
		return ctrl.Result{}, false, nil
	}
	ok, diff := baseline.unchanged(live)
	if ok {
		return next, true, nil
	}
	message := fmt.Sprintf("The source stopped after it was returned to service (%s), so it's fenced again. ", diff)
	if metav1.GetControllerOf(live) == nil {
		message += "Nothing replaces a pod without a controller, so it isn't evicted; delete it to release it"
	} else {
		message += "It's evicted so that its workload controller replaces it"
	}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionSourceFenced,
		Status:             metav1.ConditionTrue,
		Reason:             util.ReasonFenceKept,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return ctrl.Result{}, true, err
	}
	*origJob = *job.DeepCopy()
	log.FromContext(ctx).Info("The source stopped after it was unfenced; fencing it again", "pod", live.Name, "change", diff)
	r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "SourceRefencedLateStop", "%s", message)
	metrics.RecordSourceFence("refenced")
	return r.evictKeptSource(ctx, job, live)
}

// evictKeptSource evicts a source that stays fenced after a failed migration,
// so that its workload controller replaces it. It makes sure the fence is in
// place first. The eviction honours PodDisruptionBudgets and the pod's own
// grace period; a fenced source is unhealthy, so under the default
// unhealthyPodEvictionPolicy the budget allows it while its healthy pods are
// enough. A pod without a controller stays fenced: nothing would replace it.
func (r *PodMigrationJobReconciler) evictKeptSource(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (ctrl.Result, bool, error) {
	if err := r.applyFence(ctx, pod); err != nil {
		return ctrl.Result{}, true, err
	}
	if metav1.GetControllerOf(pod) == nil {
		return ctrl.Result{}, false, nil
	}
	eviction := &policyv1.Eviction{
		ObjectMeta:    metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		DeleteOptions: &metav1.DeleteOptions{},
	}
	if pod.UID != "" {
		uid := pod.UID
		eviction.DeleteOptions.Preconditions = &metav1.Preconditions{UID: &uid}
	}
	logger := log.FromContext(ctx)
	err := r.SubResource("eviction").Create(ctx, pod, eviction)
	switch {
	case err == nil || apierrors.IsNotFound(err):
		logger.Info("Evicted the fenced source of a failed migration", "pod", pod.Name)
		return ctrl.Result{RequeueAfter: fenceSettleInterval}, true, nil
	case apierrors.IsTooManyRequests(err) || apierrors.IsConflict(err) || apierrors.IsInternalError(err):
		logger.Info("The fenced source's eviction was refused for now; retrying", "pod", pod.Name, "error", err.Error())
		return ctrl.Result{RequeueAfter: keptSourceEvictionRetryInterval}, true, nil
	default:
		return ctrl.Result{}, true, fmt.Errorf("evicting fenced source pod %s: %w", pod.Name, err)
	}
}
