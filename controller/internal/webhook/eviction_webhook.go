package webhook

import (
	"context"
	"fmt"
	"net/http"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// defaultHandlerBudget bounds Handle when the request carries no "timeout"
// parameter. It fits inside the webhook's timeoutSeconds whether that is 5s or
// 10s.
const defaultHandlerBudget = 4 * time.Second

// admissionTimeoutKey is the context key for the API server's per-call webhook
// timeout.
type admissionTimeoutKey struct{}

// contextWithAdmissionTimeout is the webhook's WithContextFunc. The API server
// appends the time it will wait for this webhook call as the "timeout" query
// parameter (e.g. "?timeout=10s", rounded up to whole seconds); record it so
// Handle can answer before the API server gives up.
func contextWithAdmissionTimeout(ctx context.Context, r *http.Request) context.Context {
	if r == nil || r.URL == nil {
		return ctx
	}
	d, err := time.ParseDuration(r.URL.Query().Get("timeout"))
	if err != nil || d <= 0 {
		return ctx
	}
	return context.WithValue(ctx, admissionTimeoutKey{}, d)
}

// handlerBudget returns how long Handle may spend on API calls: 80% of the API
// server's webhook timeout, keeping the rest as headroom to send the answer.
func handlerBudget(ctx context.Context) time.Duration {
	if d, ok := ctx.Value(admissionTimeoutKey{}).(time.Duration); ok {
		return d - d/5
	}
	return defaultHandlerBudget
}

// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotstorageconfigs,verbs=get;list;watch
// EvictionGate handles eviction requests and creates PodMigrationJobs.
type EvictionGate struct {
	Client                  client.Client
	APIReader               client.Reader
	Recorder                record.EventRecorder
	DefaultMigrationTimeout time.Duration
	// Eligibility selects the runtime classes whose pods are migrated. Nil
	// means gVisor only (eligibility.DefaultMigratableRuntimeClasses).
	Eligibility *eligibility.RuntimeClassPolicy
	// Engines provides the snapshot engine for each pod: the one its
	// runtime class routes to, whether that engine can migrate the pod, and
	// its policy for the pod. Optional: nil selects GKE Pod Snapshots, built
	// from Client, for every pod Eligibility allows.
	Engines *engine.Registry
	decoder admission.Decoder
}

// engineFor returns the snapshot engine for pod, given pm, the active
// PodMigration of its namespace (nil if there's none), and false if no
// engine serves the pod.
func (a *EvictionGate) engineFor(pod *corev1.Pod, pm *pmv1alpha1.PodMigration) (engine.Engine, bool) {
	if a.Engines != nil {
		return a.Engines.ForPod(pod, pm)
	}
	return engine.NewGKEPodSnapshots(a.Client, nil), true
}

// Handle intercepts eviction requests.
func (a *EvictionGate) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)
	logger.Info("Intercepted eviction request", "subresource", req.SubResource)

	if req.SubResource != "eviction" {
		return admission.Allowed("not an eviction request")
	}

	// Check for dry-run requests
	if req.DryRun != nil && *req.DryRun {
		logger.Info("Dry-run eviction request, bypassing migration side-effects")
		return admission.Allowed("dry-run allowed")
	}

	// Answer before the API server gives up on this call: with failurePolicy
	// Ignore, a timed-out call admits the eviction and an opted-in pod is evicted
	// cold. Our API calls can be slow when API Priority and Fairness queues them
	// behind the very evictions being gated. Once the budget runs out, the
	// migration-path calls below fail and answer 429, so the evictor retries.
	ctx, cancel := context.WithTimeout(ctx, handlerBudget(ctx))
	defer cancel()

	// Fetch the Pod
	pod := &corev1.Pod{}
	// We must query the API server for the Pod because the eviction admission request object
	// only contains the Eviction subresource payload, which does not carry the parent Pod's labels.
	err := a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Pod already deleted, allowing eviction")
			return admission.Allowed("pod already deleted")
		}
		// Fail-open on transient errors fetching the pod to avoid blocking non-migrating workloads
		logger.Error(err, "Transient error fetching pod, failing open to protect cluster")
		return admission.Allowed("failing open due to transient error")
	}

	// Check if feature is enabled for this pod
	if !util.IsPodMigrationEnabled(pod) {
		logger.Info("Feature not enabled for pod, allowing eviction")
		return admission.Allowed("feature not enabled")
	}

	// Only migrate pods whose runtime class the installed snapshot engine supports.
	if !a.Eligibility.Allows(pod.Spec.RuntimeClassName) {
		runtimeClass := eligibility.RuntimeClassLabel(pod.Spec.RuntimeClassName)
		logger.Info("Pod runtime class is not migratable, allowing eviction immediately",
			"runtimeClass", runtimeClass, "migratableRuntimeClasses", a.Eligibility.String())
		return admission.Allowed(fmt.Sprintf("Pod runtime class %s is not migratable (allowed: %s), skipping migration",
			runtimeClass, a.Eligibility.String()))
	}

	// Check if this pod already had a migration that timed out waiting for PDB
	if util.HasPDBEvictionTimedOut(pod) {
		logger.Info("Prior migration for pod timed out on PDB budget, skipping re-snapshot and allowing eviction", "pod", req.Name)
		return admission.Allowed("skipping migration: prior migration timed out on PDB budget")
	}

	// Define migration job name with origin Pod UID to ensure unique, collision-free identity
	jobName := util.FormatPMJName(pod.Name, string(pod.UID))

	// Check if PodMigrationJob already exists
	job := &pmv1alpha1.PodMigrationJob{}
	err = a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: jobName}, job)
	if err != nil && apierrors.IsNotFound(err) && a.APIReader != nil {
		// Cache miss: query the live API server to prevent duplicate PMJ creation across HA replicas
		err = a.APIReader.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: jobName}, job)
	}
	restartedAfterBudgetWait := false
	if err == nil {
		logger.Info("Migration job already exists for current pod instance", "job", jobName, "phase", job.Status.Phase)
		switch {
		case job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded:
			logger.Info("Migration checkpoint complete, allowing eviction", "job", jobName, "phase", job.Status.Phase)
			return admission.Allowed("migration checkpoint complete")
		case util.FailedOnDisruptionBudget(job):
			// The job waited in Pending for the PodDisruptionBudget until its
			// deadline and checkpointed nothing (#42). The evictor is still asking,
			// so replace the expired job instead of falling back to a cold eviction.
			uid := job.UID
			if delErr := a.Client.Delete(ctx, job, client.Preconditions{UID: &uid}); delErr != nil && !apierrors.IsNotFound(delErr) {
				logger.Error(delErr, "Failed to delete a job that timed out waiting for the disruption budget", "job", jobName)
				return denied429("replacing a migration job that timed out waiting for the PodDisruptionBudget, retrying")
			}
			logger.Info("Replacing a job that timed out waiting for the disruption budget", "job", jobName)
			restartedAfterBudgetWait = true
		case job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore:
			logger.Info("Migration job concluded without restore, allowing eviction (fail-open)", "job", jobName, "phase", job.Status.Phase)
			return admission.Allowed("migration concluded without restore, falling back to cold eviction")
		case util.WaitingForDisruptionBudget(job):
			return denied429("migration job waiting for the PodDisruptionBudget to allow a disruption; nothing has been checkpointed")
		default:
			return denied429(fmt.Sprintf("migration job in progress: status %s", job.Status.Phase))
		}
	}

	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to get PodMigrationJob")
		return denied429("transient error reading migration job status, retrying")
	}

	// A replacement that its own migration is still restoring isn't migrated
	// again until that migration concludes (#162). A second checkpoint would
	// stop and restart it mid-restore, and the first job would then read the
	// restart's cold-start event as its own and report a warm restore as a
	// cold start. The wait is bounded: the first job concludes when the pod is
	// Ready, on a restore crash, or at its 5-minute restore ceiling.
	//
	// Exception: when a same-name replacement (e.g. a StatefulSet ordinal like
	// pm-redis-0) replaces its evicted origin pod inside kubectl drain's 5s 429
	// retry sleep, the origin node's drain retries POST /pods/<name>/eviction by
	// name without a UID precondition and hits the new replacement pod. Answering
	// 429 to that stale origin-node drain keeps kubectl drain looping until
	// Restoring finishes and then spawns a second migration on the healthy
	// destination node (infinite ping-pong). Answering 404 NotFound when the
	// origin node is still cordoned and the replacement pod is not on a cordoned
	// node lets kubectl drain see that the origin pod is gone without deleting
	// the replacement pod.
	restoringJob, staleOriginDrain, err := a.checkAssignedMigrationJob(ctx, pod)
	if err != nil {
		logger.Error(err, "Failed to read the migration job that restored this pod")
		return denied429("transient error reading the migration job that restored this pod, retrying")
	}
	if staleOriginDrain {
		logger.Info("Rejecting stale origin-node drain eviction targeting same-name replacement pod with 404 NotFound",
			"pod", pod.Name, "replacementUID", pod.UID, "job", restoringJob)
		return denied404(fmt.Sprintf("origin pod %s was already evicted by migration job %s and replaced by pod UID %s",
			pod.Name, restoringJob, pod.UID))
	}
	if restoringJob != "" {
		logger.Info("Pod is still being restored by its own migration job; not migrating it again yet", "job", restoringJob)
		return denied429(fmt.Sprintf("pod is still being restored by migration job %s; retrying after that job concludes", restoringJob))
	}

	// Do not start a new migration job for a pod whose declared readiness gates
	// are not satisfied (e.g. an external controller is checkpointing/pausing it
	// or the pod has not finished warming up). Checked after the existing-PMJ
	// lookup so an already-running migration is never interrupted.
	if !util.AreReadinessGatesSatisfied(pod) {
		logger.Info("Pod readiness gates are not satisfied, skipping migration and allowing eviction", "pod", req.Name)
		return admission.Allowed("skipping migration: pod readiness gates not satisfied")
	}

	// Pick the snapshot engine for the pod: the one its runtime class routes
	// to, through the namespace's PodMigration or the controller's routes.
	// This comes after the job checks above, which answer for a job in
	// flight whatever the routes say now.
	pm, err := util.ActivePodMigration(ctx, a.Client, pod.Namespace)
	if err != nil {
		logger.Error(err, "Failed to look up the namespace's PodMigration")
		return denied429("transient error reading the namespace's PodMigration, retrying")
	}
	eng, ok := a.engineFor(pod, pm)
	if !ok {
		runtimeClass := eligibility.RuntimeClassLabel(pod.Spec.RuntimeClassName)
		logger.Info("No snapshot engine serves the pod's runtime class, allowing eviction immediately", "runtimeClass", runtimeClass)
		return admission.Allowed(fmt.Sprintf("No snapshot engine serves runtime class %s, skipping migration", runtimeClass))
	}
	if verdict := eng.Eligible(pod, nil); !verdict.OK {
		logger.Info("Snapshot engine can't migrate the pod, allowing eviction immediately",
			"engine", eng.Name(), "runtimeClass", eligibility.RuntimeClassLabel(pod.Spec.RuntimeClassName), "reason", verdict.Reason)
		return admission.Allowed(verdict.Reason)
	}
	if ok, reason := engine.AllowsFencing(pod, eng); !ok {
		logger.Info("Snapshot engine does not fence the source pod and pod has not opted in, allowing eviction immediately",
			"engine", eng.Name(), "reason", reason)
		if a.Recorder != nil {
			a.Recorder.Eventf(pod, corev1.EventTypeWarning, "MigrationSkippedUnfencedEngine", "%s; allowing cold eviction", reason)
		}
		return admission.Allowed(fmt.Sprintf("skipping migration: %s", reason))
	}

	// Resolve parent owner details
	parentName, parentKind, parentUID, err := util.ResolveParentWorkload(ctx, a.Client, pod)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to resolve parent workload")
		return denied429("transient error resolving parent workload, retrying")
	}

	// 1. Find the engine's policy for the pod. For GKE Pod Snapshots, that's
	// the newest Ready, manual PodSnapshotPolicy that selects the pod, if its
	// postCheckpoint is stop.
	policy, err := eng.ResolvePolicy(ctx, pod)
	if err != nil {
		logger.Error(err, "Failed to resolve matching PodSnapshotPolicy")
		return denied429("transient error resolving snapshot policy, retrying")
	}

	if policy == nil {
		logger.Info("No matching ready manual+stop policy found, skipping migration and allowing eviction", "pod", req.Name)
		if a.Recorder != nil {
			a.Recorder.Event(pod, corev1.EventTypeWarning, "MigrationSkippedNoPolicy",
				"Pod is opted into live migration, but no matching Ready manual+stop PodSnapshotPolicy was found; allowing cold eviction")
		}
		return admission.Allowed("skipping migration: no valid manual+stop policy found")
	}

	// Create new PodMigrationJob
	logger.Info("Creating PodMigrationJob", "job", jobName)
	jobLabels, jobAnnotations := util.BuildPMJLabelsAndAnnotationsForPolicy(
		pod, parentName, parentKind, parentUID, policy.Name, a.DefaultMigrationTimeout, util.TriggerSourceEviction,
	)
	if jobAnnotations == nil {
		jobAnnotations = make(map[string]string)
	}
	// The engine picked for the pod serves the job to the end (#80).
	jobAnnotations[engine.AnnotationEngine] = eng.Name()

	newJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   req.Namespace,
			Labels:      jobLabels,
			Annotations: jobAnnotations,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: req.Name,
			},
			// Set TargetPodUID
			TargetPodUID: string(pod.UID),
		},
	}

	err = a.Client.Create(ctx, newJob)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			logger.Info("Migration job already exists (create race), retrying")
			return denied429("migration job already exists, retrying")
		}
		logger.Error(err, "Failed to create PodMigrationJob due to transient error")
		return denied429("failed to create migration job, retrying")
	}

	if restartedAfterBudgetWait && a.Recorder != nil {
		a.Recorder.Event(pod, corev1.EventTypeNormal, "MigrationRestartedAfterBudgetWait",
			"The previous migration job timed out waiting for the PodDisruptionBudget without a checkpoint; started a new one")
	}
	return denied429("migration job spawned")
}

const staleOriginDrainPostCompletionWindow = 15 * time.Second

// checkAssignedMigrationJob inspects the PodMigrationJob named in pod's
// assigned-pmj annotation (set by the replacement webhook).
//
// It returns:
//   - restoringJob: the job's name when that job is still restoring pod (#162)
//   - staleOriginDrain: true when pod is a same-name replacement (e.g. a
//     StatefulSet pod) whose origin node is still cordoned while pod itself is
//     either unscheduled or running on an uncordoned node, indicating a stale
//     by-name retry from kubectl drain on the origin node.
func (a *EvictionGate) checkAssignedMigrationJob(ctx context.Context, pod *corev1.Pod) (string, bool, error) {
	name := pod.Annotations[util.AnnotationAssignedPMJ]
	if name == "" {
		return "", false, nil
	}
	key := types.NamespacedName{Namespace: pod.Namespace, Name: name}
	job := &pmv1alpha1.PodMigrationJob{}
	err := a.Client.Get(ctx, key, job)
	if apierrors.IsNotFound(err) && a.APIReader != nil {
		err = a.APIReader.Get(ctx, key, job)
	}
	if apierrors.IsNotFound(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if job.Status.RestoredPodUID != "" && job.Status.RestoredPodUID != string(pod.UID) {
		return "", false, nil
	}

	inFlight := false
	switch job.Status.Phase {
	case pmv1alpha1.PodMigrationJobPhaseSnapshotting, pmv1alpha1.PodMigrationJobPhaseEvicting, pmv1alpha1.PodMigrationJobPhaseRestoring:
		inFlight = true
	}

	recentCompletion := job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded &&
		job.Status.CompletionTime != nil &&
		time.Since(job.Status.CompletionTime.Time) <= staleOriginDrainPostCompletionWindow

	if (inFlight || recentCompletion) && a.isStaleOriginDrainRetry(ctx, pod, job) {
		return job.Name, true, nil
	}
	if inFlight {
		return job.Name, false, nil
	}
	return "", false, nil
}

// isStaleOriginDrainRetry reports whether an eviction targeting pod is a stale
// by-name retry from draining job's origin node after the origin pod was
// already evicted and replaced under the same name.
func (a *EvictionGate) isStaleOriginDrainRetry(ctx context.Context, pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob) bool {
	if job.Spec.PodRef.Name != pod.Name || job.Spec.TargetPodUID == string(pod.UID) {
		return false
	}
	originNodeName := job.Status.OriginNodeName
	if originNodeName == "" || pod.Spec.NodeName == originNodeName {
		return false
	}
	// If the replacement pod is already scheduled onto a destination node that
	// is itself cordoned (e.g. the next node in a rolling upgrade), this is a
	// genuine drain of the destination node rather than a stale origin retry.
	if pod.Spec.NodeName != "" {
		targetNode := &corev1.Node{}
		if err := a.Client.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, targetNode); err != nil {
			return false
		}
		if targetNode.Spec.Unschedulable {
			return false
		}
	}
	originNode := &corev1.Node{}
	if err := a.Client.Get(ctx, types.NamespacedName{Name: originNodeName}, originNode); err != nil {
		return false
	}
	return originNode.Spec.Unschedulable
}

func denied404(msg string) admission.Response {
	return admission.Response{
		AdmissionResponse: admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Status:  metav1.StatusFailure,
				Reason:  metav1.StatusReasonNotFound,
				Code:    http.StatusNotFound,
				Message: msg,
			},
		},
	}
}

func denied429(msg string) admission.Response {
	return admission.Response{
		AdmissionResponse: admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Code:    http.StatusTooManyRequests,
				Message: msg,
			},
		},
	}
}

// InjectDecoder injects the decoder.
func (a *EvictionGate) InjectDecoder(d admission.Decoder) error {
	a.decoder = d
	return nil
}

// SetupEvictionWebhookWithManager registers the webhook on the manager.
// runtimeClasses selects which pods are migrated; nil means gVisor only.
// engines provides the snapshot engine for each pod; nil means GKE Pod
// Snapshots for every pod runtimeClasses allows.
func SetupEvictionWebhookWithManager(mgr ctrl.Manager, apiReader client.Reader, defaultTimeout time.Duration, runtimeClasses *eligibility.RuntimeClassPolicy, engines *engine.Registry) error {
	dec := admission.NewDecoder(mgr.GetScheme())
	mgr.GetWebhookServer().Register(
		"/validate-v1-pod-eviction",
		newEvictionWebhook(&EvictionGate{
			Client:                  mgr.GetClient(),
			APIReader:               apiReader,
			Recorder:                mgr.GetEventRecorderFor("pod-migration-controller"),
			DefaultMigrationTimeout: defaultTimeout,
			Eligibility:             runtimeClasses,
			Engines:                 engines,
			decoder:                 dec,
		}),
	)
	return nil
}

// newEvictionWebhook wraps the EvictionGate in the admission.Webhook served at
// /validate-v1-pod-eviction.
func newEvictionWebhook(gate *EvictionGate) *admission.Webhook {
	return &admission.Webhook{
		Handler:         gate,
		WithContextFunc: contextWithAdmissionTimeout,
	}
}
