package controller

import (
	"context"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotpolicies,verbs=get;list;watch

// NodePreemptionReconciler watches Nodes for spot preemption and graceful shutdown signals,
// selecting opted-in pods and spawning PodMigrationJobs within a node recovery memory budget.
type NodePreemptionReconciler struct {
	Client                  client.Client
	APIReader               client.Reader
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	DefaultMigrationTimeout time.Duration
	SpotPreemptionBudget    int64
	RuntimeClassPolicy      *eligibility.RuntimeClassPolicy

	// Engines provides the snapshot engine for each pod: the one its
	// runtime class routes to, whether that engine can migrate the pod, and
	// its policy for the pod. Optional: nil selects GKE Pod Snapshots, built
	// from Client and Scheme, for every pod RuntimeClassPolicy allows.
	Engines *engine.Registry

	// processedPods tracks pods that have already had a preemption migration triggered or skipped
	// to avoid spamming duplicate events across subsequent node heartbeat updates.
	processedPods sync.Map // map[types.UID]time.Time
}

type preemptionCandidate struct {
	pod        *corev1.Pod
	engine     engine.Engine
	policy     *engine.PolicyRef
	size       int64
	parentName string
	parentKind string
	parentUID  string
}

// engineFor returns the snapshot engine for pod, given pm, the active
// PodMigration of its namespace (nil if there's none), and false if no
// engine serves the pod.
func (r *NodePreemptionReconciler) engineFor(pod *corev1.Pod, pm *pmv1alpha1.PodMigration) (engine.Engine, bool) {
	if r.Engines != nil {
		return r.Engines.ForPod(pod, pm)
	}
	return engine.NewGKEPodSnapshots(r.Client, r.Scheme), true
}

// activePodMigration returns the active PodMigration of namespace, through
// seen, which remembers the namespaces looked up during one reconcile.
func (r *NodePreemptionReconciler) activePodMigration(ctx context.Context, seen map[string]*pmv1alpha1.PodMigration, namespace string) (*pmv1alpha1.PodMigration, error) {
	if pm, ok := seen[namespace]; ok {
		return pm, nil
	}
	pm, err := util.ActivePodMigration(ctx, r.Client, namespace)
	if err != nil {
		return nil, err
	}
	seen[namespace] = pm
	return pm, nil
}

// Reconcile reacts to Node events, scanning for preemption signals and initiating migrations.
func (r *NodePreemptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("node", req.Name)

	node := &corev1.Node{}
	err := r.Client.Get(ctx, req.NamespacedName, node)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get node")
		return ctrl.Result{}, err
	}

	// Fast-path: check if node is undergoing preemption or termination
	if !util.IsNodePreempting(node) {
		return ctrl.Result{}, nil
	}

	logger.Info("Detected node undergoing spot preemption / shutdown; scanning for migratable pods", "node", node.Name)

	// Sweep old entries from processedPods to prevent memory growth
	now := time.Now()
	r.processedPods.Range(func(key, value any) bool {
		if t, ok := value.(time.Time); ok && now.Sub(t) > 10*time.Minute {
			r.processedPods.Delete(key)
		}
		return true
	})

	// List pods scheduled on this node using the registered field index
	podList := &corev1.PodList{}
	err = r.Client.List(ctx, podList, client.MatchingFields{PodNodeNameIndex: node.Name})
	if err != nil {
		logger.Error(err, "Failed to list pods on preempting node", "node", node.Name)
		return ctrl.Result{}, err
	}

	var candidates []preemptionCandidate
	activePodMigrations := map[string]*pmv1alpha1.PodMigration{}

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName != node.Name {
			continue
		}

		// Skip system namespaces
		if pod.Namespace == "kube-system" || pod.Namespace == "pod-migration-system" {
			continue
		}

		// Skip if already terminating
		if pod.DeletionTimestamp != nil {
			continue
		}

		// Skip if not Running
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		// Check opt-in label
		if !util.IsPodMigrationEnabled(pod) {
			continue
		}

		// Skip pods whose declared readiness gates are not satisfied (e.g.
		// external checkpoint/pause in progress or restore warmup not complete)
		if !util.AreReadinessGatesSatisfied(pod) {
			continue
		}

		// Check migratable runtime class
		if !r.RuntimeClassPolicy.Allows(pod.Spec.RuntimeClassName) {
			continue
		}

		// Check if prior migration timed out waiting for PDB
		if util.HasPDBEvictionTimedOut(pod) {
			logger.Info("Skipping preemption migration for pod: prior migration timed out on PDB budget",
				"pod", pod.Name, "namespace", pod.Namespace)
			continue
		}

		// Check if PMJ already exists
		jobName := util.FormatPMJName(pod.Name, string(pod.UID))
		existingPMJ := &pmv1alpha1.PodMigrationJob{}
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: jobName}, existingPMJ)
		if err != nil && apierrors.IsNotFound(err) && r.APIReader != nil {
			err = r.APIReader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: jobName}, existingPMJ)
		}
		if err == nil {
			// PMJ already exists for this pod UID
			continue
		}
		if err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to query existing PMJ for pod", "job", jobName)
			continue
		}

		// Check if already processed in memory recently
		if _, ok := r.processedPods.Load(pod.UID); ok {
			continue
		}

		// Pick the snapshot engine for the pod (the one its runtime class
		// routes to) and check that it can migrate the pod
		pm, err := r.activePodMigration(ctx, activePodMigrations, pod.Namespace)
		if err != nil {
			logger.Error(err, "Failed to look up the PodMigration of the pod's namespace", "pod", pod.Name, "namespace", pod.Namespace)
			continue
		}
		eng, ok := r.engineFor(pod, pm)
		if !ok || !eng.Eligible(pod, node).OK {
			continue
		}
		if ok, reason := engine.AllowsFencing(pod, eng); !ok {
			logger.Info("Skipping pod on preempting node: engine does not fence the source pod and pod has not opted in",
				"pod", pod.Name, "namespace", pod.Namespace, "engine", eng.Name(), "reason", reason)
			if r.Recorder != nil {
				r.Recorder.Eventf(pod, corev1.EventTypeWarning, "MigrationSkippedUnfencedEngine",
					"%s; skipping spot preemption migration", reason)
			}
			metrics.RecordSpotPreemptionSkipped("unfenced_engine")
			r.processedPods.Store(pod.UID, time.Now())
			continue
		}

		// Verify the engine's policy for the pod (GKE Pod Snapshots: a Ready,
		// manual, stop PodSnapshotPolicy)
		policy, err := eng.ResolvePolicy(ctx, pod)
		if err != nil {
			logger.Error(err, "Failed to resolve matching PodSnapshotPolicy for pod", "pod", pod.Name, "namespace", pod.Namespace)
			continue
		}
		if policy == nil {
			logger.Info("No matching ready manual+stop policy found for pod on preempting node", "pod", pod.Name, "namespace", pod.Namespace)
			if r.Recorder != nil {
				r.Recorder.Event(pod, corev1.EventTypeWarning, "MigrationSkippedNoPolicy",
					"Pod is opted into live migration, but no matching Ready manual+stop PodSnapshotPolicy was found; skipping spot preemption migration")
			}
			metrics.RecordSpotPreemptionSkipped("no_policy")
			r.processedPods.Store(pod.UID, time.Now())
			continue
		}

		// Calculate memory footprint (requests falling back to limits).
		// BestEffort pods with zero request and limit cannot be budgeted.
		footprintBytes, known := util.CalculatePodMemoryFootprint(pod)
		if !known || footprintBytes <= 0 {
			logger.Info("Skipping pod on preempting node: zero memory request and limit (BestEffort)",
				"pod", pod.Name, "namespace", pod.Namespace)
			if r.Recorder != nil {
				r.Recorder.Event(pod, corev1.EventTypeWarning, "MigrationSkippedUnknownMemory",
					"Pod has zero memory request and limit (BestEffort QoS); cannot budget spot preemption migration")
			}
			metrics.RecordSpotPreemptionSkipped("unknown_memory")
			r.processedPods.Store(pod.UID, time.Now())
			continue
		}

		// Resolve parent workload details
		parentName, parentKind, parentUID, err := util.ResolveParentWorkload(ctx, r.Client, pod)
		if err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to resolve parent workload for pod", "pod", pod.Name)
			continue
		}

		// For operator-owned pods, verify replica identity key and refuse when ambiguous.
		if !util.IsBuiltInWorkloadKind(parentKind) {
			replicaKey, hasKey := util.ResolveOperatorReplicaKey(pod, parentKind)
			if !hasKey || replicaKey == "" {
				logger.Info("Skipping preemption migration: operator-owned pod has no resolvable replica identity key",
					"pod", pod.Name, "parentKind", parentKind, "parentName", parentName)
				if r.Recorder != nil {
					r.Recorder.Eventf(pod, corev1.EventTypeWarning, "MigrationSkippedNoReplicaKey",
						"Pod is owned by operator %s/%s but has no resolvable replica identity key; skipping spot preemption migration", parentKind, parentName)
				}
				metrics.RecordSpotPreemptionSkipped("no_replica_key")
				r.processedPods.Store(pod.UID, time.Now())
				continue
			}

			ambiguousSibling, siblingName, err := util.HasAmbiguousOperatorSibling(ctx, r.Client, pod.Namespace, parentName, parentKind, parentUID, replicaKey, string(pod.UID), pod.Name)
			if err != nil {
				logger.Error(err, "Failed to check sibling pods for replica key ambiguity", "pod", pod.Name)
				continue
			}
			if ambiguousSibling {
				logger.Info("Skipping preemption migration: operator-owned pod replica identity is ambiguous (shared with sibling pod)",
					"pod", pod.Name, "replicaKey", replicaKey, "siblingPod", siblingName)
				if r.Recorder != nil {
					r.Recorder.Eventf(pod, corev1.EventTypeWarning, "MigrationSkippedAmbiguousReplicaKey",
						"Operator-owned pod replica identity %q is shared with sibling pod %s; skipping spot preemption migration", replicaKey, siblingName)
				}
				metrics.RecordSpotPreemptionSkipped("ambiguous_replica_key")
				r.processedPods.Store(pod.UID, time.Now())
				continue
			}

			ambiguousPMJ, existingPMJName, err := util.HasActivePMJWithReplicaKey(ctx, r.Client, pod.Namespace, parentName, parentKind, replicaKey, jobName)
			if err != nil {
				logger.Error(err, "Failed to check existing PMJs for replica key ambiguity", "pod", pod.Name)
				continue
			}
			if ambiguousPMJ {
				logger.Info("Skipping preemption migration: operator-owned pod replica identity is ambiguous (in-flight PMJ already exists)",
					"pod", pod.Name, "replicaKey", replicaKey, "existingPMJ", existingPMJName)
				if r.Recorder != nil {
					r.Recorder.Eventf(pod, corev1.EventTypeWarning, "MigrationSkippedAmbiguousReplicaKey",
						"Operator-owned pod replica identity %q is already in-flight under migration job %s; skipping spot preemption migration", replicaKey, existingPMJName)
				}
				metrics.RecordSpotPreemptionSkipped("ambiguous_replica_key")
				r.processedPods.Store(pod.UID, time.Now())
				continue
			}
		}

		candidates = append(candidates, preemptionCandidate{
			pod:        pod,
			engine:     eng,
			policy:     policy,
			size:       footprintBytes,
			parentName: parentName,
			parentKind: parentKind,
			parentUID:  parentUID,
		})
	}

	if len(candidates) == 0 {
		return ctrl.Result{}, nil
	}

	// Order candidate pods by memory footprint ascending so smaller workloads fit under
	// the node's recovery budget before the budget is exhausted, maximizing the count of
	// preserved workloads. All admitted PMJs are created concurrently in this reconcile pass.
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].size != candidates[j].size {
			return candidates[i].size < candidates[j].size
		}
		if !candidates[i].pod.CreationTimestamp.Equal(&candidates[j].pod.CreationTimestamp) {
			return candidates[i].pod.CreationTimestamp.Before(&candidates[j].pod.CreationTimestamp)
		}
		return candidates[i].pod.Name < candidates[j].pod.Name
	})

	totalBudget := r.SpotPreemptionBudget
	if totalBudget <= 0 {
		totalBudget = util.DefaultSpotPreemptionNodeBudget
	}
	remainingBudget := totalBudget

	for _, cand := range candidates {
		pod := cand.pod
		reqBytes := cand.size

		if reqBytes > remainingBudget {
			logger.Info("Skipping pod preemption migration: memory footprint exceeds remaining node budget",
				"pod", pod.Name, "namespace", pod.Namespace,
				"requestedBytes", reqBytes, "remainingBudgetBytes", remainingBudget)
			if r.Recorder != nil {
				r.Recorder.Eventf(pod, corev1.EventTypeWarning, "MigrationSkippedPreemptionBudgetExceeded",
					"Pod requested %s memory, exceeding remaining node preemption budget (%s remaining of %s total); skipping spot preemption migration",
					util.FormatBytes(reqBytes), util.FormatBytes(remainingBudget), util.FormatBytes(totalBudget))
			}
			metrics.RecordSpotPreemptionSkipped("budget_exceeded")
			// Remember skipped pod to avoid spamming duplicate warning events during the ~30s shutdown window
			r.processedPods.Store(pod.UID, time.Now())
			continue
		}

		// Deduct from remaining budget
		remainingBudget -= reqBytes

		parentName := cand.parentName
		parentKind := cand.parentKind
		parentUID := cand.parentUID

		jobName := util.FormatPMJName(pod.Name, string(pod.UID))
		// For spot preemption, enforce a short 2-minute deadline so doomed migrations
		// fail fast rather than waiting up to 2 hours on a node that will vanish in seconds.
		preemptionTimeout := util.PreemptionDefaultTimeout
		jobLabels, jobAnnotations := util.BuildPMJLabelsAndAnnotationsForPolicy(
			pod, parentName, parentKind, parentUID, cand.policy.Name, preemptionTimeout, util.TriggerSourceSpotPreemption,
		)
		if jobAnnotations == nil {
			jobAnnotations = make(map[string]string)
		}
		// If the pod did not have an explicit timeout override annotation, enforce the short 2m preemption deadline
		if pod.Annotations == nil || pod.Annotations[util.AnnotationMigrationTimeout] == "" {
			jobAnnotations[util.AnnotationMigrationTimeout] = preemptionTimeout.String()
		}
		// The engine picked for the pod serves the job to the end (#80).
		jobAnnotations[engine.AnnotationEngine] = cand.engine.Name()

		newJob := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:        jobName,
				Namespace:   pod.Namespace,
				Labels:      jobLabels,
				Annotations: jobAnnotations,
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{
					Name: pod.Name,
				},
				TargetPodUID: string(pod.UID),
			},
		}

		err = r.Client.Create(ctx, newJob)
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				logger.Info("Migration job already exists (create race)", "job", jobName)
			} else {
				logger.Error(err, "Failed to create PodMigrationJob for spot preemption", "job", jobName)
				if r.Recorder != nil {
					r.Recorder.Eventf(pod, corev1.EventTypeWarning, "PreemptionMigrationCreateFailed",
						"Failed to create PodMigrationJob %s: %v", jobName, err)
				}
				continue
			}
		} else {
			logger.Info("Successfully created PodMigrationJob for spot preemption", "job", jobName, "pod", pod.Name)
			if r.Recorder != nil {
				r.Recorder.Eventf(pod, corev1.EventTypeNormal, "PreemptionMigrationTriggered",
					"Triggered spot preemption migration for pod on node %s (memory footprint: %s, remaining node budget: %s)",
					node.Name, util.FormatBytes(reqBytes), util.FormatBytes(remainingBudget))
				r.Recorder.Eventf(node, corev1.EventTypeNormal, "PreemptionMigrationInitiated",
					"Initiated preemption migration for pod %s/%s (job %s)",
					pod.Namespace, pod.Name, jobName)
			}
			metrics.RecordSpotPreemptionTriggered()
		}
		r.processedPods.Store(pod.UID, time.Now())
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager, filtering node events
// to only reconcile when preemption or shutdown signals are active.
func (r *NodePreemptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithEventFilter(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldNode, ok1 := e.ObjectOld.(*corev1.Node)
				newNode, ok2 := e.ObjectNew.(*corev1.Node)
				if !ok1 || !ok2 {
					return false
				}
				return util.IsNodePreempting(newNode) || util.IsNodePreempting(oldNode)
			},
			CreateFunc: func(e event.CreateEvent) bool {
				node, ok := e.Object.(*corev1.Node)
				if !ok {
					return false
				}
				return util.IsNodePreempting(node)
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return false
			},
		}).
		Complete(r)
}
