package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// StorageCleanupFinalizer is the finalizer added to PodMigration resources
// to ensure cluster-scoped PodSnapshotStorageConfig and associated policies are
// cleaned up when the PodMigration CR is deleted.
const (
	StorageCleanupFinalizer                = util.StorageCleanupFinalizer
	OwnerUIDLabelKey                       = engine.OwnerUIDLabel
	ReasonDuplicatePodMigrationInNamespace = "DuplicatePodMigrationInNamespace"
	maxDeletionDeferral                    = 10 * time.Minute
)

const (
	// ConditionEngineRoutesValid reports whether the runtime-class routes
	// of a PodMigration (engine.AnnotationRuntimeClassEngines) are valid.
	// It's set only while the PodMigration has the annotation.
	ConditionEngineRoutesValid = "EngineRoutesValid"

	// ReasonEngineRoutesValid: every route parses and names a registered
	// engine.
	ReasonEngineRoutesValid = "RoutesValid"
	// ReasonInvalidEngineRoutes: the routes don't parse, and are ignored.
	ReasonInvalidEngineRoutes = "InvalidRoutes"
	// ReasonEngineNotRegistered: a route, or a PodMigrationJob, names an
	// engine that the controller doesn't register.
	ReasonEngineNotRegistered = "EngineNotRegistered"
)

// PodMigrationReconciler reconciles a PodMigration object.
type PodMigrationReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	InvariantEngine *invariants.Engine

	// Engines provides the snapshot engines whose policy and storage
	// objects a PodMigration's namespace gets: those that serve a runtime
	// class there. Optional: nil selects GKE Pod Snapshots for gVisor pods,
	// built from Client and Scheme. Overridden in tests.
	Engines *engine.Registry
}

// engines returns the registry that serves the reconciler: Engines, or the
// registry of a stock deployment.
func (r *PodMigrationReconciler) engines() (*engine.Registry, error) {
	if r.Engines != nil {
		return r.Engines, nil
	}
	return engine.NewDefaultRegistry(r.Client, r.Scheme, nil)
}

// setEngineRoutesCondition reports on config whether its
// engine.AnnotationRuntimeClassEngines routes are valid. Without the
// annotation, the condition is removed.
func setEngineRoutesCondition(config *pmv1alpha1.PodMigration, registry *engine.Registry) {
	if strings.TrimSpace(config.Annotations[engine.AnnotationRuntimeClassEngines]) == "" {
		meta.RemoveStatusCondition(&config.Status.Conditions, ConditionEngineRoutesValid)
		return
	}
	cond := metav1.Condition{
		Type:               ConditionEngineRoutesValid,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonEngineRoutesValid,
		ObservedGeneration: config.Generation,
	}
	routes, err := registry.ValidatePodMigrationRoutes(config)
	switch {
	case errors.Is(err, engine.ErrEngineNotRegistered):
		cond.Status, cond.Reason = metav1.ConditionFalse, ReasonEngineNotRegistered
		cond.Message = err.Error() + "; that route is ignored"
	case err != nil:
		cond.Status, cond.Reason = metav1.ConditionFalse, ReasonInvalidEngineRoutes
		cond.Message = err.Error() + "; the annotation is ignored"
	default:
		cond.Message = "Runtime class routes: " + routes.String()
	}
	meta.SetStatusCondition(&config.Status.Conditions, cond)
}

func (r *PodMigrationReconciler) evaluateInvariants(ctx context.Context, config *pmv1alpha1.PodMigration, reconcileErr *error) {
	if !r.InvariantEngine.Enabled() || config == nil {
		return
	}
	if reconcileErr != nil && *reconcileErr != nil && config.DeletionTimestamp.IsZero() {
		return
	}

	_, _ = r.InvariantEngine.Evaluate(ctx, &invariants.ReconcileSnapshot{
		Now:              time.Now(),
		Reconciler:       "PodMigrationReconciler",
		PrimaryMigration: config,
	})
}

// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrations/finalizers,verbs=update
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotstorageconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotpolicies,verbs=get;list;watch;create;update;patch;delete

func (r *PodMigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, reconcileErr error) {
	logger := log.FromContext(ctx)

	// Fetch the PodMigration instance
	config := &pmv1alpha1.PodMigration{}
	err := r.Get(ctx, req.NamespacedName, config)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.InvariantEngine.ForgetObject("PodMigrationReconciler", "mig", req.Namespace, req.Name)
			logger.Info("PodMigration resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PodMigration config")
		return ctrl.Result{}, err
	}

	defer func() {
		r.evaluateInvariants(ctx, config, &reconcileErr)
	}()

	// Examine DeletionTimestamp to determine if object is under deletion
	if !config.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(config, StorageCleanupFinalizer) {
			// Check if any in-flight migrations owned by this PodMigration are running in this namespace before deleting storage config
			inFlight, err := r.listInFlightMigrations(ctx, config)
			if err != nil {
				logger.Error(err, "Failed to list PodMigrationJobs during finalization")
				return ctrl.Result{}, err
			}

			if len(inFlight) > 0 {
				if deferralExpired(config) {
					logger.Error(nil, "Deletion deferral exceeded maximum; forcing storage cleanup",
						"name", config.Name,
						"namespace", config.Namespace,
						"inFlight", inFlight)
					if r.Recorder != nil {
						r.Recorder.Eventf(config, corev1.EventTypeWarning, "DeletionDeferralTimeout",
							"Forcing cleanup after %s with %d migration(s) still in flight",
							maxDeletionDeferral, len(inFlight))
					}
					// fall through to cleanup
				} else {
					logger.Info("Postponing storage resource cleanup: active migrations in flight in namespace",
						"name", config.Name,
						"namespace", config.Namespace,
						"inFlightCount", len(inFlight),
						"jobs", inFlight)
					changed := meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
						Type:               "Ready",
						Status:             metav1.ConditionFalse,
						Reason:             "DeletionBlockedByInFlightMigrations",
						Message:            fmt.Sprintf("Deletion deferred: %d migration(s) in flight", len(inFlight)),
						ObservedGeneration: config.Generation,
					})
					if changed {
						if err := r.Status().Update(ctx, config); err != nil {
							logger.Error(err, "Failed to update PodMigration status during deletion deferral")
							return ctrl.Result{}, err
						}
					}
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}

			logger.Info("Cleaning up storage config and policy before deletion", "name", config.Name, "namespace", config.Namespace)
			if err := r.deleteNamespaceResources(ctx, config); err != nil {
				logger.Error(err, "Failed to clean up storage resources during finalization")
				return ctrl.Result{}, err
			}

			controllerutil.RemoveFinalizer(config, StorageCleanupFinalizer)
			if err := r.Update(ctx, config); err != nil {
				logger.Error(err, "Failed to remove finalizer from PodMigration")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Enforce singleton PodMigration per namespace (#60): every PodMigration CR
	// reconciles a PodSnapshotPolicy with the same selector (pod-migration.gke.io/enabled=true),
	// so at most one PodMigration CR may be active in a namespace at a time.
	var migList pmv1alpha1.PodMigrationList
	if err := r.List(ctx, &migList, client.InNamespace(req.Namespace)); err != nil {
		logger.Error(err, "Failed to list PodMigrations in namespace")
		return ctrl.Result{}, err
	}
	if active := selectActivePodMigration(migList.Items); active != nil && active.Name != config.Name {
		deferCleanup := false
		if controllerutil.ContainsFinalizer(config, StorageCleanupFinalizer) {
			inFlight, err := r.listInFlightMigrations(ctx, config)
			if err != nil {
				logger.Error(err, "Failed to list PodMigrationJobs during duplicate demotion")
				return ctrl.Result{}, err
			}
			if len(inFlight) > 0 {
				if deferralExpired(config) {
					logger.Error(nil, "Demotion deferral exceeded maximum; forcing storage cleanup",
						"name", config.Name,
						"namespace", config.Namespace,
						"inFlight", inFlight)
					if r.Recorder != nil {
						r.Recorder.Eventf(config, corev1.EventTypeWarning, "DeletionDeferralTimeout",
							"Forcing cleanup after %s with %d migration(s) still in flight",
							maxDeletionDeferral, len(inFlight))
					}
				} else {
					logger.Info("Postponing storage resource cleanup on duplicate PodMigration: active migrations in flight",
						"name", config.Name,
						"namespace", config.Namespace,
						"active", active.Name,
						"inFlightCount", len(inFlight),
						"jobs", inFlight)
					deferCleanup = true
				}
			}
			if !deferCleanup {
				if err := r.deleteNamespaceResources(ctx, config); err != nil {
					logger.Error(err, "Failed to clean up storage resources on duplicate PodMigration")
					return ctrl.Result{}, err
				}
				controllerutil.RemoveFinalizer(config, StorageCleanupFinalizer)
				if err := r.Update(ctx, config); err != nil {
					logger.Error(err, "Failed to remove finalizer from duplicate PodMigration")
					return ctrl.Result{}, err
				}
			}
		}
		msg := fmt.Sprintf("Only one PodMigration resource is allowed per namespace; active configuration is %q", active.Name)
		logger.Info("Skipping reconciliation for duplicate PodMigration in namespace",
			"name", config.Name, "namespace", config.Namespace, "active", active.Name)
		changed := meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             ReasonDuplicatePodMigrationInNamespace,
			Message:            msg,
			ObservedGeneration: config.Generation,
		})
		if changed {
			if r.Recorder != nil {
				r.Recorder.Event(config, corev1.EventTypeWarning, ReasonDuplicatePodMigrationInNamespace, msg)
			}
			if err := r.Status().Update(ctx, config); err != nil {
				logger.Error(err, "Failed to update status on duplicate PodMigration")
				return ctrl.Result{}, err
			}
		}
		if deferCleanup || !active.DeletionTimestamp.IsZero() {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is present
	if !controllerutil.ContainsFinalizer(config, StorageCleanupFinalizer) {
		controllerutil.AddFinalizer(config, StorageCleanupFinalizer)
		if err := r.Update(ctx, config); err != nil {
			logger.Error(err, "Failed to add finalizer to PodMigration")
			return ctrl.Result{}, err
		}
	}

	// Render the policy and storage objects of every engine that serves a
	// runtime class in the namespace, and sync each engine's objects in
	// order. For GKE Pod Snapshots that's the cluster-scoped
	// PodSnapshotStorageConfig, then the manual PodSnapshotPolicy that uses
	// it.
	registry, err := r.engines()
	if err != nil {
		logger.Error(err, "No snapshot engine registry to render the namespace resources")
		return ctrl.Result{}, err
	}
	setEngineRoutesCondition(config, registry)
	engines := registry.EnginesFor(config)
	if len(engines) == 0 {
		// EnginesFor falls back to the default engine, so that isn't
		// registered.
		err := errors.New("no snapshot engine serves the namespace")
		if _, defaultErr := registry.Default(); defaultErr != nil {
			err = defaultErr
		}
		logger.Error(err, "No snapshot engine to render the namespace resources")
		return ctrl.Result{}, err
	}
	for _, eng := range engines {
		resources, err := eng.NamespaceResources(config)
		if err != nil {
			var locationErr *engine.StorageLocationError
			if errors.As(err, &locationErr) {
				meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "InvalidStorageLocation",
					Message:            err.Error(),
					ObservedGeneration: config.Generation,
				})
				if updateErr := r.Status().Update(ctx, config); updateErr != nil {
					logger.Error(updateErr, "Failed to update status on invalid storage location")
				}
			}
			return ctrl.Result{}, err
		}

		for _, desired := range resources {
			logger.Info("Syncing "+desired.GetKind(), "engine", eng.Name(), "name", desired.GetName(), "namespace", desired.GetNamespace())
			if err := r.syncResource(ctx, desired); err != nil {
				logger.Error(err, "Failed to sync "+desired.GetKind(), "engine", eng.Name(), "name", desired.GetName())
				return ctrl.Result{}, err
			}
		}
	}

	// Set status condition to Ready
	meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "Storage config and policy synced successfully",
		ObservedGeneration: config.Generation,
	})
	err = r.Status().Update(ctx, config)
	if err != nil {
		logger.Error(err, "Failed to update PodMigration status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// getPSSCName names a PodMigration's cluster-scoped PodSnapshotStorageConfig.
func getPSSCName(namespace, name string) string {
	return engine.PodSnapshotStorageConfigName(namespace, name)
}

// getPSPManualName names a PodMigration's PodSnapshotPolicy.
func getPSPManualName(name string) string {
	return engine.PodSnapshotPolicyName(name)
}

// deleteNamespaceResources deletes the policy and storage objects that the
// engines rendered for config.
func (r *PodMigrationReconciler) deleteNamespaceResources(ctx context.Context, config *pmv1alpha1.PodMigration) error {
	if err := r.deleteStorageResources(ctx, config.Namespace, config.Name, config.UID); err != nil {
		return err
	}
	return r.deleteEngineResources(ctx, config)
}

// deleteEngineResources deletes the objects that engines other than GKE Pod
// Snapshots rendered for config; deleteStorageResources deletes GKE Pod
// Snapshots' objects. Routes can change after objects were synced, so it
// renders the objects of every registered engine, skipping an engine that
// can't render them, and deletes those that exist, in reverse sync order.
// An object labeled as rendered for another PodMigration (OwnerUIDLabel) is
// kept.
func (r *PodMigrationReconciler) deleteEngineResources(ctx context.Context, config *pmv1alpha1.PodMigration) error {
	if r.Engines == nil {
		return nil
	}
	for _, name := range r.Engines.Names() {
		eng, ok := r.Engines.Get(name)
		if !ok {
			continue
		}
		if _, isGKE := eng.(*engine.GKEPodSnapshots); isGKE {
			continue
		}
		resources, err := eng.NamespaceResources(config)
		if err != nil {
			log.FromContext(ctx).Info("Not cleaning up an engine's objects: it can't render them",
				"engine", name, "error", err.Error())
			continue
		}
		for i := len(resources) - 1; i >= 0; i-- {
			if err := r.deleteOwnedResource(ctx, resources[i], config.UID); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteOwnedResource deletes the live object that rendered names, unless
// its OwnerUIDLabel names a PodMigration other than ownerUID. A missing
// object or kind is already deleted.
func (r *PodMigrationReconciler) deleteOwnedResource(ctx context.Context, rendered *unstructured.Unstructured, ownerUID types.UID) error {
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(rendered.GroupVersionKind())
	if err := r.Get(ctx, client.ObjectKeyFromObject(rendered), live); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("failed to get %s %s: %w", rendered.GetKind(), rendered.GetName(), err)
	}
	if owner := live.GetLabels()[OwnerUIDLabelKey]; owner != "" && ownerUID != "" && owner != string(ownerUID) {
		log.FromContext(ctx).Info("Skipping deletion: owner-uid does not match (recreated by another PodMigration)",
			"kind", rendered.GetKind(), "name", rendered.GetName(), "expectedUID", ownerUID, "actualUID", owner)
		return nil
	}
	if err := r.Delete(ctx, live); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete %s %s: %w", rendered.GetKind(), rendered.GetName(), err)
	}
	return nil
}

func (r *PodMigrationReconciler) deleteStorageResources(ctx context.Context, namespace, name string, expectedOwnerUID types.UID) error {
	// Note: Clusters predating PR #5 might still have a legacy psp-<name>-on-delete
	// policy that is not tracked or deleted here.

	// 1. Delete cluster-scoped PodSnapshotStorageConfig (asserting ownership)
	psscName := getPSSCName(namespace, name)
	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pssc.SetName(psscName)
	if err := r.Get(ctx, types.NamespacedName{Name: psscName}, pssc); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get PodSnapshotStorageConfig %s: %w", psscName, err)
		}
	} else {
		psscLabels := pssc.GetLabels()
		// Only delete if owner-uid matches or is absent (legacy)
		if expectedOwnerUID != "" && psscLabels != nil && psscLabels[OwnerUIDLabelKey] != "" && psscLabels[OwnerUIDLabelKey] != string(expectedOwnerUID) {
			log.FromContext(ctx).Info("Skipping PSSC deletion: owner-uid does not match (recreated by another PodMigration)",
				"pssc", psscName, "expectedUID", expectedOwnerUID, "actualUID", psscLabels[OwnerUIDLabelKey])
		} else {
			if err := r.Delete(ctx, pssc); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete PodSnapshotStorageConfig %s: %w", psscName, err)
			}
		}
	}

	// 2. Delete namespaced PodSnapshotPolicy (manual)
	pspManualName := getPSPManualName(name)
	pspManual := &unstructured.Unstructured{}
	pspManual.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspManual.SetName(pspManualName)
	pspManual.SetNamespace(namespace)
	if err := r.Delete(ctx, pspManual); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete PodSnapshotPolicy %s: %w", pspManualName, err)
	}

	return nil
}

func isPMJOwnedByPodMigration(pmj *pmv1alpha1.PodMigrationJob, config *pmv1alpha1.PodMigration) bool {
	for _, ref := range pmj.OwnerReferences {
		if ref.Kind == "PodMigration" {
			if (ref.UID != "" && ref.UID == config.UID) || ref.Name == config.Name {
				return true
			}
		}
	}
	if pmj.Labels != nil {
		if name, ok := pmj.Labels["pod-migration.gke.io/podmigration-name"]; ok && name == config.Name {
			return true
		}
		if uid, ok := pmj.Labels[OwnerUIDLabelKey]; ok && string(config.UID) != "" && uid == string(config.UID) {
			return true
		}
	}
	return false
}

func (r *PodMigrationReconciler) listInFlightMigrations(ctx context.Context, config *pmv1alpha1.PodMigration) ([]string, error) {
	var pmjList pmv1alpha1.PodMigrationJobList
	if err := r.List(ctx, &pmjList, client.InNamespace(config.Namespace)); err != nil {
		return nil, err
	}
	var inFlight []string
	for _, pmj := range pmjList.Items {
		if !isPMJOwnedByPodMigration(&pmj, config) {
			continue
		}
		switch pmj.Status.Phase {
		case pmv1alpha1.PodMigrationJobPhaseSucceeded,
			pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			pmv1alpha1.PodMigrationJobPhaseFailed:
			// Terminal state: safe to ignore
			continue
		default:
			inFlight = append(inFlight, pmj.Name)
		}
	}
	return inFlight, nil
}

func deferralExpired(config *pmv1alpha1.PodMigration) bool {
	if !config.DeletionTimestamp.IsZero() && time.Since(config.DeletionTimestamp.Time) > maxDeletionDeferral {
		return true
	}
	if cond := meta.FindStatusCondition(config.Status.Conditions, "Ready"); cond != nil &&
		cond.Status == metav1.ConditionFalse && !cond.LastTransitionTime.IsZero() &&
		time.Since(cond.LastTransitionTime.Time) > maxDeletionDeferral {
		return true
	}
	return false
}

// syncResource creates desired if it doesn't exist. Otherwise it patches only
// the fields this controller owns: spec, which is reverted to desired
// (including removing keys someone else added), and the labels set on
// desired. Labels, annotations and finalizers that other components add, such
// as a snapshot engine's selector label, are left untouched. Nothing is
// written when the object is already in the desired state.
func (r *PodMigrationReconciler) syncResource(ctx context.Context, desired *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(desired.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.Create(ctx, desired)
		}
		return err
	}

	updated := existing.DeepCopy()
	updated.Object["spec"] = runtime.DeepCopyJSONValue(desired.Object["spec"])
	if ownLabels := desired.GetLabels(); len(ownLabels) > 0 {
		labels := updated.GetLabels()
		if labels == nil {
			labels = make(map[string]string, len(ownLabels))
		}
		for k, v := range ownLabels {
			labels[k] = v
		}
		updated.SetLabels(labels)
	}

	// A JSON merge patch computed against the live object carries only what
	// differs: spec drift (keys added elsewhere become null) and our labels.
	patch := client.MergeFrom(existing)
	data, err := patch.Data(updated)
	if err != nil {
		return fmt.Errorf("computing patch for %s %s: %w", desired.GetKind(), desired.GetName(), err)
	}
	if string(data) == "{}" {
		return nil
	}
	return r.Patch(ctx, updated, patch)
}

// selectActivePodMigration deterministically elects the single active PodMigration
// CR in a namespace; see util.SelectActivePodMigration.
func selectActivePodMigration(items []pmv1alpha1.PodMigration) *pmv1alpha1.PodMigration {
	return util.SelectActivePodMigration(items)
}

func (r *PodMigrationReconciler) enqueueNamespacePeers(ctx context.Context, obj client.Object) []reconcile.Request {
	var list pmv1alpha1.PodMigrationList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, item := range list.Items {
		if item.Name == obj.GetName() {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: item.Namespace,
				Name:      item.Name,
			},
		})
	}
	return reqs
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodMigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pmv1alpha1.PodMigration{}).
		Watches(&pmv1alpha1.PodMigration{}, handler.EnqueueRequestsFromMapFunc(r.enqueueNamespacePeers)).
		Complete(r)
}
