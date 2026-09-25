package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
)

// StorageCleanupFinalizer is the finalizer added to PodMigration resources
// to ensure cluster-scoped PodSnapshotStorageConfig and associated policies are
// cleaned up when the PodMigration CR is deleted.
const (
	StorageCleanupFinalizer = "podmigration.gke.io/storage-cleanup"
	OwnerUIDLabelKey        = "podmigration.gke.io/owner-uid"
	maxDeletionDeferral     = 10 * time.Minute
)

// PodMigrationReconciler reconciles a PodMigration object.
type PodMigrationReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	InvariantEngine *invariants.Engine
}

func (r *PodMigrationReconciler) evaluateInvariants(ctx context.Context, config *pmv1alpha1.PodMigration, reconcileErr *error) {
	if r.InvariantEngine == nil || config == nil {
		return
	}
	if reconcileErr != nil && *reconcileErr != nil {
		return
	}

	var namespacePMJs []pmv1alpha1.PodMigrationJob
	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := r.List(ctx, pmjList, client.InNamespace(config.Namespace)); err == nil {
		namespacePMJs = pmjList.Items
	}

	var namespacePods []corev1.Pod
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(config.Namespace)); err == nil {
		namespacePods = podList.Items
	}

	_, _ = r.InvariantEngine.Evaluate(ctx, &invariants.ReconcileSnapshot{
		Now:              time.Now(),
		Reconciler:       "PodMigrationReconciler",
		PrimaryMigration: config,
		NamespacePMJs:    namespacePMJs,
		NamespacePods:    namespacePods,
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
			var pmjList pmv1alpha1.PodMigrationJobList
			if err := r.List(ctx, &pmjList, client.InNamespace(req.Namespace)); err != nil {
				logger.Error(err, "Failed to list PodMigrationJobs during finalization")
				return ctrl.Result{}, err
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

			if len(inFlight) > 0 {
				if !config.DeletionTimestamp.IsZero() && time.Since(config.DeletionTimestamp.Time) > maxDeletionDeferral {
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
			if err := r.deleteStorageResources(ctx, req.Namespace, req.Name, config.UID); err != nil {
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

	// Ensure finalizer is present
	if !controllerutil.ContainsFinalizer(config, StorageCleanupFinalizer) {
		controllerutil.AddFinalizer(config, StorageCleanupFinalizer)
		if err := r.Update(ctx, config); err != nil {
			logger.Error(err, "Failed to add finalizer to PodMigration")
			return ctrl.Result{}, err
		}
	}

	// Parse bucket and path from GCS URL (gs://bucket/path)
	location := config.Spec.Storage.Location
	if !strings.HasPrefix(location, "gs://") {
		err := fmt.Errorf("invalid GCS location: %s (must start with gs://)", location)
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
		return ctrl.Result{}, err
	}
	urlStr := strings.TrimPrefix(location, "gs://")
	parts := strings.SplitN(urlStr, "/", 2)
	bucketName := parts[0]
	pathPrefix := ""
	if len(parts) > 1 {
		pathPrefix = parts[1]
	}

	// Hash-based unique name for cluster-scoped PSSC to prevent namespace conflicts
	psscName := getPSSCName(req.Namespace, req.Name)

	// 1. Reconcile PodSnapshotStorageConfig (Cluster-scoped)
	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pssc.SetName(psscName)
	labels := pssc.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[OwnerUIDLabelKey] = string(config.UID)
	pssc.SetLabels(labels)

	gcsConfig := map[string]interface{}{
		"bucket": bucketName,
	}
	if pathPrefix != "" {
		gcsConfig["path"] = pathPrefix
	}

	pssc.Object["spec"] = map[string]interface{}{
		"snapshotStorageConfig": map[string]interface{}{
			"gcs": gcsConfig,
		},
	}

	logger.Info("Syncing PodSnapshotStorageConfig", "name", psscName)
	err = r.syncResource(ctx, pssc)
	if err != nil {
		logger.Error(err, "Failed to sync PodSnapshotStorageConfig")
		return ctrl.Result{}, err
	}

	// 2. Reconcile PodSnapshotPolicy for manual (Namespaced)
	pspManualName := getPSPManualName(req.Name)
	pspManual := &unstructured.Unstructured{}
	pspManual.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspManual.SetName(pspManualName)
	pspManual.SetNamespace(req.Namespace)

	specPayloadManual := map[string]interface{}{
		"storageConfigName": psscName,
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      "pod-migration.gke.io/enabled",
					"operator": "In",
					"values":   []interface{}{"true"},
				},
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
	}
	pspManual.Object["spec"] = specPayloadManual

	logger.Info("Syncing PodSnapshotPolicy (manual)", "name", pspManualName, "namespace", req.Namespace)
	err = r.syncResource(ctx, pspManual)
	if err != nil {
		logger.Error(err, "Failed to sync PodSnapshotPolicy (manual)")
		return ctrl.Result{}, err
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

func getPSSCName(namespace, name string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s/%s", namespace, name)))
	return fmt.Sprintf("pssc-%s", hex.EncodeToString(h.Sum(nil))[:16])
}

func getPSPManualName(name string) string {
	return fmt.Sprintf("psp-%s-manual", name)
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

func (r *PodMigrationReconciler) syncResource(ctx context.Context, obj *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			err = r.Create(ctx, obj)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}

	obj.SetResourceVersion(existing.GetResourceVersion())
	err = r.Update(ctx, obj)
	if err != nil {
		return err
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodMigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pmv1alpha1.PodMigration{}).
		Complete(r)
}
