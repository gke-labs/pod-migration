package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
)

// PodMigrationReconciler reconciles a PodMigration object.
type PodMigrationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotstorageconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotpolicies,verbs=get;list;watch;create;update;patch;delete

// Reconcile coordinates the PSSC and PSP translation.
func (r *PodMigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch PodMigration config
	config := &pmv1alpha1.PodMigration{}
	err := r.Get(ctx, req.NamespacedName, config)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PodMigration config")
		return ctrl.Result{}, err
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
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s/%s", req.Namespace, req.Name)))
	psscName := fmt.Sprintf("pssc-%s", hex.EncodeToString(h.Sum(nil))[:16])

	pspName := fmt.Sprintf("psp-%s", req.Name)

	// 1. Reconcile PodSnapshotStorageConfig (Cluster-scoped)
	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pssc.SetName(psscName)

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

	// 2. Reconcile PodSnapshotPolicy (Namespaced)
	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	psp.SetName(pspName)
	psp.SetNamespace(req.Namespace)

	specPayload := map[string]interface{}{
		"storageConfigName": psscName,
		"selector": map[string]interface{}{
			"matchLabels": map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "onDelete",
			"postCheckpoint": "stop",
		},
	}

	psp.Object["spec"] = specPayload

	logger.Info("Syncing PodSnapshotPolicy", "name", pspName, "namespace", req.Namespace)
	err = r.syncResource(ctx, psp)
	if err != nil {
		logger.Error(err, "Failed to sync PodSnapshotPolicy")
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

func (r *PodMigrationReconciler) syncResource(ctx context.Context, obj *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())
	err := r.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: obj.GetName()}, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.Create(ctx, obj)
		}
		return err
	}

	obj.SetResourceVersion(existing.GetResourceVersion())
	return r.Update(ctx, obj)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodMigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pmv1alpha1.PodMigration{}).
		Complete(r)
}
