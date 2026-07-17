package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

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
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotstorageconfigs/status,verbs=get;update;patch
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

	newCondition := map[string]interface{}{
		"type":               "Ready",
		"status":             "True",
		"reason":             "PSSCReady",
		"message":            "PodSnapshotStorageConfig is ready",
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}

	changed, err := setUnstructuredCondition(pssc, newCondition)
	if err != nil {
		logger.Error(err, "Failed to set PSSC status condition")
		return ctrl.Result{}, err
	}

	if changed {
		logger.Info("Updating PodSnapshotStorageConfig status", "name", psscName)
		err = r.Status().Update(ctx, pssc)
		if err != nil {
			logger.Error(err, "Failed to update PodSnapshotStorageConfig status")
			return ctrl.Result{}, err
		}
	}

	// 2. Reconcile PodSnapshotPolicy for onDelete (Namespaced)
	pspOnDeleteName := fmt.Sprintf("psp-%s-on-delete", req.Name)
	pspOnDelete := &unstructured.Unstructured{}
	pspOnDelete.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspOnDelete.SetName(pspOnDeleteName)
	pspOnDelete.SetNamespace(req.Namespace)

	specPayloadOnDelete := map[string]interface{}{
		"storageConfigName": psscName,
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      "pod-migration.gke.io/enabled",
					"operator": "In",
					"values":   []interface{}{"true"},
				},
				map[string]interface{}{
					"key":      "pod-migration.gke.io/trigger",
					"operator": "NotIn",
					"values":   []interface{}{"manual"},
				},
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "onDelete",
			"postCheckpoint": "stop",
		},
	}
	pspOnDelete.Object["spec"] = specPayloadOnDelete

	logger.Info("Syncing PodSnapshotPolicy (onDelete)", "name", pspOnDeleteName, "namespace", req.Namespace)
	err = r.syncResource(ctx, pspOnDelete)
	if err != nil {
		logger.Error(err, "Failed to sync PodSnapshotPolicy (onDelete)")
		return ctrl.Result{}, err
	}

	// 3. Reconcile PodSnapshotPolicy for manual (Namespaced)
	pspManualName := fmt.Sprintf("psp-%s-manual", req.Name)
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
				map[string]interface{}{
					"key":      "pod-migration.gke.io/trigger",
					"operator": "In",
					"values":   []interface{}{"manual"},
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

func setUnstructuredCondition(u *unstructured.Unstructured, newCond map[string]interface{}) (bool, error) {
	conditions, found, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil {
		return false, err
	}
	if !found {
		conditions = []interface{}{}
	}

	condType, _ := newCond["type"].(string)
	foundIndex := -1
	for i, cond := range conditions {
		c, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		t, ok := c["type"].(string)
		if ok && t == condType {
			foundIndex = i
			break
		}
	}

	changed := false
	if foundIndex != -1 {
		existingCond := conditions[foundIndex].(map[string]interface{})
		if existingCond["status"] != newCond["status"] || existingCond["reason"] != newCond["reason"] || existingCond["message"] != newCond["message"] {
			conditions[foundIndex] = newCond
			changed = true
		}
	} else {
		conditions = append(conditions, newCond)
		changed = true
	}

	if !changed {
		return false, nil
	}

	err = unstructured.SetNestedSlice(u.Object, conditions, "status", "conditions")
	return true, err
}
