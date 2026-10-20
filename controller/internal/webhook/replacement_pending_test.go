package webhook

import (
	"context"
	"encoding/json"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestPodGateInjector_PendingJobDoesNotGateNewPods: a job waiting in Pending for
// the disruption budget (#42) doesn't own new pods of its workload, so a pod
// created then gets no gate and no assignment; it takes the scale-up path.
func TestPodGateInjector_PendingJobDoesNotGateNewPods(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "default",
		Name:            "mrep-rs",
		UID:             "rs-uid",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "mrep", UID: "deploy-uid"}},
	}}
	waiting := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-waiting",
			Labels: map[string]string{
				util.LabelParentName:      "mrep",
				util.LabelParentKind:      "Deployment",
				util.LabelPodTemplateHash: "h1",
			},
		},
		Spec:   pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "mrep-src"}},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhasePending},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithRuntimeObjects(rs, waiting).Build()
	handler := &PodGateInjector{Client: cl, APIReader: cl, decoder: admission.NewDecoder(scheme)}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "default",
		Name:            "mrep-new",
		Labels:          map[string]string{"pod-migration.gke.io/enabled": "true", appsv1.DefaultDeploymentUniqueLabelKey: "h1"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "mrep-rs", UID: "rs-uid"}},
	}}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}
	req := admission.Request{}
	req.Namespace = "default"
	req.Name = pod.Name
	req.Object = runtime.RawExtension{Raw: raw}

	resp := handler.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("expected the pod to be admitted, got %+v", resp.Result)
	}
	for _, p := range resp.Patches {
		if p.Path == "/spec/schedulingGates" || p.Path == "/spec/schedulingGates/-" {
			t.Errorf("a waiting job gated a new pod: %+v", p)
		}
		if p.Path == "/metadata/annotations/pod-migration.gke.io~1assigned-pmj" {
			t.Errorf("a new pod was assigned to a waiting job: %+v", p)
		}
		if m, ok := p.Value.(map[string]interface{}); ok && p.Path == "/metadata/annotations" {
			if _, assigned := m[util.AnnotationAssignedPMJ]; assigned {
				t.Errorf("a new pod was assigned to a waiting job: %+v", p)
			}
		}
	}
}
