package util

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// TestFindUnassignedActivePMJ_PendingJobNeverOwnsAReplacement covers a drain of
// several replicas where some jobs wait in Pending for the disruption budget
// (#42): a replacement must go to the job whose source was evicted, never to a
// waiting job, and a pod created while only waiting jobs exist is a scale-up.
func TestFindUnassignedActivePMJ_PendingJobNeverOwnsAReplacement(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	job := func(name, source string, phase pmv1alpha1.PodMigrationJobPhase) *pmv1alpha1.PodMigrationJob {
		return &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels: map[string]string{
					LabelParentName:      "mrep",
					LabelParentKind:      "Deployment",
					LabelPodTemplateHash: "h1",
				},
			},
			Spec:   pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: source}, TargetPodUID: "uid-" + source},
			Status: pmv1alpha1.PodMigrationJobStatus{Phase: phase},
		}
	}
	// Names sort the waiting job first, so list order alone would pick it.
	waiting := job("pmj-a-waiting", "src-a", pmv1alpha1.PodMigrationJobPhasePending)
	evicted := job("pmj-b-evicted", "src-b", pmv1alpha1.PodMigrationJobPhaseEvicting) // src-b is gone

	for name, tc := range map[string]struct {
		objs []runtime.Object
		want string
	}{
		"the evicted job wins over a waiting one": {[]runtime.Object{waiting, evicted}, "pmj-b-evicted"},
		"only waiting jobs: scale-up path":        {[]runtime.Object{waiting}, ""},
	} {
		for _, indexed := range []bool{true, false} {
			c := newTestClientLive(scheme, tc.objs...)
			if indexed {
				c = newTestClientIndexed(scheme, tc.objs...)
			}
			got, err := FindUnassignedActivePMJ(context.Background(), c, "default", "mrep-new", "mrep", "Deployment", "", "h1", "", indexed)
			if err != nil {
				t.Fatalf("%s (indexed=%t): %v", name, indexed, err)
			}
			if got != tc.want {
				t.Errorf("%s (indexed=%t): got %q, want %q", name, indexed, got, tc.want)
			}
		}
	}
}
