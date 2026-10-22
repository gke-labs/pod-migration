package util

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func candidateTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	return scheme
}

// candidateTestJob is a job under the Deployment "mrep" whose source is src.
func candidateTestJob(name, src string, phase pmv1alpha1.PodMigrationJobPhase) *pmv1alpha1.PodMigrationJob {
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
		Spec:   pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: src}, TargetPodUID: "uid-" + src},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: phase},
	}
}

func candidateTestSource(name string, terminating bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID("uid-" + name),
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
	}
	if terminating {
		pod.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		pod.Finalizers = []string{"kubernetes.io/test-finalizer"}
	}
	return pod
}

// findForNewPod runs FindUnassignedActivePMJ for a new pod of "mrep", through
// the cache index and through the live reader.
func findForNewPod(t *testing.T, name string, objs []runtime.Object, want string) {
	t.Helper()
	scheme := candidateTestScheme()
	for _, indexed := range []bool{true, false} {
		c := newTestClientLive(scheme, objs...)
		if indexed {
			c = newTestClientIndexed(scheme, objs...)
		}
		got, err := FindUnassignedActivePMJ(context.Background(), c, "default", "mrep-new", "mrep", "Deployment", "", "h1", "", indexed)
		if err != nil {
			t.Fatalf("%s (indexed=%t): %v", name, indexed, err)
		}
		if got != want {
			t.Errorf("%s (indexed=%t): got %q, want %q", name, indexed, got, want)
		}
	}
}

// TestFindUnassignedActivePMJ_CandidateOrder pins the order in which a new pod's
// candidate jobs are tried (#168): Evicting, then Restoring, then Snapshotting.
// The job names sort the job that should lose first, so list order alone would
// pick it. Every source here is gone, so each job is a candidate on its own.
func TestFindUnassignedActivePMJ_CandidateOrder(t *testing.T) {
	snapshotting := candidateTestJob("pmj-a-snapshotting", "src-a", pmv1alpha1.PodMigrationJobPhaseSnapshotting)
	restoringA := candidateTestJob("pmj-a-restoring", "src-a", pmv1alpha1.PodMigrationJobPhaseRestoring)
	evicting := candidateTestJob("pmj-b-evicting", "src-b", pmv1alpha1.PodMigrationJobPhaseEvicting)
	restoringB := candidateTestJob("pmj-b-restoring", "src-b", pmv1alpha1.PodMigrationJobPhaseRestoring)

	for name, tc := range map[string]struct {
		objs []runtime.Object
		want string
	}{
		"Evicting before Snapshotting":  {[]runtime.Object{snapshotting, evicting}, "pmj-b-evicting"},
		"Restoring before Snapshotting": {[]runtime.Object{snapshotting, restoringB}, "pmj-b-restoring"},
		"Evicting before Restoring":     {[]runtime.Object{restoringA, evicting}, "pmj-b-evicting"},
	} {
		findForNewPod(t, name, tc.objs, tc.want)
	}
}

// TestFindUnassignedActivePMJ_SnapshottingSourceAlive: while a job checkpoints
// its source, a new pod under the same parent is a scale-up or another
// replica's replacement, not this job's replacement, which only exists once the
// source is evicted (#168). So a Snapshotting job owns a new pod only once its
// source is terminating or gone, which happens when the cache lags the job's
// move to Evicting.
func TestFindUnassignedActivePMJ_SnapshottingSourceAlive(t *testing.T) {
	job := candidateTestJob("pmj-a-snapshotting", "src-a", pmv1alpha1.PodMigrationJobPhaseSnapshotting)

	for name, tc := range map[string]struct {
		objs []runtime.Object
		want string
	}{
		"source alive: the new pod is a scale-up": {[]runtime.Object{job, candidateTestSource("src-a", false)}, ""},
		"source terminating: the job owns it":     {[]runtime.Object{job, candidateTestSource("src-a", true)}, "pmj-a-snapshotting"},
		"source gone: the job owns it":            {[]runtime.Object{job}, "pmj-a-snapshotting"},
	} {
		findForNewPod(t, name, tc.objs, tc.want)
	}
}
