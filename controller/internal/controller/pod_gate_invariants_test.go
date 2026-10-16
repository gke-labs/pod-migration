package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestPodGateReconciler_InvariantsSeeTheJobMigratingThePod covers a chained migration (#151).
// The replacement of a kept, Succeeded job becomes the source of a newer job, which sets
// the cold-start bypass on it. The pod gate reconciler doesn't list the namespace, so it
// must pass the newer job (named after the pod) for I2 not to flag the old one.
func TestPodGateReconciler_InvariantsSeeTheJobMigratingThePod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	const (
		namespace = "default"
		replName  = "repl-pod"
		replUID   = "uid-repl"
	)
	firstCreated := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	objects := func(withNewerJob bool) []client.Object {
		objs := []client.Object{
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      replName,
					UID:       types.UID(replUID),
					Annotations: map[string]string{
						util.AnnotationAssignedPMJ:   "pmj-first",
						"podsnapshot.gke.io/ps-name": "",
					},
				},
			},
			&pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pmj-first", CreationTimestamp: firstCreated},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef:       corev1.LocalObjectReference{Name: "orig-pod"},
					TargetPodUID: "uid-orig",
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:           pmv1alpha1.PodMigrationJobPhaseSucceeded,
					SnapshotRef:     "snap-first",
					RestoredPodName: replName,
					RestoredPodUID:  replUID,
					Consumed:        true,
					GateReleased:    true,
				},
			},
		}
		if withNewerJob {
			objs = append(objs, &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         namespace,
					Name:              util.FormatPMJName(replName, replUID),
					CreationTimestamp: metav1.NewTime(firstCreated.Add(30 * time.Second)),
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef:       corev1.LocalObjectReference{Name: replName},
					TargetPodUID: replUID,
				},
				Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
			})
		}
		return objs
	}

	for _, tc := range []struct {
		name          string
		withNewerJob  bool
		wantViolation bool
	}{
		{"replacement migrated again", true, false},
		{"no newer job", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
				WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithObjects(objects(tc.withNewerJob)...).
				Build()
			recorder := record.NewFakeRecorder(20)
			r := &PodGateReconciler{
				Client:          cl,
				Scheme:          scheme,
				Recorder:        recorder,
				InvariantEngine: invariants.NewEngine(invariants.ModeObserve, recorder),
			}
			if _, err := r.Reconcile(context.Background(), reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: namespace, Name: replName},
			}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			var violations []string
			for len(recorder.Events) > 0 {
				if e := <-recorder.Events; strings.Contains(e, invariants.EventReasonInvariantViolation) && strings.Contains(e, "[I2:") {
					violations = append(violations, e)
				}
			}
			if got := len(violations) > 0; got != tc.wantViolation {
				t.Fatalf("I2 violation events = %q, want a violation: %t", violations, tc.wantViolation)
			}
		})
	}
}
