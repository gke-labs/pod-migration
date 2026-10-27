package snapshot

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	checkpointTestPod = "source"
	checkpointTestUID = "source-uid"
)

func checkpointTestJob(snapshotRef string) *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "pmj", Namespace: "default", UID: "job-uid"},
		Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: checkpointTestUID},
		Status:     pmv1alpha1.PodMigrationJobStatus{SnapshotRef: snapshotRef},
	}
}

func checkpointTestTrigger(snapshotName string) *unstructured.Unstructured {
	trigger := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "podsnapshot.gke.io/v1",
		"kind":       "PodSnapshotManualTrigger",
		"metadata": map[string]interface{}{
			"name":      util.FormatPSMTName(checkpointTestPod, checkpointTestUID),
			"namespace": "default",
		},
		"spec": map[string]interface{}{"targetPod": checkpointTestPod},
	}}
	if snapshotName != "" {
		trigger.Object["status"] = map[string]interface{}{
			"snapshotCreated": map[string]interface{}{"name": snapshotName},
		}
	}
	return trigger
}

// checkpointTestSnapshot returns a PodSnapshot with the given conditions
// (type, status, reason). labeled adds the label the agent puts on snapshots
// it creates for the job's trigger.
func checkpointTestSnapshot(name string, labeled bool, conditions ...[3]string) *unstructured.Unstructured {
	conds := make([]interface{}, 0, len(conditions))
	for _, c := range conditions {
		conds = append(conds, map[string]interface{}{
			"type":               c[0],
			"status":             c[1],
			"reason":             c[2],
			"lastTransitionTime": "2026-10-19T00:00:00Z",
		})
	}
	metadata := map[string]interface{}{"name": name, "namespace": "default"}
	if labeled {
		metadata["labels"] = map[string]interface{}{
			snapshotTriggeredByLabel: util.FormatPSMTName(checkpointTestPod, checkpointTestUID),
		}
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "podsnapshot.gke.io/v1",
		"kind":       "PodSnapshot",
		"metadata":   metadata,
		"status":     map[string]interface{}{"conditions": conds},
	}}
}

func TestGKEProviderCheckpointState(t *testing.T) {
	inProgress := [3]string{"Checkpoint", "False", "InProgress"}
	checkpointed := [3]string{"Checkpoint", "True", "Succeeded"}
	uploading := [3]string{"StorageReplicated", "False", "InProgress"}
	ready := [3]string{"Ready", "True", "AllSnapshotsAvailable"}
	checkpointFailed := [3]string{"Checkpoint", "False", "Failed"}
	readyFailed := [3]string{"Ready", "False", "Failed"}
	refused := [3]string{"Checkpoint", "False", "Refused"}

	tests := map[string]struct {
		snapshotRef string
		objects     []client.Object
		want        CheckpointState
	}{
		"no trigger and no snapshot": {
			want: CheckpointState{Settled: true},
		},
		"a trigger that still exists can start a checkpoint": {
			objects: []client.Object{checkpointTestTrigger("")},
			want:    CheckpointState{Settled: false},
		},
		"the trigger's snapshot completed its checkpoint": {
			objects: []client.Object{checkpointTestTrigger("snap"), checkpointTestSnapshot("snap", false, checkpointed, uploading)},
			want:    CheckpointState{Settled: false, Completed: true},
		},
		"a labeled snapshot still checkpointing after the trigger was deleted": {
			objects: []client.Object{checkpointTestSnapshot("snap", true, inProgress)},
			want:    CheckpointState{Settled: false},
		},
		"a labeled snapshot that failed its checkpoint": {
			objects: []client.Object{checkpointTestSnapshot("snap", true, checkpointFailed)},
			want:    CheckpointState{Settled: true},
		},
		"a snapshot the engine refused": {
			objects: []client.Object{checkpointTestSnapshot("snap", true, refused)},
			want:    CheckpointState{Settled: true},
		},
		"a Ready snapshot": {
			objects: []client.Object{checkpointTestSnapshot("snap", true, checkpointed, ready)},
			want:    CheckpointState{Settled: true, Completed: true},
		},
		"a snapshot found only through the job's recorded reference": {
			snapshotRef: "recorded",
			objects:     []client.Object{checkpointTestSnapshot("recorded", false, checkpointed, uploading)},
			want:        CheckpointState{Settled: false, Completed: true},
		},
		"Ready=False while the checkpoint is still in progress is the transient race of #75": {
			objects: []client.Object{checkpointTestSnapshot("snap", true, inProgress, readyFailed)},
			want:    CheckpointState{Settled: false},
		},
		"Ready=False with nothing in progress is final": {
			objects: []client.Object{checkpointTestSnapshot("snap", true, readyFailed)},
			want:    CheckpointState{Settled: true},
		},
		"another trigger's snapshot is ignored": {
			objects: []client.Object{func() client.Object {
				s := checkpointTestSnapshot("other", false, checkpointed, ready)
				s.SetLabels(map[string]string{snapshotTriggeredByLabel: "psmt-other-pod-1234"})
				return s
			}()},
			want: CheckpointState{Settled: true},
		},
		"a recorded snapshot that's gone": {
			snapshotRef: "deleted",
			want:        CheckpointState{Settled: true},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(tc.objects...).Build()
			got, err := NewGKEProvider(c, setupTestScheme()).CheckpointState(context.Background(), checkpointTestJob(tc.snapshotRef), checkpointTestPod)
			if err != nil {
				t.Fatalf("CheckpointState: %v", err)
			}
			if got != tc.want {
				t.Errorf("CheckpointState = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestGKEProviderCheckpointState_DeletingSnapshotIsNotSettled: a snapshot
// that's being deleted hasn't finished until it's gone.
func TestGKEProviderCheckpointState_DeletingSnapshotIsNotSettled(t *testing.T) {
	snap := checkpointTestSnapshot("snap", true, [3]string{"Checkpoint", "False", "Failed"})
	snap.SetFinalizers([]string{"podsnapshot.gke.io/podsnapshot-finalizer"})
	now := metav1.Now()
	snap.SetDeletionTimestamp(&now)
	c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(snap).Build()
	got, err := NewGKEProvider(c, setupTestScheme()).CheckpointState(context.Background(), checkpointTestJob(""), checkpointTestPod)
	if err != nil {
		t.Fatalf("CheckpointState: %v", err)
	}
	if got.Settled {
		t.Errorf("CheckpointState = %+v, want not Settled while the snapshot is being deleted", got)
	}
}

// The GKE provider must keep implementing the capability the controller looks for.
var _ CheckpointReporter = (*GKEProvider)(nil)
