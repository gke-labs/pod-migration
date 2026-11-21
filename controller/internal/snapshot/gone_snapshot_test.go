// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestGKEProviderCheckStatus_FoundAndTriggerUnanswered: CheckStatus says
// whether it read the snapshot the trigger names, so the controller can tell
// a snapshot it saw from one that isn't created yet (#269), and whether the
// trigger is still unanswered, for the timeout message.
func TestGKEProviderCheckStatus_FoundAndTriggerUnanswered(t *testing.T) {
	ready := [3]string{"Ready", "True", "AllSnapshotsAvailable"}
	checkpointing := [3]string{"Checkpoint", "False", "InProgress"}
	tests := map[string]struct {
		objects        []client.Object
		wantPhase      Phase
		wantRef        string
		wantFound      bool
		wantUnanswered bool
	}{
		"no trigger yet":         {wantPhase: PhaseInProgress},
		"trigger unanswered":     {objects: []client.Object{checkpointTestTrigger("")}, wantPhase: PhaseInProgress, wantUnanswered: true},
		"snapshot not found":     {objects: []client.Object{checkpointTestTrigger("snap")}, wantPhase: PhaseInProgress, wantRef: "snap"},
		"snapshot in progress":   {objects: []client.Object{checkpointTestTrigger("snap"), checkpointTestSnapshot("snap", true, checkpointing)}, wantPhase: PhaseInProgress, wantRef: "snap", wantFound: true},
		"snapshot ready":         {objects: []client.Object{checkpointTestTrigger("snap"), checkpointTestSnapshot("snap", true, ready)}, wantPhase: PhaseReady, wantRef: "snap", wantFound: true},
		"snapshot being deleted": {objects: []client.Object{checkpointTestTrigger("snap"), markDeleting(checkpointTestSnapshot("snap", true, ready))}, wantPhase: PhaseInProgress, wantRef: "snap", wantFound: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(tc.objects...).Build()
			got, err := NewGKEProvider(c, setupTestScheme()).CheckStatus(context.Background(), checkpointTestJob(""), checkpointTestPod)
			if err != nil {
				t.Fatalf("CheckStatus: %v", err)
			}
			if got.Phase != tc.wantPhase || got.SnapshotRef != tc.wantRef || got.Found != tc.wantFound || got.TriggerUnanswered != tc.wantUnanswered {
				t.Errorf("CheckStatus = phase %s, ref %q, found %t, unanswered %t; want %s, %q, %t, %t",
					got.Phase, got.SnapshotRef, got.Found, got.TriggerUnanswered, tc.wantPhase, tc.wantRef, tc.wantFound, tc.wantUnanswered)
			}
		})
	}
	if st := CheckPodSnapshotStatus(checkpointTestSnapshot("snap", true, checkpointing), time.Now()); !st.Found {
		t.Error("CheckPodSnapshotStatus: Found = false for a snapshot it was given")
	}
}

func TestPodSnapshotUnrestorable(t *testing.T) {
	ready := [3]string{"Ready", "True", "AllSnapshotsAvailable"}
	tests := map[string]struct {
		objects []client.Object
		funcs   interceptor.Funcs
		want    string
		wantErr bool
	}{
		"there":          {objects: []client.Object{checkpointTestSnapshot("snap", false, ready)}},
		"being deleted":  {objects: []client.Object{markDeleting(checkpointTestSnapshot("snap", false, ready))}, want: ReasonSnapshotDeleting},
		"gone":           {want: ReasonSnapshotGone},
		"other snapshot": {objects: []client.Object{checkpointTestSnapshot("other", false, ready)}, want: ReasonSnapshotGone},
		// No such CRD: callers keep their behavior.
		"no such CRD": {
			funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: DefaultAPIGroup, Kind: "PodSnapshot"}}
			}},
		},
		"read error": {
			funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return errors.New("apiserver unavailable")
			}},
			wantErr: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(tc.objects...).WithInterceptorFuncs(tc.funcs).Build()
			got, err := PodSnapshotUnrestorable(context.Background(), c, DefaultAPIGroup, "default", "snap")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("PodSnapshotUnrestorable = %q, want %q", got, tc.want)
			}
		})
	}
}
