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

package util

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestActivePodMigration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pm := func(namespace, name string, age time.Duration) *pmv1alpha1.PodMigration {
		return &pmv1alpha1.PodMigration{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: name, CreationTimestamp: metav1.NewTime(created.Add(-age)),
		}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		pm("team-a", "newer", time.Minute),
		pm("team-a", "older", time.Hour),
		pm("team-b", "only", time.Minute),
	).Build()

	for namespace, want := range map[string]string{"team-a": "older", "team-b": "only", "team-c": ""} {
		t.Run(namespace, func(t *testing.T) {
			got, err := ActivePodMigration(context.Background(), c, namespace)
			if err != nil {
				t.Fatalf("ActivePodMigration: %v", err)
			}
			switch {
			case want == "" && got != nil:
				t.Errorf("ActivePodMigration(%s) = %s, want none", namespace, got.Name)
			case want != "" && (got == nil || got.Name != want):
				t.Errorf("ActivePodMigration(%s) = %v, want %s", namespace, got, want)
			}
		})
	}

	t.Run("list error", func(t *testing.T) {
		failing := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("cache not synced")
			},
		}).Build()
		if got, err := ActivePodMigration(context.Background(), failing, "team-a"); err == nil || got != nil {
			t.Errorf("ActivePodMigration() = %v, %v; want the list error", got, err)
		}
	})
}
