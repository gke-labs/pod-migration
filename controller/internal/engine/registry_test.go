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

package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// fakeEngine is an Engine with a name and runtime classes, and nothing else.
type fakeEngine struct {
	name    string
	classes []string
}

func (f *fakeEngine) Name() string             { return f.name }
func (f *fakeEngine) RuntimeClasses() []string { return f.classes }
func (f *fakeEngine) NamespaceResources(*pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error) {
	return nil, nil
}
func (f *fakeEngine) Eligible(*corev1.Pod, *corev1.Node) Verdict { return Verdict{OK: true} }
func (f *fakeEngine) ResolvePolicy(context.Context, *corev1.Pod) (*PolicyRef, error) {
	return nil, nil
}
func (f *fakeEngine) Snapshots() snapshot.Provider      { return nil }
func (f *fakeEngine) RestoreFailures() []restore.Engine { return nil }
func (f *fakeEngine) Guarantees() Guarantees {
	return Guarantees{Fencing: FencingFrozen, RestoreOnce: true}
}

// podWithRuntimeClass returns a pod with the given spec.runtimeClassName;
// "" leaves it unset.
func podWithRuntimeClass(class string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	if class != "" {
		pod.Spec.RuntimeClassName = &class
	}
	return pod
}

func mustRegister(t *testing.T, r *Registry, engines ...Engine) {
	t.Helper()
	for _, e := range engines {
		if err := r.Register(e); err != nil {
			t.Fatalf("Register(%q): %v", e.Name(), err)
		}
	}
}

func TestRegistry_Register(t *testing.T) {
	tests := []struct {
		name    string
		engine  Engine
		wantErr string
	}{
		{name: "nil engine", engine: nil, wantErr: "nil engine"},
		{name: "empty name", engine: &fakeEngine{name: ""}, wantErr: "empty name"},
		{name: "blank name", engine: &fakeEngine{name: "  "}, wantErr: "empty name"},
		{name: "invalid runtime class", engine: &fakeEngine{name: "x", classes: []string{"Not_A_Class"}}, wantErr: `invalid runtime class "Not_A_Class"`},
		{name: "empty runtime class", engine: &fakeEngine{name: "x", classes: []string{""}}, wantErr: `invalid runtime class ""`},
		{name: "gvisor and the default token", engine: &fakeEngine{name: "x", classes: []string{"gvisor", "@default"}}},
		{name: "no runtime classes", engine: &fakeEngine{name: "x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry("x")
			err := r.Register(tt.engine)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Register: %v", err)
				}
				if got, ok := r.Get("x"); !ok || got != tt.engine {
					t.Fatalf("Get(x) = %v, %t; want the registered engine", got, ok)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Register error = %v, want one containing %q", err, tt.wantErr)
			}
			if _, ok := r.Get("x"); ok {
				t.Fatalf("a rejected engine was registered")
			}
		})
	}
}

func TestRegistry_RegisterRejectsDuplicateNames(t *testing.T) {
	first := &fakeEngine{name: "a", classes: []string{"gvisor"}}
	second := &fakeEngine{name: "a", classes: []string{"@default"}}
	r := NewRegistry("a")
	mustRegister(t, r, first)

	err := r.Register(second)
	if err == nil || !strings.Contains(err.Error(), `"a" is already registered`) {
		t.Fatalf("Register(duplicate) error = %v, want an already-registered error", err)
	}
	if got, _ := r.Get("a"); got != first {
		t.Fatalf("Get(a) = %v, want the first engine", got)
	}
	// The duplicate's runtime classes weren't recorded either.
	if got, _ := r.ForPod(podWithRuntimeClass(""), nil); got != first {
		t.Fatalf("ForPod(no class) = %v, want the first engine (the default)", got)
	}
}

func TestRegistry_Default(t *testing.T) {
	r := NewRegistry("a")
	if _, err := r.Default(); err == nil || !strings.Contains(err.Error(), `default engine "a" is not registered`) {
		t.Fatalf("Default() on an empty registry: error = %v, want not registered", err)
	}

	b := &fakeEngine{name: "b"}
	mustRegister(t, r, b)
	if _, err := r.Default(); err == nil {
		t.Fatalf("Default() with only a non-default engine: want an error")
	}

	a := &fakeEngine{name: "a"}
	mustRegister(t, r, a)
	got, err := r.Default()
	if err != nil || got != a {
		t.Fatalf("Default() = %v, %v; want engine a", got, err)
	}
}

func TestRegistry_ForJobReturnsTheDefaultEngine(t *testing.T) {
	a := &fakeEngine{name: "a", classes: []string{"gvisor"}}
	b := &fakeEngine{name: "b", classes: []string{"@default"}}
	r := NewRegistry("a")
	mustRegister(t, r, b, a)

	jobs := map[string]*pmv1alpha1.PodMigrationJob{
		"nil job":                nil,
		"job without annotation": {ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "default"}},
		// Until #80 records the engine on the job, an annotation naming
		// another engine is ignored.
		"job naming another engine": {ObjectMeta: metav1.ObjectMeta{
			Name: "j", Namespace: "default",
			Annotations: map[string]string{"pod-migration.gke.io/engine": "b"},
		}},
	}
	for name, job := range jobs {
		t.Run(name, func(t *testing.T) {
			got, err := r.ForJob(job)
			if err != nil || got != a {
				t.Fatalf("ForJob() = %v, %v; want the default engine a", got, err)
			}
		})
	}

	empty := NewRegistry("a")
	if _, err := empty.ForJob(nil); err == nil {
		t.Fatalf("ForJob() without a default engine: want an error")
	}
}

func TestRegistry_ForPod(t *testing.T) {
	gke := &fakeEngine{name: "gke", classes: []string{"gvisor"}}
	criu := &fakeEngine{name: "criu", classes: []string{"@default", "runc"}}
	kata := &fakeEngine{name: "kata", classes: []string{"kata", "runc"}}
	r := NewRegistry("gke")
	mustRegister(t, r, criu, kata, gke)

	tests := []struct {
		name  string
		class string
		want  Engine
	}{
		{name: "gvisor", class: "gvisor", want: gke},
		{name: "no runtime class", class: "", want: criu},
		{name: "declared by one engine", class: "kata", want: kata},
		{name: "declared by two engines, neither the default: the first registered", class: "runc", want: criu},
		// Behavior from before the registry: the default engine serves every
		// class that the operator allow-lists; its Eligible applies the list.
		{name: "declared by no engine: the default", class: "other", want: gke},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := r.ForPod(podWithRuntimeClass(tt.class), nil)
			if !ok || got != tt.want {
				t.Fatalf("ForPod(%q) = %v, %t; want %s", tt.class, got, ok, tt.want.Name())
			}
		})
	}

	t.Run("empty runtimeClassName is the default token", func(t *testing.T) {
		pod := podWithRuntimeClass("")
		empty := ""
		pod.Spec.RuntimeClassName = &empty
		if got, ok := r.ForPod(pod, nil); !ok || got != criu {
			t.Fatalf("ForPod(runtimeClassName: \"\") = %v, %t; want criu", got, ok)
		}
	})

	t.Run("declared by the default engine and another: the default", func(t *testing.T) {
		other := &fakeEngine{name: "other", classes: []string{"gvisor"}}
		r := NewRegistry("gke")
		mustRegister(t, r, other, gke)
		if got, ok := r.ForPod(podWithRuntimeClass("gvisor"), nil); !ok || got != gke {
			t.Fatalf("ForPod(gvisor) = %v, %t; want the default engine gke", got, ok)
		}
	})

	t.Run("nil pod", func(t *testing.T) {
		if got, ok := r.ForPod(nil, nil); ok || got != nil {
			t.Fatalf("ForPod(nil) = %v, %t; want nil, false", got, ok)
		}
	})

	t.Run("no engine qualifies", func(t *testing.T) {
		r := NewRegistry("gke")
		if got, ok := r.ForPod(podWithRuntimeClass("gvisor"), nil); ok || got != nil {
			t.Fatalf("ForPod on an empty registry = %v, %t; want nil, false", got, ok)
		}
		mustRegister(t, r, kata)
		if got, ok := r.ForPod(podWithRuntimeClass("gvisor"), nil); ok || got != nil {
			t.Fatalf("ForPod(gvisor) without the default engine = %v, %t; want nil, false", got, ok)
		}
	})
}

// TestRegistry_ConcurrentUse runs lookups while engines are registered; run
// it with -race.
func TestRegistry_ConcurrentUse(t *testing.T) {
	r := NewRegistry("gke")
	mustRegister(t, r, &fakeEngine{name: "gke", classes: []string{"gvisor"}})

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, ok := r.ForPod(podWithRuntimeClass("gvisor"), nil); !ok {
					t.Errorf("ForPod failed")
					return
				}
				if _, err := r.ForJob(nil); err != nil {
					t.Errorf("ForJob: %v", err)
					return
				}
				r.Get("gke")
			}
		}()
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := r.Register(&fakeEngine{name: fmt.Sprintf("e%d", i), classes: []string{"runc"}}); err != nil {
				t.Errorf("Register: %v", err)
			}
		}(i)
	}
	wg.Wait()
}
