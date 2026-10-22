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
	"errors"
	"fmt"
	"reflect"
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
	if got := r.Names(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("Names() = %v, want [a]", got)
	}
	// The duplicate's runtime classes weren't recorded either.
	if got, ok := r.ForPod(podWithRuntimeClass("gvisor"), nil); !ok || got != first {
		t.Fatalf("ForPod(gvisor) = %v, %t; want the first engine", got, ok)
	}
	if got, ok := r.ForPod(podWithRuntimeClass(""), nil); ok {
		t.Fatalf("ForPod(no class) = %v; want no engine", got)
	}
}

func TestRegistry_Names(t *testing.T) {
	r := NewRegistry("b")
	if got := r.Names(); len(got) != 0 {
		t.Fatalf("Names() on an empty registry = %v, want none", got)
	}
	mustRegister(t, r, &fakeEngine{name: "c"}, &fakeEngine{name: "a"}, &fakeEngine{name: "b"})
	got := r.Names()
	if want := []string{"c", "a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Names() = %v, want %v (registration order)", got, want)
	}
	got[0] = "changed"
	if again := r.Names(); again[0] != "c" {
		t.Fatalf("Names() returned the registry's own slice")
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

func TestRegistry_ForJob(t *testing.T) {
	a := &fakeEngine{name: "a", classes: []string{"gvisor"}}
	b := &fakeEngine{name: "b", classes: []string{"@default"}}
	r := NewRegistry("a")
	mustRegister(t, r, b, a)

	jobNaming := func(engineName string) *pmv1alpha1.PodMigrationJob {
		return &pmv1alpha1.PodMigrationJob{ObjectMeta: metav1.ObjectMeta{
			Name: "j", Namespace: "default",
			Annotations: map[string]string{AnnotationEngine: engineName},
		}}
	}
	tests := map[string]struct {
		job  *pmv1alpha1.PodMigrationJob
		want Engine
	}{
		"nil job": {job: nil, want: a},
		// Jobs created before jobs recorded their engine.
		"job without annotation":      {job: &pmv1alpha1.PodMigrationJob{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "default"}}, want: a},
		"job with a blank annotation": {job: jobNaming("  "), want: a},
		"job naming the default":      {job: jobNaming("a"), want: a},
		"job naming another engine":   {job: jobNaming("b"), want: b},
		"name with spaces around it":  {job: jobNaming(" b "), want: b},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := r.ForJob(tt.job)
			if err != nil || got != tt.want {
				t.Fatalf("ForJob() = %v, %v; want engine %s", got, err, tt.want.Name())
			}
		})
	}

	t.Run("job naming an engine that isn't registered", func(t *testing.T) {
		got, err := r.ForJob(jobNaming("criu"))
		if got != nil || !errors.Is(err, ErrEngineNotRegistered) {
			t.Fatalf("ForJob() = %v, %v; want an ErrEngineNotRegistered error", got, err)
		}
		if want := `job default/j names engine "criu": engine not registered (registered: b,a)`; err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
	})

	t.Run("no default engine", func(t *testing.T) {
		empty := NewRegistry("a")
		if _, err := empty.ForJob(nil); err == nil || errors.Is(err, ErrEngineNotRegistered) {
			t.Fatalf("ForJob() without a default engine: error = %v, want a not-registered default", err)
		}
	})
}

// Without cluster routes, the engines' declared runtime classes decide.
func TestRegistry_ForPodWithoutRoutes(t *testing.T) {
	gke := &fakeEngine{name: "gke", classes: []string{"gvisor"}}
	criu := &fakeEngine{name: "criu", classes: []string{"@default", "runc"}}
	kata := &fakeEngine{name: "kata", classes: []string{"kata", "runc"}}
	r := NewRegistry("gke")
	mustRegister(t, r, criu, kata, gke)

	tests := []struct {
		name  string
		class string
		want  Engine // nil: no engine
	}{
		{name: "gvisor", class: "gvisor", want: gke},
		{name: "no runtime class", class: "", want: criu},
		{name: "declared by one engine", class: "kata", want: kata},
		{name: "declared by two engines, neither the default: the first registered", class: "runc", want: criu},
		// The default engine doesn't serve classes it doesn't declare.
		{name: "declared by no engine", class: "other", want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := r.ForPod(podWithRuntimeClass(tt.class), nil)
			if tt.want == nil {
				if ok || got != nil {
					t.Fatalf("ForPod(%q) = %v, %t; want nil, false", tt.class, got, ok)
				}
				return
			}
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

// podMigrationRouting returns a PodMigration whose runtime-class routes
// annotation is routes.
func podMigrationRouting(routes string) *pmv1alpha1.PodMigration {
	return &pmv1alpha1.PodMigration{ObjectMeta: metav1.ObjectMeta{
		Name: "pm", Namespace: "default",
		Annotations: map[string]string{AnnotationRuntimeClassEngines: routes},
	}}
}

func mustSetRoutes(t *testing.T, r *Registry, raw string) {
	t.Helper()
	routes, err := ParseRoutes(raw)
	if err != nil {
		t.Fatalf("ParseRoutes(%q): %v", raw, err)
	}
	if err := r.SetRoutes(routes); err != nil {
		t.Fatalf("SetRoutes(%q): %v", raw, err)
	}
}

func TestRegistry_ForPodWithRoutes(t *testing.T) {
	gke := &fakeEngine{name: "gke", classes: []string{"gvisor"}}
	gke2 := &fakeEngine{name: "gke2", classes: []string{"gvisor"}}
	criu := &fakeEngine{name: "criu", classes: []string{"@default", "runc"}}
	kata := &fakeEngine{name: "kata", classes: []string{"kata"}}
	r := NewRegistry("gke")
	mustRegister(t, r, gke, gke2, criu, kata)
	// gke serves runc, which it doesn't declare. Nothing routes @default
	// or kata.
	mustSetRoutes(t, r, "gvisor=gke,runc=gke")

	tests := []struct {
		name  string
		pm    *pmv1alpha1.PodMigration
		class string
		want  Engine // nil: no engine
	}{
		{name: "routed class", class: "gvisor", want: gke},
		{name: "class routed to an engine that doesn't declare it", class: "runc", want: gke},
		{name: "unrouted class that the default engine doesn't declare", class: "other", want: nil},
		{name: "unrouted class that another engine declares", class: "kata", want: nil},
		{name: "PodMigration route overrides the cluster route to the default", pm: podMigrationRouting("gvisor=gke2"), class: "gvisor", want: gke2},
		{name: "PodMigration route to an engine that does not cover the class is ignored", pm: podMigrationRouting("gvisor=criu"), class: "gvisor", want: gke},
		{name: "PodMigration without the annotation", pm: &pmv1alpha1.PodMigration{}, class: "runc", want: gke},
		{name: "PodMigration route overrides the cluster route", pm: podMigrationRouting("runc=criu"), class: "runc", want: criu},
		{name: "PodMigration route for another class", pm: podMigrationRouting("runc=criu"), class: "gvisor", want: gke},
		{name: "PodMigration routes a class the cluster doesn't", pm: podMigrationRouting("@default=criu"), class: "", want: criu},
		{name: "PodMigration route to an unregistered engine is ignored", pm: podMigrationRouting("runc=missing,@default=criu"), class: "runc", want: gke},
		{name: "the PodMigration's other routes still apply", pm: podMigrationRouting("runc=missing,@default=criu"), class: "", want: criu},
		{name: "PodMigration routes that don't parse are ignored", pm: podMigrationRouting("runc=criu,"), class: "runc", want: gke},
		{name: "blank PodMigration routes", pm: podMigrationRouting(" "), class: "", want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := r.ForPod(podWithRuntimeClass(tt.class), tt.pm)
			if tt.want == nil {
				if ok || got != nil {
					t.Fatalf("ForPod(%q) = %v, %t; want nil, false", tt.class, got, ok)
				}
				return
			}
			if !ok || got != tt.want {
				t.Fatalf("ForPod(%q) = %v, %t; want %s", tt.class, got, ok, tt.want.Name())
			}
		})
	}
}

func TestRegistry_SetRoutes(t *testing.T) {
	r := NewRegistry("gke")
	mustRegister(t, r, &fakeEngine{name: "gke", classes: []string{"gvisor"}}, &fakeEngine{name: "criu"})
	if got := r.Routes(); len(got) != 0 {
		t.Fatalf("Routes() before SetRoutes = %v, want none", got)
	}

	routes := Routes{"gvisor": "gke", "@default": "criu"}
	if err := r.SetRoutes(routes); err != nil {
		t.Fatalf("SetRoutes: %v", err)
	}
	want := Routes{"gvisor": "gke", "@default": "criu"}
	routes["gvisor"] = "criu" // the registry keeps its own copy
	got := r.Routes()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Routes() = %v, want %v", got, want)
	}
	got["runc"] = "gke" // and hands out copies
	if _, ok := r.Routes()["runc"]; ok {
		t.Fatalf("Routes() returned the registry's own map")
	}

	tests := map[string]struct {
		routes        Routes
		wantErr       string
		notRegistered bool
	}{
		"unregistered engine": {
			routes:        Routes{"gvisor": "gke", "runc": "missing"},
			wantErr:       `runtime class "runc" routes to engine "missing": engine not registered (registered: gke,criu)`,
			notRegistered: true,
		},
		"invalid runtime class": {
			routes:  Routes{"Not_A_Class": "gke"},
			wantErr: `invalid runtime class "Not_A_Class"`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := r.SetRoutes(tt.routes)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("SetRoutes error = %v, want one containing %q", err, tt.wantErr)
			}
			if errors.Is(err, ErrEngineNotRegistered) != tt.notRegistered {
				t.Errorf("errors.Is(%v, ErrEngineNotRegistered) = %t, want %t", err, !tt.notRegistered, tt.notRegistered)
			}
			if got := r.Routes(); !reflect.DeepEqual(got, want) {
				t.Errorf("Routes() after a rejected SetRoutes = %v, want the previous %v", got, want)
			}
		})
	}

	t.Run("no routes: the declared classes decide again", func(t *testing.T) {
		if err := r.SetRoutes(nil); err != nil {
			t.Fatalf("SetRoutes(nil): %v", err)
		}
		if got, ok := r.ForPod(podWithRuntimeClass(""), nil); ok {
			t.Errorf("ForPod(no class) = %v, want no engine: none declares @default", got)
		}
		if got, ok := r.ForPod(podWithRuntimeClass("gvisor"), nil); !ok || got.Name() != "gke" {
			t.Errorf("ForPod(gvisor) = %v, %t; want gke, which declares it", got, ok)
		}
	})
}

func TestRegistry_PodMigrationRoutes(t *testing.T) {
	r := NewRegistry("gke")
	mustRegister(t, r, &fakeEngine{name: "gke", classes: []string{"gvisor"}}, &fakeEngine{name: "criu", classes: []string{"@default", "runc"}})

	tests := map[string]struct {
		pm            *pmv1alpha1.PodMigration
		want          Routes
		wantErr       string
		notRegistered bool
	}{
		"nil PodMigration": {pm: nil, want: Routes{}},
		"no annotation":    {pm: &pmv1alpha1.PodMigration{}, want: Routes{}},
		"blank annotation": {pm: podMigrationRouting("  "), want: Routes{}},
		"routes":           {pm: podMigrationRouting("runc=criu, gvisor = gke"), want: Routes{"runc": "criu", "gvisor": "gke"}},
		"routes that don't parse": {
			pm:      podMigrationRouting("runc"),
			wantErr: `invalid route "runc": want class=engine`,
		},
		"route to an unregistered engine": {
			pm:            podMigrationRouting("gvisor=gke,runc=missing"),
			wantErr:       `runtime class "runc" routes to engine "missing": engine not registered (registered: gke,criu)`,
			notRegistered: true,
		},
		"route to an engine that does not cover the runtime class": {
			pm:      podMigrationRouting("gvisor=criu"),
			wantErr: `runtime class "gvisor" routes to engine "criu": engine does not cover runtime class and does not match cluster route`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := r.PodMigrationRoutes(tt.pm)
			if tt.wantErr == "" {
				if err != nil || !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("PodMigrationRoutes() = %v, %v; want %v", got, err, tt.want)
				}
				return
			}
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("PodMigrationRoutes() error = %v, want %q", err, tt.wantErr)
			}
			if errors.Is(err, ErrEngineNotRegistered) != tt.notRegistered {
				t.Errorf("errors.Is(%v, ErrEngineNotRegistered) = %t, want %t", err, !tt.notRegistered, tt.notRegistered)
			}
		})
	}

	t.Run("route matching cluster route is valid even if not declared", func(t *testing.T) {
		r2 := NewRegistry("gke")
		mustRegister(t, r2, &fakeEngine{name: "gke", classes: []string{"gvisor"}}, &fakeEngine{name: "criu", classes: []string{"@default"}})
		mustSetRoutes(t, r2, "runc=gke")
		pm := podMigrationRouting("runc=gke")
		got, err := r2.ValidatePodMigrationRoutes(pm)
		if err != nil {
			t.Fatalf("ValidatePodMigrationRoutes: %v", err)
		}
		if got["runc"] != "gke" {
			t.Fatalf("got = %v, want runc=gke", got)
		}
	})

	t.Run("ValidatePodMigrationRoutes rejects unmatching engine", func(t *testing.T) {
		pm := podMigrationRouting("gvisor=criu")
		_, err := r.ValidatePodMigrationRoutes(pm)
		if err == nil {
			t.Fatalf("expected error from ValidatePodMigrationRoutes for gvisor=criu, got nil")
		}
	})
}

func TestRegistry_EnginesFor(t *testing.T) {
	gke := &fakeEngine{name: "gke", classes: []string{"gvisor"}}
	criu := &fakeEngine{name: "criu", classes: []string{"@default", "runc"}}
	kata := &fakeEngine{name: "kata", classes: []string{"kata"}}
	names := func(engines []Engine) []string {
		var out []string
		for _, e := range engines {
			out = append(out, e.Name())
		}
		return out
	}

	t.Run("without routes: the engines that declare a class", func(t *testing.T) {
		r := NewRegistry("gke")
		mustRegister(t, r, criu, kata, gke)
		if got, want := names(r.EnginesFor(nil)), []string{"criu", "kata", "gke"}; !reflect.DeepEqual(got, want) {
			t.Errorf("EnginesFor(nil) = %v, want %v", got, want)
		}
	})

	r := NewRegistry("gke")
	gke2 := &fakeEngine{name: "gke2", classes: []string{"gvisor"}}
	mustRegister(t, r, gke, gke2, criu, kata)
	mustSetRoutes(t, r, "gvisor=gke")
	tests := []struct {
		name string
		pm   *pmv1alpha1.PodMigration
		want []string
	}{
		{name: "with routes: the routed engines", pm: nil, want: []string{"gke"}},
		{name: "PodMigration routes another class", pm: podMigrationRouting("runc=criu"), want: []string{"gke", "criu"}},
		{name: "PodMigration routes every class elsewhere", pm: podMigrationRouting("gvisor=gke2"), want: []string{"gke2"}},
		{name: "PodMigration route to an engine that does not cover the class is ignored", pm: podMigrationRouting("gvisor=criu"), want: []string{"gke"}},
		{name: "PodMigration route to an unregistered engine", pm: podMigrationRouting("gvisor=missing"), want: []string{"gke"}},
		{name: "registration order", pm: podMigrationRouting("kata=kata,runc=criu"), want: []string{"gke", "criu", "kata"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := names(r.EnginesFor(tt.pm)); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EnginesFor() = %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("no engine serves a class: the default", func(t *testing.T) {
		r := NewRegistry("b")
		mustRegister(t, r, &fakeEngine{name: "a"}, &fakeEngine{name: "b"})
		if got, want := names(r.EnginesFor(nil)), []string{"b"}; !reflect.DeepEqual(got, want) {
			t.Errorf("EnginesFor(nil) = %v, want %v", got, want)
		}
	})

	t.Run("no engine serves a class, and no default", func(t *testing.T) {
		r := NewRegistry("missing")
		mustRegister(t, r, &fakeEngine{name: "a"})
		if got := r.EnginesFor(nil); len(got) != 0 {
			t.Errorf("EnginesFor(nil) = %v, want none", names(got))
		}
	})
}

// TestRegistry_ConcurrentUse runs lookups while engines are registered and
// routes are set; run it with -race.
func TestRegistry_ConcurrentUse(t *testing.T) {
	r := NewRegistry("gke")
	mustRegister(t, r, &fakeEngine{name: "gke", classes: []string{"gvisor"}})
	pm := podMigrationRouting("runc=gke")

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, ok := r.ForPod(podWithRuntimeClass("gvisor"), pm); !ok {
					t.Errorf("ForPod failed")
					return
				}
				if _, err := r.ForJob(nil); err != nil {
					t.Errorf("ForJob: %v", err)
					return
				}
				if got := r.EnginesFor(pm); len(got) == 0 {
					t.Errorf("EnginesFor returned no engine")
					return
				}
				r.Get("gke")
				r.Names()
				r.Routes()
				_, _ = r.PodMigrationRoutes(pm)
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
			if err := r.SetRoutes(Routes{"gvisor": "gke"}); err != nil {
				t.Errorf("SetRoutes: %v", err)
			}
		}(i)
	}
	wg.Wait()
}
