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
	"fmt"
	"slices"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
)

// Registry maps engine names to engines, and picks the engine for a pod or
// a job (C1). Engines are registered at startup; all methods are safe for
// concurrent use.
type Registry struct {
	mu            sync.RWMutex
	defaultEngine string
	byName        map[string]Engine
	// byClass lists, for each runtime class, the engines that declare it,
	// in registration order.
	byClass map[string][]string
}

// NewRegistry returns an empty registry whose default engine is the one
// named defaultEngine. It serves jobs, and pods whose runtime class no
// engine declares.
func NewRegistry(defaultEngine string) *Registry {
	return &Registry{
		defaultEngine: defaultEngine,
		byName:        map[string]Engine{},
		byClass:       map[string][]string{},
	}
}

// Register adds e. It rejects a nil engine, an empty or already registered
// name, and a runtime class that is neither a valid RuntimeClass name nor
// eligibility.DefaultRuntimeClassToken.
func (r *Registry) Register(e Engine) error {
	if e == nil {
		return fmt.Errorf("registering a nil engine")
	}
	name := e.Name()
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("registering an engine with an empty name")
	}
	classes := e.RuntimeClasses()
	for _, class := range classes {
		if class == eligibility.DefaultRuntimeClassToken {
			continue
		}
		if errs := validation.IsDNS1123Subdomain(class); len(errs) > 0 {
			return fmt.Errorf("engine %q declares invalid runtime class %q: %s", name, class, strings.Join(errs, "; "))
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byName[name]; ok {
		return fmt.Errorf("engine %q is already registered", name)
	}
	r.byName[name] = e
	for _, class := range classes {
		if !slices.Contains(r.byClass[class], name) {
			r.byClass[class] = append(r.byClass[class], name)
		}
	}
	return nil
}

// Get returns the engine registered under name.
func (r *Registry) Get(name string) (Engine, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byName[name]
	return e, ok
}

// Default returns the default engine. It fails when no engine of that name
// is registered.
func (r *Registry) Default() (Engine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultLocked()
}

func (r *Registry) defaultLocked() (Engine, error) {
	if e, ok := r.byName[r.defaultEngine]; ok {
		return e, nil
	}
	return nil, fmt.Errorf("default engine %q is not registered", r.defaultEngine)
}

// ForPod picks the engine for pod at eviction time, by the pod's runtime
// class:
//
//   - one of the engines that declare the class: the default engine if it is
//     one of them, otherwise the first one registered;
//   - otherwise the default engine.
//
// The fallback keeps the behavior from before the registry, when a single
// engine served every runtime class the operator allow-lists
// (--migratable-runtime-classes); the engine's Eligible applies that list.
// Per-pod resolution (#80) narrows the fallback and lets pm choose between
// engines that declare the same class; until then pm is unused.
//
// ForPod returns false when pod is nil or no engine qualifies.
func (r *Registry) ForPod(pod *corev1.Pod, pm *pmv1alpha1.PodMigration) (Engine, bool) {
	if pod == nil {
		return nil, false
	}
	class := eligibility.RuntimeClassLabel(pod.Spec.RuntimeClassName)

	r.mu.RLock()
	defer r.mu.RUnlock()
	if candidates := r.byClass[class]; len(candidates) > 0 {
		if slices.Contains(candidates, r.defaultEngine) {
			return r.byName[r.defaultEngine], true
		}
		return r.byName[candidates[0]], true
	}
	e, err := r.defaultLocked()
	return e, err == nil
}

// ForJob returns the engine that serves job. Until the engine is recorded on
// the job (#80), that is always the default engine.
func (r *Registry) ForJob(job *pmv1alpha1.PodMigrationJob) (Engine, error) {
	return r.Default()
}
