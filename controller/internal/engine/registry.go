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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
)

// ErrEngineNotRegistered is wrapped by the errors that report an engine name
// that no registered engine has.
var ErrEngineNotRegistered = errors.New("engine not registered")

// Registry maps engine names to engines, and picks the engine for a pod or
// a job (C1). Engines are registered at startup; all methods are safe for
// concurrent use.
type Registry struct {
	mu            sync.RWMutex
	defaultEngine string
	byName        map[string]Engine
	// names lists the registered engines in registration order.
	names []string
	// byClass lists, for each runtime class, the engines that declare it,
	// in registration order.
	byClass map[string][]string
	// routes are the cluster routes, set by SetRoutes.
	routes Routes
}

// NewRegistry returns an empty registry whose default engine is the one
// named defaultEngine. The default engine serves the jobs that don't name an
// engine, and ForPod prefers it among the engines that declare a class.
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
	r.names = append(r.names, name)
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

// Names returns the names of the registered engines, in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Clone(r.names)
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

// SetRoutes replaces the cluster routes: for each runtime class, the engine
// that serves the class's pods in every namespace whose PodMigration doesn't
// route the class itself. A route may name an engine that doesn't declare
// the class; the operator's allow-list decides which classes are migrated
// at all. SetRoutes rejects an invalid class and an engine that isn't
// registered, and then keeps the routes it had.
func (r *Registry) SetRoutes(routes Routes) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, class := range routes.classes() {
		if err := validateRouteClass(class); err != nil {
			return err
		}
		if err := r.checkRouteLocked(class, routes[class]); err != nil {
			return err
		}
	}
	r.routes = maps.Clone(routes)
	return nil
}

// Routes returns the cluster routes.
func (r *Registry) Routes() Routes {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Clone(r.routes)
}

// PodMigrationRoutes returns the routes that pm's
// AnnotationRuntimeClassEngines annotation sets for its namespace: none when
// pm is nil or the annotation is missing or blank. It fails when the
// annotation doesn't parse, names an engine that isn't registered, or
// routes a runtime class to an engine that does not cover it and does not
// match the cluster route for it. ForPod ignores invalid routes.
func (r *Registry) PodMigrationRoutes(pm *pmv1alpha1.PodMigration) (Routes, error) {
	return r.ValidatePodMigrationRoutes(pm)
}

// ValidatePodMigrationRoutes validates and returns the routes that pm's
// AnnotationRuntimeClassEngines annotation sets for its namespace.
// It fails when the annotation doesn't parse, names an engine that isn't
// registered, or specifies an override where the engine neither covers the
// runtime class nor matches the cluster route for it.
func (r *Registry) ValidatePodMigrationRoutes(pm *pmv1alpha1.PodMigration) (Routes, error) {
	routes, err := ParseRoutes(podMigrationRoutesAnnotation(pm))
	if err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, class := range routes.classes() {
		if err := r.checkPodMigrationRouteLocked(class, routes[class]); err != nil {
			return nil, err
		}
	}
	return routes, nil
}

// ForPod picks the engine that serves pod at eviction time, by the pod's
// runtime class:
//
//  1. pm's route for the class (AnnotationRuntimeClassEngines), if it names
//     a registered engine;
//  2. otherwise the cluster route for the class (SetRoutes);
//  3. otherwise, only when the registry has no cluster routes, an engine that
//     declares the class: the default engine if it is one of them, otherwise
//     the first one registered.
//
// pm is the active PodMigration of the pod's namespace; nil means there's
// none. ForPod doesn't apply the operator's allow-list
// (--migratable-runtime-classes): callers check it first. It returns false
// when pod is nil or no engine serves the pod's class.
func (r *Registry) ForPod(pod *corev1.Pod, pm *pmv1alpha1.PodMigration) (Engine, bool) {
	if pod == nil {
		return nil, false
	}
	class := eligibility.RuntimeClassLabel(pod.Spec.RuntimeClassName)

	r.mu.RLock()
	defer r.mu.RUnlock()
	name, ok := r.resolveLocked(class, r.podMigrationRoutesLocked(pm))
	if !ok {
		return nil, false
	}
	return r.byName[name], true
}

// EnginesFor returns the engines that ForPod can pick for the pods of pm's
// namespace, in registration order. The PodMigration reconciler renders
// their namespace resources. When ForPod can pick no engine at all, it
// returns the default engine, or nothing if the default isn't registered.
func (r *Registry) EnginesFor(pm *pmv1alpha1.PodMigration) []Engine {
	r.mu.RLock()
	defer r.mu.RUnlock()
	overrides := r.podMigrationRoutesLocked(pm)
	classes := slices.Collect(maps.Keys(overrides))
	if len(r.routes) > 0 {
		classes = slices.AppendSeq(classes, maps.Keys(r.routes))
	} else {
		classes = slices.AppendSeq(classes, maps.Keys(r.byClass))
	}
	picked := map[string]bool{}
	for _, class := range classes {
		if name, ok := r.resolveLocked(class, overrides); ok {
			picked[name] = true
		}
	}

	var out []Engine
	for _, name := range r.names {
		if picked[name] {
			out = append(out, r.byName[name])
		}
	}
	if len(out) == 0 {
		if e, err := r.defaultLocked(); err == nil {
			out = append(out, e)
		}
	}
	return out
}

// ForJob returns the engine that serves job: the engine that its
// AnnotationEngine annotation names. A job without the annotation, such as
// one created before jobs recorded their engine, gets the default engine. A
// name that no registered engine has is an error that wraps
// ErrEngineNotRegistered.
func (r *Registry) ForJob(job *pmv1alpha1.PodMigrationJob) (Engine, error) {
	var name string
	if job != nil {
		name = strings.TrimSpace(job.Annotations[AnnotationEngine])
	}
	if name == "" {
		return r.Default()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.byName[name]; ok {
		return e, nil
	}
	return nil, fmt.Errorf("job %s/%s names engine %q: %w (registered: %s)",
		job.Namespace, job.Name, name, ErrEngineNotRegistered, strings.Join(r.names, ","))
}

// resolveLocked returns the name of the engine that serves class, given the
// PodMigration routes overrides, whose engines are all registered.
func (r *Registry) resolveLocked(class string, overrides Routes) (string, bool) {
	if name, ok := overrides[class]; ok {
		return name, true
	}
	if len(r.routes) > 0 {
		name, ok := r.routes[class]
		return name, ok
	}
	candidates := r.byClass[class]
	if len(candidates) == 0 {
		return "", false
	}
	if slices.Contains(candidates, r.defaultEngine) {
		return r.defaultEngine, true
	}
	return candidates[0], true
}

// podMigrationRoutesLocked returns the routes of pm's annotation that name a
// registered engine covering the class or matching cluster routes. An annotation that doesn't parse routes nothing.
func (r *Registry) podMigrationRoutesLocked(pm *pmv1alpha1.PodMigration) Routes {
	routes, err := ParseRoutes(podMigrationRoutesAnnotation(pm))
	if err != nil {
		return nil
	}
	for class, name := range routes {
		if !r.canOverrideRouteLocked(class, name) {
			delete(routes, class)
		}
	}
	return routes
}

func (r *Registry) checkRouteLocked(class, name string) error {
	if _, ok := r.byName[name]; !ok {
		return fmt.Errorf("runtime class %q routes to engine %q: %w (registered: %s)",
			class, name, ErrEngineNotRegistered, strings.Join(r.names, ","))
	}
	return nil
}

func (r *Registry) checkPodMigrationRouteLocked(class, name string) error {
	if _, ok := r.byName[name]; !ok {
		return fmt.Errorf("runtime class %q routes to engine %q: %w (registered: %s)",
			class, name, ErrEngineNotRegistered, strings.Join(r.names, ","))
	}
	if !r.canOverrideRouteLocked(class, name) {
		return fmt.Errorf("runtime class %q routes to engine %q: engine does not cover runtime class and does not match cluster route", class, name)
	}
	return nil
}

func (r *Registry) canOverrideRouteLocked(class, engineName string) bool {
	if _, ok := r.byName[engineName]; !ok {
		return false
	}
	if r.engineCoversClassLocked(engineName, class) {
		return true
	}
	if len(r.routes) > 0 && r.routes[class] == engineName {
		return true
	}
	return false
}

func (r *Registry) engineCoversClassLocked(engineName, class string) bool {
	if slices.Contains(r.byClass[class], engineName) {
		return true
	}
	if class == eligibility.DefaultRuntimeClassToken && (slices.Contains(r.byClass[""], engineName) || slices.Contains(r.byClass[eligibility.DefaultRuntimeClassToken], engineName)) {
		return true
	}
	if class == "" && (slices.Contains(r.byClass[eligibility.DefaultRuntimeClassToken], engineName) || slices.Contains(r.byClass[""], engineName)) {
		return true
	}
	e, ok := r.byName[engineName]
	if !ok {
		return false
	}
	if coverer, ok := e.(interface{ CoversRuntimeClass(string) bool }); ok {
		return coverer.CoversRuntimeClass(class)
	}
	return false
}

func podMigrationRoutesAnnotation(pm *pmv1alpha1.PodMigration) string {
	if pm == nil {
		return ""
	}
	return pm.Annotations[AnnotationRuntimeClassEngines]
}
