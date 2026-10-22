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
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
)

// Routes maps runtime classes to the names of the engines that serve their
// pods. eligibility.DefaultRuntimeClassToken stands for pods without a
// runtimeClassName.
type Routes map[string]string

// ParseRoutes parses routes in the syntax of the --runtime-class-engines flag
// and the AnnotationRuntimeClassEngines annotation: comma-separated
// class=engine pairs, such as "gvisor=gke-pod-snapshots,@default=other".
// Whitespace around classes and engine names is ignored, and a blank value
// has no routes. ParseRoutes rejects an empty pair, a pair that isn't
// class=engine, an empty engine name, a class that is neither a valid
// RuntimeClass name nor eligibility.DefaultRuntimeClassToken, and a class
// routed to two engines. It doesn't check that the engines are registered.
func ParseRoutes(raw string) (Routes, error) {
	routes := Routes{}
	if strings.TrimSpace(raw) == "" {
		return routes, nil
	}
	for _, elem := range strings.Split(raw, ",") {
		pair := strings.TrimSpace(elem)
		if pair == "" {
			return nil, fmt.Errorf("invalid routes %q: empty element", raw)
		}
		class, name, ok := strings.Cut(pair, "=")
		class, name = strings.TrimSpace(class), strings.TrimSpace(name)
		if !ok || strings.Contains(name, "=") {
			return nil, fmt.Errorf("invalid route %q: want class=engine", pair)
		}
		if name == "" {
			return nil, fmt.Errorf("invalid route %q: empty engine name", pair)
		}
		if err := validateRouteClass(class); err != nil {
			return nil, err
		}
		if prev, dup := routes[class]; dup && prev != name {
			return nil, fmt.Errorf("runtime class %q is routed to two engines, %q and %q", class, prev, name)
		}
		routes[class] = name
	}
	return routes, nil
}

// String renders routes in ParseRoutes syntax, sorted by class.
func (r Routes) String() string {
	pairs := make([]string, 0, len(r))
	for _, class := range r.classes() {
		pairs = append(pairs, class+"="+r[class])
	}
	return strings.Join(pairs, ",")
}

// classes returns the routed classes, sorted.
func (r Routes) classes() []string {
	out := make([]string, 0, len(r))
	for class := range r {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
}

// RoutesFromAllowList routes every runtime class that policy allows to the
// engine named engineName; a nil policy allows gVisor only. They're the
// routes of a controller started without --runtime-class-engines: the
// default engine serves every class of the --migratable-runtime-classes
// allow-list, as the only engine did before routes.
func RoutesFromAllowList(policy *eligibility.RuntimeClassPolicy, engineName string) Routes {
	routes := Routes{}
	for _, class := range policy.Classes() {
		routes[class] = engineName
	}
	return routes
}

func validateRouteClass(class string) error {
	if class == eligibility.DefaultRuntimeClassToken {
		return nil
	}
	if errs := validation.IsDNS1123Subdomain(class); len(errs) > 0 {
		return fmt.Errorf("invalid runtime class %q: %s", class, strings.Join(errs, "; "))
	}
	return nil
}
