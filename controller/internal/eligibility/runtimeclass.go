// Package eligibility decides which pods the controller may migrate.
package eligibility

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// DefaultRuntimeClassToken stands for pods that set no spec.runtimeClassName
	// and therefore run on the node's default runtime handler. RuntimeClass names
	// are DNS-1123 subdomains, so the token cannot collide with a real class.
	DefaultRuntimeClassToken = "@default"

	// DefaultMigratableRuntimeClasses is the default --migratable-runtime-classes
	// value: gVisor only, the runtime GKE Pod Snapshots supports.
	DefaultMigratableRuntimeClasses = "gvisor"
)

// defaultPolicy is used by a nil *RuntimeClassPolicy.
var defaultPolicy = mustParseRuntimeClassPolicy(DefaultMigratableRuntimeClasses)

// RuntimeClassPolicy is the set of runtime classes whose pods may be migrated.
// A nil *RuntimeClassPolicy behaves like DefaultMigratableRuntimeClasses.
type RuntimeClassPolicy struct {
	names        map[string]struct{}
	allowDefault bool
}

// ParseRuntimeClassPolicy parses a comma-separated list of RuntimeClass names.
// The token @default matches pods without a runtime class. Whitespace around
// elements is ignored and duplicates are merged. Empty elements and names that
// are not valid RuntimeClass names are rejected.
func ParseRuntimeClassPolicy(raw string) (*RuntimeClassPolicy, error) {
	p := &RuntimeClassPolicy{names: map[string]struct{}{}}
	for _, elem := range strings.Split(raw, ",") {
		name := strings.TrimSpace(elem)
		switch {
		case name == "":
			return nil, fmt.Errorf("invalid runtime class list %q: empty element (use %s for pods without a runtimeClassName)", raw, DefaultRuntimeClassToken)
		case name == DefaultRuntimeClassToken:
			p.allowDefault = true
		default:
			if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
				return nil, fmt.Errorf("invalid runtime class name %q: %s", name, strings.Join(errs, "; "))
			}
			p.names[name] = struct{}{}
		}
	}
	return p, nil
}

func mustParseRuntimeClassPolicy(raw string) *RuntimeClassPolicy {
	p, err := ParseRuntimeClassPolicy(raw)
	if err != nil {
		panic(err)
	}
	return p
}

// Allows reports whether a pod with the given spec.runtimeClassName may be
// migrated. A nil or empty name means the pod sets no runtime class.
func (p *RuntimeClassPolicy) Allows(runtimeClassName *string) bool {
	if p == nil {
		p = defaultPolicy
	}
	if runtimeClassName == nil || *runtimeClassName == "" {
		return p.allowDefault
	}
	_, ok := p.names[*runtimeClassName]
	return ok
}

// String returns the policy in flag syntax: sorted class names, then @default
// if it is allowed.
func (p *RuntimeClassPolicy) String() string {
	if p == nil {
		p = defaultPolicy
	}
	out := make([]string, 0, len(p.names)+1)
	for name := range p.names {
		out = append(out, name)
	}
	sort.Strings(out)
	if p.allowDefault {
		out = append(out, DefaultRuntimeClassToken)
	}
	return strings.Join(out, ",")
}

// RuntimeClassLabel renders a pod's spec.runtimeClassName for logs and
// messages, using DefaultRuntimeClassToken when the pod sets none.
func RuntimeClassLabel(runtimeClassName *string) string {
	if runtimeClassName == nil || *runtimeClassName == "" {
		return DefaultRuntimeClassToken
	}
	return *runtimeClassName
}
