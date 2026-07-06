/*
Copyright 2026 The LPM Learning Demo Authors.

Generated-by-kubebuilder file (hand-written here for study). Defines the
GroupVersion for this API package and the SchemeBuilder used to register
all Kinds in this group/version with a runtime.Scheme.

Once registered, controller-runtime knows how to encode/decode our types
to/from the wire, and the typed client (`client.Client`) can do
`Get`/`List`/`Create` on them without per-type boilerplate.
*/

// +kubebuilder:object:generate=true
// +groupName=podmigration.gke.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group + version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "podmigration.gke.io", Version: "v1alpha1"}

	// SchemeBuilder collects the Go types in this package; AddToScheme is
	// what main.go calls to register them.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	AddToScheme = SchemeBuilder.AddToScheme
)
