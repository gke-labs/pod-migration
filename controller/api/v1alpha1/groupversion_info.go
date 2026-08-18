/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

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
