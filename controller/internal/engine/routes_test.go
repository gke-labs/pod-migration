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
	"reflect"
	"strings"
	"testing"
)

func TestParseRoutes(t *testing.T) {
	tests := []struct {
		raw     string
		want    Routes
		wantErr string
	}{
		{raw: "", want: Routes{}},
		{raw: "  ", want: Routes{}},
		{raw: "gvisor=gke-pod-snapshots", want: Routes{"gvisor": "gke-pod-snapshots"}},
		{raw: " gvisor = gke , @default=criu ,runc=criu", want: Routes{"gvisor": "gke", "@default": "criu", "runc": "criu"}},
		{raw: "gvisor=gke,gvisor=gke", want: Routes{"gvisor": "gke"}},
		{raw: "gvisor=gke,", wantErr: `invalid routes "gvisor=gke,": empty element`},
		{raw: ",gvisor=gke", wantErr: `invalid routes ",gvisor=gke": empty element`},
		{raw: "gvisor", wantErr: `invalid route "gvisor": want class=engine`},
		{raw: "gvisor=gke=x", wantErr: `invalid route "gvisor=gke=x": want class=engine`},
		{raw: "gvisor= ", wantErr: `invalid route "gvisor=": empty engine name`},
		{raw: "=gke", wantErr: `invalid runtime class ""`},
		{raw: "Not_A_Class=gke", wantErr: `invalid runtime class "Not_A_Class"`},
		{raw: "gvisor=gke,gvisor=criu", wantErr: `runtime class "gvisor" is routed to two engines, "gke" and "criu"`},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseRoutes(tt.raw)
			if tt.wantErr == "" {
				if err != nil || !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("ParseRoutes(%q) = %v, %v; want %v", tt.raw, got, err, tt.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ParseRoutes(%q) error = %v, want one containing %q", tt.raw, err, tt.wantErr)
			}
			if got != nil {
				t.Errorf("ParseRoutes(%q) = %v with an error, want nil", tt.raw, got)
			}
		})
	}
}

func TestRoutes_String(t *testing.T) {
	routes := Routes{"runc": "criu", "gvisor": "gke", "@default": "criu"}
	if got, want := routes.String(), "@default=criu,gvisor=gke,runc=criu"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if parsed, err := ParseRoutes(routes.String()); err != nil || !reflect.DeepEqual(parsed, routes) {
		t.Errorf("ParseRoutes(String()) = %v, %v; want %v", parsed, err, routes)
	}
	if got := Routes(nil).String(); got != "" {
		t.Errorf("Routes(nil).String() = %q, want empty", got)
	}
}

func TestRoutesFromAllowList(t *testing.T) {
	tests := []struct {
		allowList string // "" leaves the policy nil: gVisor only
		want      Routes
	}{
		{allowList: "", want: Routes{"gvisor": "e"}},
		{allowList: "@default", want: Routes{"@default": "e"}},
		{allowList: "runc,gvisor,@default", want: Routes{"gvisor": "e", "runc": "e", "@default": "e"}},
	}
	for _, tt := range tests {
		t.Run(tt.allowList, func(t *testing.T) {
			if got := RoutesFromAllowList(mustParsePolicy(t, tt.allowList), "e"); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RoutesFromAllowList(%q) = %v, want %v", tt.allowList, got, tt.want)
			}
		})
	}
}
