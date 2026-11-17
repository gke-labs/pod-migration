// SPDX-License-Identifier: Apache-2.0
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

package node

import (
	"testing"
)

func TestCheckContainerdVersion(t *testing.T) {
	tests := []struct {
		v       string
		wantErr bool
	}{
		{"containerd://2.0.2", false},
		{"containerd://2.1.0", true},
		{"containerd://2.2.6", true},
		{"containerd://2.2.7", false},
		{"containerd://2.3.1", true},
		{"containerd://2.3.4", false},
		{"containerd://3.0.0", false},
		{"1.6.20", true},
	}
	for _, tt := range tests {
		err := CheckContainerdVersion(tt.v)
		if (err != nil) != tt.wantErr {
			t.Errorf("CheckContainerdVersion(%q) err = %v, wantErr %v", tt.v, err, tt.wantErr)
		}
	}
}
