// SPDX-License-Identifier: Apache-2.0
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
