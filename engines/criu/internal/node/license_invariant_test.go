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

// License boundary test: CRIU is exec'd as an external subprocess, never
// linked. go.mod must never acquire a CRIU Go binding or a copyleft module.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoCriuOrCopyleftInGoMod(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	mod := strings.ToLower(string(b))
	for _, forbidden := range []string{"go-criu", "checkpoint-restore/criu", "libcriu"} {
		if strings.Contains(mod, forbidden) {
			t.Errorf("go.mod references %q — criu must stay an exec-only subprocess", forbidden)
		}
	}
}
