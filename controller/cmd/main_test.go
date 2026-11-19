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

package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/gke-labs/pod-migration/controller/internal/engine"
)

func TestValidateCRIUAPIGroup(t *testing.T) {
	if err := validateCRIUAPIGroup(engine.DefaultCRIUAPIGroup); err != nil {
		t.Fatalf("default CRIU API group %q rejected: %v", engine.DefaultCRIUAPIGroup, err)
	}
	if err := validateCRIUAPIGroup(engine.CRIUAPIGroup); err != nil {
		t.Fatalf("dedicated CRIU API group %q rejected: %v", engine.CRIUAPIGroup, err)
	}

	invalid := []string{
		"",
		"criu",
		"CRIU.podmigration.gke.io",
		"-bad.gke.io",
		strings.Repeat("a", 60) + "." + strings.Repeat("b", 60) + "." + strings.Repeat("c", 60) + "." + strings.Repeat("d", 45),
	}
	for _, g := range invalid {
		if err := validateCRIUAPIGroup(g); err == nil {
			t.Errorf("validateCRIUAPIGroup(%q) = nil, want error", g)
		}
	}
}

func TestMainExitsNonZeroOnInvalidCRIUAPIGroupFlag(t *testing.T) {
	if os.Getenv("TEST_SUBPROCESS_MAIN") == "1" {
		os.Args = []string{"manager", "--criu-api-group=invalid_no_dot"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestMainExitsNonZeroOnInvalidCRIUAPIGroupFlag$")
	cmd.Env = append(os.Environ(), "TEST_SUBPROCESS_MAIN=1")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1 on invalid --criu-api-group, got err=%v output=%s", err, string(out))
	}
	if !strings.Contains(string(out), "invalid --criu-api-group flag") {
		t.Errorf("expected startup error mentioning --criu-api-group, got: %s", string(out))
	}
}
