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

func TestMainExitsNonZeroOnBucketRootStorageWithoutNamespaces(t *testing.T) {
	if os.Getenv("TEST_SUBPROCESS_MAIN") == "1" {
		os.Args = []string{"manager", "--allow-bucket-root-storage=true"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestMainExitsNonZeroOnBucketRootStorageWithoutNamespaces$")
	cmd.Env = append(os.Environ(), "TEST_SUBPROCESS_MAIN=1")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1 on --allow-bucket-root-storage=true without a namespace list, got err=%v output=%s", err, string(out))
	}
	if !strings.Contains(string(out), "--allow-bucket-root-storage-namespaces") {
		t.Errorf("expected startup error naming --allow-bucket-root-storage-namespaces, got: %s", string(out))
	}
}

func TestMainExitsNonZeroOnBucketBoundToTwoNamespaces(t *testing.T) {
	if os.Getenv("TEST_SUBPROCESS_MAIN") == "1" {
		os.Args = []string{"manager", "--allow-bucket-root-storage-namespaces=team-a=shared-bucket,team-b=shared-bucket"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestMainExitsNonZeroOnBucketBoundToTwoNamespaces$")
	cmd.Env = append(os.Environ(), "TEST_SUBPROCESS_MAIN=1")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1 when one bucket is bound to two namespaces, got err=%v output=%s", err, string(out))
	}
	if !strings.Contains(string(out), "bound to two namespaces") {
		t.Errorf("expected startup error naming the doubly bound bucket, got: %s", string(out))
	}
}

func TestShouldRegisterCRIU(t *testing.T) {
	gvisorOnly, err := engine.ParseRoutes("gvisor=gke-pod-snapshots")
	if err != nil {
		t.Fatalf("ParseRoutes: %v", err)
	}
	withCRIU, err := engine.ParseRoutes("gvisor=gke-pod-snapshots,@default=criu")
	if err != nil {
		t.Fatalf("ParseRoutes: %v", err)
	}
	if shouldRegisterCRIU(false, nil) {
		t.Error("shouldRegisterCRIU(false, nil) = true, want false on stock default install")
	}
	if shouldRegisterCRIU(false, gvisorOnly) {
		t.Error("shouldRegisterCRIU(false, gvisorOnly) = true, want false when routes do not name criu")
	}
	if !shouldRegisterCRIU(true, nil) {
		t.Error("shouldRegisterCRIU(true, nil) = false, want true when --criu-api-group is explicitly set")
	}
	if !shouldRegisterCRIU(false, withCRIU) {
		t.Error("shouldRegisterCRIU(false, withCRIU) = false, want true when --runtime-class-engines names criu")
	}
}
