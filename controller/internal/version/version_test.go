package version

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGet(t *testing.T) {
	info := Get()
	if info.Version == "" {
		t.Errorf("Expected Version to not be empty")
	}
	if info.GitCommit == "" {
		t.Errorf("Expected GitCommit to not be empty")
	}
	if info.BuildDate == "" {
		t.Errorf("Expected BuildDate to not be empty")
	}
	if info.GoVersion == "" {
		t.Errorf("Expected GoVersion to not be empty")
	}
	if info.Compiler == "" {
		t.Errorf("Expected Compiler to not be empty")
	}
	if info.Platform == "" {
		t.Errorf("Expected Platform to not be empty")
	}

	str := info.String()
	if !strings.Contains(str, "pod-migration-controller") {
		t.Errorf("Expected String() to contain 'pod-migration-controller', got %s", str)
	}
	if !strings.Contains(str, info.Version) {
		t.Errorf("Expected String() to contain Version %s, got %s", info.Version, str)
	}
	if !strings.Contains(str, info.Compiler) {
		t.Errorf("Expected String() to contain Compiler %s, got %s", info.Compiler, str)
	}

	// Verify custom struct formatting
	customInfo := Info{
		Version:   "v1.2.3",
		GitCommit: "abcdef1",
		BuildDate: "2026-09-01T12:00:00Z",
		GoVersion: "go1.24.0",
		Compiler:  "gc",
		Platform:  "linux/amd64",
	}
	customStr := customInfo.String()
	expectedSubstrings := []string{"v1.2.3", "abcdef1", "2026-09-01T12:00:00Z", "go1.24.0", "gc", "linux/amd64"}
	for _, sub := range expectedSubstrings {
		if !strings.Contains(customStr, sub) {
			t.Errorf("Expected custom String() to contain %s, got %s", sub, customStr)
		}
	}

	// Verify JSON marshaling & unmarshaling
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Failed to marshal version Info to JSON: %v", err)
	}
	var unmarshaled Info
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal version Info JSON: %v", err)
	}
	if unmarshaled.Version != info.Version {
		t.Errorf("Expected unmarshaled Version %s, got %s", info.Version, unmarshaled.Version)
	}
	if unmarshaled.Platform != info.Platform {
		t.Errorf("Expected unmarshaled Platform %s, got %s", info.Platform, unmarshaled.Platform)
	}
}

func TestGet_InjectedVersionNotMutated(t *testing.T) {
	origVersion := Version
	origCommit := GitCommit
	origDate := BuildDate
	defer func() {
		Version = origVersion
		GitCommit = origCommit
		BuildDate = origDate
	}()

	Version = "v0.1.0-custom"
	GitCommit = "unknown"
	BuildDate = "unknown"

	info := Get()
	if info.Version != "v0.1.0-custom" {
		t.Errorf("Expected Version to remain 'v0.1.0-custom', got %s", info.Version)
	}
}
