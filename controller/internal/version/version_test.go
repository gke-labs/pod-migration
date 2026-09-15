package version

import (
	"encoding/json"
	"runtime/debug"
	"strings"
	"testing"
)

func withMockBuildInfo(settings []debug.BuildSetting, fn func()) {
	origRead := readBuildInfo
	defer func() { readBuildInfo = origRead }()
	readBuildInfo = func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Settings: settings,
		}, true
	}
	fn()
}

func resetGlobals() func() {
	origV, origC, origD := Version, GitCommit, BuildDate
	return func() {
		Version, GitCommit, BuildDate = origV, origC, origD
	}
}

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

func TestGet_DefaultVersionDirty(t *testing.T) {
	defer resetGlobals()()
	Version = DefaultVersion
	GitCommit = "unknown"
	BuildDate = "unknown"

	withMockBuildInfo([]debug.BuildSetting{
		{Key: "vcs.revision", Value: "0123456789abcdef"},
		{Key: "vcs.time", Value: "2026-09-04T12:00:00Z"},
		{Key: "vcs.modified", Value: "true"},
	}, func() {
		info := Get()
		expected := DefaultVersion + "-dirty"
		if info.Version != expected {
			t.Errorf("Expected Version %q on dirty tree, got %q", expected, info.Version)
		}
		if info.GitCommit != "0123456" {
			t.Errorf("Expected GitCommit %q, got %q", "0123456", info.GitCommit)
		}
		if info.BuildDate != "2026-09-04T12:00:00Z" {
			t.Errorf("Expected BuildDate %q, got %q", "2026-09-04T12:00:00Z", info.BuildDate)
		}
	})
}

func TestGet_InjectedVersionDirty(t *testing.T) {
	defer resetGlobals()()
	Version = "v0.2.0"
	GitCommit = "unknown"
	BuildDate = "unknown"

	withMockBuildInfo([]debug.BuildSetting{
		{Key: "vcs.modified", Value: "true"},
	}, func() {
		info := Get()
		expected := "v0.2.0-dirty"
		if info.Version != expected {
			t.Errorf("Expected Version %q on dirty tree, got %q", expected, info.Version)
		}
	})
}

func TestGet_InjectedVersionAlreadyDirty(t *testing.T) {
	defer resetGlobals()()
	Version = "v0.2.0-dirty"
	GitCommit = "unknown"
	BuildDate = "unknown"

	withMockBuildInfo([]debug.BuildSetting{
		{Key: "vcs.modified", Value: "true"},
	}, func() {
		info := Get()
		expected := "v0.2.0-dirty"
		if info.Version != expected {
			t.Errorf("Expected Version %q to not become %q-dirty, got %q", expected, expected, info.Version)
		}
	})
}

func TestGet_CleanTree(t *testing.T) {
	defer resetGlobals()()

	tests := []struct {
		name     string
		inputVer string
		expected string
	}{
		{
			name:     "default version on clean tree",
			inputVer: DefaultVersion,
			expected: DefaultVersion,
		},
		{
			name:     "injected version on clean tree",
			inputVer: "v0.2.0",
			expected: "v0.2.0",
		},
		{
			name:     "injected dirty version on clean tree preserved",
			inputVer: "v0.2.0-dirty",
			expected: "v0.2.0-dirty",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			Version = tc.inputVer
			GitCommit = "unknown"
			BuildDate = "unknown"

			withMockBuildInfo([]debug.BuildSetting{
				{Key: "vcs.modified", Value: "false"},
			}, func() {
				info := Get()
				if info.Version != tc.expected {
					t.Errorf("Expected Version %q on clean tree, got %q", tc.expected, info.Version)
				}
			})
		})
	}
}

func TestGet_LdflagsInjected(t *testing.T) {
	defer resetGlobals()()
	Version = "v1.0.0"
	GitCommit = "abcdef1"
	BuildDate = "2026-09-01T00:00:00Z"

	// Even if debug.ReadBuildInfo indicates modified=true, ldflags override fallback
	withMockBuildInfo([]debug.BuildSetting{
		{Key: "vcs.modified", Value: "true"},
	}, func() {
		info := Get()
		if info.Version != "v1.0.0" {
			t.Errorf("Expected Version %q to remain unmodified when ldflags injected, got %q", "v1.0.0", info.Version)
		}
		if info.GitCommit != "abcdef1" {
			t.Errorf("Expected GitCommit %q, got %q", "abcdef1", info.GitCommit)
		}
		if info.BuildDate != "2026-09-01T00:00:00Z" {
			t.Errorf("Expected BuildDate %q, got %q", "2026-09-01T00:00:00Z", info.BuildDate)
		}
	})
}
