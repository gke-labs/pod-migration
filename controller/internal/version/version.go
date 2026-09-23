package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

const DefaultVersion = "v0.1.0-dev"

var (
	// Version is the current SemVer release tag (injected via -ldflags at build time).
	Version = DefaultVersion
	// GitCommit is the git commit SHA (injected via -ldflags at build time).
	GitCommit = "unknown"
	// BuildDate is the commit timestamp (or build timestamp) in ISO 8601 (injected via -ldflags at build time).
	BuildDate = "unknown"
)

var readBuildInfo = debug.ReadBuildInfo

// Info holds structured runtime and build metadata reconciled with k8s.io/apimachinery/pkg/version.Info.
type Info struct {
	apimachineryversion.Info
	// Version is maintained as an alias to GitVersion for backward compatibility.
	Version string `json:"version,omitempty"`
}

// Get returns structured version information for the controller.
func Get() Info {
	v := Version
	c := GitCommit
	d := BuildDate

	// Fallback to runtime/debug.ReadBuildInfo if ldflags were omitted (e.g. plain go build ./cmd/)
	if c == "unknown" || c == "" || d == "unknown" || d == "" {
		if bi, ok := readBuildInfo(); ok {
			for _, setting := range bi.Settings {
				switch setting.Key {
				case "vcs.revision":
					if c == "unknown" || c == "" {
						c = setting.Value
					}
				case "vcs.time":
					if d == "unknown" || d == "" {
						d = setting.Value
					}
				case "vcs.modified":
					if setting.Value == "true" && !strings.HasSuffix(v, "-dirty") {
						v += "-dirty"
					}
				}
			}
		}
	}

	gitTreeState := "clean"
	if strings.HasSuffix(v, "-dirty") {
		gitTreeState = "dirty"
	}

	major, minor := parseMajorMinor(v)

	return Info{
		Info: apimachineryversion.Info{
			Major:        major,
			Minor:        minor,
			GitVersion:   v,
			GitCommit:    c,
			GitTreeState: gitTreeState,
			BuildDate:    d,
			GoVersion:    runtime.Version(),
			Compiler:     runtime.Compiler,
			Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		},
		Version: v,
	}
}

func parseMajorMinor(v string) (string, string) {
	clean := strings.TrimPrefix(v, "v")
	parts := strings.Split(clean, ".")
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	if len(parts) == 1 && parts[0] != "" {
		return parts[0], ""
	}
	return "", ""
}

// String returns a human-readable version string.
func (i Info) String() string {
	ver := i.GitVersion
	if ver == "" {
		ver = i.Version
	}
	return fmt.Sprintf("pod-migration-controller %s (commit: %s, built: %s, go: %s, compiler: %s, platform: %s)",
		ver, i.GitCommit, i.BuildDate, i.GoVersion, i.Compiler, i.Platform)
}
