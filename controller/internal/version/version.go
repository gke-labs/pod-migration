package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
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

// Info holds structured runtime and build metadata.
type Info struct {
	Version   string `json:"version"`
	GitCommit string `json:"gitCommit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
	Compiler  string `json:"compiler"`
	Platform  string `json:"platform"`
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
						if len(setting.Value) > 7 {
							c = setting.Value[:7]
						} else {
							c = setting.Value
						}
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

	return Info{
		Version:   v,
		GitCommit: c,
		BuildDate: d,
		GoVersion: runtime.Version(),
		Compiler:  runtime.Compiler,
		Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// String returns a human-readable version string.
func (i Info) String() string {
	return fmt.Sprintf("pod-migration-controller %s (commit: %s, built: %s, go: %s, compiler: %s, platform: %s)",
		i.Version, i.GitCommit, i.BuildDate, i.GoVersion, i.Compiler, i.Platform)
}
