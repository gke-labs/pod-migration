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

// holder is the target-pod placeholder binary.
//
// Modes:
//   - `/holder` (default): pause forever in the target pod's app container so
//     the kubelet keeps the pod sandbox (network/IPC/UTS namespaces + cgroup
//     slice) alive while the agent restores the real workload as a sibling
//     container inside those namespaces. It never opens a socket.
//   - `/holder seed <dst>`: copy this static binary to <dst> and chmod 0755.
//     Used by the seeded initContainer so the app container can run its own
//     image while executing `/pmhold/holder` as its entrypoint.
//   - `/holder wrap <annotations-file> -- <orig-cmd...>`: psengine mode. Wait
//     briefly for a `podsnapshot.gke.io/ps-name` annotation via the downward
//     API; if a snapshot is assigned, hold while the node agent restores a
//     sibling (falling through to cold-start exec if the agent stamps
//     restore=failed or times out); if no snapshot is assigned, exec the
//     original command immediately (zero-regression cold start).
package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) >= 3 && os.Args[1] == "seed" {
		if err := seedSelf(os.Args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "holder seed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) >= 4 && os.Args[1] == "wrap" {
		runWrap(os.Args[2:])
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
}

func seedSelf(dst string) error {
	src, err := os.Open("/proc/self/exe")
	if err != nil {
		return err
	}
	defer src.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, src); err != nil {
		return err
	}
	return out.Close()
}
