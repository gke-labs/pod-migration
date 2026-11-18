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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// kubeletCRIDeadline is the kubelet's default --runtime-request-timeout,
// which bounds the CRI CheckpointContainer call behind the checkpoint API.
const kubeletCRIDeadline = 2 * time.Minute

// The client must outlast the kubelet's own CRI deadline. If it gives up
// first, the kubelet cancels while the dump keeps running with the source
// frozen (#106), and the agent would report an outcome as if the source ran
// again.
func TestKubeletCheckpointClientOutlastsCRIDeadline(t *testing.T) {
	if got := newKubeletCheckpoint().Client.Timeout; got <= kubeletCRIDeadline {
		t.Fatalf("kubelet checkpoint client timeout %v, want more than the kubelet's %v CRI deadline", got, kubeletCRIDeadline)
	}
}

func testKubelet(t *testing.T, h http.HandlerFunc, timeout time.Duration) *kubeletCheckpoint {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &kubeletCheckpoint{TokenPath: token, Endpoint: srv.URL, Client: &http.Client{Timeout: timeout}}
}

func isTimeout(err error) bool {
	var te interface{ Timeout() bool }
	return errors.As(err, &te) && te.Timeout()
}

// A checkpoint call that ran out of time, on either side, is reported as a
// timeout: the dump may still be running, so the caller must not treat the
// source as running again.
func TestKubeletCheckpointTimeoutsAreMarked(t *testing.T) {
	t.Run("client timeout", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		k := testKubelet(t, func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}, 20*time.Millisecond)
		_, err := k.checkpoint(context.Background(), "default", "web-0", "app")
		if err == nil || !isTimeout(err) {
			t.Fatalf("want a timeout error, got %v", err)
		}
	})
	t.Run("kubelet CRI deadline", func(t *testing.T) {
		k := testKubelet(t, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "checkpointing of default/web-0/app failed (rpc error: code = DeadlineExceeded desc = context deadline exceeded)", http.StatusInternalServerError)
		}, time.Second)
		_, err := k.checkpoint(context.Background(), "default", "web-0", "app")
		if err == nil || !isTimeout(err) {
			t.Fatalf("want a timeout error, got %v", err)
		}
	})
	t.Run("a dump failure is not a timeout", func(t *testing.T) {
		k := testKubelet(t, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "checkpointing of default/web-0/app failed (rpc error: code = Unknown desc = criu dump failed)", http.StatusInternalServerError)
		}, time.Second)
		_, err := k.checkpoint(context.Background(), "default", "web-0", "app")
		if err == nil || isTimeout(err) {
			t.Fatalf("want a non-timeout error, got %v", err)
		}
	})
}
