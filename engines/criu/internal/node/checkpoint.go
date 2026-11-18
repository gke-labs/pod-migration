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
	"archive/tar"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// kubeletCheckpoint drives POST /checkpoint/{ns}/{pod}/{container} on the
// node-local kubelet (agent runs hostNetwork, so localhost:10250) using the
// mounted ServiceAccount token (RBAC: nodes/checkpoint create only).
type kubeletCheckpoint struct {
	TokenPath string // default /var/run/secrets/kubernetes.io/serviceaccount/token
	Endpoint  string // default https://localhost:10250
	Client    *http.Client
}

// kubeletCheckpointTimeout outlasts the kubelet's own deadline on the CRI
// checkpoint call (its --runtime-request-timeout, 2 min by default), with a
// margin for the response. The kubelet runs that call on the request's
// context, so a client that gives up first cancels it while the dump keeps
// running with the source frozen, and the agent would hear back before the
// source runs again (#106). A kubelet configured with a longer runtime
// request timeout than this brings that back.
const kubeletCheckpointTimeout = 150 * time.Second

func newKubeletCheckpoint() *kubeletCheckpoint {
	return &kubeletCheckpoint{
		TokenPath: "/var/run/secrets/kubernetes.io/serviceaccount/token",
		Endpoint:  "https://localhost:10250",
		Client: &http.Client{
			Timeout: kubeletCheckpointTimeout,
			// kubelet serves a self-signed serving cert; authn is the bearer
			// token, authz is nodes/checkpoint.
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		},
	}
}

// checkpointTimeoutError marks a checkpoint call that ran out of time: the
// client's timeout, or the kubelet's CRI deadline. The dump may still be
// running, and past the CRI deadline containerd can leave the source frozen
// until it is resumed by hand (#106). Callers find it through
// errors.As(err, &interface{ Timeout() bool }).
type checkpointTimeoutError struct{ err error }

func (e *checkpointTimeoutError) Error() string { return e.err.Error() }
func (e *checkpointTimeoutError) Unwrap() error { return e.err }
func (e *checkpointTimeoutError) Timeout() bool { return true }

type checkpointResult struct {
	TarPath    string
	TarBytes   int64
	DurationMs int64
	HTTPStatus int
}

func (k *kubeletCheckpoint) checkpoint(ctx context.Context, ns, pod, container string) (*checkpointResult, error) {
	token, err := os.ReadFile(k.TokenPath)
	if err != nil {
		return nil, fmt.Errorf("read SA token: %w", err)
	}
	url := fmt.Sprintf("%s/checkpoint/%s/%s/%s", k.Endpoint, ns, pod, container)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))

	start := time.Now()
	resp, err := k.Client.Do(req)
	if err != nil {
		err = fmt.Errorf("kubelet checkpoint: %w", err)
		var te interface{ Timeout() bool }
		if errors.As(err, &te) && te.Timeout() {
			return nil, &checkpointTimeoutError{err}
		}
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	res := &checkpointResult{DurationMs: time.Since(start).Milliseconds(), HTTPStatus: resp.StatusCode}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("kubelet checkpoint HTTP %d: %s", resp.StatusCode, truncate(string(body), 300))
		// The kubelet reports its CRI deadline as a gRPC DeadlineExceeded
		// inside a 500.
		if resp.StatusCode == http.StatusInternalServerError &&
			(strings.Contains(string(body), "DeadlineExceeded") || strings.Contains(string(body), "context deadline exceeded")) {
			return res, &checkpointTimeoutError{err}
		}
		return res, err
	}
	var out struct {
		Items []string `json:"items"`
	}
	if err := json.Unmarshal(body, &out); err != nil || len(out.Items) == 0 {
		return res, fmt.Errorf("kubelet checkpoint: unparseable response %q", truncate(string(body), 300))
	}
	res.TarPath = out.Items[0]
	if fi, err := os.Stat(res.TarPath); err == nil {
		res.TarBytes = fi.Size()
	}
	return res, nil
}

func checkSafeTarHeader(hdr *tar.Header) error {
	name := filepath.Clean(hdr.Name)
	if strings.HasPrefix(name, "..") || filepath.IsAbs(name) {
		return fmt.Errorf("checkpoint tar has unsafe path %q", hdr.Name)
	}
	if hdr.Typeflag == tar.TypeGNUSparse {
		return fmt.Errorf("sparse tar entry %q not supported", hdr.Name)
	}
	if hdr.Typeflag != tar.TypeDir && hdr.Typeflag != tar.TypeReg {
		return fmt.Errorf("unsupported tar entry type: %v", hdr.Typeflag)
	}
	for k := range hdr.PAXRecords {
		if strings.HasPrefix(k, "GNU.sparse.") || k == "GNU.sparse" {
			return fmt.Errorf("PAX sparse tar entry %q not supported", hdr.Name)
		}
	}
	return nil
}

// validateCheckpointTarFile lists the tar's entries and verifies that the
// required CRIU checkpoint files are present without extracting.
func validateCheckpointTarFile(tarPath string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()
	var entries []string
	tr := tar.NewReader(io.LimitReader(f, 8<<30))
	var entryCount int
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		entryCount++
		if entryCount > 4096 {
			return fmt.Errorf("tar file has more than 4096 entries")
		}
		if err != nil {
			return fmt.Errorf("read checkpoint tar: %w", err)
		}
		if err := checkSafeTarHeader(hdr); err != nil {
			return err
		}
		entries = append(entries, hdr.Name)
	}
	return ValidateCheckpointTar(entries)
}

// extractCheckpoint validates the tar and unpacks it into dst, returning the
// entry list.
func extractCheckpoint(tarPath, dst string) ([]string, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []string
	tr := tar.NewReader(io.LimitReader(f, 8<<30))
	var entryCount int
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		entryCount++
		if entryCount > 4096 {
			return entries, fmt.Errorf("tar file has more than 4096 entries")
		}
		if err != nil {
			return entries, fmt.Errorf("read checkpoint tar: %w", err)
		}
		if err := checkSafeTarHeader(hdr); err != nil {
			return entries, err
		}
		name := filepath.Clean(hdr.Name)
		entries = append(entries, hdr.Name)
		target := filepath.Join(dst, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return entries, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return entries, err
			}
			w, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return entries, err
			}
			if _, err := io.Copy(w, tr); err != nil {
				w.Close()
				return entries, err
			}
			w.Close()
		}
	}
	if err := ValidateCheckpointTar(entries); err != nil {
		return entries, err
	}
	return entries, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
