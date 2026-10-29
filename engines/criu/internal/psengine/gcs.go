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

package psengine

// GCS data path for the criu-snapshot-engine: the PSSC names a bucket;
// archives must be durable there before a PodSnapshot goes Ready, because the
// source container is stopped right after. Implemented against the GCS JSON
// API with a metadata-server token — deliberately without a cloud SDK
// dependency so the module dependency tree stays lean.

import (
	"context"
	"crypto/md5" //nolint:gosec // GCS integrity metadata is MD5; not used for security
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ObjectInfo is what the store learned about a written object — used for
// upload verification without a full read-back (size + MD5 from the GCS
// insert response, compared against locally computed values).
type ObjectInfo struct {
	Bytes int64
	MD5   string // base64, as GCS reports md5Hash
}

// ObjectStore is the engine's storage seam (fake in unit tests, GCS in prod).
type ObjectStore interface {
	Put(ctx context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error)
	Get(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	Delete(ctx context.Context, bucket, object string) error
}

// GCSStore talks to the GCS JSON API using Workload Identity via the GCE
// metadata server. Base URLs are injectable for tests.
type GCSStore struct {
	HTTP       *http.Client
	UploadBase string // default https://storage.googleapis.com/upload/storage/v1
	APIBase    string // default https://storage.googleapis.com/storage/v1
	TokenURL   string // default GCE metadata server token endpoint

	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

// NewGCSStore returns a production-configured store.
func NewGCSStore() *GCSStore {
	return &GCSStore{
		HTTP:       &http.Client{Timeout: 10 * time.Minute}, // uploads scale with RSS
		UploadBase: "https://storage.googleapis.com/upload/storage/v1",
		APIBase:    "https://storage.googleapis.com/storage/v1",
		TokenURL:   "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
	}
}

func (g *GCSStore) bearer(ctx context.Context) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.token != "" && time.Now().Before(g.tokenExp.Add(-30*time.Second)) {
		return g.token, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.TokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := g.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("metadata token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata token: HTTP %d", resp.StatusCode)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return "", err
	}
	g.token = tok.AccessToken
	g.tokenExp = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)
	return g.token, nil
}

func (g *GCSStore) do(ctx context.Context, method, url string, body io.Reader) (*http.Response, error) {
	tok, err := g.bearer(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return g.HTTP.Do(req)
}

// Put streams an object up (uploadType=media) and returns the size + MD5 GCS
// recorded — the caller verifies against its own accounting.
func (g *GCSStore) Put(ctx context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	u := fmt.Sprintf("%s/b/%s/o?uploadType=media&name=%s", g.UploadBase, url.PathEscape(bucket), url.QueryEscape(object))
	resp, err := g.do(ctx, http.MethodPost, u, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("gcs put %s: HTTP %d: %s", object, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var meta struct {
		Size    string `json:"size"`
		MD5Hash string `json:"md5Hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("gcs put %s: decode response: %w", object, err)
	}
	n, _ := strconv.ParseInt(meta.Size, 10, 64)
	return &ObjectInfo{Bytes: n, MD5: meta.MD5Hash}, nil
}

// Get streams an object down.
func (g *GCSStore) Get(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	u := fmt.Sprintf("%s/b/%s/o/%s?alt=media", g.APIBase, url.PathEscape(bucket), url.PathEscape(object))
	resp, err := g.do(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("gcs get %s: HTTP %d: %s", object, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp.Body, nil
}

// Delete removes an object (used by the probe; 404 is success).
func (g *GCSStore) Delete(ctx context.Context, bucket, object string) error {
	u := fmt.Sprintf("%s/b/%s/o/%s", g.APIBase, url.PathEscape(bucket), url.PathEscape(object))
	resp, err := g.do(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("gcs delete %s: HTTP %d", object, resp.StatusCode)
	}
	return nil
}

// VerifiedPut uploads while hashing locally and fails unless GCS's recorded
// size + MD5 match — Ready must mean durably restorable.
func VerifiedPut(ctx context.Context, s ObjectStore, bucket, object string, r io.Reader) (int64, error) {
	h := md5.New() //nolint:gosec
	cr := &countingReader{r: io.TeeReader(r, h)}
	info, err := s.Put(ctx, bucket, object, cr)
	if err != nil {
		return 0, err
	}
	localMD5 := base64.StdEncoding.EncodeToString(h.Sum(nil))
	if info.Bytes != cr.n {
		return 0, fmt.Errorf("verify %s: uploaded %d bytes, GCS recorded %d", object, cr.n, info.Bytes)
	}
	if info.MD5 != "" && info.MD5 != localMD5 {
		return 0, fmt.Errorf("verify %s: local md5 %s != remote %s", object, localMD5, info.MD5)
	}
	if info.MD5 == "" {
		return 0, fmt.Errorf("verify %s: store reported no md5; refusing unverified upload", object)
	}
	return cr.n, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// SHA256Reader hashes a stream for manifest integrity fields.
func SHA256Reader(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// NewGCSProber returns the PSP reconciler's StorageProber: write, read back,
// delete a probe object under the PSSC path.
func NewGCSProber(s ObjectStore) StorageProber {
	return func(ctx context.Context, bucket, prefix string) error {
		var rnd [6]byte
		if _, err := rand.Read(rnd[:]); err != nil {
			return err
		}
		object := path.Join(prefix, ".pm-probe-"+hex.EncodeToString(rnd[:]))
		payload := []byte("pod-migrate psengine probe " + time.Now().UTC().Format(time.RFC3339))
		if _, err := VerifiedPut(ctx, s, bucket, object, strings.NewReader(string(payload))); err != nil {
			return err
		}
		rc, err := s.Get(ctx, bucket, object)
		if err != nil {
			return fmt.Errorf("probe read-back: %w", err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return err
		}
		if string(got) != string(payload) {
			return fmt.Errorf("probe read-back mismatch")
		}
		return s.Delete(ctx, bucket, object)
	}
}
