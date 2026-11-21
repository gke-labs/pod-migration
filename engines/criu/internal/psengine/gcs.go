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
	"bytes"
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

// CompositeObjectStore is an ObjectStore that supports server-side composition
// of multiple uploaded part objects into a single destination object.
type CompositeObjectStore interface {
	ObjectStore
	Compose(ctx context.Context, bucket, destination string, sources []string) (*ObjectInfo, error)
}

// RangeObjectStore is an ObjectStore that supports byte-range GETs.
type RangeObjectStore interface {
	ObjectStore
	GetRange(ctx context.Context, bucket, object string, start, end int64) (io.ReadCloser, error)
}

// GCSStore talks to the GCS JSON API using the GCE metadata server's default
// service account token (note: because the node agent runs with hostNetwork,
// GKE Workload Identity does not intercept metadata requests; dedicated
// credentials are tracked in #129 R5). Base URLs are injectable for tests.
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
	return g.GetRange(ctx, bucket, object, 0, -1)
}

// GetRange streams down a byte range [start, end] inclusive (-1 end indicates to EOF).
func (g *GCSStore) GetRange(ctx context.Context, bucket, object string, start, end int64) (io.ReadCloser, error) {
	u := fmt.Sprintf("%s/b/%s/o/%s?alt=media", g.APIBase, url.PathEscape(bucket), url.PathEscape(object))
	tok, err := g.bearer(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	wantStatus := http.StatusOK
	if end >= 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		wantStatus = http.StatusPartialContent
	} else if start > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
		wantStatus = http.StatusPartialContent
	}
	resp, err := g.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != wantStatus {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("gcs get range %s: HTTP %d (want %d): %s", object, resp.StatusCode, wantStatus, strings.TrimSpace(string(b)))
	}
	return resp.Body, nil
}

// Compose combines up to 32 source objects into a single destination object
// using the GCS compose API (POST /b/{bucket}/o/{destination}/compose).
func (g *GCSStore) Compose(ctx context.Context, bucket, destination string, sources []string) (*ObjectInfo, error) {
	u := fmt.Sprintf("%s/b/%s/o/%s/compose", g.APIBase, url.PathEscape(bucket), url.PathEscape(destination))
	type sourceObj struct {
		Name string `json:"name"`
	}
	type composeReq struct {
		SourceObjects []sourceObj `json:"sourceObjects"`
		Destination   struct {
			ContentType string `json:"contentType,omitempty"`
		} `json:"destination,omitempty"`
	}
	reqBody := composeReq{
		SourceObjects: make([]sourceObj, len(sources)),
	}
	for i, s := range sources {
		reqBody.SourceObjects[i] = sourceObj{Name: s}
	}
	reqBody.Destination.ContentType = "application/x-tar"

	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("compose request encode: %w", err)
	}

	tok, err := g.bearer(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(jsonBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("gcs compose %s: HTTP %d: %s", destination, resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var meta struct {
		Size    string `json:"size"`
		MD5Hash string `json:"md5Hash"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("gcs compose %s: decode response: %w", destination, err)
	}
	n, _ := strconv.ParseInt(meta.Size, 10, 64)
	return &ObjectInfo{Bytes: n, MD5: meta.MD5Hash}, nil
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

// ParallelUploadConfig controls multi-stream chunked uploads.
type ParallelUploadConfig struct {
	MinPartSize    int64 // minimum part size (default: 16 MiB)
	MaxConcurrency int   // maximum concurrent upload streams (default: 8)
	ThresholdBytes int64 // minimum file size to trigger parallel multi-stream upload (default: 32 MiB)
}

// DefaultParallelUploadConfig provides concurrency defaults for parallel
// object-storage uploads.
var DefaultParallelUploadConfig = ParallelUploadConfig{
	MinPartSize:    16 * 1024 * 1024, // 16 MiB
	MaxConcurrency: 8,                // up to 8 parallel streams
	ThresholdBytes: 32 * 1024 * 1024, // 32 MiB
}

// ParallelVerifiedPut uploads a reader in parallel multi-stream chunks using
// composite objects if supported by the store, falling back to VerifiedPut.
func ParallelVerifiedPut(ctx context.Context, s ObjectStore, bucket, object string, r io.ReaderAt, size int64) (int64, error) {
	return ParallelVerifiedPutWithConfig(ctx, s, bucket, object, r, size, DefaultParallelUploadConfig)
}

// ParallelVerifiedPutWithConfig performs parallel composite upload using explicit configuration.
func ParallelVerifiedPutWithConfig(ctx context.Context, s ObjectStore, bucket, object string, r io.ReaderAt, size int64, cfg ParallelUploadConfig) (int64, error) {
	compStore, isComposite := s.(CompositeObjectStore)
	if !isComposite || size < cfg.ThresholdBytes || cfg.MaxConcurrency <= 1 {
		return VerifiedPut(ctx, s, bucket, object, io.NewSectionReader(r, 0, size))
	}

	minPartSize := cfg.MinPartSize
	if minPartSize <= 0 {
		minPartSize = DefaultParallelUploadConfig.MinPartSize
	}
	maxConcurrency := cfg.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = DefaultParallelUploadConfig.MaxConcurrency
	}

	numParts := int(size / minPartSize)
	if numParts > maxConcurrency {
		numParts = maxConcurrency
	}
	if numParts > 32 { // GCS compose limit
		numParts = 32
	}
	if numParts <= 1 {
		return VerifiedPut(ctx, s, bucket, object, io.NewSectionReader(r, 0, size))
	}

	type partInfo struct {
		index  int
		name   string
		offset int64
		bytes  int64
	}

	partSize := size / int64(numParts)
	parts := make([]partInfo, numParts)
	partNames := make([]string, numParts)
	for i := 0; i < numParts; i++ {
		offset := int64(i) * partSize
		b := partSize
		if i == numParts-1 {
			b = size - offset
		}
		name := fmt.Sprintf("%s.part-%03d", object, i)
		parts[i] = partInfo{index: i, name: name, offset: offset, bytes: b}
		partNames[i] = name
	}

	uploadCtx, cancelUpload := context.WithCancel(ctx)
	defer cancelUpload()

	cleanupParts := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, name := range partNames {
			_ = s.Delete(cleanupCtx, bucket, name)
		}
	}

	// Concurrently upload parts
	errCh := make(chan error, numParts)
	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		go func(p partInfo) {
			defer wg.Done()
			secReader := io.NewSectionReader(r, p.offset, p.bytes)
			n, err := VerifiedPut(uploadCtx, s, bucket, p.name, secReader)
			if err != nil {
				cancelUpload()
				errCh <- fmt.Errorf("upload part %d (%s): %w", p.index, p.name, err)
				return
			}
			if n != p.bytes {
				cancelUpload()
				errCh <- fmt.Errorf("upload part %d (%s): uploaded %d bytes, want %d", p.index, p.name, n, p.bytes)
				return
			}
		}(p)
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		cleanupParts()
		return 0, err
	}

	// Compose the parts into the destination object
	compInfo, err := compStore.Compose(ctx, bucket, object, partNames)
	cleanupParts()
	if err != nil {
		return 0, fmt.Errorf("compose %s: %w", object, err)
	}

	if compInfo.Bytes != size {
		return 0, fmt.Errorf("verify composed %s: expected %d bytes, got %d", object, size, compInfo.Bytes)
	}

	return size, nil
}
