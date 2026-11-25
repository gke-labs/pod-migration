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

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// benchStore models a cloud object store supporting composite object composition
// and byte-range reads, with simulated streaming bandwidth.
type benchStore struct {
	mu                  sync.Mutex
	objects             map[string][]byte
	uploadedByte        int64
	streamSpeed         float64 // simulated MB/s per stream
	failOnPart          string
	failOnDelete        bool
	corruptComposedSize bool
}

func newBenchStore(streamSpeedMBps float64) *benchStore {
	return &benchStore{
		objects:     make(map[string][]byte),
		streamSpeed: streamSpeedMBps,
	}
}

func (b *benchStore) Put(ctx context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	if b.failOnPart != "" && object == b.failOnPart {
		return nil, fmt.Errorf("injected upload failure on %s", object)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// Simulate streaming transfer duration if configured
	if b.streamSpeed > 0 {
		secs := float64(len(data)) / (b.streamSpeed * 1024 * 1024)
		duration := time.Duration(secs * float64(time.Second))
		select {
		case <-time.After(duration):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	b.mu.Lock()
	b.objects[bucket+"/"+object] = data
	atomic.AddInt64(&b.uploadedByte, int64(len(data)))
	b.mu.Unlock()

	return &ObjectInfo{Bytes: int64(len(data)), MD5: md5b64(data)}, nil
}

func (b *benchStore) Get(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return b.GetRange(ctx, bucket, object, 0, -1)
}

func (b *benchStore) GetRange(ctx context.Context, bucket, object string, start, end int64) (io.ReadCloser, error) {
	b.mu.Lock()
	data, ok := b.objects[bucket+"/"+object]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("not found: %s", object)
	}
	if start > int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if end < 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}

	// Simulate download streaming latency
	if b.streamSpeed > 0 {
		length := end - start + 1
		secs := float64(length) / (b.streamSpeed * 1024 * 1024)
		duration := time.Duration(secs * float64(time.Second))
		select {
		case <-time.After(duration):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}

func (b *benchStore) Delete(_ context.Context, bucket, object string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failOnDelete {
		return fmt.Errorf("403 Forbidden: storage.objects.delete denied on %s", object)
	}
	delete(b.objects, bucket+"/"+object)
	return nil
}

func (b *benchStore) Compose(ctx context.Context, bucket, destination string, sources []string) (*ObjectInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var combined []byte
	for _, src := range sources {
		data, ok := b.objects[bucket+"/"+src]
		if !ok {
			return nil, fmt.Errorf("compose source not found: %s", src)
		}
		combined = append(combined, data...)
	}
	b.objects[bucket+"/"+destination] = combined
	size := int64(len(combined))
	if b.corruptComposedSize {
		size -= 100
	}
	return &ObjectInfo{Bytes: size, MD5: md5b64(combined)}, nil
}

func TestParallelUpload_MultiStreamCompositeAndRestoreDownload(t *testing.T) {
	store := newBenchStore(0)
	ctx := context.Background()

	// Generate 4 MiB payload
	payloadSize := int64(4 * 1024 * 1024)
	buf := make([]byte, payloadSize)
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	h := sha256.New()
	h.Write(buf)
	expectedSHA := hex.EncodeToString(h.Sum(nil))

	cfg := ParallelUploadConfig{
		MinPartSize:    1024 * 1024,
		MaxConcurrency: 4,
		ThresholdBytes: 2 * 1024 * 1024,
	}

	reader := bytes.NewReader(buf)
	n, err := ParallelVerifiedPutWithConfig(ctx, store, "test-bucket", "snaps/ps-1/checkpoint.tar", reader, payloadSize, cfg)
	if err != nil {
		t.Fatalf("ParallelVerifiedPut failed: %v", err)
	}
	if n != payloadSize {
		t.Fatalf("expected uploaded %d bytes, got %d", payloadSize, n)
	}

	// 1. Verify composed object exists
	store.mu.Lock()
	composedData, ok := store.objects["test-bucket/snaps/ps-1/checkpoint.tar"]
	store.mu.Unlock()
	if !ok {
		t.Fatal("composed object not found in store")
	}
	if int64(len(composedData)) != payloadSize {
		t.Fatalf("composed size %d != %d", len(composedData), payloadSize)
	}

	// 2. Verify temporary parts were deleted
	store.mu.Lock()
	for k := range store.objects {
		if filepath.Base(k) != "checkpoint.tar" {
			t.Fatalf("temporary part object leaked: %s", k)
		}
	}
	store.mu.Unlock()

	// 3. Test parallel download via restore reconciler fetch
	tmpDir := t.TempDir()
	dstFile := filepath.Join(tmpDir, "downloaded.tar")
	nr := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID,
		Store: store,
	}

	_, err = nr.fetch(ctx, "test-bucket", "snaps/ps-1/checkpoint.tar", dstFile, expectedSHA, payloadSize, 100*1024*1024)
	if err != nil {
		t.Fatalf("parallel fetch failed: %v", err)
	}

	downloadedBytes, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if int64(len(downloadedBytes)) != payloadSize {
		t.Fatalf("downloaded size mismatch: %d != %d", len(downloadedBytes), payloadSize)
	}
	if !bytes.Equal(downloadedBytes, buf) {
		t.Fatal("downloaded content mismatch")
	}
}

func TestParallelUpload_AbortCleansUpParts(t *testing.T) {
	store := newBenchStore(0)
	store.failOnPart = "snaps/ps-2/checkpoint.tar.part-002"
	ctx := context.Background()

	payloadSize := int64(4 * 1024 * 1024)
	buf := make([]byte, payloadSize)
	reader := bytes.NewReader(buf)

	cfg := ParallelUploadConfig{
		MinPartSize:    1024 * 1024,
		MaxConcurrency: 4,
		ThresholdBytes: 2 * 1024 * 1024,
	}

	_, err := ParallelVerifiedPutWithConfig(ctx, store, "test-bucket", "snaps/ps-2/checkpoint.tar", reader, payloadSize, cfg)
	if err == nil {
		t.Fatal("expected error on injected part failure, got nil")
	}

	// Assert zero temporary parts remained in the store
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.objects) != 0 {
		t.Fatalf("expected all parts cleaned up on abort, found %d objects: %v", len(store.objects), store.objects)
	}
}

func TestParallelUpload_FetchEnforcesMaxBytes(t *testing.T) {
	store := newBenchStore(0)
	ctx := context.Background()

	payloadSize := int64(4 * 1024 * 1024)
	buf := make([]byte, payloadSize)
	reader := bytes.NewReader(buf)

	cfg := ParallelUploadConfig{
		MinPartSize:    1024 * 1024,
		MaxConcurrency: 4,
		ThresholdBytes: 2 * 1024 * 1024,
	}

	_, err := ParallelVerifiedPutWithConfig(ctx, store, "test-bucket", "snaps/ps-limit/checkpoint.tar", reader, payloadSize, cfg)
	if err != nil {
		t.Fatalf("ParallelVerifiedPut failed: %v", err)
	}

	tmpDir := t.TempDir()
	dstFile := filepath.Join(tmpDir, "downloaded.tar")
	nr := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID,
		Store: store,
	}

	// Fetch with maxBytes smaller than payloadSize must fail fast
	maxBytes := int64(512 * 1024) // 512 KiB cap on 4 MiB payload
	_, err = nr.fetch(ctx, "test-bucket", "snaps/ps-limit/checkpoint.tar", dstFile, "", payloadSize, maxBytes)
	if err == nil {
		t.Fatal("expected fetch to fail when payloadSize > maxBytes, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds maximum allowed download size") {
		t.Fatalf("expected error mentioning exceeds maximum allowed download size, got: %v", err)
	}
}

func TestParallelUpload_FetchEnforcesSHA(t *testing.T) {
	store := newBenchStore(0)
	ctx := context.Background()

	payloadSize := int64(4 * 1024 * 1024)
	buf := make([]byte, payloadSize)
	reader := bytes.NewReader(buf)

	cfg := ParallelUploadConfig{
		MinPartSize:    1024 * 1024,
		MaxConcurrency: 4,
		ThresholdBytes: 2 * 1024 * 1024,
	}

	_, err := ParallelVerifiedPutWithConfig(ctx, store, "test-bucket", "snaps/ps-sha/checkpoint.tar", reader, payloadSize, cfg)
	if err != nil {
		t.Fatalf("ParallelVerifiedPut failed: %v", err)
	}

	tmpDir := t.TempDir()
	dstFile := filepath.Join(tmpDir, "downloaded.tar")
	nr := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID,
		Store: store,
	}

	// Fetch with bogus expected SHA must fail
	_, err = nr.fetch(ctx, "test-bucket", "snaps/ps-sha/checkpoint.tar", dstFile, "badbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadb", payloadSize, 100*1024*1024)
	if err == nil {
		t.Fatal("expected fetch to fail on SHA mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("expected SHA mismatch error, got: %v", err)
	}
}

func TestParallelUpload_CleanupFailureDoesNotFailUpload(t *testing.T) {
	store := newBenchStore(0)
	store.failOnDelete = true // simulate missing storage.objects.delete IAM permission on node agent
	ctx := context.Background()

	payloadSize := int64(4 * 1024 * 1024)
	buf := make([]byte, payloadSize)
	reader := bytes.NewReader(buf)

	cfg := ParallelUploadConfig{
		MinPartSize:    1024 * 1024,
		MaxConcurrency: 4,
		ThresholdBytes: 2 * 1024 * 1024,
	}

	n, err := ParallelVerifiedPutWithConfig(ctx, store, "test-bucket", "snaps/ps-leak/checkpoint.tar", reader, payloadSize, cfg)
	if err != nil {
		t.Fatalf("expected upload to succeed despite part cleanup failure (best-effort cleanup), got: %v", err)
	}
	if n != payloadSize {
		t.Fatalf("uploaded %d bytes, want %d", n, payloadSize)
	}
	if _, ok := store.objects["test-bucket/snaps/ps-leak/checkpoint.tar"]; !ok {
		t.Fatal("expected composed checkpoint.tar to exist")
	}
}

func TestParallelUpload_ComposedSizeMismatchFails(t *testing.T) {
	store := newBenchStore(0)
	store.corruptComposedSize = true
	ctx := context.Background()

	payloadSize := int64(4 * 1024 * 1024)
	buf := make([]byte, payloadSize)
	reader := bytes.NewReader(buf)

	cfg := ParallelUploadConfig{
		MinPartSize:    1024 * 1024,
		MaxConcurrency: 4,
		ThresholdBytes: 2 * 1024 * 1024,
	}

	_, err := ParallelVerifiedPutWithConfig(ctx, store, "test-bucket", "snaps/ps-size/checkpoint.tar", reader, payloadSize, cfg)
	if err == nil {
		t.Fatal("expected error on composed size mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "verify composed") {
		t.Fatalf("expected error mentioning verify composed, got: %v", err)
	}
}

func TestNodePipeline_ParallelUploadAndRestoreEndToEnd(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	// The work order answers a trigger of newClient's test-pmj, so the
	// archive is bound to the job that restores it (#244).
	_ = unstructured.SetNestedField(ps.Object, cancelTrigger, "spec", specTriggerName)
	_ = unstructured.SetNestedField(ps.Object, "trigger-uid-1", "spec", specTriggerUID)
	trigger := psmtWithUID("web-0", "trigger-uid-1")
	trigger.SetOwnerReferences([]metav1.OwnerReference{testJobControllerRef()})
	c := newClient(t, ps, trigger)
	payload := strings.Repeat("PARALLEL_DATA_1234567890", 100) // 2400 bytes
	ops := &fakeNodeOps{tarContent: payload, t: t}
	store := newMemStore()
	cfg := ParallelUploadConfig{
		ThresholdBytes: 500,
		MinPartSize:    200,
		MaxConcurrency: 4,
	}
	r := &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID,
		Client:               c,
		NodeName:             "node-a",
		Ops:                  ops,
		Store:                store,
		CPUFamily:            "Intel(R) Xeon(R) test",
		ParallelUploadConfig: cfg,
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}

	// Verify checkpoint.tar exists and matches payload
	if string(store.objects["bkt/snaps/ps-web-0-deadbeef/checkpoint.tar"]) != payload {
		t.Fatal("tar content mismatch")
	}

	// Assert Compose was actually called on the store for the parallel upload path
	if store.composeCalls == 0 {
		t.Fatal("expected Compose to be called during parallel upload")
	}

	// Verify temporary parts were cleaned up
	for k := range store.objects {
		if strings.Contains(k, ".part-") {
			t.Fatalf("part leaked in store: %s", k)
		}
	}

	// Setup replacement pod and snapshot Ready condition for restore reconciliation
	if err := contract.SetCondition(ps, metav1.Condition{
		Type:   contract.CondReady,
		Status: metav1.ConditionTrue,
		Reason: contract.ReasonSucceeded,
	}); err != nil {
		t.Fatal(err)
	}
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c = newClient(t, ps, pod)

	restoreOps := &fakeRestoreOps{}
	nr := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID,
		Client:               c,
		NodeName:             "node-a",
		Ops:                  restoreOps,
		Store:                store,
		WorkDir:              t.TempDir(),
		ParallelUploadConfig: cfg,
	}

	if _, err := nr.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatalf("reconcile restore: %v", err)
	}

	// Assert GetRange was called during restore through the production Reconcile path
	if store.rangeCalls == 0 {
		t.Fatal("expected GetRange to be called for parallel fetch during restore reconciliation")
	}

	if len(restoreOps.restored) != 1 {
		t.Fatalf("restore calls: %d", len(restoreOps.restored))
	}
	gotPod := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, gotPod); err != nil {
		t.Fatal(err)
	}
	if gotPod.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusRestored {
		t.Fatalf("restore status: %q", gotPod.Annotations[contract.AnnotationRestoreStatus])
	}
}
