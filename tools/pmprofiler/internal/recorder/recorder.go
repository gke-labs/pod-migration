// Package recorder provides a crash-safe, thread-safe NDJSON event log for a
// profiling run. Every record is a single JSON line with a wall-clock
// timestamp assigned at write time; the file is flushed on every write so a
// killed collector loses at most the record being written.
package recorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record is one line in records.ndjson.
type Record struct {
	// TS is the collector-side receipt time (RFC3339Nano, UTC).
	TS string `json:"ts"`
	// Type is one of: list, add, update, delete, podmetrics, nodemetrics,
	// gcs, meta, error.
	Type string `json:"type"`
	// GVR identifies the resource for object records, e.g.
	// "podmigrationjobs.v1alpha1.podmigration.gke.io".
	GVR string `json:"gvr,omitempty"`
	// Obj is the (pruned) object for object records, or an arbitrary
	// payload for sampler records.
	Obj json.RawMessage `json:"obj,omitempty"`
}

// Recorder appends records to <dir>/records.ndjson.
type Recorder struct {
	mu sync.Mutex
	f  *os.File
}

// Open creates dir if needed and opens records.ndjson for appending.
func Open(dir string) (*Recorder, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "records.ndjson"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Recorder{f: f}, nil
}

// Write appends one record, stamping it with the current time.
func (r *Recorder) Write(typ, gvr string, obj any) error {
	raw, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal %s record: %w", typ, err)
	}
	line, err := json.Marshal(Record{
		TS:   time.Now().UTC().Format(time.RFC3339Nano),
		Type: typ,
		GVR:  gvr,
		Obj:  raw,
	})
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.f.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

// Error records a collector-side error without interrupting collection.
func (r *Recorder) Error(context string, err error) {
	_ = r.Write("error", "", map[string]string{
		"context": context, "error": err.Error(),
	})
}

// Close closes the underlying file.
func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.f.Close()
}
