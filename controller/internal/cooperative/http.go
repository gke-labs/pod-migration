// Copyright 2026 Microsoft Corporation
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

package cooperative

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxJSONBytes = 16 * 1024

// Handler exposes unauthenticated control APIs. Bind it only to loopback or an
// otherwise trusted, isolated network; the nonce is an identity, not a credential.
func (c *Counter) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := http.MethodPost
		switch r.URL.Path {
		case "/state":
			method = http.MethodGet
		case "/pause", "/resume", "/stop":
		default:
			writeError(w, http.StatusNotFound, errors.New("unknown endpoint"))
			return
		}
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
			return
		}
		if r.URL.Path == "/state" {
			writeJSON(w, http.StatusOK, c.State())
			return
		}
		defer r.Body.Close()
		var state State
		var err error
		if r.URL.Path == "/pause" {
			var request PauseRequest
			if err = decodeJSON(r.Body, &request, "nonce", "migrationID"); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			state, err = c.Pause(request)
		} else {
			var expected State
			if err = decodeState(r.Body, &expected); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			if r.URL.Path == "/resume" {
				state, err = c.Resume(expected)
			} else {
				state, err = c.stop(expected)
			}
		}
		if err != nil {
			status := http.StatusConflict
			if errors.Is(err, ErrInvalidRequest) {
				status = http.StatusBadRequest
			}
			writeError(w, status, err)
			return
		}
		writeJSON(w, http.StatusOK, state)
		if r.URL.Path == "/stop" {
			// Attempt to flush even if the peer disconnected; stopping is one-way.
			_ = http.NewResponseController(w).Flush()
			c.signalDone()
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: err.Error()})
}

func decodeState(r io.Reader, state *State) error {
	if err := decodeJSON(r, state, "nonce", "counter", "paused", "epoch", "migrationID"); err != nil {
		return err
	}
	if strings.TrimSpace(state.Nonce) == "" {
		return errors.New("state nonce is required")
	}
	return nil
}

// decodeJSON accepts exactly one bounded object with all specified fields,
// rejecting duplicate/unknown keys, nulls, missing fields, and trailing data.
func decodeJSON(r io.Reader, value any, fields ...string) error {
	data, err := io.ReadAll(io.LimitReader(r, maxJSONBytes+1))
	if err != nil {
		return fmt.Errorf("read JSON: %w", err)
	}
	if len(data) > maxJSONBytes {
		return fmt.Errorf("JSON exceeds %d bytes", maxJSONBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return errors.New("expected a JSON object")
	}
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		seen[field] = false
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON key: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("expected a JSON object key")
		}
		present, allowed := seen[key]
		if !allowed || present {
			return fmt.Errorf("unknown or duplicate JSON field %q", key)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return fmt.Errorf("decode JSON field %q: %w", key, err)
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("JSON field %q must not be null", key)
		}
		seen[key] = true
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("close JSON object: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("unexpected trailing JSON data")
	}
	for _, field := range fields {
		if !seen[field] {
			return fmt.Errorf("missing JSON field %q", field)
		}
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	return nil
}
