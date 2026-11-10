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

// Package tcpcontinuity observes one real TCP stream. It performs no address
// handoff, reconnection, state import, or migration evidence verification.
package tcpcontinuity

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
)

const MaxFrameBytes = 1024

type Frame struct {
	Nonce    string `json:"nonce"`
	Sequence uint64 `json:"sequence"`
	Counter  uint64 `json:"counter"`
}

// ValidateAddress requires one concrete IP literal, avoiding DNS fallback and
// implicit wildcard binds. A peer target must not use port zero.
func ValidateAddress(address string, allowZeroPort bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("explicit IP:port required: %w", err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Unmap().IsUnspecified() || ip.Unmap().IsMulticast() {
		return errors.New("address must use a concrete, non-multicast IP literal (for a local lab use 127.0.0.1)")
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return errors.New("port must be numeric")
		}
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || (!allowZeroPort && number == 0) {
		return errors.New("port must be in range 1..65535 (listener may use 0)")
	}
	return nil
}

// DecodeFrame accepts one complete JSON line with exactly the protocol fields.
func DecodeFrame(line []byte) (Frame, error) {
	var frame Frame
	if len(line) == 0 || len(line) > MaxFrameBytes || line[len(line)-1] != '\n' {
		return frame, errors.New("frame must be a bounded, newline-terminated JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return frame, errors.New("frame must be a JSON object")
	}
	seen := map[string]bool{"nonce": false, "sequence": false, "counter": false}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return frame, fmt.Errorf("decode frame key: %w", err)
		}
		key, ok := token.(string)
		present, allowed := seen[key]
		if !ok || !allowed || present {
			return frame, fmt.Errorf("unknown or duplicate frame key %q", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return frame, fmt.Errorf("decode frame field: %w", err)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return frame, errors.New("null frame fields are not allowed")
		}
		seen[key] = true
	}
	if _, err := decoder.Token(); err != nil {
		return frame, fmt.Errorf("close frame: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return frame, errors.New("trailing frame data")
	}
	for key, present := range seen {
		if !present {
			return frame, fmt.Errorf("missing frame field %q", key)
		}
	}
	if err := json.Unmarshal(line, &frame); err != nil {
		return frame, fmt.Errorf("decode frame: %w", err)
	}
	nonce, err := hex.DecodeString(frame.Nonce)
	if err != nil || len(nonce) != 32 || hex.EncodeToString(nonce) != frame.Nonce {
		return frame, errors.New("nonce must be 32 bytes of lowercase hexadecimal")
	}
	if frame.Sequence == 0 {
		return frame, errors.New("sequence must start at one")
	}
	return frame, nil
}
