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

package tcpcontinuity

import (
	"encoding/json"
	"strings"
	"testing"
)

var testNonce = strings.Repeat("ab", 32)

func frameLine(sequence, counter uint64) string {
	data, _ := json.Marshal(Frame{Nonce: testNonce, Sequence: sequence, Counter: counter})
	return string(data) + "\n"
}

func TestFrameValidation(t *testing.T) {
	line := frameLine(1, 0)
	if frame, err := DecodeFrame([]byte(line)); err != nil || frame.Nonce != testNonce || frame.Sequence != 1 || frame.Counter != 0 {
		t.Fatalf("valid frame rejected: %+v, %v", frame, err)
	}
	for name, invalid := range map[string]string{
		"missing newline": strings.TrimSuffix(line, "\n"),
		"not JSON":        "bad\n",
		"not object":      "[]\n",
		"null object":     "null\n",
		"missing field":   `{"nonce":"` + testNonce + `","sequence":1}` + "\n",
		"duplicate field": strings.Replace(line, `"counter":0`, `"counter":0,"counter":1`, 1),
		"unknown field":   strings.Replace(line, `"counter":0`, `"counter":0,"extra":1`, 1),
		"null field":      strings.Replace(line, `"counter":0`, `"counter":null`, 1),
		"wrong type":      strings.Replace(line, `"counter":0`, `"counter":"0"`, 1),
		"negative":        strings.Replace(line, `"counter":0`, `"counter":-1`, 1),
		"fraction":        strings.Replace(line, `"counter":0`, `"counter":0.5`, 1),
		"overflow":        strings.Replace(line, `"counter":0`, `"counter":18446744073709551616`, 1),
		"sequence zero":   frameLine(0, 0),
		"empty nonce":     strings.Replace(line, testNonce, "", 1),
		"invalid nonce":   strings.Replace(line, testNonce, strings.Repeat("zz", 32), 1),
		"uppercase nonce": strings.Replace(line, testNonce, strings.ToUpper(testNonce), 1),
		"trailing JSON":   strings.TrimSuffix(line, "\n") + "{}\n",
		"oversized":       strings.Repeat(" ", MaxFrameBytes) + line,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeFrame([]byte(invalid)); err == nil {
				t.Fatal("invalid frame accepted")
			}
		})
	}
}

func TestAddressValidation(t *testing.T) {
	for _, address := range []string{"127.0.0.1:9090", "[::1]:9090", "192.0.2.1:8080"} {
		if err := ValidateAddress(address, false); err != nil {
			t.Fatalf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"", ":9090", "0.0.0.0:9090", "[::]:9090", "[::ffff:0.0.0.0]:9090", "224.0.0.1:9090", "localhost:9090", "127.0.0.1", "127.0.0.1:0", "127.0.0.1:-1", "127.0.0.1:65536", "127.0.0.1:http"} {
		if err := ValidateAddress(address, false); err == nil {
			t.Fatalf("invalid target accepted: %s", address)
		}
	}
	if err := ValidateAddress("127.0.0.1:0", true); err != nil {
		t.Fatalf("ephemeral listener rejected: %v", err)
	}
}
