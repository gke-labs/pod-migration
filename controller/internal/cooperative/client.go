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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultHTTPTimeout = 5 * time.Second

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (c *Client) State(ctx context.Context) (State, error) {
	return c.request(ctx, http.MethodGet, "/state", nil)
}

func (c *Client) Pause(ctx context.Context, request PauseRequest) (State, error) {
	if strings.TrimSpace(request.Nonce) == "" || strings.TrimSpace(request.MigrationID) == "" {
		return State{}, fmt.Errorf("%w: nonce and migrationID are required", ErrInvalidRequest)
	}
	state, err := c.request(ctx, http.MethodPost, "/pause", request)
	if err != nil {
		return state, err
	}
	// The counter answers a late same-ID retry with its running state instead of
	// re-pausing an activated process. That reply must never read as a pause.
	if !state.Paused || state.Nonce != request.Nonce || state.MigrationID != request.MigrationID {
		return state, fmt.Errorf("%w: pause for migration %q not in effect; the process is running", ErrConflict, request.MigrationID)
	}
	return state, nil
}

func (c *Client) Resume(ctx context.Context, expected State) (State, error) {
	if err := validateExpected(expected); err != nil {
		return State{}, err
	}
	return c.request(ctx, http.MethodPost, "/resume", expected)
}

func (c *Client) Stop(ctx context.Context, expected State) (State, error) {
	if err := validateExpected(expected); err != nil {
		return State{}, err
	}
	return c.request(ctx, http.MethodPost, "/stop", expected)
}

func (c *Client) request(ctx context.Context, method, path string, body any) (State, error) {
	base, err := url.Parse(c.BaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" ||
		base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return State{}, fmt.Errorf("invalid cooperative base URL %q", c.BaseURL)
	}
	base.Path = strings.TrimRight(base.Path, "/") + path
	base.RawPath = ""
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return State{}, fmt.Errorf("encode cooperative request: %w", err)
		}
		if len(data) > maxJSONBytes {
			return State{}, fmt.Errorf("request exceeds %d bytes", maxJSONBytes)
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, base.String(), reader)
	if err != nil {
		return State{}, fmt.Errorf("create cooperative request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	client := http.Client{Timeout: defaultHTTPTimeout}
	if c.HTTPClient != nil {
		client = *c.HTTPClient
		if client.Timeout <= 0 {
			client.Timeout = defaultHTTPTimeout
		}
	}
	// Do not replay control commands at a redirected, possibly unrelated process.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return State{}, fmt.Errorf("cooperative %s %s: %w", method, path, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxJSONBytes+1))
		if readErr != nil {
			return State{}, fmt.Errorf("cooperative %s %s: HTTP %d: read error: %w", method, path, response.StatusCode, readErr)
		}
		if len(data) > maxJSONBytes {
			return State{}, fmt.Errorf("cooperative %s %s: HTTP %d: response exceeds %d bytes", method, path, response.StatusCode, maxJSONBytes)
		}
		return State{}, fmt.Errorf("cooperative %s %s: HTTP %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(data)))
	}
	var state State
	if err := decodeState(response.Body, &state); err != nil {
		return State{}, fmt.Errorf("cooperative %s %s response: %w", method, path, err)
	}
	return state, nil
}
