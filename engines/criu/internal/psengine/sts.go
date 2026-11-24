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

// Bucket credentials for the node agent (contract clause C10 R5, #244). The
// agent runs with hostNetwork, where the GKE metadata server can't tell its
// requests from the node's and answers as the node's VM service account,
// which every workload on the node can use too. So the agent exchanges its
// own projected Kubernetes service account token at Google's Security Token
// Service for an access token of that service account's Workload Identity
// principal, which the bucket grants directly: no Google service account,
// no impersonation.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// DefaultGCSTokenFile is where the agent's DaemonSet mounts the projected
// service account token that the agent exchanges for bucket access.
const DefaultGCSTokenFile = "/var/run/secrets/pod-migrate/gcs-token/token"

// DefaultSTSEndpoint is the token endpoint of Google's Security Token Service.
const DefaultSTSEndpoint = "https://sts.googleapis.com/v1/token"

const (
	// gcsScope is what an exchanged token may do: read, write and delete
	// objects, which checkpoints, restores and upload cleanup need.
	gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

	// stsRefreshMargin is how long before a token expires the store gets the
	// next one. A request to GCS authenticates once, when it starts, so the
	// margin only has to cover clock skew between the node and Google and
	// the time a request takes to get there, which are seconds. Five minutes
	// covers both with room to spare and costs about one exchange an hour
	// per agent. A token that lives less than twice the margin is refreshed
	// halfway through its life instead.
	stsRefreshMargin = 5 * time.Minute

	// stsMaxResponse caps how much of an STS response the agent reads.
	stsMaxResponse = 64 << 10
)

// stsTimeout bounds each exchange. STS answers in well under a second; the
// bound matters because the store holds its mutex during an exchange, so
// every other bucket call of the agent waits behind it. A variable for
// tests.
var stsTimeout = 10 * time.Second

// STSTokenSource gets access tokens for the GCS JSON API as the pod's own
// Kubernetes service account: it exchanges the pod's projected service
// account token at Google's Security Token Service, which trusts the
// cluster's tokens through its Workload Identity pool. Errors never carry
// the service account token or an access token.
type STSTokenSource struct {
	// TokenFile is the projected service account token, with the workload
	// pool as its audience. It is read again for every exchange: the kubelet
	// rotates it.
	TokenFile string
	// WorkloadPool is the cluster's Workload Identity pool,
	// <PROJECT_ID>.svc.id.goog.
	WorkloadPool string
	// IdentityProvider is the cluster's identity provider,
	// https://container.googleapis.com/v1/projects/<PROJECT_ID>/locations/<LOCATION>/clusters/<CLUSTER>.
	IdentityProvider string
	// Endpoint is the STS token endpoint. Empty means DefaultSTSEndpoint.
	Endpoint string
	// HTTP makes the exchanges, each bounded by stsTimeout, and never follows
	// a redirect. Nil means a client of its own, never the store's long-lived
	// upload client.
	HTTP *http.Client

	now func() time.Time // nil means time.Now; tests set it
}

var _ TokenSource = (*STSTokenSource)(nil)

// stsRequest is an OAuth 2.0 token exchange (RFC 8693), in the JSON form
// that Google's STS accepts.
type stsRequest struct {
	GrantType          string `json:"grantType"`
	Audience           string `json:"audience"`
	Scope              string `json:"scope"`
	RequestedTokenType string `json:"requestedTokenType"`
	SubjectToken       string `json:"subjectToken"`
	SubjectTokenType   string `json:"subjectTokenType"`
}

var defaultSTSClient = &http.Client{}

// refuseRedirects stops a client from following a redirect: on a 307 or 308
// it would send the request body, and with it the service account token, to
// wherever the redirect points. The redirect itself is the response then,
// which isn't a 200, so the exchange fails.
func refuseRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// Token exchanges the current service account token for an access token,
// and says when to refresh it: stsRefreshMargin before it expires.
func (s *STSTokenSource) Token(ctx context.Context) (string, time.Time, error) {
	subject, err := ReadSubjectToken(s.TokenFile)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sts token exchange: %w", err)
	}
	body, err := json.Marshal(stsRequest{
		GrantType:          "urn:ietf:params:oauth:grant-type:token-exchange",
		Audience:           "identitynamespace:" + s.WorkloadPool + ":" + s.IdentityProvider,
		Scope:              gcsScope,
		RequestedTokenType: "urn:ietf:params:oauth:token-type:access_token",
		SubjectToken:       subject,
		SubjectTokenType:   "urn:ietf:params:oauth:token-type:jwt",
	})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sts token exchange: encode the request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, stsTimeout)
	defer cancel()
	endpoint := s.Endpoint
	if endpoint == "" {
		endpoint = DefaultSTSEndpoint
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sts token exchange: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := *defaultSTSClient
	if s.HTTP != nil {
		client = *s.HTTP
	}
	client.CheckRedirect = refuseRedirects
	// The lifetime counts from before the request: the token can only
	// expire later than that, never sooner.
	issued := s.clock()
	resp, err := client.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sts token exchange: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, stsMaxResponse))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sts token exchange: HTTP %d, reading the response: %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, stsError(resp.StatusCode, raw, subject)
	}
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		// Not the decoder's error: it can quote the response, which holds
		// the access token.
		return "", time.Time{}, fmt.Errorf("sts token exchange: HTTP %d, but the response isn't the JSON that STS returns", resp.StatusCode)
	}
	if out.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("sts token exchange: HTTP %d, but the response has no access_token", resp.StatusCode)
	}
	return out.AccessToken, issued.Add(refreshAfter(time.Duration(out.ExpiresIn) * time.Second)), nil
}

func (s *STSTokenSource) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// refreshAfter is how long after it was issued a token of the given lifetime
// is refreshed: stsRefreshMargin before it expires, or halfway through a
// lifetime shorter than twice the margin. A token without a lifetime isn't
// reused.
func refreshAfter(lifetime time.Duration) time.Duration {
	if lifetime < 2*stsRefreshMargin {
		return lifetime / 2
	}
	return lifetime - stsRefreshMargin
}

// ReadSubjectToken reads the projected service account token at path.
func ReadSubjectToken(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read the projected service account token: %w", err)
	}
	tok := strings.TrimSpace(string(raw))
	if tok == "" {
		return "", fmt.Errorf("the projected service account token %s is empty", path)
	}
	return tok, nil
}

// stsError reports a refused exchange by its HTTP status and STS's error, and
// nothing else from the response. STS answers in the OAuth 2.0 form
// ({"error": "...", "error_description": "..."}) or in Google's API form
// ({"error": {"code": ..., "status": "...", "message": "..."}}).
func stsError(status int, raw []byte, subject string) error {
	msg := fmt.Sprintf("sts token exchange: HTTP %d", status)
	var e struct {
		Error       json.RawMessage `json:"error"`
		Description string          `json:"error_description"`
	}
	if json.Unmarshal(raw, &e) != nil {
		return errors.New(msg)
	}
	var oauth string
	var google struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	var parts []string
	switch {
	case json.Unmarshal(e.Error, &oauth) == nil:
		parts = []string{oauth, e.Description}
	case json.Unmarshal(e.Error, &google) == nil:
		parts = []string{google.Status, google.Message}
	}
	for _, p := range parts {
		if p != "" {
			msg += fmt.Sprintf(": %q", scrubSTS(p, subject))
		}
	}
	return errors.New(msg)
}

// jwtLike matches anything shaped like a JWT, such as a service account
// token.
var jwtLike = regexp.MustCompile(`eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]*)?`)

// stsErrorMaxLen is the most of each value from an STS error that an error
// message carries.
const stsErrorMaxLen = 256

// scrubSTS takes the subject token, and anything else shaped like a token,
// out of a value STS returned, in case STS quotes it, and clips it.
func scrubSTS(s, subject string) string {
	if subject != "" {
		s = strings.ReplaceAll(s, subject, "[service account token]")
	}
	s = jwtLike.ReplaceAllString(s, "[token]")
	if len(s) > stsErrorMaxLen {
		n := stsErrorMaxLen
		for n > 0 && n > stsErrorMaxLen-utf8.UTFMax && !utf8.RuneStart(s[n]) {
			n--
		}
		s = s[:n] + "..."
	}
	return s
}
