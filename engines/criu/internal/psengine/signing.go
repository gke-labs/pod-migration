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

// Archive provenance (contract clause C10 R2, #244). The node agent signs the
// archive manifest with a key that workloads can't read: a Secret mounted only
// into the agent. The manifest carries the SHA-256 of spec.json and
// checkpoint.tar and says which restore the archive belongs to, so the
// signature covers the whole archive and its bindings. A restoring agent
// refuses an archive whose manifest isn't signed with a key it holds, before
// it downloads anything else.
//
// manifest.json is a DSSE envelope (protocol v1,
// https://github.com/secure-systems-lab/dsse):
//
//	{"payloadType": ManifestPayloadType,
//	 "payload": base64(Manifest JSON),
//	 "signatures": [{"keyid": "<key ID>", "sig": base64(HMAC-SHA256(key, PAE))}]}
//
// where PAE is DSSE's pre-authentication encoding of the payload type and the
// raw payload bytes. The MAC is over those bytes, so no JSON canonicalization
// is involved, and the payload is decoded only after a signature verifies.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManifestPayloadType is the DSSE payload type of a signed archive manifest.
const ManifestPayloadType = "application/vnd.gke-labs.pod-migration.criu-manifest.v1+json"

// DefaultArchiveSigningKeysDir is where the node agent reads its archive
// signing keys: the mount of the pod-migrate-archive-signing-keys Secret in
// deploy/deploy.yaml.
const DefaultArchiveSigningKeysDir = "/etc/pod-migrate/archive-signing"

// MinArchiveSigningKeyBytes is the shortest archive signing key accepted.
const MinArchiveSigningKeyBytes = 32

// Errors of signing and opening a manifest envelope. Every reason an envelope
// doesn't verify wraps ErrSignatureInvalid.
var (
	// ErrSigningKeyUnavailable: the agent has no key to sign or verify with.
	ErrSigningKeyUnavailable = errors.New("archive signing key unavailable")
	// ErrSignatureInvalid: the manifest isn't a validly signed envelope.
	ErrSignatureInvalid = errors.New("archive manifest signature invalid")

	errEnvelopeMalformed = fmt.Errorf("%w: malformed envelope", ErrSignatureInvalid)
	errPayloadType       = fmt.Errorf("%w: unexpected payload type", ErrSignatureInvalid)
	errUnsigned          = fmt.Errorf("%w: not signed", ErrSignatureInvalid)
	errUnknownKey        = fmt.Errorf("%w: signed with a key this agent doesn't hold", ErrSignatureInvalid)
	errBadSignature      = fmt.Errorf("%w: signature doesn't match", ErrSignatureInvalid)
)

// Envelope is a DSSE envelope.
type Envelope struct {
	PayloadType string              `json:"payloadType"`
	Payload     string              `json:"payload"`
	Signatures  []EnvelopeSignature `json:"signatures"`
}

// EnvelopeSignature is one signature of an Envelope.
type EnvelopeSignature struct {
	KeyID string `json:"keyid"`
	Sig   string `json:"sig"`
}

// PAE is DSSE v1's pre-authentication encoding:
// "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body, with the lengths in
// ASCII decimal bytes.
func PAE(payloadType string, payload []byte) []byte {
	var b bytes.Buffer
	b.WriteString("DSSEv1 ")
	b.WriteString(strconv.Itoa(len(payloadType)))
	b.WriteByte(' ')
	b.WriteString(payloadType)
	b.WriteByte(' ')
	b.WriteString(strconv.Itoa(len(payload)))
	b.WriteByte(' ')
	b.Write(payload)
	return b.Bytes()
}

func manifestMAC(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(PAE(ManifestPayloadType, payload))
	return mac.Sum(nil)
}

// keyIDPattern is a valid key ID, which is also the name of the key's file in
// the keys directory. Kubernetes' own files in a Secret volume (..data and
// the timestamped directory it points to) never match it.
var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)

// Keyring holds the archive signing keys by key ID. Every key verifies:
// archives signed before a key rotation verify with the old key for as long
// as it stays in the ring.
type Keyring struct {
	keys map[string][]byte
}

// NewKeyring returns a Keyring of keys by key ID. It rejects an invalid key
// ID, a key shorter than MinArchiveSigningKeyBytes, and an empty set.
func NewKeyring(keys map[string][]byte) (*Keyring, error) {
	if len(keys) == 0 {
		return nil, errors.New("no archive signing keys")
	}
	k := &Keyring{keys: make(map[string][]byte, len(keys))}
	for id, key := range keys {
		if !keyIDPattern.MatchString(id) {
			return nil, fmt.Errorf("archive signing key ID %q isn't valid (want %s)", id, keyIDPattern)
		}
		if len(key) < MinArchiveSigningKeyBytes {
			return nil, fmt.Errorf("archive signing key %q is %d bytes; want at least %d", id, len(key), MinArchiveSigningKeyBytes)
		}
		k.keys[id] = append([]byte(nil), key...)
	}
	return k, nil
}

// LoadKeyring reads the archive signing keys from dir, a mounted Kubernetes
// Secret: each file whose name is a valid key ID is one key, the key ID is
// its name, and the key is its content without surrounding whitespace. It
// follows symbolic links, since a Secret volume's files link into its ..data
// directory, and skips directories and names starting with "..". A key
// shorter than MinArchiveSigningKeyBytes fails the load, as does a directory
// with no keys.
func LoadKeyring(dir string) (*Keyring, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading archive signing keys: %w", err)
	}
	keys := map[string][]byte{}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "..") || !keyIDPattern.MatchString(name) {
			continue
		}
		p := filepath.Join(dir, name)
		fi, err := os.Stat(p) // follows the link
		if err != nil {
			return nil, fmt.Errorf("reading archive signing key %q: %w", name, err)
		}
		if !fi.Mode().IsRegular() {
			continue
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("reading archive signing key %q: %w", name, err)
		}
		keys[name] = bytes.TrimSpace(raw)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no archive signing keys in %s", dir)
	}
	return NewKeyring(keys)
}

// KeyIDs returns the key IDs in the ring, sorted.
func (k *Keyring) KeyIDs() []string {
	if k == nil {
		return nil
	}
	ids := make([]string, 0, len(k.keys))
	for id := range k.keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Signer returns a Signer that signs with the key keyID, or, when keyID is
// empty, with the ring's only key. It fails when keyID isn't in the ring, or
// when it is empty and the ring holds several keys: which one signs new
// archives must then be said.
func (k *Keyring) Signer(keyID string) (*Signer, error) {
	if k == nil || len(k.keys) == 0 {
		return nil, ErrSigningKeyUnavailable
	}
	if keyID == "" {
		if len(k.keys) != 1 {
			return nil, fmt.Errorf("%d archive signing keys (%s) and no active key ID: choose the key that signs new archives",
				len(k.keys), strings.Join(k.KeyIDs(), ", "))
		}
		for id := range k.keys {
			keyID = id
		}
	}
	key, ok := k.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("active archive signing key %q isn't among the keys (%s)", keyID, strings.Join(k.KeyIDs(), ", "))
	}
	return &Signer{keyID: keyID, key: key}, nil
}

// MaxEnvelopeSignatures is the most signatures an envelope may carry. An
// agent writes one; a few leave room for a future key per signer. More is a
// malformed envelope, refused before any MAC is computed, so a bucket writer
// can't make Open burn CPU with thousands of signatures.
const MaxEnvelopeSignatures = 8

// encodedSigLen is the length of a base64-encoded HMAC-SHA256.
var encodedSigLen = base64.StdEncoding.EncodedLen(sha256.Size)

// Open verifies a manifest envelope and returns its payload and the ID of the
// key that verified it. It accepts the envelope when any of its signatures
// verifies with a key in the ring. A nil Keyring opens nothing
// (ErrSigningKeyUnavailable); every other failure wraps ErrSignatureInvalid.
// Its errors never name key IDs: they reach events on tenants' pods (see
// EnvelopeKeyIDs for logs).
func (k *Keyring) Open(envelope []byte) (payload []byte, keyID string, err error) {
	if k == nil || len(k.keys) == 0 {
		return nil, "", ErrSigningKeyUnavailable
	}
	var env Envelope
	if err := json.Unmarshal(envelope, &env); err != nil {
		return nil, "", fmt.Errorf("%w: %v", errEnvelopeMalformed, err)
	}
	if env.PayloadType == "" && env.Payload == "" && len(env.Signatures) == 0 {
		return nil, "", fmt.Errorf("%w: manifest.json isn't a signed envelope (an archive from an agent without archive signing?)", errUnsigned)
	}
	if env.PayloadType != ManifestPayloadType {
		return nil, "", fmt.Errorf("%w %q", errPayloadType, clip(env.PayloadType))
	}
	if len(env.Signatures) > MaxEnvelopeSignatures {
		return nil, "", fmt.Errorf("%w: %d signatures, at most %d", errEnvelopeMalformed, len(env.Signatures), MaxEnvelopeSignatures)
	}
	payload, err = base64.StdEncoding.DecodeString(env.Payload)
	if err != nil {
		return nil, "", fmt.Errorf("%w: payload: %v", errEnvelopeMalformed, err)
	}
	if len(env.Signatures) == 0 {
		return nil, "", errUnsigned
	}
	known := false
	macs := map[string][]byte{} // each key's MAC, computed at most once
	for _, s := range env.Signatures {
		key, ok := k.keys[s.KeyID]
		if !ok {
			continue
		}
		known = true
		// Anything but an HMAC-SHA256 never verifies; skip it before
		// decoding or hashing.
		if len(s.Sig) != encodedSigLen {
			continue
		}
		sig, err := base64.StdEncoding.DecodeString(s.Sig)
		if err != nil || len(sig) != sha256.Size {
			continue
		}
		mac, ok := macs[s.KeyID]
		if !ok {
			mac = manifestMAC(key, payload)
			macs[s.KeyID] = mac
		}
		if hmac.Equal(sig, mac) {
			return payload, s.KeyID, nil
		}
	}
	if known {
		return nil, "", errBadSignature
	}
	return nil, "", errUnknownKey
}

// EnvelopeKeyIDs returns the key IDs an envelope's signatures name, for the
// agent's log when Open refuses it, or nil if it isn't an envelope.
func EnvelopeKeyIDs(envelope []byte) []string {
	var env Envelope
	if json.Unmarshal(envelope, &env) != nil {
		return nil
	}
	var ids []string
	for i, s := range env.Signatures {
		if i == MaxEnvelopeSignatures {
			break
		}
		ids = append(ids, clip(s.KeyID))
	}
	return ids
}

// Signer signs archive manifests with one key of a Keyring.
type Signer struct {
	keyID string
	key   []byte
}

// KeyID is the ID of the key that signs.
func (s *Signer) KeyID() string {
	if s == nil {
		return ""
	}
	return s.keyID
}

// Sign returns the envelope of payload (a Manifest's JSON), signed with the
// signer's key.
func (s *Signer) Sign(payload []byte) ([]byte, error) {
	if s == nil || len(s.key) == 0 {
		return nil, ErrSigningKeyUnavailable
	}
	return json.MarshalIndent(Envelope{
		PayloadType: ManifestPayloadType,
		Payload:     base64.StdEncoding.EncodeToString(payload),
		Signatures: []EnvelopeSignature{{
			KeyID: s.keyID,
			Sig:   base64.StdEncoding.EncodeToString(manifestMAC(s.key, payload)),
		}},
	}, "", "  ")
}

// clip shortens a value read from an unverified envelope for an error
// message; %q then escapes it.
func clip(s string) string {
	const max = 64
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// ReadClusterUIDAttemptTimeout bounds each read of ReadClusterUID, so one
// hung request can't hold the agent's start (a variable for tests).
var ReadClusterUIDAttemptTimeout = 5 * time.Second

// ReadClusterUID returns what archive manifests bind the cluster by: the UID
// of the kube-system Namespace, which a cluster keeps for its lifetime and no
// other cluster shares. It retries with backoff, so an API server that isn't
// reachable yet when the agent starts doesn't fail it at once; it doesn't
// retry Forbidden, which won't fix itself. Each attempt is bounded by
// ReadClusterUIDAttemptTimeout, and backoff.Steps bounds the attempts.
func ReadClusterUID(ctx context.Context, r client.Reader, backoff wait.Backoff) (string, error) {
	var uid string
	err := retry.OnError(backoff,
		func(err error) bool { return ctx.Err() == nil && !apierrors.IsForbidden(err) },
		func() error {
			attempt, cancel := context.WithTimeout(ctx, ReadClusterUIDAttemptTimeout)
			defer cancel()
			ns := &corev1.Namespace{}
			if err := r.Get(attempt, types.NamespacedName{Name: metav1.NamespaceSystem}, ns); err != nil {
				return err
			}
			if ns.UID == "" {
				return fmt.Errorf("namespace %s has no UID", metav1.NamespaceSystem)
			}
			uid = string(ns.UID)
			return nil
		})
	if err != nil {
		return "", fmt.Errorf("reading the cluster UID (namespace %s): %w", metav1.NamespaceSystem, err)
	}
	return uid, nil
}
