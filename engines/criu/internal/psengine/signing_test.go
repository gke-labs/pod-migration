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
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The archive signing keys the tests sign and verify with, the cluster they
// bind archives to, and the UID of newClient's test-pmj.
const (
	testArchiveKeyID = "test-k1"
	testClusterUID   = "test-cluster-uid"
	testJobUID       = "test-pmj-uid"
)

var (
	testArchiveKeyring = mustKeyring(map[string][]byte{
		testArchiveKeyID: []byte("test-archive-signing-key-0123456789abcdef"),
	})
	testArchiveSigner = mustSigner(testArchiveKeyring, testArchiveKeyID)
)

func mustKeyring(keys map[string][]byte) *Keyring {
	k, err := NewKeyring(keys)
	if err != nil {
		panic(err)
	}
	return k
}

func mustSigner(k *Keyring, keyID string) *Signer {
	s, err := k.Signer(keyID)
	if err != nil {
		panic(err)
	}
	return s
}

func key32(c byte) []byte { return []byte(strings.Repeat(string(c), MinArchiveSigningKeyBytes)) }

func hmacOver(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// The DSSE v1 pre-authentication encoding, as the DSSE protocol document
// gives it, and for the manifest's payload type.
func TestPAE(t *testing.T) {
	if got, want := string(PAE("http://example.com/HelloWorld", []byte("hello world"))),
		"DSSEv1 29 http://example.com/HelloWorld 11 hello world"; got != want {
		t.Errorf("PAE = %q, want %q", got, want)
	}
	if got, want := string(PAE(ManifestPayloadType, []byte("{}"))),
		"DSSEv1 60 application/vnd.gke-labs.pod-migration.criu-manifest.v1+json 2 {}"; got != want {
		t.Errorf("PAE = %q, want %q", got, want)
	}
	if got, want := string(PAE("", nil)), "DSSEv1 0  0 "; got != want {
		t.Errorf("PAE of empty type and body = %q, want %q", got, want)
	}
}

func decodeEnvelope(t *testing.T, raw []byte) Envelope {
	t.Helper()
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decoding envelope: %v", err)
	}
	return env
}

func encodeEnvelope(t *testing.T, env Envelope) []byte {
	t.Helper()
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestSignOpenRoundTrip(t *testing.T) {
	payload := []byte(`{"snapshot":"ps-web-0-deadbeef","tarSha256":"abc"}`)
	raw, err := testArchiveSigner.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnvelope(t, raw)
	if env.PayloadType != ManifestPayloadType || len(env.Signatures) != 1 || env.Signatures[0].KeyID != testArchiveKeyID {
		t.Fatalf("envelope = %+v", env)
	}
	// The MAC is HMAC-SHA256 over the PAE of the raw payload bytes.
	sig, _ := base64.StdEncoding.DecodeString(env.Signatures[0].Sig)
	if want := manifestMAC([]byte("test-archive-signing-key-0123456789abcdef"), payload); string(sig) != string(want) {
		t.Fatal("signature isn't HMAC-SHA256 over the PAE")
	}
	got, keyID, err := testArchiveKeyring.Open(raw)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(got) != string(payload) || keyID != testArchiveKeyID {
		t.Fatalf("Open = %q, %q; want the payload and %q", got, keyID, testArchiveKeyID)
	}
}

// Each change to a signed envelope, short of a new signature with a key the
// agent holds, is refused, and each refusal is an ErrSignatureInvalid.
func TestOpenRefusesTamperedEnvelopes(t *testing.T) {
	payload := []byte(`{"snapshot":"ps-web-0-deadbeef","jobUID":"job-1"}`)
	ring := mustKeyring(map[string][]byte{"k1": key32('a'), "k2": key32('b')})
	k1 := mustSigner(ring, "k1")
	signed, err := k1.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(f func(*Envelope)) []byte {
		env := decodeEnvelope(t, signed)
		f(&env)
		return encodeEnvelope(t, env)
	}
	cases := map[string]struct {
		envelope []byte
		want     error
	}{
		"payload changed": {mutate(func(e *Envelope) {
			e.Payload = base64.StdEncoding.EncodeToString([]byte(`{"snapshot":"ps-web-0-deadbeef","jobUID":"job-2"}`))
		}), errBadSignature},
		"signature changed": {mutate(func(e *Envelope) {
			sig, _ := base64.StdEncoding.DecodeString(e.Signatures[0].Sig)
			sig[0] ^= 0xff
			e.Signatures[0].Sig = base64.StdEncoding.EncodeToString(sig)
		}), errBadSignature},
		"signature truncated": {mutate(func(e *Envelope) {
			sig, _ := base64.StdEncoding.DecodeString(e.Signatures[0].Sig)
			e.Signatures[0].Sig = base64.StdEncoding.EncodeToString(sig[:16])
		}), errBadSignature},
		"signature not base64":              {mutate(func(e *Envelope) { e.Signatures[0].Sig = "!!!" }), errBadSignature},
		"key ID of another key in the ring": {mutate(func(e *Envelope) { e.Signatures[0].KeyID = "k2" }), errBadSignature},
		"unknown key ID":                    {mutate(func(e *Envelope) { e.Signatures[0].KeyID = "k9" }), errUnknownKey},
		"payload type changed":              {mutate(func(e *Envelope) { e.PayloadType = "application/vnd.in-toto+json" }), errPayloadType},
		"payload type empty":                {mutate(func(e *Envelope) { e.PayloadType = "" }), errPayloadType},
		"signed over another payload type": {mutate(func(e *Envelope) {
			mac := hmacOver(key32('a'), PAE("application/other", payload))
			e.Signatures[0].Sig = base64.StdEncoding.EncodeToString(mac)
		}), errBadSignature},
		"no signatures":      {mutate(func(e *Envelope) { e.Signatures = nil }), errUnsigned},
		"payload not base64": {mutate(func(e *Envelope) { e.Payload = "%%%" }), errEnvelopeMalformed},
		"not JSON":           {[]byte("not json"), errEnvelopeMalformed},
		// A manifest from an agent without archive signing.
		"unsigned legacy manifest": {[]byte(`{"snapshot":"ps-web-0-deadbeef","tarSha256":"abc","specSha256":"def"}`), errUnsigned},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, keyID, err := ring.Open(tc.envelope)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open error = %v, want %v", err, tc.want)
			}
			if !errors.Is(err, ErrSignatureInvalid) {
				t.Fatalf("Open error %v isn't an ErrSignatureInvalid", err)
			}
			if got != nil || keyID != "" {
				t.Fatalf("Open returned %q, %q with its error", got, keyID)
			}
		})
	}
}

// Any signature with a key in the ring opens the envelope, whatever else it
// carries.
func TestOpenAcceptsAnyKnownSignature(t *testing.T) {
	payload := []byte(`{"snapshot":"s"}`)
	ring := mustKeyring(map[string][]byte{"k1": key32('a')})
	signed, err := mustSigner(ring, "k1").Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnvelope(t, signed)
	env.Signatures = append([]EnvelopeSignature{{KeyID: "other-cluster", Sig: "AAAA"}}, env.Signatures...)
	if _, keyID, err := ring.Open(encodeEnvelope(t, env)); err != nil || keyID != "k1" {
		t.Fatalf("Open = %q, %v; want k1", keyID, err)
	}
}

// A bucket writer can't make Open burn CPU: more than MaxEnvelopeSignatures
// is a malformed envelope, refused before any MAC; a signature that isn't an
// HMAC-SHA256 is never hashed; and each key's MAC is computed once.
func TestOpenBoundsItsWork(t *testing.T) {
	ring := mustKeyring(map[string][]byte{"k1": key32('a')})
	signed, err := mustSigner(ring, "k1").Sign([]byte(`{"snapshot":"s"}`))
	if err != nil {
		t.Fatal(err)
	}
	good := decodeEnvelope(t, signed).Signatures[0]
	bad := EnvelopeSignature{KeyID: "k1", Sig: base64.StdEncoding.EncodeToString(make([]byte, 32))}

	t.Run("more signatures than the cap", func(t *testing.T) {
		env := decodeEnvelope(t, signed)
		env.Signatures = nil
		for i := 0; i < MaxEnvelopeSignatures; i++ {
			env.Signatures = append(env.Signatures, bad)
		}
		env.Signatures = append(env.Signatures, good) // the valid one comes too late
		if _, _, err := ring.Open(encodeEnvelope(t, env)); !errors.Is(err, errEnvelopeMalformed) {
			t.Fatalf("%d signatures: Open error = %v, want %v", len(env.Signatures), err, errEnvelopeMalformed)
		}
		env.Signatures = env.Signatures[1:] // exactly the cap, the valid one last
		if _, keyID, err := ring.Open(encodeEnvelope(t, env)); err != nil || keyID != "k1" {
			t.Fatalf("%d signatures: Open = %q, %v; want it verified", len(env.Signatures), keyID, err)
		}
	})

	t.Run("ten thousand signatures over a 1 MiB payload", func(t *testing.T) {
		env := Envelope{
			PayloadType: ManifestPayloadType,
			Payload:     base64.StdEncoding.EncodeToString(make([]byte, 1<<20)),
		}
		for i := 0; i < 10000; i++ {
			env.Signatures = append(env.Signatures, bad)
		}
		if _, _, err := ring.Open(encodeEnvelope(t, env)); !errors.Is(err, errEnvelopeMalformed) {
			t.Fatalf("Open error = %v, want %v", err, errEnvelopeMalformed)
		}
	})

	t.Run("the same key ID twice, one bad and one good signature", func(t *testing.T) {
		env := decodeEnvelope(t, signed)
		env.Signatures = []EnvelopeSignature{bad, good}
		if _, keyID, err := ring.Open(encodeEnvelope(t, env)); err != nil || keyID != "k1" {
			t.Fatalf("Open = %q, %v; want it verified by the good signature", keyID, err)
		}
	})

	t.Run("a signature of the wrong length", func(t *testing.T) {
		mac, _ := base64.StdEncoding.DecodeString(good.Sig)
		for _, sig := range [][]byte{mac[:31], append(append([]byte(nil), mac...), 0)} {
			env := decodeEnvelope(t, signed)
			env.Signatures = []EnvelopeSignature{{KeyID: "k1", Sig: base64.StdEncoding.EncodeToString(sig)}}
			if _, _, err := ring.Open(encodeEnvelope(t, env)); !errors.Is(err, errBadSignature) {
				t.Fatalf("a %d-byte signature: Open error = %v, want %v", len(sig), err, errBadSignature)
			}
		}
	})
}

// Open's errors reach events on tenants' pods, so they never name key IDs;
// EnvelopeKeyIDs gives them to the agent's log.
func TestOpenErrorsDontNameKeys(t *testing.T) {
	ring := mustKeyring(map[string][]byte{"agent-key-7": key32('a')})
	foreign := mustSigner(mustKeyring(map[string][]byte{"foreign-key-3": key32('b')}), "")
	envelope, err := foreign.Sign([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = ring.Open(envelope)
	if !errors.Is(err, errUnknownKey) || strings.Contains(err.Error(), "agent-key-7") || strings.Contains(err.Error(), "foreign-key-3") {
		t.Fatalf("unknown key: Open error = %v, want %v naming no key", err, errUnknownKey)
	}
	env := decodeEnvelope(t, envelope)
	env.Signatures[0].KeyID = "agent-key-7"
	_, _, err = ring.Open(encodeEnvelope(t, env))
	if !errors.Is(err, errBadSignature) || strings.Contains(err.Error(), "agent-key-7") {
		t.Fatalf("bad signature: Open error = %v, want %v naming no key", err, errBadSignature)
	}
	if got := EnvelopeKeyIDs(envelope); len(got) != 1 || got[0] != "foreign-key-3" {
		t.Fatalf("EnvelopeKeyIDs = %v, want [foreign-key-3]", got)
	}
	if got := EnvelopeKeyIDs([]byte("not json")); got != nil {
		t.Fatalf("EnvelopeKeyIDs of garbage = %v, want nil", got)
	}
}

// After a rotation, archives signed with the old key verify for as long as
// the old key stays in the ring, and new ones are signed with the new key.
func TestKeyRotation(t *testing.T) {
	payload := []byte(`{"snapshot":"s"}`)
	before := mustKeyring(map[string][]byte{"k1": key32('a')})
	old, err := mustSigner(before, "").Sign(payload)
	if err != nil {
		t.Fatal(err)
	}

	during := mustKeyring(map[string][]byte{"k1": key32('a'), "k2": key32('b')})
	if _, keyID, err := during.Open(old); err != nil || keyID != "k1" {
		t.Fatalf("an archive signed before the rotation: Open = %q, %v; want it verified with k1", keyID, err)
	}
	if _, err := during.Signer(""); err == nil {
		t.Fatal("with two keys and no active key ID, Signer must fail rather than pick one")
	}
	signer, err := during.Signer("k2")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, keyID, err := during.Open(fresh); err != nil || keyID != "k2" {
		t.Fatalf("a new archive: Open = %q, %v; want it verified with k2", keyID, err)
	}

	after := mustKeyring(map[string][]byte{"k2": key32('b')})
	if _, _, err := after.Open(old); !errors.Is(err, errUnknownKey) {
		t.Fatalf("an old archive once k1 is removed: Open error = %v, want %v", err, errUnknownKey)
	}
	if _, _, err := after.Open(fresh); err != nil {
		t.Fatalf("a new archive once k1 is removed: %v", err)
	}
}

func TestSignerSelection(t *testing.T) {
	one := mustKeyring(map[string][]byte{"only": key32('a')})
	if s, err := one.Signer(""); err != nil || s.KeyID() != "only" {
		t.Fatalf("the only key: Signer(\"\") = %v, %v", s, err)
	}
	two := mustKeyring(map[string][]byte{"k1": key32('a'), "k2": key32('b')})
	if _, err := two.Signer(""); err == nil || !strings.Contains(err.Error(), "k1, k2") {
		t.Fatalf("ambiguous: Signer(\"\") error = %v, want one naming both keys", err)
	}
	if _, err := two.Signer("k3"); err == nil {
		t.Fatal("a key ID that isn't in the ring must fail")
	}
	if s, err := two.Signer("k2"); err != nil || s.KeyID() != "k2" {
		t.Fatalf("Signer(k2) = %v, %v", s, err)
	}
}

func TestNilKeysFailClosed(t *testing.T) {
	var ring *Keyring
	if _, _, err := ring.Open([]byte(`{}`)); !errors.Is(err, ErrSigningKeyUnavailable) {
		t.Fatalf("nil Keyring Open error = %v, want ErrSigningKeyUnavailable", err)
	}
	if _, err := ring.Signer(""); !errors.Is(err, ErrSigningKeyUnavailable) {
		t.Fatalf("nil Keyring Signer error = %v, want ErrSigningKeyUnavailable", err)
	}
	var signer *Signer
	if _, err := signer.Sign([]byte(`{}`)); !errors.Is(err, ErrSigningKeyUnavailable) {
		t.Fatalf("nil Signer Sign error = %v, want ErrSigningKeyUnavailable", err)
	}
}

func TestNewKeyringValidates(t *testing.T) {
	for name, keys := range map[string]map[string][]byte{
		"no keys":            {},
		"short key":          {"k1": key32('a')[:MinArchiveSigningKeyBytes-1]},
		"invalid key ID":     {"..data": key32('a')},
		"key ID with spaces": {"my key": key32('a')},
		"key ID too long":    {strings.Repeat("k", 64): key32('a')},
	} {
		if _, err := NewKeyring(keys); err == nil {
			t.Errorf("%s: NewKeyring accepted %v", name, keys)
		}
	}
	if _, err := NewKeyring(map[string][]byte{strings.Repeat("k", 63): key32('a')}); err != nil {
		t.Errorf("a 63-character key ID: %v", err)
	}
}

// writeSecretVolume lays keys out the way the kubelet projects a Secret: the
// files live in a timestamped directory, ..data links to it, and each key's
// name links to ..data/<key>.
func writeSecretVolume(t *testing.T, keys map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	ts := "..2026_11_23_10_00_00.000000001"
	if err := os.Mkdir(filepath.Join(dir, ts), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(ts, filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	for name, content := range keys {
		if err := os.WriteFile(filepath.Join(dir, ts, name), []byte(content), 0o400); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestLoadKeyringFromSecretVolume(t *testing.T) {
	k1 := strings.Repeat("a", 64)
	dir := writeSecretVolume(t, map[string]string{
		"k1":     k1 + "\n", // kubectl create secret --from-file keeps a trailing newline
		"k2.old": "  " + strings.Repeat("b", 40) + "\t\n",
	})
	// Neither a directory nor a name that isn't a key ID is a key.
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "not a key"), []byte(strings.Repeat("c", 40)), 0o400); err != nil {
		t.Fatal(err)
	}
	ring, err := LoadKeyring(dir)
	if err != nil {
		t.Fatalf("LoadKeyring: %v", err)
	}
	if got := strings.Join(ring.KeyIDs(), ","); got != "k1,k2.old" {
		t.Fatalf("key IDs = %s, want k1,k2.old (..data, the timestamped directory, subdir and \"not a key\" skipped)", got)
	}
	if string(ring.keys["k1"]) != k1 || string(ring.keys["k2.old"]) != strings.Repeat("b", 40) {
		t.Fatal("key bytes must be the file contents without surrounding whitespace")
	}

	// What an agent signs with this ring, the same ring opens.
	signed, err := mustSigner(ring, "k1").Sign([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, keyID, err := ring.Open(signed); err != nil || keyID != "k1" {
		t.Fatalf("Open = %q, %v", keyID, err)
	}
}

func TestLoadKeyringRejects(t *testing.T) {
	t.Run("a short key", func(t *testing.T) {
		dir := writeSecretVolume(t, map[string]string{
			"k1": strings.Repeat("a", 64),
			"k2": strings.Repeat("b", MinArchiveSigningKeyBytes-1) + "\n",
		})
		if _, err := LoadKeyring(dir); err == nil || !strings.Contains(err.Error(), `"k2"`) {
			t.Fatalf("LoadKeyring error = %v, want one naming the short key k2", err)
		}
	})
	t.Run("a key that is only whitespace", func(t *testing.T) {
		dir := writeSecretVolume(t, map[string]string{"k1": strings.Repeat(" ", 64)})
		if _, err := LoadKeyring(dir); err == nil {
			t.Fatal("LoadKeyring accepted an empty key")
		}
	})
	t.Run("no keys", func(t *testing.T) {
		if _, err := LoadKeyring(writeSecretVolume(t, nil)); err == nil {
			t.Fatal("LoadKeyring accepted a directory without keys")
		}
	})
	t.Run("no directory", func(t *testing.T) {
		if _, err := LoadKeyring(filepath.Join(t.TempDir(), "missing")); err == nil {
			t.Fatal("LoadKeyring accepted a missing directory")
		}
	})
}

func TestReadClusterUID(t *testing.T) {
	ctx := context.Background()
	fast := wait.Backoff{Steps: 3, Duration: time.Millisecond, Factor: 1}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: metav1.NamespaceSystem, UID: types.UID("cluster-a")}}

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ns).Build()
	if uid, err := ReadClusterUID(ctx, c, fast); err != nil || uid != "cluster-a" {
		t.Fatalf("ReadClusterUID = %q, %v; want cluster-a", uid, err)
	}

	// A brief outage is ridden out.
	calls := 0
	flaky := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ns).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			calls++
			if calls < 3 {
				return errors.New("apiserver unavailable")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}).Build()
	if uid, err := ReadClusterUID(ctx, flaky, fast); err != nil || uid != "cluster-a" {
		t.Fatalf("after two failed reads: ReadClusterUID = %q, %v", uid, err)
	}

	// Forbidden isn't retried: the agent lacks get on namespaces/kube-system.
	calls = 0
	forbidden := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ns).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			calls++
			return apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, metav1.NamespaceSystem, errors.New("rbac"))
		},
	}).Build()
	if _, err := ReadClusterUID(ctx, forbidden, fast); err == nil || calls != 1 {
		t.Fatalf("Forbidden: err = %v after %d reads, want an error after 1", err, calls)
	}

	// A namespace without a UID is no identity.
	noUID := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: metav1.NamespaceSystem}}).Build()
	if _, err := ReadClusterUID(ctx, noUID, fast); err == nil {
		t.Fatal("ReadClusterUID accepted a namespace without a UID")
	}

	// Each attempt has its own deadline, so one hung request doesn't hold
	// the agent's start: the next attempt gets through.
	saved := ReadClusterUIDAttemptTimeout
	ReadClusterUIDAttemptTimeout = 20 * time.Millisecond
	t.Cleanup(func() { ReadClusterUIDAttemptTimeout = saved })
	calls = 0
	var hungErr error
	hung := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ns).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			calls++
			if calls == 1 {
				// Hang until the attempt's deadline, if it has one.
				if _, ok := ctx.Deadline(); !ok {
					hungErr = errors.New("the attempt had no deadline")
					return hungErr
				}
				<-ctx.Done()
				hungErr = ctx.Err()
				return hungErr
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}).Build()
	if uid, err := ReadClusterUID(ctx, hung, fast); err != nil || uid != "cluster-a" || calls != 2 {
		t.Fatalf("after a hung read: ReadClusterUID = %q, %v after %d reads; want cluster-a after 2", uid, err, calls)
	}
	if !errors.Is(hungErr, context.DeadlineExceeded) {
		t.Fatalf("the hung read ended with %v, want it cut off by the attempt's deadline", hungErr)
	}
}
