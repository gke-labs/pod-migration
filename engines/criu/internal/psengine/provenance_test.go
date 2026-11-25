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

// Archive provenance (C10 R2, #244): the checkpoint signs and binds its
// manifest, and the restore refuses an archive whose signature or bindings
// don't verify, before it downloads anything else.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	k8syaml "sigs.k8s.io/yaml"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// testJobControllerRef is the controller reference the orchestrator sets on
// the trigger it creates for newClient's test-pmj.
func testJobControllerRef() metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: contract.GVKPodMigrationJob.GroupVersion().String(),
		Kind:       contract.GVKPodMigrationJob.Kind,
		Name:       "test-pmj",
		UID:        types.UID(testJobUID),
		Controller: &controller,
	}
}

// jobWorkOrder is workOrderSnapshot(name) answering a trigger of newClient's
// test-pmj, plus that trigger: a work order whose archive the agent can bind
// to a job, as every work order in production is.
func jobWorkOrder(name string) (ps, trigger *unstructured.Unstructured) {
	ps = workOrderSnapshot(name)
	_ = unstructured.SetNestedField(ps.Object, cancelTrigger, "spec", specTriggerName)
	_ = unstructured.SetNestedField(ps.Object, "trigger-uid-1", "spec", specTriggerUID)
	trigger = jobPSMT("web-0")
	trigger.SetUID("trigger-uid-1")
	return ps, trigger
}

// jobWorkOrderObjects is jobWorkOrder's two objects, for newClient.
func jobWorkOrderObjects(name string) []client.Object {
	ps, trigger := jobWorkOrder(name)
	return []client.Object{ps, trigger}
}

// signedFixture is cancelFixture with a trigger the way the orchestrator
// creates it: with a UID and controlled by test-pmj.
func signedFixture(t *testing.T) client.Client {
	t.Helper()
	trigger := psmtWithUID("web-0", "trigger-uid-1")
	trigger.SetOwnerReferences([]metav1.OwnerReference{testJobControllerRef()})
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		trigger,
	)
	reconcilePSMT(t, c, nil, cancelTrigger)
	return c
}

func openManifest(t *testing.T, store *memStore, object string) Manifest {
	t.Helper()
	raw, ok := store.objects[object]
	if !ok {
		t.Fatalf("%s wasn't uploaded", object)
	}
	payload, keyID, err := testArchiveKeyring.Open(raw)
	if err != nil {
		t.Fatalf("%s doesn't verify: %v", object, err)
	}
	if keyID != testArchiveKeyID {
		t.Fatalf("%s verified with key %q, want %q", object, keyID, testArchiveKeyID)
	}
	var mf Manifest
	if err := json.Unmarshal(payload, &mf); err != nil {
		t.Fatal(err)
	}
	return mf
}

// The uploaded manifest.json is a signed envelope whose payload binds the
// archive to its snapshot, pod, location, trigger, job and cluster, and
// carries the digests of what was uploaded.
func TestPipelineSignsAndBindsManifest(t *testing.T) {
	c := signedFixture(t)
	store := newMemStore()
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store})
	if ps := getSnapshot(t, c, cancelSnapshot); !contract.IsSnapshotReady(ps) {
		t.Fatalf("snapshot not Ready: %+v", contract.Conditions(ps))
	}

	object := "bkt/snaps/" + cancelSnapshot + "/manifest.json"
	var env Envelope
	if err := json.Unmarshal(store.objects[object], &env); err != nil || env.PayloadType != ManifestPayloadType || len(env.Signatures) != 1 {
		t.Fatalf("manifest.json isn't a signed envelope: %s (%v)", store.objects[object], err)
	}
	mf := openManifest(t, store, object)
	want := Manifest{
		Snapshot: cancelSnapshot, Namespace: "default",
		SourcePod: "web-0", SourcePodUID: "deadbeef-0000-1111", Container: "c0",
		Bucket: "bkt", Path: "snaps",
		TriggerName: cancelTrigger, TriggerUID: "trigger-uid-1", JobUID: testJobUID, ClusterUID: testClusterUID,
	}
	// checkBindings covers what the restore enforces; the trigger is recorded
	// too, for audit.
	if err := checkBindings(&mf, &want); err != nil || mf.TriggerName != want.TriggerName || mf.TriggerUID != want.TriggerUID {
		t.Fatalf("manifest bindings: %v (trigger %q, UID %q)", err, mf.TriggerName, mf.TriggerUID)
	}
	tarSHA, tarBytes, _ := SHA256Reader(strings.NewReader("TARBYTES"))
	specSHA, _, _ := SHA256Reader(strings.NewReader(`{"ociVersion":"1.1.0"}`))
	if mf.TarSHA256 != tarSHA || mf.TarBytes != tarBytes || mf.SpecSHA256 != specSHA {
		t.Fatalf("manifest digests: tar %s (%d bytes), spec %s", mf.TarSHA256, mf.TarBytes, mf.SpecSHA256)
	}
	if mf.EngineVersion != "psengine/v1alpha2" {
		t.Fatalf("engineVersion = %q", mf.EngineVersion)
	}
}

// Without a signing key or the cluster UID, the work order fails before the
// source is touched: no restore would accept its archive.
func TestPipelineWithoutSigningFailsBeforeFreeze(t *testing.T) {
	for name, r := range map[string]*NodeSnapshotReconciler{
		"no signing key": {ClusterUID: testClusterUID},
		"no cluster UID": {Signer: testArchiveSigner},
	} {
		t.Run(name, func(t *testing.T) {
			c := signedFixture(t)
			store := newMemStore()
			ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
			r.Client, r.NodeName, r.Ops, r.Store = c, "node-a", ops, store
			runAgent(t, r)

			if len(ops.events) != 0 {
				t.Fatalf("the source was touched: %v (Freeze must never be called)", ops.events)
			}
			if len(store.order) != 0 {
				t.Fatalf("objects were uploaded: %v", store.order)
			}
			ps := getSnapshot(t, c, cancelSnapshot)
			cond := condition(ps, contract.CondCheckpoint)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != contract.ReasonFailed ||
				!strings.Contains(cond.Message, "the source container was not touched") {
				t.Fatalf("want Checkpoint=False/Failed saying the source wasn't touched, got %+v", cond)
			}
			if name == "no signing key" && !strings.Contains(cond.Message, "archive signing key not configured") {
				t.Fatalf("the message must say the signing key isn't configured: %q", cond.Message)
			}
			if contract.IsSourceFrozen(ps) {
				t.Fatal("SourceFrozen recorded for a source that was never frozen")
			}
		})
	}
}

// The manifest binds the trigger the work order answers when it is signed: a
// trigger created again that took the work order over during the checkpoint
// (#262) is the one bound, with its job.
func TestPipelineBindsTheTriggerThatTookOver(t *testing.T) {
	ctx := context.Background()
	c := signedFixture(t)
	takeOver := func() {
		old := contract.New(contract.GVKManualTrigger)
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: cancelTrigger}, old); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ctx, old); err != nil {
			t.Fatal(err)
		}
		again := psmtWithUID("web-0", "trigger-uid-2")
		ref := testJobControllerRef()
		ref.UID = "later-pmj-uid"
		again.SetOwnerReferences([]metav1.OwnerReference{ref})
		if err := c.Create(ctx, again); err != nil {
			t.Fatal(err)
		}
		ps := getSnapshot(t, c, cancelSnapshot)
		if res, err := (&PSMTReconciler{Client: c}).adoptWorkOrder(ctx, ps, again); err != nil || !res.IsZero() {
			t.Fatalf("adoptWorkOrder = %v, %v", res, err)
		}
	}
	store := newMemStore()
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t, afterCheckpoint: takeOver}
	runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})

	mf := openManifest(t, store, "bkt/snaps/"+cancelSnapshot+"/manifest.json")
	if mf.TriggerUID != "trigger-uid-2" || mf.JobUID != "later-pmj-uid" {
		t.Fatalf("manifest binds trigger %q of job %q, want trigger-uid-2 of later-pmj-uid", mf.TriggerUID, mf.JobUID)
	}
}

// replaceTrigger deletes the work order's trigger and creates it again as
// instance uid, controlled by jobUID's PodMigrationJob: what the
// orchestrator does when a job gives up and a job (the same or a later one)
// creates the pod's trigger again (#262). The work order isn't taken over.
func replaceTrigger(t *testing.T, c client.Client, uid, jobUID string) {
	t.Helper()
	ctx := context.Background()
	old := contract.New(contract.GVKManualTrigger)
	if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: cancelTrigger}, old); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, old); err != nil {
		t.Fatal(err)
	}
	again := psmtWithUID("web-0", uid)
	ref := testJobControllerRef()
	ref.UID = types.UID(jobUID)
	again.SetOwnerReferences([]metav1.OwnerReference{ref})
	if err := c.Create(ctx, again); err != nil {
		t.Fatal(err)
	}
}

// requireNotCommitted checks a work order that failed before its commit
// point: the source thawed and not stopped, the snapshot Failed with want in
// its message, and, with noManifest, no manifest.json uploaded.
func requireNotCommitted(t *testing.T, c client.Client, ops *fakeNodeOps, store *memStore, want string, noManifest bool) {
	t.Helper()
	if len(ops.unfrozen) != 1 || len(ops.stopped) != 0 {
		t.Fatalf("want the source thawed and not committed: events %v", ops.events)
	}
	ps := getSnapshot(t, c, cancelSnapshot)
	cond := condition(ps, contract.CondCheckpoint)
	if contract.IsSnapshotReady(ps) || cond == nil || cond.Reason != contract.ReasonFailed || !strings.Contains(cond.Message, want) {
		t.Fatalf("want Checkpoint=Failed saying %q, got %+v", want, cond)
	}
	if contract.IsSourceFrozen(ps) {
		t.Fatal("SourceFrozen still recorded after the thaw")
	}
	if _, ok := store.objects["bkt/snaps/"+cancelSnapshot+"/manifest.json"]; noManifest && ok {
		t.Fatal("manifest.json was uploaded for an archive that no restore would accept")
	}
}

// An archive that no restore would accept, because it can't be bound to the
// work order's PodMigrationJob, is never uploaded or committed: the source
// thaws and the work order fails.
func TestPipelineNeverCommitsAnArchiveBoundToNoJob(t *testing.T) {
	fastPreStopBackoff(t)
	t.Run("trigger without a PodMigrationJob controller", func(t *testing.T) {
		c := newClient(t,
			pssc("pssc-x", "bkt", "snaps"),
			readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
			sourcePod("web-0", 1, "node-a"),
			criuNode("node-a", true),
			psmtFor("web-0"), // no owner
		)
		reconcilePSMT(t, c, nil, cancelTrigger)
		store := newMemStore()
		ops := &fakeNodeOps{tarContent: "T", t: t}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store})
		requireNotCommitted(t, c, ops, store, "isn't controlled by a PodMigrationJob", true)
	})
	t.Run("work order naming no trigger", func(t *testing.T) {
		c := newClient(t, workOrderSnapshot(cancelSnapshot))
		store := newMemStore()
		ops := &fakeNodeOps{tarContent: "T", t: t}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store})
		requireNotCommitted(t, c, ops, store, "names no trigger", true)
	})
	t.Run("trigger created again that never takes the work order over", func(t *testing.T) {
		c := signedFixture(t)
		store := newMemStore()
		reads := &triggerReadHook{Reader: c}
		ops := &fakeNodeOps{tarContent: "T", t: t, afterCheckpoint: func() {
			replaceTrigger(t, c, "trigger-uid-2", testJobUID)
			reads.armed = true
		}}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, APIReader: reads,
			NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})
		requireNotCommitted(t, c, ops, store, "hasn't taken this work order over", true)
		if reads.triggerGets != preCommitCheckBackoff.Steps {
			t.Fatalf("trigger read %d times while binding, want %d (read again until the budget ran out)", reads.triggerGets, preCommitCheckBackoff.Steps)
		}
	})
}

// triggerReadHook counts trigger Gets once armed, and runs onTriggerGet before
// each.
type triggerReadHook struct {
	client.Reader
	armed        bool
	triggerGets  int
	onTriggerGet func(call int)
}

func (r *triggerReadHook) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && r.armed && u.GetKind() == contract.GVKManualTrigger.Kind {
		r.triggerGets++
		if r.onTriggerGet != nil {
			r.onTriggerGet(r.triggerGets)
		}
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

// A trigger created again whose take-over lands while the agent binds the
// archive (#262) is read again, and its job is bound and committed.
func TestPipelineWaitsOutATakeOverWhileBinding(t *testing.T) {
	fastPreStopBackoff(t)
	ctx := context.Background()
	c := signedFixture(t)
	reads := &triggerReadHook{Reader: c}
	reads.onTriggerGet = func(call int) {
		if call != 1 {
			return
		}
		// The take-over lands right after the agent read the work order.
		again := contract.New(contract.GVKManualTrigger)
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: cancelTrigger}, again); err != nil {
			t.Fatal(err)
		}
		if res, err := (&PSMTReconciler{Client: c}).adoptWorkOrder(ctx, getSnapshot(t, c, cancelSnapshot), again); err != nil || !res.IsZero() {
			t.Fatalf("adoptWorkOrder = %v, %v", res, err)
		}
	}
	store := newMemStore()
	ops := &fakeNodeOps{tarContent: "T", t: t, afterCheckpoint: func() {
		replaceTrigger(t, c, "trigger-uid-2", "later-pmj-uid")
		reads.armed = true
	}}
	runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, APIReader: reads,
		NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})

	if reads.triggerGets < 2 {
		t.Fatalf("trigger read %d times, want it read again after the mismatch", reads.triggerGets)
	}
	if ps := getSnapshot(t, c, cancelSnapshot); !contract.IsSnapshotReady(ps) || len(ops.stopped) != 1 {
		t.Fatalf("want the commit: events %v, conditions %+v", ops.events, contract.Conditions(ps))
	}
	if mf := openManifest(t, store, "bkt/snaps/"+cancelSnapshot+"/manifest.json"); mf.TriggerUID != "trigger-uid-2" || mf.JobUID != "later-pmj-uid" {
		t.Fatalf("manifest binds trigger %q of job %q, want trigger-uid-2 of later-pmj-uid", mf.TriggerUID, mf.JobUID)
	}
}

// Between the binding read and the commit point, the trigger is checked by
// name: if another job controls it now, the source isn't stopped for an
// archive that job's restore would refuse; if the same job created it again
// (#262), taken over or not, the checkpoint commits.
func TestPipelineCommitRequiresTheBoundJob(t *testing.T) {
	t.Run("another job took the trigger", func(t *testing.T) {
		c := signedFixture(t)
		store := &hookStore{memStore: newMemStore(), object: "manifest.json"}
		store.onPut = func() { replaceTrigger(t, c, "trigger-uid-2", "other-pmj-uid") }
		ops := &fakeNodeOps{tarContent: "T", t: t}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
			Ops: ops, Store: store, CancelPollInterval: time.Hour})
		requireNotCommitted(t, c, ops, store.memStore, "another PodMigrationJob now controls trigger "+cancelTrigger, false)
	})
	t.Run("the same job created the trigger again", func(t *testing.T) {
		c := signedFixture(t)
		store := &hookStore{memStore: newMemStore(), object: "manifest.json"}
		store.onPut = func() { replaceTrigger(t, c, "trigger-uid-2", testJobUID) }
		ops := &fakeNodeOps{tarContent: "T", t: t}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
			Ops: ops, Store: store, CancelPollInterval: time.Hour})
		if ps := getSnapshot(t, c, cancelSnapshot); !contract.IsSnapshotReady(ps) || len(ops.stopped) != 1 || len(ops.unfrozen) != 0 {
			t.Fatalf("want the commit: events %v, conditions %+v", ops.events, contract.Conditions(ps))
		}
	})
	t.Run("the work order no longer names its trigger", func(t *testing.T) {
		c := signedFixture(t)
		store := &hookStore{memStore: newMemStore(), object: "manifest.json"}
		store.onPut = func() { clearSnapshotTrigger(t, c) }
		ops := &fakeNodeOps{tarContent: "T", t: t}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
			Ops: ops, Store: store, CancelPollInterval: time.Hour})
		requireNotCommitted(t, c, ops, store.memStore, "no longer names its trigger", false)
	})
}

// clearSnapshotTrigger removes the trigger name from the work order, so the
// check before the commit point can't read the trigger's PodMigrationJob.
func clearSnapshotTrigger(t *testing.T, c client.Client) {
	t.Helper()
	ps := getSnapshot(t, c, cancelSnapshot)
	unstructured.RemoveNestedField(ps.Object, "spec", specTriggerName)
	if err := c.Update(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
}

// A trigger deleted before the archive is bound cancels the work order: the
// source thaws, nothing more is uploaded, and the outcome is Cancelled.
func TestPipelineTriggerDeletedBeforeBindingCancels(t *testing.T) {
	c := signedFixture(t)
	store := newMemStore()
	// The engine controller is down, so only the agent's own read sees it.
	ops := &fakeNodeOps{tarContent: "T", t: t, afterCheckpoint: func() { deleteTrigger(t, c, false) }}
	runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
		Ops: ops, Store: store, CancelPollInterval: time.Hour})
	if len(ops.unfrozen) != 1 || len(ops.stopped) != 0 {
		t.Fatalf("want the source thawed and not committed: events %v", ops.events)
	}
	cond := requireCancelled(t, c)
	if !strings.Contains(cond.Message, "while binding the archive") {
		t.Fatalf("the cancel must come from the binding read, got %q", cond.Message)
	}
	if _, ok := store.objects["bkt/snaps/"+cancelSnapshot+"/manifest.json"]; ok {
		t.Fatal("manifest.json was uploaded for a cancelled work order")
	}
}

// failingTriggerReader fails every trigger Get once armed.
type failingTriggerReader struct {
	client.Reader
	armed bool
}

func (f *failingTriggerReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && f.armed && u.GetKind() == contract.GVKManualTrigger.Kind {
		return errors.New("apiserver unavailable")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

// A binding that can't be read fails closed: no manifest, the source thawed,
// the work order Failed.
func TestPipelineBindingReadFailureFailsClosed(t *testing.T) {
	fastPreStopBackoff(t)
	c := signedFixture(t)
	reader := &failingTriggerReader{Reader: c}
	store := &hookStore{memStore: newMemStore(), object: "checkpoint.tar", onPut: func() { reader.armed = true }}
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, APIReader: reader,
		NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})

	if _, ok := store.objects["bkt/snaps/"+cancelSnapshot+"/manifest.json"]; ok {
		t.Fatal("manifest.json was uploaded without its bindings")
	}
	if len(ops.unfrozen) != 1 || len(ops.stopped) != 0 {
		t.Fatalf("want the source thawed and not committed: %v", ops.events)
	}
	if cond := condition(getSnapshot(t, c, cancelSnapshot), contract.CondCheckpoint); cond == nil || cond.Reason != contract.ReasonFailed ||
		!strings.Contains(cond.Message, "bind the archive") {
		t.Fatalf("want Failed naming the binding, got %+v", cond)
	}
}

// --- manifest -----------------------------------------------------------------

// The agent DaemonSet mounts the signing keys Secret, read-only and required,
// where --archive-signing-keys-dir looks by default; the manifest ships no
// key material of its own.
func TestDeployManifestMountsArchiveSigningKeys(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deploy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var agent *appsv1.DaemonSet
	for _, doc := range strings.Split(string(raw), "\n---") {
		var meta struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := k8syaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatal(err)
		}
		if meta.Kind == "Secret" {
			t.Errorf("deploy.yaml ships a Secret (%s): signing keys must be created per install, never shipped", meta.Metadata.Name)
		}
		if meta.Kind == "DaemonSet" && meta.Metadata.Name == "pod-migrate-agent" {
			agent = &appsv1.DaemonSet{}
			if err := k8syaml.Unmarshal([]byte(doc), agent); err != nil {
				t.Fatal(err)
			}
		}
	}
	if agent == nil {
		t.Fatal("deploy.yaml has no pod-migrate-agent DaemonSet")
	}
	var volume *corev1.Volume
	for i, v := range agent.Spec.Template.Spec.Volumes {
		if v.Secret != nil && v.Secret.SecretName == "pod-migrate-archive-signing-keys" {
			volume = &agent.Spec.Template.Spec.Volumes[i]
		}
	}
	if volume == nil {
		t.Fatal("the agent doesn't mount the pod-migrate-archive-signing-keys Secret")
	}
	if volume.Secret.Optional != nil && *volume.Secret.Optional {
		t.Error("the signing keys Secret must not be optional: an agent without keys can neither checkpoint nor restore")
	}
	if volume.Secret.DefaultMode == nil || *volume.Secret.DefaultMode != 0o400 {
		t.Errorf("the signing keys Secret's defaultMode = %v, want 0400", volume.Secret.DefaultMode)
	}
	mounted := false
	for _, c := range agent.Spec.Template.Spec.Containers {
		for _, m := range c.VolumeMounts {
			if m.Name == volume.Name {
				mounted = true
				if m.MountPath != DefaultArchiveSigningKeysDir || !m.ReadOnly {
					t.Errorf("container %s mounts the keys at %s (readOnly %v), want %s read-only", c.Name, m.MountPath, m.ReadOnly, DefaultArchiveSigningKeysDir)
				}
			}
		}
	}
	if !mounted {
		t.Error("no agent container mounts the signing keys volume")
	}
}

// --- restore ------------------------------------------------------------------

type restoreResult struct {
	pod     *corev1.Pod
	ops     *fakeRestoreOps
	gets    []string
	event   string
	updates []map[string]string // the pod's annotations in each update
}

// restoreArchive runs the node restore of web-1 from
// workOrderSnapshot("ps-web-0-deadbeef") (Ready, after mutate) with keyring,
// against what store holds.
func restoreArchive(t *testing.T, store *memStore, keyring *Keyring, mutate func(ps *unstructured.Unstructured)) restoreResult {
	t.Helper()
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded}); err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(ps)
	}
	var res restoreResult
	c := newClientWithInterceptor(t, interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if pod, ok := obj.(*corev1.Pod); ok {
				anns := map[string]string{}
				for k, v := range pod.Annotations {
					anns[k] = v
				}
				res.updates = append(res.updates, anns)
			}
			return cl.Update(ctx, obj, opts...)
		},
	}, ps, replacementPodOnNode("ps-web-0-deadbeef"))
	tracking := &trackingStore{memStore: store}
	rec := record.NewFakeRecorder(10)
	res.ops = &fakeRestoreOps{}
	r := &NodeRestoreReconciler{Keyring: keyring, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
		Ops: res.ops, Store: tracking, WorkDir: t.TempDir(), Recorder: rec}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	res.pod = &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, res.pod); err != nil {
		t.Fatal(err)
	}
	res.gets = tracking.gets
	select {
	case res.event = <-rec.Events:
	default:
	}
	return res
}

// requireRefused checks a restore that must fail with reason: nothing
// restored, failed and the reason stamped together in one update, and the
// reason in the RestoreFailed event.
func requireRefused(t *testing.T, res restoreResult, reason string) {
	t.Helper()
	if len(res.ops.restored) != 0 || len(res.ops.extracted) != 0 {
		t.Fatalf("archive extracted %v or restored %v; want neither", res.ops.extracted, res.ops.restored)
	}
	if got := res.pod.Annotations[contract.AnnotationRestoreStatus]; got != contract.RestoreStatusFailed {
		t.Fatalf("%s = %q, want failed", contract.AnnotationRestoreStatus, got)
	}
	if got := res.pod.Annotations[contract.AnnotationRestoreReason]; got != reason {
		t.Fatalf("%s = %q, want %s (event %q)", contract.AnnotationRestoreReason, got, reason, res.event)
	}
	if len(res.updates) != 1 || res.updates[0][contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed ||
		res.updates[0][contract.AnnotationRestoreReason] != reason {
		t.Fatalf("want one update carrying failed and the reason together, got %v", res.updates)
	}
	if !strings.Contains(res.event, "Warning RestoreFailed") || !strings.Contains(res.event, reason) {
		t.Fatalf("event = %q, want a Warning RestoreFailed naming %s", res.event, reason)
	}
	// The event and the annotation are the tenant's to read: neither echoes
	// a cluster UID or a key ID (the agent's log has them).
	for _, private := range []string{testClusterUID, "other-cluster-uid", testArchiveKeyID, "k-other"} {
		if strings.Contains(res.event, private) || strings.Contains(res.pod.Annotations[contract.AnnotationRestoreReason], private) {
			t.Fatalf("event %q echoes %q", res.event, private)
		}
	}
}

// requireNotFetched checks that the restore downloaded none of objects.
func requireNotFetched(t *testing.T, res restoreResult, objects ...string) {
	t.Helper()
	for _, got := range res.gets {
		for _, o := range objects {
			if strings.HasSuffix(got, "/"+o) {
				t.Fatalf("%s was fetched (fetches: %v); it must not be before the manifest verifies", got, res.gets)
			}
		}
	}
}

var (
	testSpecJSON = []byte(`{"ociVersion":"1.1.0"}`)
	testTar      = []byte("TARBYTES")
)

func TestRestoreAcceptsSignedBoundArchive(t *testing.T) {
	store := newMemStore()
	mf := boundManifest("ps-web-0-deadbeef")
	mf.TriggerName, mf.TriggerUID = cancelTrigger, "trigger-uid-1"
	putArchive(t, store, mf, testSpecJSON, testTar)
	res := restoreArchive(t, store, testArchiveKeyring, func(ps *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(ps.Object, "trigger-uid-1", "spec", specTriggerUID)
	})
	if len(res.ops.restored) != 1 || res.ops.restored[0].SourcePodUID != "deadbeef-0000-1111" {
		t.Fatalf("restores: %+v", res.ops.restored)
	}
	if got := res.pod.Annotations[contract.AnnotationRestoreStatus]; got != contract.RestoreStatusRestored {
		t.Fatalf("%s = %q, want restored", contract.AnnotationRestoreStatus, got)
	}
	if v, ok := res.pod.Annotations[contract.AnnotationRestoreReason]; ok {
		t.Fatalf("%s = %q on a restored pod", contract.AnnotationRestoreReason, v)
	}
}

// What an attacker who can write to the bucket can do: change the archive,
// change or replace the manifest, or sign it with a key of their own.
func TestRestoreRefusesTamperedArchives(t *testing.T) {
	const prefix = "bkt/snaps/ps-web-0-deadbeef/"
	genuine := func(t *testing.T) *memStore {
		store := newMemStore()
		putArchive(t, store, boundManifest("ps-web-0-deadbeef"), testSpecJSON, testTar)
		return store
	}

	t.Run("checkpoint.tar replaced", func(t *testing.T) {
		store := genuine(t)
		store.objects[prefix+"checkpoint.tar"] = []byte("EVILBYTE")
		requireRefused(t, restoreArchive(t, store, testArchiveKeyring, nil), contract.RestoreReasonDigestMismatch)
	})
	t.Run("spec.json replaced", func(t *testing.T) {
		store := genuine(t)
		store.objects[prefix+"spec.json"] = []byte(`{"ociVersion":"1.1.0","process":{"user":{"uid":0}}}`)
		res := restoreArchive(t, store, testArchiveKeyring, nil)
		requireRefused(t, res, contract.RestoreReasonDigestMismatch)
		requireNotFetched(t, res, "checkpoint.tar")
	})

	signatureCases := map[string]func(t *testing.T, store *memStore){
		"manifest payload changed, signature kept": func(t *testing.T, store *memStore) {
			// The tar and its digest replaced together, the old signature kept.
			store.objects[prefix+"checkpoint.tar"] = []byte("EVILBYTE")
			env := decodeEnvelope(t, store.objects[prefix+"manifest.json"])
			payload, _ := base64.StdEncoding.DecodeString(env.Payload)
			var mf Manifest
			_ = json.Unmarshal(payload, &mf)
			mf.TarSHA256, _, _ = SHA256Reader(strings.NewReader("EVILBYTE"))
			payload, _ = json.Marshal(mf)
			env.Payload = base64.StdEncoding.EncodeToString(payload)
			store.objects[prefix+"manifest.json"] = encodeEnvelope(t, env)
		},
		"unsigned legacy manifest": func(t *testing.T, store *memStore) {
			mf := boundManifest("ps-web-0-deadbeef")
			mf.SpecSHA256, _, _ = SHA256Reader(strings.NewReader(string(testSpecJSON)))
			mf.TarSHA256, mf.TarBytes, _ = SHA256Reader(strings.NewReader(string(testTar)))
			raw, _ := json.Marshal(mf)
			store.objects[prefix+"manifest.json"] = raw
		},
		"signed with a key the agent doesn't hold": func(t *testing.T, store *memStore) {
			attacker := mustSigner(mustKeyring(map[string][]byte{testArchiveKeyID: key32('x')}), testArchiveKeyID)
			env := decodeEnvelope(t, store.objects[prefix+"manifest.json"])
			payload, _ := base64.StdEncoding.DecodeString(env.Payload)
			signed, err := attacker.Sign(payload)
			if err != nil {
				t.Fatal(err)
			}
			store.objects[prefix+"manifest.json"] = signed
		},
		"signed with an unknown key ID": func(t *testing.T, store *memStore) {
			env := decodeEnvelope(t, store.objects[prefix+"manifest.json"])
			env.Signatures[0].KeyID = "k-other"
			store.objects[prefix+"manifest.json"] = encodeEnvelope(t, env)
		},
		"not JSON": func(t *testing.T, store *memStore) {
			store.objects[prefix+"manifest.json"] = []byte("\x00garbage")
		},
	}
	for name, tamper := range signatureCases {
		t.Run(name, func(t *testing.T) {
			store := genuine(t)
			tamper(t, store)
			res := restoreArchive(t, store, testArchiveKeyring, nil)
			requireRefused(t, res, contract.RestoreReasonSignatureInvalid)
			requireNotFetched(t, res, "spec.json", "checkpoint.tar")
		})
	}
}

// A genuine archive restored anywhere but where it belongs is refused: its
// bindings don't match this restore's work order, job or cluster.
func TestRestoreRefusesMisboundArchives(t *testing.T) {
	const prefix = "bkt/snaps/ps-web-0-deadbeef/"
	cases := map[string]func(mf *Manifest){
		"from another job (an earlier migration of the same pod)": func(mf *Manifest) { mf.JobUID = "earlier-pmj-uid" },
		"bound to no job":         func(mf *Manifest) { mf.JobUID = "" },
		"from another cluster":    func(mf *Manifest) { mf.ClusterUID = "other-cluster-uid" },
		"bound to no cluster":     func(mf *Manifest) { mf.ClusterUID = "" },
		"from another namespace":  func(mf *Manifest) { mf.Namespace = "tenant-b" },
		"from another bucket":     func(mf *Manifest) { mf.Bucket = "other-bkt" },
		"from another path":       func(mf *Manifest) { mf.Path = "other" },
		"of another pod instance": func(mf *Manifest) { mf.SourcePodUID = "cafebabe-0000-2222" },
		"of another container":    func(mf *Manifest) { mf.Container = "sidecar" },
	}
	for name, misbind := range cases {
		t.Run(name, func(t *testing.T) {
			mf := boundManifest("ps-web-0-deadbeef")
			misbind(&mf)
			// Signed with the cluster's own key, stored where this restore looks.
			store := newMemStore()
			signedAt(t, store, prefix, mf)
			res := restoreArchive(t, store, testArchiveKeyring, nil)
			requireRefused(t, res, contract.RestoreReasonBindingMismatch)
			requireNotFetched(t, res, "spec.json", "checkpoint.tar")
		})
	}

	// The copy attack itself: another snapshot's genuine archive, copied
	// object by object into this snapshot's prefix.
	t.Run("copied from another snapshot", func(t *testing.T) {
		store := newMemStore()
		other := boundManifest("ps-web-0-cafebabe")
		putArchive(t, store, other, testSpecJSON, testTar)
		for _, o := range []string{"manifest.json", "spec.json", "checkpoint.tar"} {
			store.objects[prefix+o] = store.objects["bkt/snaps/ps-web-0-cafebabe/"+o]
		}
		res := restoreArchive(t, store, testArchiveKeyring, nil)
		requireRefused(t, res, contract.RestoreReasonBindingMismatch)
		requireNotFetched(t, res, "spec.json", "checkpoint.tar")
		if !strings.Contains(res.event, `snapshot "ps-web-0-cafebabe" (want "ps-web-0-deadbeef")`) {
			t.Fatalf("event = %q, want it to name the snapshot binding", res.event)
		}
	})
}

// signedAt stores mf, with the test archive's digests, signed with the test
// key, plus that archive, under prefix, whatever mf says its location is.
func signedAt(t *testing.T, store *memStore, prefix string, mf Manifest) {
	t.Helper()
	mf.SpecSHA256, _, _ = SHA256Reader(strings.NewReader(string(testSpecJSON)))
	mf.TarSHA256, mf.TarBytes, _ = SHA256Reader(strings.NewReader(string(testTar)))
	payload, _ := json.Marshal(mf)
	envelope, err := testArchiveSigner.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	store.objects[prefix+"manifest.json"] = envelope
	store.objects[prefix+"spec.json"] = testSpecJSON
	store.objects[prefix+"checkpoint.tar"] = testTar
}

// Without keys the agent restores nothing, and downloads nothing.
func TestRestoreWithoutKeysFailsClosed(t *testing.T) {
	store := newMemStore()
	putArchive(t, store, boundManifest("ps-web-0-deadbeef"), testSpecJSON, testTar)
	res := restoreArchive(t, store, nil, nil)
	requireRefused(t, res, contract.RestoreReasonSigningKeyUnavailable)
	if len(res.gets) != 0 {
		t.Fatalf("fetched %v without a key to verify them", res.gets)
	}
}

// Other failures carry the catch-all reason.
func TestRestoreOtherFailuresAreRestoreFailed(t *testing.T) {
	res := restoreArchive(t, newMemStore(), testArchiveKeyring, nil) // no archive at all
	requireRefused(t, res, contract.RestoreReasonFailed)
}

// A reason is only ever stamped with failed: a restored pod loses one it was
// created with, so the admission policy doesn't refuse the agent's write.
func TestRestoreDropsReasonOnSuccess(t *testing.T) {
	store := newMemStore()
	putArchive(t, store, boundManifest("ps-web-0-deadbeef"), testSpecJSON, testTar)
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationRestoreReason] = contract.RestoreReasonSignatureInvalid
	c := newClient(t, ps, pod)
	r := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
		Ops: &fakeRestoreOps{}, Store: store, WorkDir: t.TempDir()}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusRestored {
		t.Fatalf("status = %q, want restored", got.Annotations[contract.AnnotationRestoreStatus])
	}
	if v, ok := got.Annotations[contract.AnnotationRestoreReason]; ok {
		t.Fatalf("%s = %q left on a restored pod", contract.AnnotationRestoreReason, v)
	}
}

// What the checkpoint writes, the restore accepts: one key, both halves.
func TestCheckpointThenRestoreRoundTrip(t *testing.T) {
	c := signedFixture(t)
	store := newMemStore()
	runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
		Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: store})
	repl := replacementPodOnNode(cancelSnapshot)
	repl.Spec.Containers = []corev1.Container{{Name: "c0"}}
	if err := c.Create(context.Background(), repl); err != nil {
		t.Fatal(err)
	}
	ops := &fakeRestoreOps{}
	r := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
		Ops: ops, Store: store, WorkDir: t.TempDir()}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.restored) != 1 {
		got := &corev1.Pod{}
		_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
		t.Fatalf("restores = %d, annotations %v", len(ops.restored), got.Annotations)
	}

	// The same archive, restored by an agent of another cluster with the same
	// key, is refused.
	other := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: "other-cluster-uid", Client: c, NodeName: "node-a",
		Ops: &fakeRestoreOps{}, Store: store, WorkDir: t.TempDir()}
	if err := other.restore(context.Background(), repl, cancelSnapshot); !errors.Is(err, errArchiveBindingMismatch) {
		t.Fatalf("restore in another cluster = %v, want a binding mismatch", err)
	}
}

// The trigger is recorded in the manifest, not enforced at restore: a job
// that creates its trigger again after its checkpoint is Ready takes its own
// work order over (#262), and the archive, which names the earlier trigger
// instance, still restores for that job. The job UID is what ties it.
func TestRestoreAcceptsArchiveOfTheSameJobsEarlierTrigger(t *testing.T) {
	t.Run("work order taken over by the job's trigger created again", func(t *testing.T) {
		ctx := context.Background()
		c := signedFixture(t)
		store := newMemStore()
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
			Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: store})

		// A stale reconcile of test-pmj creates its trigger again after the
		// Evicting cleanup deleted it; the engine controller hands it the
		// job's own Ready work order.
		old := contract.New(contract.GVKManualTrigger)
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: cancelTrigger}, old); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ctx, old); err != nil {
			t.Fatal(err)
		}
		again := psmtWithUID("web-0", "trigger-uid-2")
		again.SetOwnerReferences([]metav1.OwnerReference{testJobControllerRef()})
		if err := c.Create(ctx, again); err != nil {
			t.Fatal(err)
		}
		reconcilePSMT(t, c, nil, cancelTrigger)
		if got := workOrderString(getSnapshot(t, c, cancelSnapshot), specTriggerUID); got != "trigger-uid-2" {
			t.Fatalf("test setup: the job's new trigger didn't take its work order over (triggerUID %q)", got)
		}
		// The manifest still records the trigger it was signed for.
		if mf := openManifest(t, store, "bkt/snaps/"+cancelSnapshot+"/manifest.json"); mf.TriggerName != cancelTrigger || mf.TriggerUID != "trigger-uid-1" {
			t.Fatalf("manifest records trigger %q (%q), want %s (trigger-uid-1)", mf.TriggerName, mf.TriggerUID, cancelTrigger)
		}

		repl := replacementPodOnNode(cancelSnapshot)
		repl.Spec.Containers = []corev1.Container{{Name: "c0"}}
		if err := c.Create(ctx, repl); err != nil {
			t.Fatal(err)
		}
		ops := &fakeRestoreOps{}
		r := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID, Client: c, NodeName: "node-a",
			Ops: ops, Store: store, WorkDir: t.TempDir()}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
			t.Fatal(err)
		}
		got := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "web-1"}, got); err != nil {
			t.Fatal(err)
		}
		if len(ops.restored) != 1 || got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusRestored {
			t.Fatalf("restores = %d, annotations %v; want the job's own archive restored", len(ops.restored), got.Annotations)
		}
	})

	t.Run("work order answering another trigger UID", func(t *testing.T) {
		mf := boundManifest("ps-web-0-deadbeef")
		mf.TriggerName, mf.TriggerUID = cancelTrigger, "trigger-uid-1"
		store := newMemStore()
		putArchive(t, store, mf, testSpecJSON, testTar)
		res := restoreArchive(t, store, testArchiveKeyring, func(ps *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(ps.Object, cancelTrigger, "spec", specTriggerName)
			_ = unstructured.SetNestedField(ps.Object, "trigger-uid-2", "spec", specTriggerUID)
		})
		if len(res.ops.restored) != 1 || res.pod.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusRestored {
			t.Fatalf("restores = %d, annotations %v (event %q); want restored", len(res.ops.restored), res.pod.Annotations, res.event)
		}
	})
}

// checkBindings requires a job and a cluster even when the restore expects
// none (both sides empty): an archive bound to no job or no cluster is never
// restored. A cluster mismatch doesn't echo the cluster UIDs.
func TestCheckBindingsRequiresJobAndCluster(t *testing.T) {
	mf := boundManifest("ps-web-0-deadbeef")
	if err := checkBindings(&mf, &mf); err != nil {
		t.Fatalf("matching bindings: %v", err)
	}
	for field, clear := range map[string]func(*Manifest){
		"jobUID":     func(m *Manifest) { m.JobUID = "" },
		"clusterUID": func(m *Manifest) { m.ClusterUID = "" },
	} {
		got, want := mf, mf
		clear(&got)
		clear(&want)
		if err := checkBindings(&got, &want); !errors.Is(err, errArchiveBindingMismatch) || !strings.Contains(err.Error(), field+" missing") {
			t.Errorf("%s empty on both sides: error = %v, want %s missing", field, err, field)
		}
	}
	other := mf
	other.ClusterUID = "other-cluster-uid"
	err := checkBindings(&other, &mf)
	if !errors.Is(err, errArchiveBindingMismatch) || !strings.Contains(err.Error(), "clusterUID doesn't match") ||
		strings.Contains(err.Error(), "other-cluster-uid") || strings.Contains(err.Error(), testClusterUID) {
		t.Fatalf("another cluster: error = %v, want a mismatch that names no cluster UID", err)
	}
}

// Only a controller reference to a PodMigrationJob of the orchestrator's
// group names the job an archive is bound to.
func TestControllingJob(t *testing.T) {
	notController := false
	ref := func(mutate func(*metav1.OwnerReference)) []metav1.OwnerReference {
		r := testJobControllerRef()
		mutate(&r)
		return []metav1.OwnerReference{r}
	}
	cases := map[string]struct {
		refs []metav1.OwnerReference
		want string
	}{
		"the PodMigrationJob controller":     {ref(func(*metav1.OwnerReference) {}), testJobUID},
		"no owner":                           {nil, ""},
		"an owner that isn't the controller": {ref(func(r *metav1.OwnerReference) { r.Controller = &notController }), ""},
		"a controller of another kind":       {ref(func(r *metav1.OwnerReference) { r.Kind, r.APIVersion = "ReplicaSet", "apps/v1" }), ""},
		"a PodMigrationJob of another group": {ref(func(r *metav1.OwnerReference) { r.APIVersion = "example.com/v1alpha1" }), ""},
		"a PodMigrationJob without a group":  {ref(func(r *metav1.OwnerReference) { r.APIVersion = "v1" }), ""},
		"an apiVersion that doesn't parse":   {ref(func(r *metav1.OwnerReference) { r.APIVersion = "a/b/c" }), ""},
	}
	for name, tc := range cases {
		psmt := psmtFor("web-0")
		psmt.SetOwnerReferences(tc.refs)
		if got := controllingJob(psmt); got != tc.want {
			t.Errorf("%s: controllingJob = %q, want %q", name, got, tc.want)
		}
	}
}
