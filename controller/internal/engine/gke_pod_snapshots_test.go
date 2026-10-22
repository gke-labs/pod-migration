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

package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

func mustParsePolicy(t *testing.T, raw string) *eligibility.RuntimeClassPolicy {
	t.Helper()
	if raw == "" {
		return nil
	}
	p, err := eligibility.ParseRuntimeClassPolicy(raw)
	if err != nil {
		t.Fatalf("ParseRuntimeClassPolicy(%q): %v", raw, err)
	}
	return p
}

func TestGKEPodSnapshots_Identity(t *testing.T) {
	e := NewGKEPodSnapshots(nil, nil, nil)
	if got := e.Name(); got != "gke-pod-snapshots" {
		t.Errorf("Name() = %q, want gke-pod-snapshots", got)
	}
	if got := e.RuntimeClasses(); !reflect.DeepEqual(got, []string{"gvisor"}) {
		t.Errorf("RuntimeClasses() = %v, want [gvisor]", got)
	}
	if got, want := e.Guarantees(), (Guarantees{Fencing: FencingStopped, RestoreOnce: false}); got != want {
		t.Errorf("Guarantees() = %+v, want %+v", got, want)
	}
	var asEngine Engine = e
	if _, ok := asEngine.(RestoreOrderer); !ok {
		t.Errorf("GKE Pod Snapshots doesn't implement RestoreOrderer")
	}
}

func TestNewDefaultRegistry(t *testing.T) {
	r, err := NewDefaultRegistry(fake.NewClientBuilder().WithScheme(testScheme()).Build(), testScheme(), nil)
	if err != nil {
		t.Fatalf("NewDefaultRegistry: %v", err)
	}
	def, err := r.Default()
	if err != nil || def.Name() != GKEPodSnapshotsName {
		t.Fatalf("Default() = %v, %v; want gke-pod-snapshots", def, err)
	}
	if got, err := r.ForJob(nil); err != nil || got != def {
		t.Errorf("ForJob() = %v, %v; want gke-pod-snapshots", got, err)
	}
	// GKE Pod Snapshots serves every pod, as the only engine did before the
	// registry; its Eligible applies the operator allow-list.
	for _, class := range []string{"gvisor", "", "runc"} {
		if got, ok := r.ForPod(podWithRuntimeClass(class), nil); !ok || got != def {
			t.Errorf("ForPod(%q) = %v, %t; want gke-pod-snapshots", class, got, ok)
		}
	}
}

// The expected messages are the ones eviction_webhook_test.go asserts for
// the same pods and allow-lists.
func TestGKEPodSnapshots_EligibleIsTheRuntimeClassAllowList(t *testing.T) {
	tests := []struct {
		name       string
		allowList  string // "" leaves the policy nil: gVisor only
		class      string // "" leaves runtimeClassName unset
		wantOK     bool
		wantReason string
	}{
		{name: "gvisor pod, default list", class: "gvisor", wantOK: true},
		{name: "pod without a runtime class, default list", class: "",
			wantReason: "Pod runtime class @default is not migratable (allowed: gvisor), skipping migration"},
		{name: "other runtime class, default list", class: "other",
			wantReason: "Pod runtime class other is not migratable (allowed: gvisor), skipping migration"},
		{name: "gvisor pod, @default only", allowList: "@default", class: "gvisor",
			wantReason: "Pod runtime class gvisor is not migratable (allowed: @default), skipping migration"},
		{name: "pod without a runtime class, @default only", allowList: "@default", class: "", wantOK: true},
		{name: "other runtime class, gvisor and @default", allowList: "gvisor,@default", class: "other",
			wantReason: "Pod runtime class other is not migratable (allowed: gvisor,@default), skipping migration"},
		{name: "pod without a runtime class, gvisor and @default", allowList: "gvisor,@default", class: "", wantOK: true},
		{name: "gvisor pod, gvisor and @default", allowList: "gvisor,@default", class: "gvisor", wantOK: true},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewGKEPodSnapshots(nil, nil, mustParsePolicy(t, tt.allowList))
			want := Verdict{OK: tt.wantOK, Reason: tt.wantReason}
			// The node isn't used yet: with or without it, the verdict is the same.
			for _, n := range []*corev1.Node{nil, node} {
				if got := e.Eligible(podWithRuntimeClass(tt.class), n); got != want {
					t.Errorf("Eligible(node=%v) = %+v, want %+v", n != nil, got, want)
				}
			}
		})
	}

	if got := NewGKEPodSnapshots(nil, nil, nil).Eligible(nil, nil); got.OK {
		t.Errorf("Eligible(nil pod) = %+v, want a refusal", got)
	}
}

func gkePolicy(name, namespace string, created time.Time, triggerType, postCheckpoint string, ready bool, selector map[string]interface{}) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotPolicy"})
	u.SetName(name)
	u.SetNamespace(namespace)
	u.SetCreationTimestamp(metav1.NewTime(created))
	u.Object["spec"] = map[string]interface{}{
		"triggerConfig": map[string]interface{}{
			"type":           triggerType,
			"postCheckpoint": postCheckpoint,
		},
		"selector": selector,
	}
	status := "False"
	if ready {
		status = "True"
	}
	u.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{"type": "Ready", "status": status},
		},
	}
	return u
}

// TestGKEPodSnapshots_ResolvePolicyIsTheExistingLookup checks ResolvePolicy
// against util.FindLatestReadyManualStopPSP, the lookup the eviction webhook
// and the preemption trigger call, case by case.
func TestGKEPodSnapshots_ResolvePolicyIsTheExistingLookup(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	matchApp := map[string]interface{}{"matchLabels": map[string]interface{}{"app": "redis"}}
	generated := map[string]interface{}{
		"matchExpressions": []interface{}{
			map[string]interface{}{"key": "pod-migration.gke.io/enabled", "operator": "In", "values": []interface{}{"true"}},
		},
	}
	podLabels := map[string]string{"app": "redis", "pod-migration.gke.io/enabled": "true"}

	tests := []struct {
		name     string
		policies []client.Object
		labels   map[string]string
		want     string // "" means no policy
	}{
		{
			name:     "ready manual stop policy that selects the pod",
			policies: []client.Object{gkePolicy("psp-valid", "default", now, "manual", "stop", true, matchApp)},
			labels:   podLabels,
			want:     "psp-valid",
		},
		{
			name:     "the generated policy's selector",
			policies: []client.Object{gkePolicy("psp-test-manual", "default", now, "manual", "stop", true, generated)},
			labels:   podLabels,
			want:     "psp-test-manual",
		},
		{
			name:     "not ready",
			policies: []client.Object{gkePolicy("psp-not-ready", "default", now, "manual", "stop", false, matchApp)},
			labels:   podLabels,
		},
		{
			name:     "not a manual trigger",
			policies: []client.Object{gkePolicy("psp-scheduled", "default", now, "scheduled", "stop", true, matchApp)},
			labels:   podLabels,
		},
		{
			name:     "selector doesn't match",
			policies: []client.Object{gkePolicy("psp-valid", "default", now, "manual", "stop", true, matchApp)},
			labels:   map[string]string{"app": "other"},
		},
		{
			name:     "policy in another namespace",
			policies: []client.Object{gkePolicy("psp-valid", "elsewhere", now, "manual", "stop", true, matchApp)},
			labels:   podLabels,
		},
		{
			name: "newest of two stop policies",
			policies: []client.Object{
				gkePolicy("psp-old", "default", now.Add(-time.Minute), "manual", "stop", true, matchApp),
				gkePolicy("psp-new", "default", now, "manual", "stop", true, matchApp),
			},
			labels: podLabels,
			want:   "psp-new",
		},
		{
			name: "newest matching policy doesn't stop the source",
			policies: []client.Object{
				gkePolicy("psp-stop", "default", now.Add(-time.Minute), "manual", "stop", true, matchApp),
				gkePolicy("psp-resume", "default", now, "manual", "resume", true, matchApp),
			},
			labels: podLabels,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(tt.policies...).Build()
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", Labels: tt.labels}}

			got, err := NewGKEPodSnapshots(c, testScheme(), nil).ResolvePolicy(context.Background(), pod)
			if err != nil {
				t.Fatalf("ResolvePolicy: %v", err)
			}
			existing, err := util.FindLatestReadyManualStopPSP(context.Background(), c, pod.Namespace, pod.Labels)
			if err != nil {
				t.Fatalf("FindLatestReadyManualStopPSP: %v", err)
			}

			if tt.want == "" {
				if got != nil || existing != nil {
					t.Fatalf("ResolvePolicy = %+v, existing lookup = %v; want no policy from both", got, existing)
				}
				return
			}
			want := &PolicyRef{Group: "podsnapshot.gke.io", Kind: "PodSnapshotPolicy", Name: tt.want}
			if got == nil || *got != *want {
				t.Fatalf("ResolvePolicy = %+v, want %+v", got, want)
			}
			if existing == nil || existing.GetName() != got.Name {
				t.Fatalf("existing lookup = %v, ResolvePolicy = %q; want the same policy", existing, got.Name)
			}
		})
	}
}

func TestGKEPodSnapshots_ResolvePolicyListFailures(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	listErr := func(err error) client.Client {
		return fake.NewClientBuilder().WithScheme(testScheme()).WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return err },
		}).Build()
	}

	t.Run("missing PodSnapshotPolicy CRD means no policy", func(t *testing.T) {
		c := listErr(&meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "podsnapshot.gke.io", Kind: "PodSnapshotPolicy"}})
		got, err := NewGKEPodSnapshots(c, testScheme(), nil).ResolvePolicy(context.Background(), pod)
		if err != nil || got != nil {
			t.Fatalf("ResolvePolicy = %+v, %v; want no policy and no error", got, err)
		}
	})

	t.Run("list error is returned", func(t *testing.T) {
		c := listErr(apierrors.NewServiceUnavailable("etcd is down"))
		got, err := NewGKEPodSnapshots(c, testScheme(), nil).ResolvePolicy(context.Background(), pod)
		if err == nil || got != nil {
			t.Fatalf("ResolvePolicy = %+v, %v; want an error", got, err)
		}
		if !apierrors.IsServiceUnavailable(errors.Unwrap(err)) {
			t.Errorf("ResolvePolicy error = %v, want it to wrap the list error", err)
		}
	})

	t.Run("nil pod", func(t *testing.T) {
		if _, err := NewGKEPodSnapshots(listErr(nil), testScheme(), nil).ResolvePolicy(context.Background(), nil); err == nil {
			t.Fatalf("ResolvePolicy(nil) returned no error")
		}
	})
}

// TestGKEPodSnapshots_SnapshotsIsTheGKEProvider checks that Snapshots hands
// out the GKE provider, including the cold-start bypass it sets on the
// source before it creates the trigger (#151).
func TestGKEPodSnapshots_SnapshotsIsTheGKEProvider(t *testing.T) {
	const podName, podUID = "src", "src-uid"
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default", UID: types.UID("job-uid")},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: podUID,
		},
	}
	source := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: podName, Namespace: "default", UID: types.UID(podUID),
		Annotations: map[string]string{"keep": "me", "podsnapshot.gke.io/ps-name": "stale-snapshot"},
	}}
	var ops []string
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(source).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				ops = append(ops, "patch source")
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "PodSnapshotManualTrigger" {
				ops = append(ops, "create trigger")
			}
			return cl.Create(ctx, obj, opts...)
		},
	}).Build()

	e := NewGKEPodSnapshots(c, scheme, nil)
	provider := e.Snapshots()
	if _, ok := provider.(*snapshot.GKEProvider); !ok {
		t.Fatalf("Snapshots() = %T, want *snapshot.GKEProvider", provider)
	}
	if e.Snapshots() != provider {
		t.Errorf("Snapshots() returned a different provider on the second call")
	}

	ctx := context.Background()
	if _, err := provider.EnsureTrigger(ctx, job, podName); err != nil {
		t.Fatalf("EnsureTrigger: %v", err)
	}
	if want := []string{"patch source", "create trigger"}; !reflect.DeepEqual(ops, want) {
		t.Errorf("operations = %v, want %v", ops, want)
	}
	got := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(source), got); err != nil {
		t.Fatalf("getting the source: %v", err)
	}
	if want := map[string]string{"keep": "me", "podsnapshot.gke.io/ps-name": ""}; !reflect.DeepEqual(got.Annotations, want) {
		t.Errorf("source annotations = %v, want %v", got.Annotations, want)
	}

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotManualTrigger"})
	key := types.NamespacedName{Namespace: "default", Name: util.FormatPSMTName(podName, podUID)}
	if err := c.Get(ctx, key, trigger); err != nil {
		t.Fatalf("getting the trigger: %v", err)
	}
	if target, _, _ := unstructured.NestedString(trigger.Object, "spec", "targetPod"); target != podName {
		t.Errorf("trigger spec.targetPod = %q, want %q", target, podName)
	}
	if refs := trigger.GetOwnerReferences(); len(refs) != 1 || refs[0].UID != job.UID {
		t.Errorf("trigger owner references = %v, want the job", refs)
	}

	status, err := provider.CheckStatus(ctx, job, podName)
	if err != nil || status.Phase != snapshot.PhaseInProgress {
		t.Errorf("CheckStatus = %+v, %v; want InProgress", status, err)
	}

	if err := provider.Cleanup(ctx, job, podName); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if err := c.Get(ctx, key, trigger); !apierrors.IsNotFound(err) {
		t.Errorf("trigger after Cleanup: err = %v, want NotFound", err)
	}
}

func TestGKEPodSnapshots_RestoreFailuresIsTheGVisorClassifier(t *testing.T) {
	failures := NewGKEPodSnapshots(nil, nil, nil).RestoreFailures()
	if !reflect.DeepEqual(failures, restore.DefaultEngines()) {
		t.Fatalf("RestoreFailures() = %v, want restore.DefaultEngines() = %v", failures, restore.DefaultEngines())
	}

	restoreCrash := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
		Name: "app",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason:   "StartError",
			ExitCode: 128,
			Message:  "failed to start containerd task: OCI runtime restore failed",
		}},
	}}}}
	if got := restore.Classify(restoreCrash, failures...); got.Class != restore.FailureFatal || got.Engine != "gvisor" {
		t.Errorf("Classify(restore crash) = %+v, want FailureFatal from gvisor", got)
	}

	appCrash := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
		Name:                 "app",
		State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}},
	}}}}
	if got := restore.Classify(appCrash, failures...); got.Class != restore.FailureNone {
		t.Errorf("Classify(application crash) = %+v, want FailureNone", got)
	}
}

func testPodMigration(location string) *pmv1alpha1.PodMigration {
	return &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "test-migration", Namespace: "default", UID: types.UID("pm-uid")},
		Spec:       pmv1alpha1.PodMigrationSpec{Storage: pmv1alpha1.StorageSpec{Location: location}},
	}
}

func wantStorageConfig(gcs map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "podsnapshot.gke.io/v1",
		"kind":       "PodSnapshotStorageConfig",
		"metadata": map[string]interface{}{
			"name":   "pssc-bf06f33fdcf4cafc",
			"labels": map[string]interface{}{"podmigration.gke.io/owner-uid": "pm-uid"},
		},
		"spec": map[string]interface{}{
			"snapshotStorageConfig": map[string]interface{}{"gcs": gcs},
		},
	}
}

var wantPolicy = map[string]interface{}{
	"apiVersion": "podsnapshot.gke.io/v1",
	"kind":       "PodSnapshotPolicy",
	"metadata": map[string]interface{}{
		"name":      "psp-test-migration-manual",
		"namespace": "default",
	},
	"spec": map[string]interface{}{
		"storageConfigName": "pssc-bf06f33fdcf4cafc",
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      "pod-migration.gke.io/enabled",
					"operator": "In",
					"values":   []interface{}{"true"},
				},
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
	},
}

func TestGKEPodSnapshots_NamespaceResources(t *testing.T) {
	tests := []struct {
		name     string
		location string
		wantGCS  map[string]interface{}
	}{
		{name: "bucket and path", location: "gs://my-test-bucket/snapshots/path",
			wantGCS: map[string]interface{}{"bucket": "my-test-bucket", "path": "snapshots/path"}},
		{name: "bucket only", location: "gs://my-test-bucket",
			wantGCS: map[string]interface{}{"bucket": "my-test-bucket"}},
		{name: "bucket with a trailing slash", location: "gs://my-test-bucket/",
			wantGCS: map[string]interface{}{"bucket": "my-test-bucket"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewGKEPodSnapshots(nil, nil, nil).NamespaceResources(testPodMigration(tt.location))
			if err != nil {
				t.Fatalf("NamespaceResources: %v", err)
			}
			want := []map[string]interface{}{wantStorageConfig(tt.wantGCS), wantPolicy}
			if len(got) != len(want) {
				t.Fatalf("NamespaceResources returned %d objects, want %d", len(got), len(want))
			}
			for i := range want {
				if !reflect.DeepEqual(got[i].Object, want[i]) {
					t.Errorf("object %d:\n got %v\nwant %v", i, got[i].Object, want[i])
				}
			}
		})
	}
}

func TestGKEPodSnapshots_NamespaceResourcesRejectsNonGCSLocations(t *testing.T) {
	for _, location := range []string{"http://my-test-bucket", "my-test-bucket", ""} {
		got, err := NewGKEPodSnapshots(nil, nil, nil).NamespaceResources(testPodMigration(location))
		var locationErr *StorageLocationError
		if !errors.As(err, &locationErr) {
			t.Fatalf("NamespaceResources(%q) error = %v, want a *StorageLocationError", location, err)
		}
		if want := "invalid GCS location: " + location + " (must start with gs://)"; err.Error() != want {
			t.Errorf("error = %q, want %q", err.Error(), want)
		}
		if locationErr.Location != location {
			t.Errorf("Location = %q, want %q", locationErr.Location, location)
		}
		if got != nil {
			t.Errorf("NamespaceResources(%q) returned objects %v with the error", location, got)
		}
	}

	if _, err := NewGKEPodSnapshots(nil, nil, nil).NamespaceResources(nil); err == nil {
		t.Errorf("NamespaceResources(nil) returned no error")
	}
}

func TestGKEPodSnapshots_ResourceNames(t *testing.T) {
	// The storage config name is a hash of namespace/name; existing clusters
	// have objects with these names, so they must never change.
	if got := PodSnapshotStorageConfigName("default", "test-migration"); got != "pssc-bf06f33fdcf4cafc" {
		t.Errorf("PodSnapshotStorageConfigName(default, test-migration) = %q", got)
	}
	if got := PodSnapshotStorageConfigName("team-a", "mig"); got != "pssc-c5130a48141ab376" {
		t.Errorf("PodSnapshotStorageConfigName(team-a, mig) = %q", got)
	}
	if got := PodSnapshotPolicyName("mig"); got != "psp-mig-manual" {
		t.Errorf("PodSnapshotPolicyName(mig) = %q", got)
	}
}

func TestGKEPodSnapshots_OrderRestore(t *testing.T) {
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"},
		Status:     pmv1alpha1.PodMigrationJobStatus{SnapshotRef: "snap-1"},
	}
	tests := []struct {
		name        string
		annotations map[string]string
		want        map[string]string
	}{
		{name: "no annotations", want: map[string]string{"podsnapshot.gke.io/ps-name": "snap-1"}},
		{name: "keeps other annotations",
			annotations: map[string]string{"pod-migration.gke.io/assigned-pmj": "job"},
			want:        map[string]string{"pod-migration.gke.io/assigned-pmj": "job", "podsnapshot.gke.io/ps-name": "snap-1"}},
		{name: "replaces the cold-start bypass",
			annotations: map[string]string{"podsnapshot.gke.io/ps-name": ""},
			want:        map[string]string{"podsnapshot.gke.io/ps-name": "snap-1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "replacement", Annotations: tt.annotations}}
			if err := NewGKEPodSnapshots(nil, nil, nil).OrderRestore(pod, job); err != nil {
				t.Fatalf("OrderRestore: %v", err)
			}
			if !reflect.DeepEqual(pod.Annotations, tt.want) {
				t.Errorf("annotations = %v, want %v", pod.Annotations, tt.want)
			}
		})
	}

	t.Run("a job without a snapshot is rejected", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "replacement"}}
		noSnapshot := job.DeepCopy()
		noSnapshot.Status.SnapshotRef = ""
		if err := NewGKEPodSnapshots(nil, nil, nil).OrderRestore(pod, noSnapshot); err == nil {
			t.Fatalf("OrderRestore without a snapshot returned no error")
		}
		if pod.Annotations != nil {
			t.Errorf("annotations = %v, want the pod unchanged", pod.Annotations)
		}
	})

	t.Run("nil pod or job", func(t *testing.T) {
		e := NewGKEPodSnapshots(nil, nil, nil)
		if err := e.OrderRestore(nil, job); err == nil {
			t.Errorf("OrderRestore(nil pod) returned no error")
		}
		if err := e.OrderRestore(&corev1.Pod{}, nil); err == nil {
			t.Errorf("OrderRestore(nil job) returned no error")
		}
	})
}

func TestGKEPodSnapshots_OrderColdStartBypass(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        map[string]string
	}{
		{name: "no annotations", want: map[string]string{"podsnapshot.gke.io/ps-name": ""}},
		{name: "replaces a stale snapshot name and keeps other annotations",
			annotations: map[string]string{"podsnapshot.gke.io/ps-name": "stale", "keep": "me"},
			want:        map[string]string{"podsnapshot.gke.io/ps-name": "", "keep": "me"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "replacement", Annotations: tt.annotations}}
			if err := NewGKEPodSnapshots(nil, nil, nil).OrderColdStartBypass(pod); err != nil {
				t.Fatalf("OrderColdStartBypass: %v", err)
			}
			if !reflect.DeepEqual(pod.Annotations, tt.want) {
				t.Errorf("annotations = %v, want %v", pod.Annotations, tt.want)
			}
		})
	}

	if err := NewGKEPodSnapshots(nil, nil, nil).OrderColdStartBypass(nil); err == nil {
		t.Errorf("OrderColdStartBypass(nil) returned no error")
	}
}
