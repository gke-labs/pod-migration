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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// GKEPodSnapshotsName is the registry name of the GKE Pod Snapshots engine.
const GKEPodSnapshotsName = "gke-pod-snapshots"

const (
	gkeGroup   = "podsnapshot.gke.io"
	gkeVersion = "v1"

	// gkeRuntimeClass is the runtime class GKE Pod Snapshots supports.
	gkeRuntimeClass = "gvisor"

	// gkeRestoreAnnotation is the GKE Pod Snapshots restore order. When the
	// agent creates a pod's sandbox, a snapshot name in it means "restore
	// from that snapshot", and an empty value means "start cold".
	gkeRestoreAnnotation = "podsnapshot.gke.io/ps-name"

	// migrationEnabledLabel opts a pod into migration. The generated policy
	// selects the pods that carry it.
	migrationEnabledLabel = "pod-migration.gke.io/enabled"
)

// GKEPodSnapshots is the GKE Pod Snapshots engine, for gVisor pods. Each
// method hands out what the controller used before the engine package
// existed.
type GKEPodSnapshots struct {
	reader   client.Reader
	provider *snapshot.GKEProvider
	// events reads the events that VerifyRestore inspects, from the API
	// server rather than the cache.
	events client.Reader
}

var (
	_ Engine          = (*GKEPodSnapshots)(nil)
	_ RestoreOrderer  = (*GKEPodSnapshots)(nil)
	_ RestoreVerifier = (*GKEPodSnapshots)(nil)
)

// GKEOption configures the GKE Pod Snapshots engine.
type GKEOption func(*GKEPodSnapshots)

// WithAPIReader sets the reader of the events that VerifyRestore inspects.
// Events aren't cached, so pass a reader that reads the API server, such as
// a manager's API reader. Without it, the engine's client reads them.
func WithAPIReader(r client.Reader) GKEOption {
	return func(e *GKEPodSnapshots) {
		if r != nil {
			e.events = r
		}
	}
}

// NewGKEPodSnapshots returns the GKE Pod Snapshots engine. c serves the
// policy lookup and the snapshot provider, and scheme sets the owner
// reference on checkpoint triggers.
func NewGKEPodSnapshots(c client.Client, scheme *runtime.Scheme, opts ...GKEOption) *GKEPodSnapshots {
	e := &GKEPodSnapshots{
		reader:   c,
		provider: snapshot.NewGKEProvider(c, scheme),
		events:   c,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// NewDefaultRegistry returns the registry of a stock deployment: GKE Pod
// Snapshots, registered as the default engine, serving every runtime class
// that runtimeClasses allows (RoutesFromAllowList). runtimeClasses is the
// operator allow-list (--migratable-runtime-classes); nil means gVisor only.
// opts configure the engine.
func NewDefaultRegistry(c client.Client, scheme *runtime.Scheme, runtimeClasses *eligibility.RuntimeClassPolicy, opts ...GKEOption) (*Registry, error) {
	r := NewRegistry(GKEPodSnapshotsName)
	if err := r.Register(NewGKEPodSnapshots(c, scheme, opts...)); err != nil {
		return nil, err
	}
	if err := r.SetRoutes(RoutesFromAllowList(runtimeClasses, GKEPodSnapshotsName)); err != nil {
		return nil, err
	}
	return r, nil
}

// Name implements Engine.
func (*GKEPodSnapshots) Name() string { return GKEPodSnapshotsName }

// RuntimeClasses implements Engine: gVisor.
func (*GKEPodSnapshots) RuntimeClasses() []string { return []string{gkeRuntimeClass} }

// Eligible implements Engine. GKE Pod Snapshots refuses no pod: the
// operator allow-list (--migratable-runtime-classes), which callers check
// first, and the routes decide which pods reach it, as the allow-list alone
// did before the registry. The node isn't used yet.
func (*GKEPodSnapshots) Eligible(pod *corev1.Pod, _ *corev1.Node) Verdict {
	if pod == nil {
		return Verdict{Reason: "no pod to migrate"}
	}
	return Verdict{OK: true}
}

// ResolvePolicy implements Engine with util.FindLatestReadyManualStopPSP: the
// newest Ready, manual PodSnapshotPolicy that selects the pod, provided its
// postCheckpoint is stop. Missing PodSnapshotPolicy CRDs mean no policy.
func (e *GKEPodSnapshots) ResolvePolicy(ctx context.Context, pod *corev1.Pod) (*PolicyRef, error) {
	if pod == nil {
		return nil, errors.New("resolving a PodSnapshotPolicy for a nil pod")
	}
	psp, err := util.FindLatestReadyManualStopPSP(ctx, e.reader, pod.Namespace, pod.Labels)
	if err != nil || psp == nil {
		return nil, err
	}
	return &PolicyRef{Group: gkeGroup, Kind: "PodSnapshotPolicy", Name: psp.GetName()}, nil
}

// Snapshots implements Engine with the GKE provider. It sets the cold-start
// bypass on the source before it creates the checkpoint trigger (#151).
func (e *GKEPodSnapshots) Snapshots() snapshot.Provider { return e.provider }

// RestoreFailures implements Engine with restore.DefaultEngines(), the gVisor
// classifier. Invariant I9 classifies with the same set.
func (*GKEPodSnapshots) RestoreFailures() []restore.Engine { return restore.DefaultEngines() }

// Guarantees implements Engine. GKE Pod Snapshots stops the source after the
// checkpoint (postCheckpoint: stop), and nothing shows it restores a
// replacement at most once (#152).
func (*GKEPodSnapshots) Guarantees() Guarantees {
	return Guarantees{Fencing: FencingStopped, RestoreOnce: false}
}

// NamespaceResources implements Engine. For a PodMigration that stores
// snapshots at gs://bucket/path, it renders:
//
//   - a cluster-scoped PodSnapshotStorageConfig that points at the bucket and
//     path, labeled with the PodMigration's UID;
//   - a PodSnapshotPolicy in the PodMigration's namespace, with a manual
//     trigger and postCheckpoint: stop, that selects the pods opted into
//     migration and uses that storage config.
//
// A location that doesn't start with gs:// is a *StorageLocationError.
func (*GKEPodSnapshots) NamespaceResources(pm *pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error) {
	if pm == nil {
		return nil, errors.New("rendering namespace resources for a nil PodMigration")
	}
	location := pm.Spec.Storage.Location
	if !strings.HasPrefix(location, "gs://") {
		return nil, &StorageLocationError{
			Location: location,
			Message:  fmt.Sprintf("invalid GCS location: %s (must start with gs://)", location),
		}
	}
	parts := strings.SplitN(strings.TrimPrefix(location, "gs://"), "/", 2)
	gcs := map[string]interface{}{
		"bucket": parts[0],
	}
	if len(parts) > 1 && parts[1] != "" {
		gcs["path"] = parts[1]
	}

	storageConfigName := PodSnapshotStorageConfigName(pm.Namespace, pm.Name)
	storageConfig := &unstructured.Unstructured{}
	storageConfig.SetGroupVersionKind(gkeGVK("PodSnapshotStorageConfig"))
	storageConfig.SetName(storageConfigName)
	storageConfig.SetLabels(map[string]string{OwnerUIDLabel: string(pm.UID)})
	storageConfig.Object["spec"] = map[string]interface{}{
		"snapshotStorageConfig": map[string]interface{}{
			"gcs": gcs,
		},
	}

	policy := &unstructured.Unstructured{}
	policy.SetGroupVersionKind(gkeGVK("PodSnapshotPolicy"))
	policy.SetName(PodSnapshotPolicyName(pm.Name))
	policy.SetNamespace(pm.Namespace)
	policy.Object["spec"] = map[string]interface{}{
		"storageConfigName": storageConfigName,
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      migrationEnabledLabel,
					"operator": "In",
					"values":   []interface{}{"true"},
				},
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
	}

	return []*unstructured.Unstructured{storageConfig, policy}, nil
}

// OrderRestore implements RestoreOrderer: it sets the restore annotation to
// the job's snapshot.
func (*GKEPodSnapshots) OrderRestore(pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob) error {
	if pod == nil || job == nil {
		return errors.New("ordering a restore needs a pod and a job")
	}
	if job.Status.SnapshotRef == "" {
		return fmt.Errorf("job %s/%s has no snapshot to restore", job.Namespace, job.Name)
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[gkeRestoreAnnotation] = job.Status.SnapshotRef
	return nil
}

// OrderColdStartBypass implements RestoreOrderer: an empty restore
// annotation makes GKE Pod Snapshots start the pod cold, even when a
// snapshot matches it. It also replaces a stale snapshot name.
func (*GKEPodSnapshots) OrderColdStartBypass(pod *corev1.Pod) error {
	if pod == nil {
		return errors.New("ordering a cold start needs a pod")
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[gkeRestoreAnnotation] = ""
	return nil
}

// gkeColdStartReason is the Reason of the ColdStart verdict.
const gkeColdStartReason = "GKE runtime skipped snapshot restore and fell back to cold start"

// VerifyRestore implements RestoreVerifier with the check the controller made
// before engines reported their verdicts. GKE Pod Snapshots exposes no
// positive restore signal (C6), so:
//
//   - a restore-failure Warning event on pod, since job started restoring, is
//     a ColdStart verdict, whether or not the pod is Ready;
//   - otherwise, a Ready pod is a Restored verdict that isn't confirmed: it's
//     inferred from the absence of that event;
//   - otherwise, the restore is still in progress.
//
// The events are listed from the API server, through the reader that
// WithAPIReader sets.
func (e *GKEPodSnapshots) VerifyRestore(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (RestoreVerdict, error) {
	if pod == nil {
		return RestoreVerdict{}, errors.New("verifying the restore of a nil pod")
	}
	event, err := e.restoreFailureEvent(ctx, job, pod)
	if err != nil {
		return RestoreVerdict{}, err
	}
	if event != nil {
		return RestoreVerdict{
			Outcome:  ColdStart,
			Reason:   gkeColdStartReason,
			Evidence: fmt.Sprintf("Warning event %s: %s", event.Reason, event.Message),
		}, nil
	}
	if !podReady(pod) {
		return RestoreVerdict{Outcome: RestoreInProgress}, nil
	}
	return RestoreVerdict{
		Outcome:  Restored,
		Evidence: "pod Ready, with no restore-failure Warning event since restoring started",
	}, nil
}

// restoreFailureEvent returns the first Warning event on pod that reports a
// skipped or failed restore, ignoring events from before job started
// restoring. It returns nil when there's none.
func (e *GKEPodSnapshots) restoreFailureEvent(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (*corev1.Event, error) {
	if e.events == nil {
		return nil, errors.New("no reader for the replacement pod's events")
	}
	events := &corev1.EventList{}
	if err := e.events.List(ctx, events,
		client.InNamespace(pod.Namespace),
		client.MatchingFields{"involvedObject.uid": string(pod.UID)},
	); err != nil {
		return nil, fmt.Errorf("failed to list events for replacement pod %s: %w", pod.Name, err)
	}
	for i := range events.Items {
		event := &events.Items[i]
		if event.InvolvedObject.UID != pod.UID {
			continue
		}
		// Ignore events timestamped before this migration's Restoring phase started.
		if job != nil && job.Status.RestoringStartTime != nil && !event.LastTimestamp.IsZero() &&
			event.LastTimestamp.Before(job.Status.RestoringStartTime) {
			continue
		}
		// Only Warning events report a skipped or failed restore.
		if event.Type == corev1.EventTypeWarning && isRestoreFailureEvent(event) {
			return event, nil
		}
	}
	return nil, nil
}

// isRestoreFailureEvent reports whether a Warning event's reason or message
// says that GKE Pod Snapshots skipped or failed a restore.
func isRestoreFailureEvent(event *corev1.Event) bool {
	switch event.Reason {
	case "FallbackToColdStart", "FailedRestore", "RestoreFailed", "SkipRestore", "GKEPodSnapshotting":
		return true
	}
	msg := strings.ToLower(event.Message)
	return strings.Contains(msg, "falling back to a cold start") ||
		strings.Contains(msg, "failed to restore") ||
		strings.Contains(msg, "skipped restore")
}

func podReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// PodSnapshotStorageConfigName names the cluster-scoped storage config of
// the PodMigration namespace/name. It's a hash, so PodMigrations with the
// same name in different namespaces don't collide.
func PodSnapshotStorageConfigName(namespace, name string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s/%s", namespace, name)))
	return fmt.Sprintf("pssc-%s", hex.EncodeToString(h.Sum(nil))[:16])
}

// PodSnapshotPolicyName names the PodSnapshotPolicy of the PodMigration
// name, in the PodMigration's namespace.
func PodSnapshotPolicyName(name string) string {
	return fmt.Sprintf("psp-%s-manual", name)
}

func gkeGVK(kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: gkeGroup, Version: gkeVersion, Kind: kind}
}
