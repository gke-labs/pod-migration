package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	deadlineTestNamespace = "default"
	deadlineTestPod       = "flight-db-0"
	deadlineTestPodUID    = "uid-flight-db-0"
)

func deadlineTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	return scheme
}

// deadlineTestObjects returns an opted-in gVisor pod plus a Ready manual+stop
// PodSnapshotPolicy selecting it, so an eviction takes the PMJ-creation path.
func deadlineTestObjects() []client.Object {
	runtimeClass := "gvisor"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: deadlineTestNamespace,
			Name:      deadlineTestPod,
			UID:       types.UID(deadlineTestPodUID),
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
		Spec: corev1.PodSpec{RuntimeClassName: &runtimeClass},
	}

	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotPolicy"})
	psp.SetNamespace(deadlineTestNamespace)
	psp.SetName("psp-manual-stop")
	psp.Object["spec"] = map[string]interface{}{
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{"pod-migration.gke.io/enabled": "true"},
		},
		"triggerConfig": map[string]interface{}{"type": "manual", "postCheckpoint": "stop"},
	}
	psp.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{map[string]interface{}{"type": "Ready", "status": "True"}},
	}
	return []client.Object{pod, psp}
}

// evictionReviewRequest builds the AdmissionReview POST the API server sends for
// an eviction of the test pod. target carries the query string, e.g. the
// "?timeout=10s" the API server appends to every webhook call.
func evictionReviewRequest(t *testing.T, ctx context.Context, target string) *http.Request {
	t.Helper()
	body, err := json.Marshal(admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request: &admissionv1.AdmissionRequest{
			UID:         types.UID("review-" + deadlineTestPod),
			Kind:        metav1.GroupVersionKind{Group: "policy", Version: "v1", Kind: "Eviction"},
			Resource:    metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
			SubResource: "eviction",
			Namespace:   deadlineTestNamespace,
			Name:        deadlineTestPod,
			Operation:   admissionv1.Create,
		},
	})
	if err != nil {
		t.Fatalf("marshalling AdmissionReview: %v", err)
	}
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func decodeReviewResponse(t *testing.T, rec *httptest.ResponseRecorder) *admissionv1.AdmissionResponse {
	t.Helper()
	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(rec.Body.Bytes(), &review); err != nil {
		t.Fatalf("decoding AdmissionReview response: %v (body %q)", err, rec.Body.String())
	}
	if review.Response == nil {
		t.Fatalf("AdmissionReview carries no response: %q", rec.Body.String())
	}
	return review.Response
}

// blockUntilDone stands in for an API call stuck in the API server's
// priority-and-fairness queue: it returns only once the caller gives up.
func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestEvictionWebhook_AnswersBeforeAdmissionDeadline covers #68. When the gate's
// own API calls are slow (e.g. queued by API Priority and Fairness behind the
// very evictions it is gating), the API server gives up on the webhook after
// timeoutSeconds and, because failurePolicy is Ignore, admits the eviction: the
// opted-in pod is evicted cold. The gate must instead answer 429 (retry) before
// the deadline the API server passes in the "timeout" query parameter.
func TestEvictionWebhook_AnswersBeforeAdmissionDeadline(t *testing.T) {
	scheme := deadlineTestScheme()

	tests := []struct {
		name           string
		clientFuncs    interceptor.Funcs
		apiReaderFuncs interceptor.Funcs
	}{
		{
			name: "live PodMigrationJob read is slow",
			apiReaderFuncs: interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok {
						return blockUntilDone(ctx)
					}
					return c.Get(ctx, key, obj, opts...)
				},
			},
		},
		{
			name: "PodSnapshotPolicy list is slow",
			clientFuncs: interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*unstructured.UnstructuredList); ok {
						return blockUntilDone(ctx)
					}
					return c.List(ctx, list, opts...)
				},
			},
		},
		{
			name: "PodMigrationJob create is slow",
			clientFuncs: interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok {
						return blockUntilDone(ctx)
					}
					return c.Create(ctx, obj, opts...)
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := deadlineTestObjects()
			wh := newEvictionWebhook(&EvictionGate{
				Client:    fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(tt.clientFuncs).Build(),
				APIReader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithInterceptorFuncs(tt.apiReaderFuncs).Build(),
			})

			// The request context is only cancelled at cleanup, as when the API
			// server's connection outlives our answer; a handler that ignores the
			// deadline blocks until then.
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			req := evictionReviewRequest(t, ctx, "/validate-v1-pod-eviction?timeout=1s")

			rec := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				wh.ServeHTTP(rec, req)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("eviction gate did not answer within the API server's 1s webhook timeout; with failurePolicy=Ignore the eviction would fail open")
			}

			resp := decodeReviewResponse(t, rec)
			if resp.Allowed {
				t.Fatalf("expected 429 (retry) when API calls are slow, got allowed: %+v", resp.Result)
			}
			if resp.Result == nil || resp.Result.Code != http.StatusTooManyRequests {
				t.Fatalf("expected status code 429, got %+v", resp.Result)
			}
		})
	}
}

// TestEvictionWebhook_ServeHTTPCreatesMigrationJob guards the normal path through
// the HTTP wrapper: fast API calls create the PodMigrationJob and answer 429.
func TestEvictionWebhook_ServeHTTPCreatesMigrationJob(t *testing.T) {
	scheme := deadlineTestScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deadlineTestObjects()...).Build()
	wh := newEvictionWebhook(&EvictionGate{Client: c, APIReader: c})

	rec := httptest.NewRecorder()
	wh.ServeHTTP(rec, evictionReviewRequest(t, context.Background(), "/validate-v1-pod-eviction?timeout=10s"))

	resp := decodeReviewResponse(t, rec)
	if resp.Allowed || resp.Result == nil || resp.Result.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after spawning the migration job, got allowed=%v result=%+v", resp.Allowed, resp.Result)
	}
	if resp.Result.Message != "migration job spawned" {
		t.Errorf("expected message %q, got %q", "migration job spawned", resp.Result.Message)
	}
	job := &pmv1alpha1.PodMigrationJob{}
	key := client.ObjectKey{Namespace: deadlineTestNamespace, Name: util.FormatPMJName(deadlineTestPod, deadlineTestPodUID)}
	if err := c.Get(context.Background(), key, job); err != nil {
		t.Fatalf("expected PodMigrationJob %s to be created: %v", key.Name, err)
	}
}

func TestHandlerBudget(t *testing.T) {
	tests := []struct {
		query string
		want  time.Duration
	}{
		{query: "?timeout=10s", want: 8 * time.Second},
		{query: "?timeout=5s", want: 4 * time.Second},
		{query: "?timeout=1s", want: 800 * time.Millisecond},
		{query: "", want: defaultHandlerBudget},
		{query: "?timeout=soon", want: defaultHandlerBudget},
		{query: "?timeout=0s", want: defaultHandlerBudget},
		{query: "?timeout=-3s", want: defaultHandlerBudget},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/validate-v1-pod-eviction"+tt.query, nil)
			if got := handlerBudget(contextWithAdmissionTimeout(context.Background(), r)); got != tt.want {
				t.Errorf("handlerBudget(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}
