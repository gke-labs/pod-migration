package collect

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/recorder"
)

var podLogOptions = corev1.PodLogOptions{Follow: true, Timestamps: true}

var (
	podMetricsGVR  = schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods"}
	nodeMetricsGVR = schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "nodes"}
)

// sampleMetrics polls the resource-metrics API for pod and node usage.
func sampleMetrics(ctx context.Context, dyn dynamic.Interface, rec *recorder.Recorder, every time.Duration) {
	if every <= 0 {
		every = 15 * time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for typ, gvr := range map[string]schema.GroupVersionResource{
				"podmetrics":  podMetricsGVR,
				"nodemetrics": nodeMetricsGVR,
			} {
				list, err := dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
				if err != nil {
					rec.Error("metrics:"+typ, err)
					continue
				}
				for i := range list.Items {
					_ = rec.Write(typ, gvrKey(gvr), list.Items[i].Object)
				}
			}
		}
	}
}

// sampleGCS records per-object snapshot sizes with `gcloud storage du`.
// gcloud is shelled out deliberately: it keeps the tool free of a cloud SDK
// dependency, and storage sampling is optional and never on the hot path.
func sampleGCS(ctx context.Context, rec *recorder.Recorder, bucket string, every time.Duration) {
	if every <= 0 {
		every = 60 * time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			out, err := exec.CommandContext(ctx, "gcloud", "storage", "du", bucket).Output()
			if err != nil {
				rec.Error("gcs:du", err)
				continue
			}
			objects := map[string]int64{}
			var total int64
			sc := bufio.NewScanner(strings.NewReader(string(out)))
			sc.Buffer(make([]byte, 1024*1024), 1024*1024)
			for sc.Scan() {
				fields := strings.Fields(sc.Text())
				if len(fields) < 2 {
					continue
				}
				size, err := strconv.ParseInt(fields[0], 10, 64)
				if err != nil {
					continue
				}
				objects[fields[len(fields)-1]] = size
				total += size
			}
			_ = rec.Write("gcs", "", map[string]any{
				"bucket": bucket, "totalBytes": total, "objects": objects,
			})
		}
	}
}

// streamControllerLogs follows the logs of every pod in the controller
// namespace, restarting streams as pods come and go. Logs land in
// <run>/controller-<pod>.log.
func streamControllerLogs(ctx context.Context, cs *kubernetes.Clientset, runDir, ns string) {
	if ns == "" {
		return
	}
	active := map[string]context.CancelFunc{}
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err == nil {
			seen := map[string]bool{}
			for _, p := range pods.Items {
				seen[p.Name] = true
				if _, ok := active[p.Name]; !ok && p.Status.Phase == "Running" {
					sctx, cancel := context.WithCancel(ctx)
					active[p.Name] = cancel
					go func(pod string) {
						defer func() { delete(active, pod) }()
						streamOnePodLog(sctx, cs, runDir, ns, pod)
					}(p.Name)
				}
			}
			for name, cancel := range active {
				if !seen[name] {
					cancel()
					delete(active, name)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func streamOnePodLog(ctx context.Context, cs *kubernetes.Clientset, runDir, ns, pod string) {
	f, err := os.OpenFile(filepath.Join(runDir, "controller-"+pod+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	req := cs.CoreV1().Pods(ns).GetLogs(pod, &podLogOptions)
	stream, err := req.Stream(ctx)
	if err != nil {
		return
	}
	defer stream.Close()
	_, _ = io.Copy(f, stream)
}

// sampleControllerPrometheusMetrics scrapes the controller's Prometheus /metrics
// endpoint (:8080/metrics via the apiserver pod proxy) on startup and every
// interval so `pmprofiler analyze` can verify pod_migration_invariant_violations_total == 0.
func sampleControllerPrometheusMetrics(ctx context.Context, cs *kubernetes.Clientset, rec *recorder.Recorder, ns string, every time.Duration) {
	if ns == "" {
		return
	}
	if every <= 0 {
		every = 15 * time.Second
	}
	scrapeControllerPrometheusOnce(ctx, cs, rec, ns)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			scrapeControllerPrometheusOnce(ctx, cs, rec, ns)
		}
	}
}

func scrapeControllerPrometheusOnce(ctx context.Context, cs *kubernetes.Clientset, rec *recorder.Recorder, ns string) {
	if ns == "" || cs == nil {
		return
	}
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		rec.Error("prommetrics:list-pods", err)
		return
	}
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil {
			continue
		}
		raw, err := cs.CoreV1().RESTClient().Get().
			Namespace(ns).
			Resource("pods").
			SubResource("proxy").
			Name(p.Name + ":8080").
			Suffix("metrics").
			DoRaw(ctx)
		if err != nil {
			rec.Error("prommetrics:"+p.Name, err)
			continue
		}
		_ = rec.Write("prommetrics", "metrics.prometheus", map[string]any{
			"pod":       p.Name,
			"namespace": ns,
			"body":      string(raw),
		})
	}
}
