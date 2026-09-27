// Package collect implements the event-driven collector. It opens list+watch
// streams (with automatic re-list on expiry) against the migration-relevant
// resources and appends every observed state transition to the run's record
// log with an exact receipt timestamp. Periodic samplers add resource-usage
// and storage-size series.
package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/recorder"
)

// Options configures a collection run.
type Options struct {
	RunDir       string
	Kubeconfig   string
	Context      string
	Namespace    string // optional workload namespace filter ("" = all namespaces)
	PodSelector  string // label selector for workload pods
	ControllerNS string // namespace of the pod-migration controller
	GCSBucket    string // optional gs:// URL for snapshot-size sampling
	MetricsEvery time.Duration
	GCSEvery     time.Duration
	Scenario     string // free-form scenario tag stored in meta.json
}

// watchTarget describes one resource stream.
type watchTarget struct {
	gvr      schema.GroupVersionResource
	selector string // label selector, "" for none
	ns       string // "" = all namespaces
}

// Run starts the collector and blocks until ctx is cancelled.
func Run(ctx context.Context, o Options) error {
	cfg, err := restConfig(o.Kubeconfig, o.Context)
	if err != nil {
		return err
	}
	// The collector is read-only and bursty during mass drains; allow
	// generous client-side throughput so we never sample-skew under load.
	cfg.QPS = 50
	cfg.Burst = 100

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return err
	}

	rec, err := recorder.Open(o.RunDir)
	if err != nil {
		return err
	}
	defer func() { _ = rec.Close() }()

	if err := writeMeta(ctx, o, cs, disc); err != nil {
		log.Printf("meta: %v (continuing)", err)
	}

	targets := []watchTarget{
		{gvr: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}, selector: o.PodSelector, ns: o.Namespace},
		{gvr: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}, ns: o.ControllerNS},
		{gvr: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}},
		{gvr: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}},
	}
	// CRDs are resolved by discovery so the collector keeps working if the
	// addon or controller bumps its API version.
	for _, group := range []string{"podmigration.gke.io", "podsnapshot.gke.io", "pod-migrate.io"} {
		gvrs, err := discoverGroup(disc, group)
		if err != nil {
			if group != "pod-migrate.io" {
				log.Printf("discovery %s: %v (group skipped)", group, err)
				rec.Error("discovery:"+group, err)
			}
			continue
		}
		for _, gvr := range gvrs {
			targets = append(targets, watchTarget{gvr: gvr})
		}
	}

	for _, t := range targets {
		go watchLoop(ctx, dyn, rec, t)
	}
	go sampleMetrics(ctx, dyn, rec, o.MetricsEvery)
	go sampleControllerPrometheusMetrics(ctx, cs, rec, o.ControllerNS, o.MetricsEvery)
	if o.GCSBucket != "" {
		go sampleGCS(ctx, rec, o.GCSBucket, o.GCSEvery)
	}
	go streamControllerLogs(ctx, cs, o.RunDir, o.ControllerNS)

	<-ctx.Done()
	// Capture a final Prometheus /metrics snapshot before closing the log.
	finalCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	scrapeControllerPrometheusOnce(finalCtx, cs, rec, o.ControllerNS)
	cancel()
	// Give in-flight handlers a moment to drain before the file closes.
	time.Sleep(200 * time.Millisecond)
	return nil
}

func restConfig(kubeconfig, kctx string) (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{CurrentContext: kctx}).ClientConfig()
}

// discoverGroup returns every list+watchable resource in the given API group.
func discoverGroup(disc discovery.DiscoveryInterface, group string) ([]schema.GroupVersionResource, error) {
	lists, err := disc.ServerPreferredResources()
	if err != nil && len(lists) == 0 {
		return nil, err
	}
	var out []schema.GroupVersionResource
	for _, rl := range lists {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil || gv.Group != group {
			continue
		}
		for _, r := range rl.APIResources {
			if strings.Contains(r.Name, "/") { // subresource
				continue
			}
			if !hasVerbs(r.Verbs, "list", "watch") {
				continue
			}
			out = append(out, gv.WithResource(r.Name))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no watchable resources found in group %s", group)
	}
	return out, nil
}

func hasVerbs(verbs metav1.Verbs, want ...string) bool {
	for _, w := range want {
		found := false
		for _, v := range verbs {
			if v == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func gvrKey(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return gvr.Resource + "." + gvr.Version
	}
	return gvr.Resource + "." + gvr.Version + "." + gvr.Group
}

// watchLoop lists the resource, records the baseline, then follows the watch
// stream, re-listing whenever the watch expires or errors.
func watchLoop(ctx context.Context, dyn dynamic.Interface, rec *recorder.Recorder, t watchTarget) {
	key := gvrKey(t.gvr)
	backoff := time.Second
	for ctx.Err() == nil {
		rv, err := listAndRecord(ctx, dyn, rec, t)
		if err != nil {
			rec.Error("list:"+key, err)
			sleepCtx(ctx, backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		if err := followWatch(ctx, dyn, rec, t, rv); err != nil && ctx.Err() == nil {
			rec.Error("watch:"+key, err)
			sleepCtx(ctx, backoff)
		}
	}
}

func listAndRecord(ctx context.Context, dyn dynamic.Interface, rec *recorder.Recorder, t watchTarget) (string, error) {
	ri := resourceInterface(dyn, t)
	list, err := ri.List(ctx, metav1.ListOptions{LabelSelector: t.selector})
	if err != nil {
		return "", err
	}
	key := gvrKey(t.gvr)
	for i := range list.Items {
		prune(&list.Items[i])
		if err := rec.Write("list", key, list.Items[i].Object); err != nil {
			return "", err
		}
	}
	return list.GetResourceVersion(), nil
}

func followWatch(ctx context.Context, dyn dynamic.Interface, rec *recorder.Recorder, t watchTarget, rv string) error {
	ri := resourceInterface(dyn, t)
	w, err := ri.Watch(ctx, metav1.ListOptions{
		LabelSelector:       t.selector,
		ResourceVersion:     rv,
		AllowWatchBookmarks: true,
	})
	if err != nil {
		return err
	}
	defer w.Stop()
	key := gvrKey(t.gvr)
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				u, ok := ev.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				prune(u)
				typ := map[watch.EventType]string{
					watch.Added:    "add",
					watch.Modified: "update",
					watch.Deleted:  "delete",
				}[ev.Type]
				if err := rec.Write(typ, key, u.Object); err != nil {
					return err
				}
			case watch.Bookmark:
				// keep-alive only
			case watch.Error:
				return fmt.Errorf("watch error event: %v", ev.Object)
			}
		}
	}
}

func resourceInterface(dyn dynamic.Interface, t watchTarget) dynamic.ResourceInterface {
	if t.ns != "" {
		return dyn.Resource(t.gvr).Namespace(t.ns)
	}
	return dyn.Resource(t.gvr)
}

// prune drops fields that bloat the log without analytic value.
func prune(u *unstructured.Unstructured) {
	unstructured.RemoveNestedField(u.Object, "metadata", "managedFields")
	ann, found, _ := unstructured.NestedStringMap(u.Object, "metadata", "annotations")
	if found {
		delete(ann, "kubectl.kubernetes.io/last-applied-configuration")
		_ = unstructured.SetNestedStringMap(u.Object, ann, "metadata", "annotations")
	}
}

func writeMeta(ctx context.Context, o Options, cs *kubernetes.Clientset, disc discovery.DiscoveryInterface) error {
	meta := map[string]any{
		"scenario":  o.Scenario,
		"namespace": o.Namespace,
		"startedAt": time.Now().UTC().Format(time.RFC3339),
		"gcsBucket": o.GCSBucket,
	}
	if v, err := disc.ServerVersion(); err == nil {
		meta["serverVersion"] = v.GitVersion
	}
	if nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err == nil {
		var ns []map[string]string
		for _, n := range nodes.Items {
			ns = append(ns, map[string]string{
				"name":        n.Name,
				"machineType": n.Labels["node.kubernetes.io/instance-type"],
				"pool":        n.Labels["cloud.google.com/gke-nodepool"],
				"kubelet":     n.Status.NodeInfo.KubeletVersion,
				"runtime":     n.Status.NodeInfo.ContainerRuntimeVersion,
			})
		}
		meta["nodes"] = ns
	}
	raw, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(o.RunDir, "meta.json"), raw, 0o644)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
