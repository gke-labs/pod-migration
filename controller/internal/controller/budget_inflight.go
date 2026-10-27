package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// reasonMigrationsInFlight is the budget condition reason when the dry run
// passed but other in-flight migrations already use the budget (#42).
const reasonMigrationsInFlight = "MigrationsInFlight"

// inFlightReservationTTL bounds how long a reservation outlives the job's own
// status write (normally milliseconds, until the cache shows the job past
// Pending).
const inFlightReservationTTL = 2 * time.Minute

// InFlightBudgetChecker adds the in-flight count to the dry-run budget check
// (#42). A dry run doesn't reserve anything, so when a drain evicts several
// replicas covered by one PodDisruptionBudget at the same time, every one of
// them would pass it. So after the dry run passes, the checker counts, for
// every PDB covering the source, the sources of the other migrations in
// flight as disrupted, and lets the job through only if disrupting its
// source still leaves the PDB's desiredHealthy pods healthy.
//
// A migration is in flight from the moment it passes this check until its
// source is gone: then the PDB's own status, and the replacement's readiness,
// account for it again. A per-PDB lock and an in-memory reservation keep the
// reconciler's concurrent workers, and the cache's lag behind our own status
// writes, from letting two jobs through on the same budget.
type InFlightBudgetChecker struct {
	// Client reads PDBs, pods and jobs, normally from the cache.
	Client client.Client
	// DryRun is the first check, normally EvictionBudgetChecker.
	DryRun DisruptionBudgetChecker
	// now returns the current time; nil means time.Now.
	now func() time.Time

	mu       sync.Mutex
	pdbLocks map[string]*sync.Mutex
	reserved map[string]map[types.UID]time.Time // PDB key -> job UID -> reserved at
}

// Check reads the source and the PodDisruptionBudgets that cover it from the
// cache, skips the dry run when the cached budget already settles it, and
// otherwise runs the dry run and then the in-flight count.
func (c *InFlightBudgetChecker) Check(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (string, error) {
	source := &corev1.Pod{}
	if err := c.Client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: podName}, source); err != nil {
		if !apierrors.IsNotFound(err) {
			return reasonBudgetCheckError, fmt.Errorf("reading the source pod: %w", err)
		}
		// Not in the cache: the dry run tells whether the source is gone, and
		// the Pending phase handles that.
		return c.DryRun.Check(ctx, job, podName)
	}
	pdbs := &policyv1.PodDisruptionBudgetList{}
	if err := c.Client.List(ctx, pdbs, client.InNamespace(job.Namespace)); err != nil {
		return reasonBudgetCheckError, fmt.Errorf("listing PodDisruptionBudgets: %w", err)
	}
	type coveringPDB struct {
		pdb      *policyv1.PodDisruptionBudget
		selector labels.Selector
	}
	var covering []coveringPDB
	for i := range pdbs.Items {
		pdb := &pdbs.Items[i]
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		// An empty selector (&metav1.LabelSelector{}) selects all pods in the
		// namespace, matching the eviction API; a nil selector selects nothing.
		if err != nil || !selector.Matches(labels.Set(source.Labels)) {
			continue
		}
		covering = append(covering, coveringPDB{pdb, selector})
	}
	// With more than one PDB, the dry run gets the eviction API's own error.
	if len(covering) == 1 {
		if refusal := cachedBudgetRefusal(source, covering[0].pdb); refusal != nil {
			return reasonBudgetExhausted, refusal
		}
	}
	// Pre-check the local in-flight count before the dry run. If in-flight
	// siblings already exhaust the budget, return without sending an
	// unnecessary dry-run eviction POST.
	for _, cov := range covering {
		if reason, err := c.checkPDB(ctx, job, source, cov.pdb, cov.selector, false); reason != reasonBudgetAllowed {
			return reason, err
		}
	}
	if reason, err := c.DryRun.Check(ctx, job, podName); reason != reasonBudgetAllowed {
		return reason, err
	}
	for _, cov := range covering {
		if reason, err := c.checkPDB(ctx, job, source, cov.pdb, cov.selector, true); reason != reasonBudgetAllowed {
			return reason, err
		}
	}
	return reasonBudgetAllowed, nil
}

// cachedBudgetRefusal returns the refusal that the eviction API would give a
// healthy source under pdb, judging by the cached PDB status, or nil when the
// cache doesn't settle it. A PDB whose status lags its spec, or that allows
// no disruption, refuses a healthy pod's eviction, so a waiting job needn't
// send a dry run until the cache shows the budget freed. The refusal carries
// the DisruptionBudget cause, worded like the eviction API's. An unhealthy
// source depends on unhealthyPodEvictionPolicy, so it's left to the dry run.
func cachedBudgetRefusal(source *corev1.Pod, pdb *policyv1.PodDisruptionBudget) error {
	if !podCountsHealthy(source) {
		return nil
	}
	var cause string
	switch {
	case pdb.Status.ObservedGeneration < pdb.Generation:
		cause = fmt.Sprintf("The disruption budget %s is still being processed by the server.", pdb.Name)
	case pdb.Status.DisruptionsAllowed == 0:
		cause = fmt.Sprintf("The disruption budget %s needs %d healthy pods and has %d currently",
			pdb.Name, pdb.Status.DesiredHealthy, pdb.Status.CurrentHealthy)
	default:
		return nil
	}
	err := apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes,
		metav1.StatusCause{Type: policyv1.DisruptionBudgetCause, Message: cause})
	return err
}

// checkPDB counts one PDB, and reserves the budget for job when reserve is true
// and the check passes. When reserve is false, it runs as a read-only pre-check
// without acquiring the per-PDB lock, skipping dry-run eviction POSTs when
// in-flight siblings already exhaust the budget.
func (c *InFlightBudgetChecker) checkPDB(ctx context.Context, job *pmv1alpha1.PodMigrationJob, source *corev1.Pod, pdb *policyv1.PodDisruptionBudget, selector labels.Selector, reserve bool) (string, error) {
	key := pdb.Namespace + "/" + pdb.Name
	if reserve {
		lock := c.pdbLock(key)
		lock.Lock()
		defer lock.Unlock()
	}

	sourceHealthy := podCountsHealthy(source)
	if !sourceHealthy && pdb.Spec.UnhealthyPodEvictionPolicy != nil &&
		*pdb.Spec.UnhealthyPodEvictionPolicy == policyv1.AlwaysAllow {
		// The eviction API lets an unhealthy pod go whatever the budget says.
		return reasonBudgetAllowed, nil
	}

	jobs := &pmv1alpha1.PodMigrationJobList{}
	if err := c.Client.List(ctx, jobs, client.InNamespace(job.Namespace)); err != nil {
		return reasonBudgetCheckError, fmt.Errorf("listing migration jobs: %w", err)
	}
	pods := &corev1.PodList{}
	if err := c.Client.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return reasonBudgetCheckError, fmt.Errorf("listing the PDB's pods: %w", err)
	}

	reservations := c.pruneReservations(key, jobs.Items)
	podsByUID := make(map[types.UID]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		podsByUID[pods.Items[i].UID] = &pods.Items[i]
	}
	inFlight := map[types.UID]string{} // source pod UID -> job name
	for i := range jobs.Items {
		other := &jobs.Items[i]
		if other.UID == job.UID || other.Spec.TargetPodUID == "" {
			continue
		}
		_, isReserved := reservations[other.UID]
		if !isReserved && !pastBudgetCheck(other) {
			continue
		}
		if pod, ok := podsByUID[types.UID(other.Spec.TargetPodUID)]; ok && pod.DeletionTimestamp == nil {
			inFlight[pod.UID] = other.Name
		}
	}

	healthy := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if _, disrupted := inFlight[pod.UID]; !disrupted && podCountsHealthy(pod) {
			healthy++
		}
	}
	if sourceHealthy {
		// The source is one of the healthy pods; disrupting it takes one away.
		healthy--
	}
	if healthy < int(pdb.Status.DesiredHealthy) {
		names := make([]string, 0, len(inFlight))
		for _, name := range inFlight {
			names = append(names, name)
		}
		sort.Strings(names)
		return reasonMigrationsInFlight, fmt.Errorf("PodDisruptionBudget %s needs %d healthy pods, and %d would remain while %d other migrations are in flight (%s)",
			pdb.Name, pdb.Status.DesiredHealthy, healthy, len(inFlight), strings.Join(names, ", "))
	}
	if reserve {
		c.reserve(key, job.UID)
	}
	return reasonBudgetAllowed, nil
}

// pastBudgetCheck reports whether a job has passed the budget check (or never
// needed it, like spot preemption) and may still disrupt its source.
func pastBudgetCheck(job *pmv1alpha1.PodMigrationJob) bool {
	switch job.Status.Phase {
	case pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		pmv1alpha1.PodMigrationJobPhaseEvicting,
		pmv1alpha1.PodMigrationJobPhaseRestoring:
		return true
	default:
		return false
	}
}

// podCountsHealthy reports whether the disruption controller counts pod as
// healthy: Ready and not terminating.
func podCountsHealthy(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (c *InFlightBudgetChecker) pdbLock(key string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pdbLocks == nil {
		c.pdbLocks = map[string]*sync.Mutex{}
	}
	l, ok := c.pdbLocks[key]
	if !ok {
		l = &sync.Mutex{}
		c.pdbLocks[key] = l
	}
	return l
}

func (c *InFlightBudgetChecker) reserve(key string, jobUID types.UID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reserved == nil {
		c.reserved = map[string]map[types.UID]time.Time{}
	}
	if c.reserved[key] == nil {
		c.reserved[key] = map[types.UID]time.Time{}
	}
	c.reserved[key][jobUID] = c.clock()
}

// pruneReservations drops the reservations that the cache now covers (the
// job is past Pending), that can't disrupt anything any more (the job is
// gone or terminal), or that are older than the TTL, and returns the rest.
func (c *InFlightBudgetChecker) pruneReservations(key string, jobs []pmv1alpha1.PodMigrationJob) map[types.UID]time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.reserved[key]
	if len(res) == 0 {
		return nil
	}
	byUID := make(map[types.UID]*pmv1alpha1.PodMigrationJob, len(jobs))
	for i := range jobs {
		byUID[jobs[i].UID] = &jobs[i]
	}
	now := c.clock()
	out := make(map[types.UID]time.Time, len(res))
	for uid, at := range res {
		job, ok := byUID[uid]
		if !ok || pastBudgetCheck(job) || job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
			now.Sub(at) > inFlightReservationTTL {
			delete(res, uid)
			continue
		}
		out[uid] = at
	}
	return out
}

func (c *InFlightBudgetChecker) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}
