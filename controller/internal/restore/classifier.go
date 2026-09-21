package restore

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// startFailureExitCode is the exit code the kubelet reports when the container
// runtime fails to *start* a container (as opposed to the container itself
// exiting). A failed restore always surfaces as StartError/128; a genuine
// application bug surfaces as Error/1. This is a kubelet+CRI convention, not an
// engine-specific one.
const startFailureExitCode = 128

// FailureClass is the three-state verdict on a replacement pod.
type FailureClass int

const (
	// FailureNone: no runtime start failure. Healthy pods AND genuine
	// application crash loops both land here.
	FailureNone FailureClass = iota
	// FailureFatal: an engine recognised its own restore failure. Actionable.
	FailureFatal
	// FailureUnrecognized: a start failure no engine claimed. Reported, never acted on.
	FailureUnrecognized
)

// Failure is the result of Classify.
type Failure struct {
	Class     FailureClass
	Engine    string // Engine.Name() of the engine that matched, if any
	Signature string // engine-specific token naming the rule that matched
	Message   string // the raw container-status message the verdict came from
	Container string
}

// Engine names a checkpoint/restore engine and the failures it emits.
type Engine interface {
	Name() string
	MatchesRestoreFailure(msg string) (signature string, ok bool)
}

// DefaultEngines returns the engine set for a stock deployment.
// Today the repo targets gVisor only.
func DefaultEngines() []Engine { return []Engine{GVisor{}} }

// startFailure is one container's start failure, already filtered to the
// kubelet shape meaning the runtime never got the container running.
type startFailure struct {
	container string
	messages  []string
}

// startFailures inspects a replacement pod's container statuses and finds
// containers that the runtime failed to start.
//
// Detection keys strictly off the start-failure shape observed on a live
// cluster (see pm-notes/technical_docs/restore-failure-signal-taxonomy.md):
//
//	waiting.reason=RunContainerError, terminated.reason=StartError, exitCode=128
//
// versus a genuine application bug:
//
//	waiting.reason=CrashLoopBackOff, terminated.reason=Error, exitCode=1
//
// DO NOT add a restartCount threshold here.  On a restore failure the kubelet
// never restarts the container, so restartCount is pinned at 0 forever; any
// "restartCount > N" gate would make this detector permanently blind.  The
// exit code plus the StartError/RunContainerError reasons are the only signals
// that actually separate the two cases. This remains true for all engines.
//
// Both kubelet shapes are handled: the container may be parked in
// Waiting(RunContainerError) with the StartError in LastTerminationState, or —
// racing the status update — land directly in Terminated(StartError).
func startFailures(pod *corev1.Pod) []startFailure {
	if pod == nil {
		return nil
	}

	// Init containers are scanned too: a sandbox that fails to restore can
	// surface the failure on whichever container the runtime tried to start
	// first. The engine signature requirement bounds the blast radius — an
	// ordinary init-container start failure carries no restore signature and so
	// classifies as FailureUnrecognized, which is never acted on.
	statuses := make([]corev1.ContainerStatus, 0, len(pod.Status.ContainerStatuses)+len(pod.Status.InitContainerStatuses))
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)

	var failures []startFailure
	for _, cs := range statuses {
		waiting := cs.State.Waiting
		runContainerError := waiting != nil && waiting.Reason == "RunContainerError"

		// Examine both the current and the previous termination: which one
		// holds the StartError depends on where the kubelet is in its status
		// update cycle.
		var startFail *corev1.ContainerStateTerminated
		for _, term := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
			if term == nil || term.ExitCode != startFailureExitCode {
				continue
			}
			if term.Reason == "StartError" || runContainerError {
				startFail = term
				break
			}
		}
		if startFail == nil {
			continue
		}

		// The wrapped runtime error can land on either the terminated state or
		// the waiting state, so both are scanned.
		messages := []string{startFail.Message}
		if waiting != nil && waiting.Message != "" {
			messages = append(messages, waiting.Message)
		}

		failures = append(failures, startFailure{
			container: cs.Name,
			messages:  messages,
		})
	}
	return failures
}

// Classify runs the shared kubelet start-failure walk once, then offers each
// message to each engine IN ORDER (so more specific engines should be listed
// before more generic ones).
func Classify(pod *corev1.Pod, engines ...Engine) Failure {
	failures := startFailures(pod)
	if len(failures) == 0 {
		return Failure{Class: FailureNone}
	}

	var unmatched *Failure
	for _, fail := range failures {
		for _, msg := range fail.messages {
			for _, engine := range engines {
				if sig, ok := engine.MatchesRestoreFailure(msg); ok {
					return Failure{
						Class:     FailureFatal,
						Engine:    engine.Name(),
						Signature: sig,
						Message:   msg,
						Container: fail.container,
					}
				}
			}
		}
		if unmatched == nil {
			unmatched = &Failure{
				Class:     FailureUnrecognized,
				Message:   strings.Join(fail.messages, " | "),
				Container: fail.container,
			}
		}
	}

	// unmatched is non-nil whenever the loop ran at least once without a fatal
	// match, but do not rely on that implicitly: a future edit that adds a
	// `continue` to the loop would turn this into a nil dereference.
	if unmatched != nil {
		return *unmatched
	}
	return Failure{Class: FailureNone}
}
