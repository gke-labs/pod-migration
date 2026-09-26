package restore

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	restoreCrashOCIMessage     = "failed to create containerd task: failed to create shim task: OCI runtime restore failed"
	restoreCrashUnknownMessage = "failed to create containerd task: UNKNOWN ERROR"
)

// restoreCrashPodStatus reproduces the kubelet shape where the container is
// parked in Waiting(RunContainerError) with the StartError in
// LastTerminationState.
func restoreCrashPodStatus(message string) corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodPending,
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "app",
				RestartCount: 0,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "RunContainerError",
						Message: message,
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "StartError",
						ExitCode: 128,
						Message:  message,
					},
				},
			},
		},
	}
}

// T6: every signature in the pattern set independently classifies as a restore
// crash, in both kubelet shapes (Waiting+LastTerminationState and a direct
// Terminated), and neither app crashes nor healthy pods trip the detector.
func TestClassify(t *testing.T) {
	terminatedOnly := func(reason, message string, exitCode int32) corev1.PodStatus {
		return corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   reason,
							ExitCode: exitCode,
							Message:  message,
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name          string
		status        corev1.PodStatus
		wantClass     FailureClass
		wantEngine    string
		wantSignature string
	}{
		{
			name:          "OCI runtime restore failed (waiting + lastTerminationState)",
			status:        restoreCrashPodStatus("OCI runtime restore failed: unable to restore"),
			wantClass:     FailureFatal,
			wantEngine:    "gvisor",
			wantSignature: "oci runtime restore failed",
		},
		{
			name:          "restore failed (waiting + lastTerminationState)",
			status:        restoreCrashPodStatus("runc: restore failed: criu returned 1"),
			wantClass:     FailureFatal,
			wantEngine:    "gvisor",
			wantSignature: "restore failed",
		},
		{
			// "does not match" was removed from the gVisor signature set: it
			// was never observed on a live cluster. The taxonomy records only
			// "OCI runtime restore failed" for a failed restore, and its
			// "does not exist" sibling belongs to the missing-checkpoint
			// scenario, which is out of scope and does not even produce a
			// StartError. Acting destructively on an invented signature
			// inverts the report-first design. If a real checkpoint-mismatch
			// message exists, it surfaces here as Unrecognized — metric plus
			// Warning event — and can then be added with evidence.
			name:      "unobserved checkpoint-mismatch wording is reported, not acted on",
			status:    restoreCrashPodStatus("image digest sha256:abc does not match snapshot digest sha256:def"),
			wantClass: FailureUnrecognized,
		},
		{
			name:          "direct Terminated StartError carries the signature",
			status:        terminatedOnly("StartError", "OCI runtime restore failed: unable to restore", 128),
			wantClass:     FailureFatal,
			wantEngine:    "gvisor",
			wantSignature: "oci runtime restore failed",
		},
		{
			name:      "StartError with an unrecognised message",
			status:    restoreCrashPodStatus(restoreCrashUnknownMessage),
			wantClass: FailureUnrecognized,
		},
		{
			name:      "direct Terminated StartError with an unrecognised message",
			status:    terminatedOnly("StartError", restoreCrashUnknownMessage, 128),
			wantClass: FailureUnrecognized,
		},
		{
			name: "genuine app bug: CrashLoopBackOff / exit 1",
			status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:         "app",
						RestartCount: 7,
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason:  "CrashLoopBackOff",
								Message: "Back-off restarting failed container",
							},
						},
						LastTerminationState: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								Reason:   "Error",
								ExitCode: 1,
								Message:  "restore failed to parse config",
							},
						},
					},
				},
			},
			wantClass: FailureNone,
		},
		{
			name:      "clean exit 0",
			status:    terminatedOnly("Completed", "", 0),
			wantClass: FailureNone,
		},
		{
			name:      "no container statuses yet",
			status:    corev1.PodStatus{Phase: corev1.PodPending},
			wantClass: FailureNone,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p"},
				Status:     tc.status,
			}
			got := Classify(pod, DefaultEngines()...)
			if got.Class != tc.wantClass {
				t.Fatalf("Expected class %v, got %v (verdict %+v)", tc.wantClass, got.Class, got)
			}
			if tc.wantSignature != "" && got.Signature != tc.wantSignature {
				t.Errorf("Expected signature %q, got %q", tc.wantSignature, got.Signature)
			}
			if tc.wantEngine != "" && got.Engine != tc.wantEngine {
				t.Errorf("Expected engine %q, got %q", tc.wantEngine, got.Engine)
			}
		})
	}
}

// A container that failed to start once and then came up keeps StartError/128
// in LastTerminationState forever.  The first version of this detector read
// that history unconditionally, so it classified recovered pods as fatal and
// DELETED them; where the message did not match a signature it instead pinned
// the PMJ in a 2s requeue loop forever.
//
// Every fixture below carries a stale history entry with a real gVisor
// signature in it, so nothing but the container's CURRENT state distinguishes
// them from a genuine crash.  All must classify FailureNone.
func TestClassify_IgnoresStaleTerminationHistory(t *testing.T) {
	staleHistory := corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			Reason:   "StartError",
			ExitCode: 128,
			Message:  restoreCrashOCIMessage,
		},
	}

	tests := []struct {
		name string
		cs   corev1.ContainerStatus
	}{
		{
			// The bug as reported: a Running, Ready container that recovered.
			name: "Running container with stale StartError history",
			cs: corev1.ContainerStatus{
				Name:                 "app",
				Ready:                true,
				RestartCount:         1,
				State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				LastTerminationState: staleHistory,
			},
		},
		{
			// CrashLoopBackOff means the container DID start and the
			// application exited. Whatever is in its history, this is an
			// application failure and deleting the pod would not help.
			name: "CrashLoopBackOff with stale StartError history",
			cs: corev1.ContainerStatus{
				Name:         "app",
				RestartCount: 4,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "CrashLoopBackOff",
						Message: "Back-off restarting failed container",
					},
				},
				LastTerminationState: staleHistory,
			},
		},
		{
			// An init container that retried and then completed. Init
			// containers are in scope for detection, so this is a real
			// exposure, not a hypothetical.
			name: "Completed init container with stale StartError history",
			cs: corev1.ContainerStatus{
				Name:         "init",
				RestartCount: 1,
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "Completed",
						ExitCode: 0,
					},
				},
				LastTerminationState: staleHistory,
			},
		},
		{
			// K3: reason and exit code must agree. A StartError that did not
			// exit 128 is not the runtime-level failure we act on.
			name: "StartError with a non-128 exit code",
			cs: corev1.ContainerStatus{
				Name: "app",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "StartError",
						ExitCode: 1,
						Message:  restoreCrashOCIMessage,
					},
				},
			},
		},
		{
			// K3, mirrored on the Waiting branch: RunContainerError is only a
			// licence to READ the history, not to trust whatever is in it.
			name: "RunContainerError whose history exited 1, not 128",
			cs: corev1.ContainerStatus{
				Name: "app",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "RunContainerError",
						Message: restoreCrashOCIMessage,
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "Error",
						ExitCode: 1,
						Message:  restoreCrashOCIMessage,
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, place := range []string{"container", "initContainer"} {
				t.Run(place, func(t *testing.T) {
					status := corev1.PodStatus{Phase: corev1.PodRunning}
					if place == "container" {
						status.ContainerStatuses = []corev1.ContainerStatus{tc.cs}
					} else {
						status.InitContainerStatuses = []corev1.ContainerStatus{tc.cs}
					}
					pod := &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p"},
						Status:     status,
					}
					if got := Classify(pod, DefaultEngines()...); got.Class != FailureNone {
						t.Errorf("Expected FailureNone, got %v (verdict %+v)", got.Class, got)
					}
				})
			}
		})
	}
}

// Classify must never key off restartCount: on a failed restore the kubelet
// pins it at 0 forever, so a threshold would make the detector permanently
// blind. This test exists to break anyone who "fixes" the detector by adding
// one. It holds for every engine, not just gVisor.
func TestClassify_IgnoresRestartCount(t *testing.T) {
	for _, restartCount := range []int32{0, 1, 5} {
		status := restoreCrashPodStatus(restoreCrashOCIMessage)
		status.ContainerStatuses[0].RestartCount = restartCount
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p"},
			Status:     status,
		}
		if got := Classify(pod, DefaultEngines()...); got.Class != FailureFatal {
			t.Errorf("restartCount=%d: expected FailureFatal, got %v", restartCount, got.Class)
		}
	}
}

type fakeEngine struct {
	name string
	sig  string
}

func (e fakeEngine) Name() string { return e.name }
func (e fakeEngine) MatchesRestoreFailure(msg string) (string, bool) {
	if msg == "criu returned 99" {
		return e.sig, true
	}
	return "", false
}

func TestClassifySeamWorks(t *testing.T) {
	// A new test proving the seam works: define a fake engine matching "criu returned 99",
	// and assert Classify returns FailureFatal with Engine="criu" on a pod whose msg matches this.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p"},
		Status:     restoreCrashPodStatus("criu returned 99"),
	}

	got := Classify(pod, fakeEngine{name: "criu", sig: "criu returned 99"})
	if got.Class != FailureFatal {
		t.Fatalf("expected fatal class, got %v", got.Class)
	}
	if got.Engine != "criu" {
		t.Errorf("expected engine criu, got %q", got.Engine)
	}
	if got.Signature != "criu returned 99" {
		t.Errorf("expected signature 'criu returned 99', got %q", got.Signature)
	}
}

type matchAllEngine struct {
	name string
}

func (e matchAllEngine) Name() string { return e.name }
func (e matchAllEngine) MatchesRestoreFailure(msg string) (string, bool) {
	return "match-all", true
}

func TestClassifyEngineOrder(t *testing.T) {
	// A test that engine ORDER governs attribution when two engines both match a generic message.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p"},
		Status:     restoreCrashPodStatus("generic message"),
	}

	got := Classify(pod, matchAllEngine{name: "first"}, matchAllEngine{name: "second"})
	if got.Engine != "first" {
		t.Errorf("expected the first engine in list to win, got %q", got.Engine)
	}
}

func TestClassify_MultiContainerAndSidecar(t *testing.T) {
	tests := []struct {
		name              string
		initStatus        corev1.ContainerStatus
		appStatus         corev1.ContainerStatus
		sidecarStatus     corev1.ContainerStatus
		expectedClass     FailureClass
		expectedContainer string
	}{
		{
			name: "All containers healthy and running",
			initStatus: corev1.ContainerStatus{
				Name: "init-seed",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 0,
						Reason:   "Completed",
					},
				},
			},
			appStatus: corev1.ContainerStatus{
				Name: "app",
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			},
			sidecarStatus: corev1.ContainerStatus{
				Name: "sidecar",
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			},
			expectedClass: FailureNone,
		},
		{
			name: "Sidecar fails runtime restore while app is running",
			initStatus: corev1.ContainerStatus{
				Name: "init-seed",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 0,
						Reason:   "Completed",
					},
				},
			},
			appStatus: corev1.ContainerStatus{
				Name: "app",
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			},
			sidecarStatus: corev1.ContainerStatus{
				Name: "sidecar",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "RunContainerError",
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 128,
						Reason:   "StartError",
						Message:  restoreCrashOCIMessage,
					},
				},
			},
			expectedClass:     FailureFatal,
			expectedContainer: "sidecar",
		},
		{
			name: "App fails runtime restore while sidecar is waiting",
			initStatus: corev1.ContainerStatus{
				Name: "init-seed",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 0,
						Reason:   "Completed",
					},
				},
			},
			appStatus: corev1.ContainerStatus{
				Name: "app",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 128,
						Reason:   "StartError",
						Message:  restoreCrashOCIMessage,
					},
				},
			},
			sidecarStatus: corev1.ContainerStatus{
				Name: "sidecar",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "ContainerCreating",
					},
				},
			},
			expectedClass:     FailureFatal,
			expectedContainer: "app",
		},
		{
			name: "Init container fails runtime restore",
			initStatus: corev1.ContainerStatus{
				Name: "init-seed",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 128,
						Reason:   "StartError",
						Message:  restoreCrashOCIMessage,
					},
				},
			},
			appStatus: corev1.ContainerStatus{
				Name: "app",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "PodInitializing",
					},
				},
			},
			sidecarStatus: corev1.ContainerStatus{
				Name: "sidecar",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "PodInitializing",
					},
				},
			},
			expectedClass:     FailureFatal,
			expectedContainer: "init-seed",
		},
		{
			name: "Sidecar has genuine application bug (exit 1 CrashLoopBackOff), not restore failure",
			initStatus: corev1.ContainerStatus{
				Name: "init-seed",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 0,
						Reason:   "Completed",
					},
				},
			},
			appStatus: corev1.ContainerStatus{
				Name: "app",
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{},
				},
			},
			sidecarStatus: corev1.ContainerStatus{
				Name: "sidecar",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason: "CrashLoopBackOff",
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 1,
						Reason:   "Error",
						Message:  "fatal: connection refused to redis",
					},
				},
			},
			expectedClass: FailureNone,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pm-multicontainer-0"},
				Status: corev1.PodStatus{
					Phase:                 corev1.PodRunning,
					InitContainerStatuses: []corev1.ContainerStatus{tc.initStatus},
					ContainerStatuses:     []corev1.ContainerStatus{tc.appStatus, tc.sidecarStatus},
				},
			}

			failure := Classify(pod, DefaultEngines()...)
			if failure.Class != tc.expectedClass {
				t.Fatalf("expected class %v, got %v (failure: %+v)", tc.expectedClass, failure.Class, failure)
			}
			if tc.expectedContainer != "" && failure.Container != tc.expectedContainer {
				t.Errorf("expected container %q, got %q", tc.expectedContainer, failure.Container)
			}
		})
	}
}
