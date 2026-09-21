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
