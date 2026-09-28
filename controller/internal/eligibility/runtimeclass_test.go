package eligibility

import (
	"strings"
	"testing"
)

func ptr(s string) *string { return &s }

func TestParseRuntimeClassPolicy(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		want       string
		wantErrSub string
	}{
		{name: "default", raw: "gvisor", want: "gvisor"},
		{name: "default token only", raw: "@default", want: "@default"},
		{name: "whitespace trimmed", raw: " gvisor , @default ", want: "gvisor,@default"},
		{name: "duplicates merged", raw: "gvisor,gvisor,@default,@default", want: "gvisor,@default"},
		{name: "sorted output", raw: "runsc-debug,gvisor", want: "gvisor,runsc-debug"},
		{name: "empty value", raw: "", wantErrSub: "empty element (use @default"},
		{name: "trailing comma", raw: "gvisor,", wantErrSub: "empty element (use @default"},
		{name: "leading comma", raw: ",gvisor", wantErrSub: "empty element"},
		{name: "uppercase name", raw: "gVisor", wantErrSub: `invalid runtime class name "gVisor"`},
		{name: "underscore name", raw: "run_c", wantErrSub: `invalid runtime class name "run_c"`},
		{name: "unknown token", raw: "@runc", wantErrSub: `invalid runtime class name "@runc"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParseRuntimeClassPolicy(tc.raw)
			if tc.wantErrSub != "" {
				if err == nil {
					t.Fatalf("ParseRuntimeClassPolicy(%q) = %q, want error containing %q", tc.raw, p.String(), tc.wantErrSub)
				}
				if !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("ParseRuntimeClassPolicy(%q) error = %q, want it to contain %q", tc.raw, err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRuntimeClassPolicy(%q) unexpected error: %v", tc.raw, err)
			}
			if got := p.String(); got != tc.want {
				t.Errorf("ParseRuntimeClassPolicy(%q).String() = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestRuntimeClassPolicyAllows(t *testing.T) {
	policies := map[string]*RuntimeClassPolicy{
		"nil":             nil,
		"gvisor":          mustParseRuntimeClassPolicy("gvisor"),
		"@default":        mustParseRuntimeClassPolicy("@default"),
		"gvisor,@default": mustParseRuntimeClassPolicy("gvisor,@default"),
	}
	pods := map[string]*string{
		"unset":  nil,
		"empty":  ptr(""),
		"gvisor": ptr("gvisor"),
		"runc":   ptr("runc"),
		"other":  ptr("other"),
	}
	want := map[string]map[string]bool{
		"nil":             {"unset": false, "empty": false, "gvisor": true, "runc": false, "other": false},
		"gvisor":          {"unset": false, "empty": false, "gvisor": true, "runc": false, "other": false},
		"@default":        {"unset": true, "empty": true, "gvisor": false, "runc": false, "other": false},
		"gvisor,@default": {"unset": true, "empty": true, "gvisor": true, "runc": false, "other": false},
	}
	for policyName, policy := range policies {
		for podName, runtimeClassName := range pods {
			if got := policy.Allows(runtimeClassName); got != want[policyName][podName] {
				t.Errorf("policy %s: Allows(%s) = %v, want %v", policyName, podName, got, want[policyName][podName])
			}
		}
	}
}

func TestNilPolicyStringIsDefault(t *testing.T) {
	var p *RuntimeClassPolicy
	if got := p.String(); got != DefaultMigratableRuntimeClasses {
		t.Errorf("nil policy String() = %q, want %q", got, DefaultMigratableRuntimeClasses)
	}
}

func TestRuntimeClassLabel(t *testing.T) {
	for _, tc := range []struct {
		in   *string
		want string
	}{
		{in: nil, want: "@default"},
		{in: ptr(""), want: "@default"},
		{in: ptr("runc"), want: "runc"},
	} {
		if got := RuntimeClassLabel(tc.in); got != tc.want {
			t.Errorf("RuntimeClassLabel(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
