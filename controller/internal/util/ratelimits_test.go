package util

import (
	"strings"
	"testing"
)

func TestValidateClientGoRateLimits(t *testing.T) {
	tests := []struct {
		name    string
		qps     float64
		burst   int
		wantErr string
	}{
		{name: "defaults are valid", qps: 500, burst: 1000},
		{name: "small positive values are valid", qps: 0.5, burst: 1},
		{
			// client-go treats QPS==0 as "use the 5-QPS default", which would
			// silently starve 50 workers; reject it instead of surprising the operator.
			name:    "zero qps rejected",
			qps:     0,
			burst:   1000,
			wantErr: "client-go-qps",
		},
		{
			// Negative disables client-side rate limiting entirely.
			name:    "negative qps rejected",
			qps:     -1,
			burst:   1000,
			wantErr: "client-go-qps",
		},
		{name: "zero burst rejected", qps: 500, burst: 0, wantErr: "client-go-burst"},
		{name: "negative burst rejected", qps: 500, burst: -5, wantErr: "client-go-burst"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateClientGoRateLimits(tt.qps, tt.burst)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected valid, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error mentioning %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error to mention %q, got: %v", tt.wantErr, err)
			}
		})
	}
}
