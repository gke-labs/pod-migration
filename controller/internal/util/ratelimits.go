package util

import "fmt"

// ValidateClientGoRateLimits rejects rate-limit flag values that client-go
// would reinterpret silently: QPS==0 falls back to the 5-QPS client-go
// default (starving concurrent workers), and negative values disable
// client-side rate limiting entirely.
func ValidateClientGoRateLimits(qps float64, burst int) error {
	if qps <= 0 {
		return fmt.Errorf("--client-go-qps must be > 0, got %v (0 silently reinstates client-go's 5-QPS default; negative disables client-side rate limiting)", qps)
	}
	if burst <= 0 {
		return fmt.Errorf("--client-go-burst must be > 0, got %d", burst)
	}
	return nil
}
