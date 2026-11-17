// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
