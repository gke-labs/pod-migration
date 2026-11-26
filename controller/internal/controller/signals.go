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

package controller

import (
	"regexp"
	"strings"

	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
)

// Signals for external controllers (#294). The messages of the
// PlacementTimeout and RestoreIncompatibleNode conditions and Warning events
// start with machine-readable fields, then ": " and the human text:
//
//	engine=<engine> requirements=<key>[,<key>...]: <text>
//	engine=<engine> fact=<fact> node=<node> sourceNode=<node>: <text>
//
// Fields are key=value, separated by single spaces, in that order; a later
// version may add some at the end. Values never contain a space or a colon
// (engine names, label keys and node names can't, and validSignalValue
// checks the engine's own), and may be empty when not known. README.md, "Signals for external
// controllers", documents them.

// maxListedCPUFlagKeys is how many of the CRIU engine's missing-CPU-flag
// requirement keys a requirements field lists one by one. With more, they're
// listed as one key, criuplacement.MissingCPUFlagLabelPrefix followed by
// "*", so that the message stays short: a source node can have about 90
// flags.
const maxListedCPUFlagKeys = 8

// placementSignalPattern matches the fields at the start of a
// PlacementTimeout message, with any that a later version adds at the end.
// No class matches a space or a colon, so a match never runs into human
// text that contains ": ".
var placementSignalPattern = regexp.MustCompile(`^(engine=[^ :]+ requirements=[^ :]*(?: [^ :=]+=[^ :]*)*): `)

// placementSignal renders the fields of a PlacementTimeout message: the
// engine that stated the node constraints, and the keys of the ones that
// failed (failedPlacementKeys).
func placementSignal(engineName string, keys []string) string {
	return "engine=" + engineName + " requirements=" + strings.Join(keys, ",")
}

// incompatibleNodeSignal renders the fields of a RestoreIncompatibleNode
// message: the engine, the node fact that didn't match, the replacement's
// node, and the checkpoint's source node.
func incompatibleNodeSignal(engineName, fact, node, sourceNode string) string {
	return "engine=" + engineName + " fact=" + fact + " node=" + node + " sourceNode=" + sourceNode
}

// signalValuePattern matches a field value that an engine supplies, such
// as RestoreVerdict.IncompatibleFact: a non-empty token without a space or
// a colon, so that the format holds.
var signalValuePattern = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

// validSignalValue reports whether v can be a field value that an engine
// supplies. A verdict whose IncompatibleFact isn't concludes as any other
// cold start, FallbackToColdStart.
func validSignalValue(v string) bool {
	return signalValuePattern.MatchString(v)
}

// withSignal prefixes text with signal's fields.
func withSignal(signal, text string) string {
	return signal + ": " + text
}

// placementSignalOf returns the fields at the start of message, a
// PlacementTimeout message, if it has them: one written by an earlier
// version doesn't.
func placementSignalOf(message string) (string, bool) {
	m := placementSignalPattern.FindStringSubmatch(message)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// compactRequirementKeys drops repeated keys, keeping the first, and lists
// more than maxListedCPUFlagKeys missing-CPU-flag keys as one wildcard key,
// where the first of them was.
func compactRequirementKeys(keys []string) []string {
	seen := map[string]bool{}
	var unique []string
	flags := 0
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, key)
		if strings.HasPrefix(key, criuplacement.MissingCPUFlagLabelPrefix) {
			flags++
		}
	}
	if flags <= maxListedCPUFlagKeys {
		return unique
	}
	wildcard := criuplacement.MissingCPUFlagLabelPrefix + "*"
	var compact []string
	for _, key := range unique {
		if !strings.HasPrefix(key, criuplacement.MissingCPUFlagLabelPrefix) {
			compact = append(compact, key)
		} else if !seen[wildcard] {
			seen[wildcard] = true
			compact = append(compact, wildcard)
		}
	}
	return compact
}
