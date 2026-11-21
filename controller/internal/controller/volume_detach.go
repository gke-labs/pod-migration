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
	"context"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReasonVolumeDetachTimeout is recorded when origin pod volume detachment exceeds migration timeout.
const ReasonVolumeDetachTimeout = "VolumeDetachTimeout"

// ReasonWaitingForVolumeDetach is the Ready condition reason while waiting for origin volumes to detach.
const ReasonWaitingForVolumeDetach = "WaitingForVolumeDetach"

// isRWOAccessMode reports whether accessModes specifies ReadWriteOnce or ReadWriteOncePod.
// An empty slice is treated as RWO for test fixture compatibility where AccessModes was omitted.
func isRWOAccessMode(accessModes []corev1.PersistentVolumeAccessMode) bool {
	if len(accessModes) == 0 {
		return true
	}
	for _, mode := range accessModes {
		if mode == corev1.ReadWriteOnce || mode == corev1.ReadWriteOncePod {
			return true
		}
	}
	return false
}

// areOriginVolumesDetached reports whether all RWO/RWOP PersistentVolumes listed in pvs have
// detached from originNodeName. Attachments on other nodes (such as the destination node
// or existing ReadWriteMany attachments) do not block detachment verification. Non-RWO
// volumes (e.g. ReadWriteMany, ReadOnlyMany) are skipped.
func areOriginVolumesDetached(ctx context.Context, cl client.Reader, originNodeName string, pvs []string) (bool, error) {
	for _, targetPV := range pvs {
		if targetPV == "" {
			continue
		}
		// If the PersistentVolume object is accessible, skip non-RWO volumes (RWX/ROX).
		pv := &corev1.PersistentVolume{}
		if err := cl.Get(ctx, types.NamespacedName{Name: targetPV}, pv); err == nil {
			if len(pv.Spec.AccessModes) > 0 && !isRWOAccessMode(pv.Spec.AccessModes) {
				continue
			}
		}

		vaList := &storagev1.VolumeAttachmentList{}
		if err := cl.List(ctx, vaList, client.MatchingFields{VolumeAttachmentPVIndex: targetPV}); err != nil {
			return false, err
		}

		for _, va := range vaList.Items {
			// If originNodeName is known, only check detachment from the origin node.
			// Attachments on other nodes (e.g. destination node or existing RWX attachments) must not block migration.
			if originNodeName != "" && va.Spec.NodeName != originNodeName {
				continue
			}
			if va.Status.Attached {
				return false, nil
			}
		}
	}
	return true, nil
}
