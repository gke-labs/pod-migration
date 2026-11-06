# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# holder — the target pod placeholder (serves nothing; keeps the pod sandbox
# alive while the node agent restores a sibling container, or falls through to
# exec the original command on cold start).
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd/holder/ cmd/holder/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /holder ./cmd/holder

FROM scratch
COPY --from=build /holder /holder
USER 65532:65532
ENTRYPOINT ["/holder"]
