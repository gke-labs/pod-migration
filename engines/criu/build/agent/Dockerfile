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

# criu-engine node agent image.
#
# Stage 1 bundles a self-contained criu >= 3.19 (3.16 hits the cgroup2 cgns
# restore quirk; Ubuntu 24.04 dropped criu). Debian trixie ships criu 4.x; the
# bundle carries the binary + all shared libs + its own ld-linux so it runs on
# the GKE host without touching host libraries. CRIU stays an unmodified
# external executable (exec-only subprocess).
FROM debian:trixie AS criu
RUN apt-get update && apt-get install -y --no-install-recommends criu && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    ver=$(criu --version | head -1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?'); \
    maj=${ver%%.*}; rest=${ver#*.}; min=${rest%%.*}; \
    if [ "$maj" -lt 3 ] || { [ "$maj" -eq 3 ] && [ "$min" -lt 19 ]; }; then \
      echo "criu $ver < 3.19 — refusing (cgroup2 cgns restore quirk)"; exit 1; fi; \
    mkdir -p /bundle; \
    cp /usr/sbin/criu /bundle/criu.bin; \
    for lib in $(ldd /usr/sbin/criu | grep -oE '/[^ ]+\.so[^ ]*'); do cp -L "$lib" /bundle/ || true; done; \
    cp -L /lib64/ld-linux-x86-64.so.2 /bundle/ 2>/dev/null || cp -L /lib/ld-linux-x86-64.so.2 /bundle/; \
    criu --version | head -1 > /bundle/VERSION; \
    printf '#!/bin/sh\nexec /var/lib/pod-migrate/criu-bundle/ld-linux-x86-64.so.2 --library-path /var/lib/pod-migrate/criu-bundle /var/lib/pod-migrate/criu-bundle/criu.bin "$@"\n' > /bundle/criu; \
    chmod +x /bundle/criu; ls -l /bundle; cat /bundle/VERSION

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal/ internal/
COPY cmd/ cmd/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /agent ./cmd/agent

# Runtime: needs nsenter (util-linux) to exec host runc in host namespaces.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends util-linux ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=criu /bundle /opt/criu-bundle
COPY --from=build /agent /agent
ENTRYPOINT ["/agent"]
