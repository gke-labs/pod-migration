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
# Stage 1 bundles a self-contained criu >= 4.2.1. Older CRIU can't dump a
# container on newer kernels: on Linux >= 6.16 every TCP socket fails the dump
# (SO_PASSCRED/SO_PASSSEC return EOPNOTSUPP), and on Linux >= 7.1, which removed
# UDP-Lite, every dump fails; 3.16 also hits the cgroup2 cgns restore quirk.
# Ubuntu 24.04 dropped criu and Debian trixie ships 4.1.1, so this takes Debian
# forky's. The bundle carries the binary + all shared libs + its own ld-linux
# so it runs on the GKE host without touching host libraries. CRIU stays an
# unmodified external executable (exec-only subprocess).
# licenses/package-versions.txt lists the source packages and versions, which
# is what the GPL source release fetches (tools/release/package_gpl_sources.sh)
# from forky or, once forky has moved on, from snapshot.debian.org.
FROM debian:forky AS criu
RUN apt-get update && apt-get install -y --no-install-recommends criu && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    ver=$(criu --version | head -1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?'); \
    if [ "$(printf '%s\n' 4.2.1 "$ver" | sort -V | head -1)" != 4.2.1 ]; then \
      echo "criu $ver < 4.2.1: refusing (it fails dumps on Linux >= 6.16)"; exit 1; fi; \
    mkdir -p /bundle /bundle/licenses; \
    cp /usr/sbin/criu /bundle/criu.bin; \
    for lib in $(ldd /usr/sbin/criu | grep -oE '/[^ ]+\.so[^ ]*'); do cp -L "$lib" /bundle/ || true; done; \
    cp -L /lib64/ld-linux-x86-64.so.2 /bundle/ 2>/dev/null || cp -L /lib/ld-linux-x86-64.so.2 /bundle/; \
    cp -L /usr/share/doc/criu/copyright /bundle/licenses/criu.copyright; \
    for f in $(ldd /usr/sbin/criu | grep -oE '/[^ ]+\.so[^ ]*'); do \
      dpkg -S "$(realpath "$f")" 2>/dev/null || echo "WARNING: dpkg -S failed for $f" >&2; \
    done | cut -d: -f1 | sort -u | while read -r pkg; do \
      if [ -f "/usr/share/doc/$pkg/copyright" ]; then \
        cp -L "/usr/share/doc/$pkg/copyright" "/bundle/licenses/$pkg.copyright"; \
      fi; \
    done; \
    { \
      dpkg-query -W -f='${source:Package} ${source:Version}\n' criu; \
      for f in $(ldd /usr/sbin/criu | grep -oE '/[^ ]+\.so[^ ]*'); do \
        dpkg -S "$(realpath "$f")" 2>/dev/null || echo "WARNING: dpkg -S failed for $f" >&2; \
      done | cut -d: -f1 | sort -u | while read -r pkg; do \
        dpkg-query -W -f='${source:Package} ${source:Version}\n' "$pkg"; \
      done; \
    } | sort -u > /bundle/licenses/package-versions.txt; \
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

# Runtime: needs nsenter (util-linux) to exec host runc in host namespaces and
# iptables to flush CRIU network-lock rules on restore failure (#126).
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends util-linux ca-certificates iptables && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /licenses && dpkg-query -W -f='${Package} ${Version}\n' | sort -u > /licenses/runtime-package-versions.txt
COPY --from=criu /bundle /opt/criu-bundle
COPY --from=build /agent /agent
ENTRYPOINT ["/agent"]
