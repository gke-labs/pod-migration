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
# so it runs on the GKE host without touching host libraries. Its criu wrapper
# names the default install directory, /var/lib/pod-migrate/criu-bundle; the
# agent writes its own for the node's layout (/home/kubernetes/bin on COS,
# #109), identical to this one on other nodes. CRIU stays an
# unmodified external executable (exec-only subprocess).
# Forky is Debian testing, whose packages change from week to week, so the
# stage pins its inputs: the base image by digest, and apt to the
# snapshot.debian.org timestamp that image was built from (its
# /etc/apt/sources.list.d/debian.sources names it). The build then checks the
# bundle against package-versions.lock. Testing gets no Debian security
# support, and the pinned snapshot gets no fixes until someone bumps the
# digest, DEBIAN_SNAPSHOT and the lock together (docs/release-process.md).
# licenses/package-versions.txt lists the source packages and versions, which
# is what the GPL source release fetches (tools/release/package_gpl_sources.sh)
# from forky or, once forky has moved on, from snapshot.debian.org.
FROM debian:forky@sha256:f3671d5a279b1ae356c7318ce6cdb84c600c2063d708ee97d3cce607bebfb85a AS criu
ARG DEBIAN_SNAPSHOT=20261123T000000Z
# Plain http, since the base image has no CA certificates: apt still checks the
# signed indexes against the archive keyring. Snapshot indexes are past their
# Valid-Until date by design, so apt can't tell an older snapshot from this
# one; the lock check below compares the versions instead.
RUN set -eux; \
    printf '%s\n' \
      'Types: deb' \
      "URIs: http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}" \
      'Suites: forky forky-updates' \
      'Components: main' \
      'Signed-By: /usr/share/keyrings/debian-archive-keyring.pgp' \
      'Check-Valid-Until: no' \
      '' \
      'Types: deb' \
      "URIs: http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}" \
      'Suites: forky-security' \
      'Components: main' \
      'Signed-By: /usr/share/keyrings/debian-archive-keyring.pgp' \
      'Check-Valid-Until: no' \
      >/etc/apt/sources.list.d/debian.sources; \
    apt-get update --error-on=any; \
    apt-get install -y --no-install-recommends criu; \
    rm -rf /var/lib/apt/lists/*
# min is the oldest CRIU the bundle may carry. Keep it equal to
# MinContainerdCRIUVersion in internal/node/containerd_readiness.go, the
# minimum the agent's readiness check accepts on containerd's path.
RUN set -eux; \
    min=4.2.1; \
    ver=$(criu --version | head -1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?'); \
    if [ "$(printf '%s\n' "$min" "$ver" | sort -V | head -1)" != "$min" ]; then \
      echo "criu $ver < $min: refusing (it fails dumps on Linux >= 6.16)"; exit 1; fi; \
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

# Fails if the bundle's source package versions differ from the lock, as they
# do after a new base image or snapshot, and almost surely would if an older
# snapshot were served in place of this one. To update the lock with the pin,
# build only the stage above (--target criu) and copy its
# /bundle/licenses/package-versions.txt (docs/release-process.md).
FROM criu AS criu-locked
COPY build/agent/package-versions.lock /package-versions.lock
RUN diff -u /package-versions.lock /bundle/licenses/package-versions.txt

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal/ internal/
COPY cmd/ cmd/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /agent ./cmd/agent && \
    # Keep this notice-collection block in sync across controller/Dockerfile,
    # engines/criu/build/engine/Dockerfile, and engines/criu/build/agent/Dockerfile
    # (verified by tools/release/dockerfile_notices_test.sh).
    sh -eu -c ' \
      target_pkg="$1"; \
      out="/third-party-notices"; \
      mkdir -p "${out}/go-stdlib" || exit 1; \
      : > "${out}/go-modules.txt" || exit 1; \
      go_lic=""; \
      for cand in "$(go env GOROOT)/LICENSE" /usr/local/go/LICENSE /usr/share/doc/golang-go/copyright; do \
        if [ -f "${cand}" ]; then go_lic="${cand}"; break; fi; \
      done; \
      if [ -z "${go_lic}" ]; then echo "ERROR: Go stdlib LICENSE not found" >&2; exit 1; fi; \
      cp "${go_lic}" "${out}/go-stdlib/LICENSE" || exit 1; \
      CGO_ENABLED=0 GOOS=linux go list -deps \
        -f "{{if and .Module (not .Module.Main)}}{{.Module.Path}} {{.Module.Version}} {{.Module.Dir}} {{.Dir}}{{end}}" \
        "${target_pkg}" > /tmp/go-deps-pkgs.txt || exit 1; \
      awk "{print \$1, \$2, \$3}" /tmp/go-deps-pkgs.txt | sort -u > /tmp/go-deps-mods.txt || exit 1; \
      awk "{ \
        mod=\$1; mdir=\$3; cur=\$4; \
        while (cur != \"\" && cur != mdir && cur != \"/\" && cur != \".\" && index(cur, mdir \"/\") == 1) { \
          print mod, mdir, cur; \
          sub(/\/[^\/]+$/, \"\", cur); \
        } \
      }" /tmp/go-deps-pkgs.txt | sort -u > /tmp/go-deps-subdirs.txt || exit 1; \
      copy_dir_notices() { \
        src_dir="$1"; \
        dst_dir="$2"; \
        saw_lic=0; \
        saw_recip=0; \
        for f in "${src_dir}"/*; do \
          [ -f "${f}" ] || continue; \
          base="${f##*/}"; \
          case "${base}" in \
            *.[Gg][Oo]|*.[SsCcHh]|*.[Cc][Cc]|*.[Cc][Pp][Pp]|*.[Hh][Hh]|*.[Pp][Rr][Oo][Tt][Oo]|*.[Ss][Hh]|*.[Pp][YyLl]|*.[Rr][BbSs]|*.[Jj][Aa][Vv][Aa]|*.[JjTt][Ss]) \
              continue ;; \
            [Ll][Ii][Cc][Ee][Nn][SsCc][Ee]*|[Cc][Oo][Pp][Yy][Ii][Nn][Gg]*|[Uu][Nn][Ll][Ii][Cc][Ee][Nn][SsCc][Ee]*) \
              mkdir -p "${dst_dir}" || exit 1; \
              cp -f "${f}" "${dst_dir}/${base}" || exit 1; \
              chmod 0644 "${dst_dir}/${base}" || exit 1; \
              saw_lic=1 ;; \
            [Nn][Oo][Tt][Ii][Cc][Ee]*) \
              mkdir -p "${dst_dir}" || exit 1; \
              cp -f "${f}" "${dst_dir}/${base}" || exit 1; \
              chmod 0644 "${dst_dir}/${base}" || exit 1 ;; \
            *) \
              continue ;; \
          esac; \
          case "${base}" in \
            *.[Dd][Oo][Cc][Ss]|*.[Dd][Oo][Cc]) ;; \
            *) \
              if grep -qiE "Mozilla Public License|GNU (Lesser |Affero )?General Public License|Eclipse Public License|Common Development and Distribution License|Attribution-ShareAlike" "${f}"; then \
                saw_recip=1; \
              fi ;; \
          esac; \
        done; \
        LAST_SAW_LIC="${saw_lic}"; \
        LAST_SAW_RECIP="${saw_recip}"; \
      }; \
      while read -r mod ver mdir; do \
        [ -n "${mod}" ] || continue; \
        printf "%s %s\n" "${mod}" "${ver}" >> "${out}/go-modules.txt" || exit 1; \
        copy_dir_notices "${mdir}" "${out}/${mod}"; \
        if [ "${LAST_SAW_LIC}" -eq 0 ]; then \
          echo "ERROR: no license file found at module root for ${mod}@${ver} in ${mdir}" >&2; \
          exit 1; \
        fi; \
        if [ "${LAST_SAW_RECIP}" -eq 1 ]; then \
          src_dest="${out}/reciprocal-source/${mod}"; \
          rm -rf "${src_dest}" || exit 1; \
          mkdir -p "${src_dest%/*}" || exit 1; \
          cp -R "${mdir}" "${src_dest}" || exit 1; \
        fi; \
      done < /tmp/go-deps-mods.txt; \
      while read -r mod mdir cur; do \
        [ -n "${mod}" ] || continue; \
        rel="${cur#"${mdir}/"}"; \
        copy_dir_notices "${cur}" "${out}/${mod}/${rel}"; \
        if [ "${LAST_SAW_RECIP}" -eq 1 ] && [ ! -d "${out}/reciprocal-source/${mod}" ]; then \
          src_dest="${out}/reciprocal-source/${mod}"; \
          mkdir -p "${src_dest%/*}" || exit 1; \
          cp -R "${mdir}" "${src_dest}" || exit 1; \
        fi; \
      done < /tmp/go-deps-subdirs.txt; \
      rm -f /tmp/go-deps-pkgs.txt /tmp/go-deps-mods.txt /tmp/go-deps-subdirs.txt; \
      chmod -R u+w "${out}" || exit 1; \
      find "${out}" -type d -exec chmod 0755 {} + || exit 1; \
      find "${out}" -type f -exec chmod 0644 {} + || exit 1; \
    ' _ ./cmd/agent

# Runtime: needs nsenter (util-linux) to exec host runc in host namespaces and
# iptables to flush CRIU network-lock rules on restore failure (#126).
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends util-linux ca-certificates iptables && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /licenses && dpkg-query -W -f='${Package} ${Version}\n' | sort -u > /licenses/runtime-package-versions.txt
COPY --from=criu-locked /bundle /opt/criu-bundle
COPY --from=build /agent /agent
COPY --from=build /third-party-notices /THIRD_PARTY_NOTICES
ENTRYPOINT ["/agent"]
