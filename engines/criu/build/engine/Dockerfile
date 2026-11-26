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

# criu-snapshot-engine cluster controller manager image.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal/ internal/
COPY cmd/ cmd/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /engine ./cmd/engine && \
    set -eu && \
    mkdir -p /third-party-notices/go-stdlib && \
    : > /third-party-notices/go-modules.txt && \
    go_lic="" && \
    for cand in "$(go env GOROOT)/LICENSE" /usr/local/go/LICENSE /usr/share/doc/golang-go/copyright; do \
      if [ -f "${cand}" ]; then go_lic="${cand}"; break; fi; \
    done && \
    if [ -z "${go_lic}" ]; then echo "ERROR: Go stdlib LICENSE not found" >&2; exit 1; fi && \
    cp "${go_lic}" /third-party-notices/go-stdlib/LICENSE && \
    CGO_ENABLED=0 GOOS=linux go list -deps -f '{{if and .Module (not .Module.Main)}}{{.Module.Path}} {{.Module.Version}} {{.Module.Dir}}{{end}}' ./cmd/engine > /tmp/go-deps.txt && \
    sort -u /tmp/go-deps.txt \
      | while read -r mod ver dir; do \
          [ -n "${mod}" ] || continue; \
          printf '%s %s\n' "${mod}" "${ver}" >> /third-party-notices/go-modules.txt; \
          dest="/third-party-notices/${mod}"; \
          mkdir -p "${dest}"; \
          found=0; \
          reciprocal=0; \
          for f in "${dir}"/*; do \
            [ -f "${f}" ] || continue; \
            base="$(basename "${f}")"; \
            case "$(printf '%s' "${base}" | tr '[:upper:]' '[:lower:]')" in \
              license*|licence*|copying*|notice*) \
                cp "${f}" "${dest}/${base}"; \
                found=1; \
                if grep -qiE 'Mozilla Public License|GNU (Lesser |Affero )?General Public License' "${f}"; then \
                  reciprocal=1; \
                fi; \
                ;; \
            esac; \
          done; \
          if [ "${found}" -eq 0 ]; then \
            echo "ERROR: no license/notice file found for ${mod}@${ver} in ${dir}" >&2; \
            exit 1; \
          fi; \
          if [ "${reciprocal}" -eq 1 ]; then \
            src_dest="/third-party-notices/reciprocal-source/${mod}"; \
            mkdir -p "$(dirname "${src_dest}")"; \
            cp -R "${dir}" "${src_dest}"; \
            chmod -R u+w,a+rX "${src_dest}"; \
          fi; \
        done && \
    rm -f /tmp/go-deps.txt && \
    chmod -R u+w /third-party-notices && \
    find /third-party-notices -type d -exec chmod 0755 {} + && \
    find /third-party-notices -type f -exec chmod 0644 {} +

FROM gcr.io/distroless/static:nonroot
COPY --from=build /engine /engine
COPY --from=build /third-party-notices /THIRD_PARTY_NOTICES
USER 65532:65532
ENTRYPOINT ["/engine"]
