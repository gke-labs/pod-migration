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
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /holder ./cmd/holder && \
    sh -eu -c ' \
      out="/third-party-notices"; \
      mkdir -p "${out}/go-stdlib" || exit 1; \
      : > "${out}/go-modules.txt" || exit 1; \
      go_lic=""; \
      for cand in "$(go env GOROOT)/LICENSE" /usr/local/go/LICENSE /usr/share/doc/golang-go/copyright; do \
        if [ -f "${cand}" ]; then go_lic="${cand}"; break; fi; \
      done; \
      if [ -z "${go_lic}" ]; then echo "ERROR: Go stdlib LICENSE not found" >&2; exit 1; fi; \
      cp "${go_lic}" "${out}/go-stdlib/LICENSE" || exit 1; \
      third_party="$(CGO_ENABLED=0 GOOS=linux go list -deps -f "{{if and .Module (not .Module.Main)}}{{.Module.Path}}{{end}}" ./cmd/holder | sort -u)" || exit 1; \
      if [ -n "${third_party}" ]; then \
        echo "ERROR: holder unexpectedly imports third-party Go modules: ${third_party}" >&2; \
        exit 1; \
      fi; \
      chmod -R u+w "${out}" || exit 1; \
      find "${out}" -type d -exec chmod 0755 {} + || exit 1; \
      find "${out}" -type f -exec chmod 0644 {} + || exit 1; \
    '

FROM scratch
COPY --from=build /holder /holder
COPY --from=build /third-party-notices /THIRD_PARTY_NOTICES
USER 65532:65532
ENTRYPOINT ["/holder"]
