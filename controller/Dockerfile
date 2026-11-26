# syntax=docker/dockerfile:1
# Build
FROM golang:1.25 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY cmd/   cmd/
COPY api/   api/
COPY internal/ internal/
ARG LDFLAGS=""
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    if [ -z "${LDFLAGS}" ]; then echo "ERROR: LDFLAGS is required. Build image via 'make docker-build'." >&2; exit 1; fi && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "${LDFLAGS}" -o manager cmd/main.go && \
    set -eu && \
    mkdir -p /third-party-notices/go-stdlib && \
    : > /third-party-notices/go-modules.txt && \
    go_lic="" && \
    for cand in "$(go env GOROOT)/LICENSE" /usr/local/go/LICENSE /usr/share/doc/golang-go/copyright; do \
      if [ -f "${cand}" ]; then go_lic="${cand}"; break; fi; \
    done && \
    if [ -z "${go_lic}" ]; then echo "ERROR: Go stdlib LICENSE not found" >&2; exit 1; fi && \
    cp "${go_lic}" /third-party-notices/go-stdlib/LICENSE && \
    CGO_ENABLED=0 GOOS=linux go list -deps -f '{{if and .Module (not .Module.Main)}}{{.Module.Path}} {{.Module.Version}} {{.Module.Dir}}{{end}}' ./cmd > /tmp/go-deps.txt && \
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


# Runtime: distroless, nonroot.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /third-party-notices /THIRD_PARTY_NOTICES
USER 65532:65532
ENTRYPOINT ["/manager"]
