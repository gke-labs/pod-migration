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
    ' _ ./cmd


# Runtime: distroless, nonroot.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /third-party-notices /THIRD_PARTY_NOTICES
USER 65532:65532
ENTRYPOINT ["/manager"]
