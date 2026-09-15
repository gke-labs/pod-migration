# Build
FROM golang:1.25 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/   cmd/
COPY api/   api/
COPY internal/ internal/
ARG LDFLAGS=""
RUN if [ -z "${LDFLAGS}" ]; then echo "ERROR: LDFLAGS is required. Build image via 'make docker-build'." >&2; exit 1; fi && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "${LDFLAGS}" -o manager cmd/main.go


# Runtime: distroless, nonroot.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532
ENTRYPOINT ["/manager"]
