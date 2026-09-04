# Build
FROM golang:1.25 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/   cmd/
COPY api/   api/
COPY internal/ internal/
ARG LDFLAGS=""
RUN if [ -z "${LDFLAGS}" ]; then echo "WARNING: LDFLAGS is empty. Built image will have default/unknown version metadata." >&2; fi && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "${LDFLAGS}" -o manager cmd/main.go


# Runtime: distroless, nonroot.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532
ENTRYPOINT ["/manager"]
