# Release Process

This document details the step-by-step procedure for cutting official releases of the `pod-migration` project (Epic #110), including quality verification, container image publishing, third-party license notice compliance, and GPL/LGPL source code packaging (#136).

---

## 1. Release Cadence & Versioning

- **Semantic Versioning**: Releases follow Semantic Versioning `vMAJOR.MINOR.PATCH[-PRERELEASE]` (e.g. `v0.1.0-alpha`).
- **Tag Conventions**: Git tags must be annotated and prefixed with `v` (e.g. `v0.1.0-alpha`).

---

## 2. Pre-Release Quality Gates

Before initiating a release build, all automated verification gates must pass cleanly on `main`:

```bash
# 1. Invariant governance gate (Lane 1 verification)
./verification-suite/verify_invariant_governance.sh --base upstream/main

# 2. Apache-2.0 license header check
./verification-suite/verify_license_headers.sh

# 3. Unit test suites with race detection
cd controller && go test -race ./...
cd ../engines/criu && go test -race ./...
cd ..

# 4. Static analysis and code formatting
cd controller && go vet ./...
cd ../engines/criu && go vet ./...
cd ..
```

---

## 3. Third-Party License & Image Inventory Verification

Before publishing images, verify that third-party licensing disclosures remain up to date:

1. Review `docs/third-party-licenses.md` against any newly introduced Go dependencies or base image updates.
2. Ensure `engines/criu/build/agent/Dockerfile` continues to extract package copyright files from Debian forky into `/opt/criu-bundle/licenses/`. Its CRIU stage is pinned, so security fixes reach the bundle only when someone bumps the pin: check for one before each release. To bump it:
   - Set the `debian:forky` digest to a newer image, and `DEBIAN_SNAPSHOT` to the snapshot that image was built from: `docker run --rm debian:forky@sha256:<digest> grep -m1 snapshot /etc/apt/sources.list.d/debian.sources`.
   - Build the image. It fails until `engines/criu/build/agent/package-versions.lock` matches the bundle. To update the lock, run this from `engines/criu` and review the difference in git: `docker run --rm "$(docker build -q --target criu -f build/agent/Dockerfile .)" cat /bundle/licenses/package-versions.txt > build/agent/package-versions.lock`.
   - If the bundled libraries changed, update them in `docs/third-party-licenses.md`.

   The build fetches from snapshot.debian.org, so while that service is down, the agent image can't be built.
3. Verify that all components in published images are properly categorized by license (Apache-2.0, MIT, BSD-3-Clause, GPL-2.0, LGPL-2.1).

---

## 4. Container Image Building & Publishing

The release publishes four container images to the project registry:

Set the release version and target registry:

```bash
export REGISTRY="gke-labs" # or target distribution registry
export VERSION="v0.1.0-alpha"

export CONTROLLER_IMG="${REGISTRY}/pod-migration-controller:${VERSION}"
export ENGINE_IMG="${REGISTRY}/pod-migration-criu-engine:${VERSION}"
export AGENT_IMG="${REGISTRY}/pod-migration-criu-agent:${VERSION}"
export HOLDER_IMG="${REGISTRY}/pod-migration-criu-holder:${VERSION}"
```

### 4.1 Build Controller Manager Image

```bash
cd controller
make docker-build IMG="${CONTROLLER_IMG}"
cd ..
```

### 4.2 Build CRIU Engine Images

```bash
cd engines/criu
docker build -f build/engine/Dockerfile -t "${ENGINE_IMG}" .
docker build -f build/agent/Dockerfile  -t "${AGENT_IMG}" .
docker build -f build/holder/Dockerfile -t "${HOLDER_IMG}" .
cd ../..
```

### 4.3 Verify In-Image License Disclosures

Verify that each image carries `/THIRD_PARTY_NOTICES/` (Go stdlib and all compiled third-party Go module notices, plus reciprocal Go source where applicable) and that the agent image carries the bundled CRIU license notices:

```bash
# Distroless/scratch images have no shell; inspect via docker create + docker export:
for img in "${CONTROLLER_IMG}" "${ENGINE_IMG}" "${HOLDER_IMG}"; do
  cid="$(docker create "${img}")"
  docker export "${cid}" | tar -t | grep -E '^THIRD_PARTY_NOTICES/(go-stdlib/LICENSE|go-modules\.txt)$'
  docker rm "${cid}" >/dev/null
done

# Agent image (Ubuntu base):
docker run --rm --entrypoint ls "${AGENT_IMG}" -la /THIRD_PARTY_NOTICES/ /opt/criu-bundle/licenses/
docker run --rm --entrypoint head "${AGENT_IMG}" -n 15 /opt/criu-bundle/licenses/criu.copyright
```

### 4.4 Push Images to Registry

```bash
docker push "${CONTROLLER_IMG}"
docker push "${ENGINE_IMG}"
docker push "${AGENT_IMG}"
docker push "${HOLDER_IMG}"
```

---

## 5. Package GPL and LGPL Sources (#136)

Per GPL-2.0 Section 3, any release distributing the `pod-migration-criu-agent` container image must make the corresponding source code available.

Run the automated packaging script to extract the matching Debian source packages and generate a release source archive. Pass the package versions recorded in the agent image you publish: its CRIU stage builds from Debian forky (testing) at the snapshot.debian.org timestamp that `engines/criu/build/agent/Dockerfile` pins (`DEBIAN_SNAPSHOT`), and that image's own list names exactly the versions it carries (the build checks it against `package-versions.lock`). Without the list, the script fetches whatever versions forky has that day.

```bash
# Package matching source files into ./dist
cid="$(docker create "${AGENT_IMG}")"
docker cp "${cid}:/opt/criu-bundle/licenses/package-versions.txt" /tmp/agent-package-versions.txt
docker rm "${cid}" >/dev/null
./tools/release/package_gpl_sources.sh --version "${VERSION}" --output-dir dist/ \
  --package-versions /tmp/agent-package-versions.txt
```

This generates:

- `dist/criu-gpl-source-${VERSION}.tar.gz`
- `dist/criu-gpl-source-${VERSION}.tar.gz.sha256`

Verify the archive contents:

```bash
tar -ztvf "dist/criu-gpl-source-${VERSION}.tar.gz"
cat "dist/criu-gpl-source-${VERSION}.tar.gz.sha256"
```

---

## 6. Create Git Tag and GitHub Release

### 6.1 Create and Push Annotated Git Tag

```bash
git tag -a "${VERSION}" -m "Release ${VERSION}"
git push upstream "${VERSION}"
```

### 6.2 Create GitHub Release and Attach GPL Source

Create the GitHub release using `gh release create`, attaching the packaged GPL/LGPL source archive and checksum:

```bash
gh release create "${VERSION}" \
  --title "Release ${VERSION}" \
  --notes-file release_notes.md \
  dist/criu-gpl-source-${VERSION}.tar.gz \
  dist/criu-gpl-source-${VERSION}.tar.gz.sha256
```

### 6.3 Link GPL Source in Release Notes

Ensure the release notes explicitly reference the attached GPL source archive and licensing documentation:

```markdown
### Third-Party Software & Source Code Availability

The CRIU node agent image (`pod-migration-criu-agent`) redistributes pre-compiled binaries of CRIU and shared libraries licensed under GPL-2.0 and LGPL-2.1.

- Complete third-party license notices: [docs/third-party-licenses.md](docs/third-party-licenses.md)
- Matching source code archive: Attached to this release as `criu-gpl-source-${VERSION}.tar.gz`
```
