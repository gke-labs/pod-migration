# Third-Party License Notices & Source Code Availability

This document provides the third-party software inventory, licensing disclosures, and GPL/LGPL source code availability instructions for all container images published by the `pod-migration` project.

---

## 1. Project Licensing Overview

The `pod-migration` repository is licensed under the **Apache License, Version 2.0** (`Apache-2.0`).

All Go controllers, reconcilers, CRIU engine drivers, holder placeholders, and toolings developed in this repository are distributed under Apache-2.0.

Certain published container images (specifically the CRIU snapshot engine node agent) bundle third-party binaries and shared libraries licensed under open-source licenses, including the **GNU General Public License Version 2.0** (`GPL-2.0-only`) and the **GNU Lesser General Public License Version 2.1 or later** (`LGPL-2.1-or-later`).

---

## 2. Published Container Images Inventory

The project publishes four container images across its controllers and the CRIU snapshot engine:

### 2.1 Controller Manager (`pod-migration-controller`)

- **Dockerfile**: `controller/Dockerfile`
- **Base Image**: `gcr.io/distroless/static:nonroot`
- **Purpose**: Core migration controller managing `PodMigration` and `PodMigrationJob` custom resources.
- **Runtime Components**:
  - `/manager` (compiled Go binary): `Apache-2.0`
- **Go Direct Module Dependencies**:
  - `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go`, `k8s.io/component-helpers`: `Apache-2.0`
  - `sigs.k8s.io/controller-runtime`: `Apache-2.0`
  - `github.com/evanphx/json-patch/v5`: `BSD-3-Clause`
  - `github.com/prometheus/client_golang`, `github.com/prometheus/client_model`: `Apache-2.0`
  - `gomodules.xyz/jsonpatch/v2`: `Apache-2.0`
- **In-Image License Disclosures**:
  - Go runtime and all compiled third-party Go module `LICENSE` / `NOTICE` files (plus `go-modules.txt`): `/THIRD_PARTY_NOTICES/`
  - Distroless base license notices (`/usr/share/doc/*/copyright`).

### 2.2 CRIU Engine Controller (`pod-migration-criu-engine`)

- **Dockerfile**: `engines/criu/build/engine/Dockerfile`
- **Base Image**: `gcr.io/distroless/static:nonroot`
- **Purpose**: Cluster reconciler managing `PodSnapshotStorageConfig` and `PodSnapshotPolicy` under the CRIU API group.
- **Runtime Components**:
  - `/engine` (compiled Go binary): `Apache-2.0`
- **Go Direct Module Dependencies**:
  - `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go`: `Apache-2.0`
  - `sigs.k8s.io/controller-runtime`: `Apache-2.0`
  - `sigs.k8s.io/yaml`: `MIT` and `BSD-3-Clause`
  - `github.com/prometheus/client_golang`: `Apache-2.0`
- **In-Image License Disclosures**:
  - Go runtime and all compiled third-party Go module and subpackage `LICENSE` / `NOTICE` files (plus `go-modules.txt`): `/THIRD_PARTY_NOTICES/`
  - Distroless base license notices (`/usr/share/doc/*/copyright`).

### 2.3 Pod Sandbox Holder (`pod-migration-criu-holder`)

- **Dockerfile**: `engines/criu/build/holder/Dockerfile`
- **Base Image**: `scratch` (empty base layer)
- **Purpose**: Target pod placeholder container keeping the target pod sandbox alive until checkpoint restoration completes.
- **Runtime Components**:
  - `/holder` (compiled Go binary): `Apache-2.0`
- **Go Direct Module Dependencies**: Go standard library only (`CGO_ENABLED=0`).
- **In-Image License Disclosures**:
  - Go runtime (`BSD-3-Clause`) license text and module manifest: `/THIRD_PARTY_NOTICES/` (`go-stdlib/LICENSE`, `go-modules.txt`).

### 2.4 CRIU Node Agent (`pod-migration-criu-agent`)

- **Dockerfile**: `engines/criu/build/agent/Dockerfile`
- **Base Image**: `ubuntu:24.04`
- **Purpose**: Privileged DaemonSet node agent driving CRIU checkpoints and OCI container restores.
- **Runtime Components**:
  - `/agent` (compiled Go binary): `Apache-2.0`
- **Go Direct Module Dependencies**:
  - `github.com/containerd/containerd/v2`, `github.com/containerd/errdefs`: `Apache-2.0`
  - `github.com/moby/sys/mountinfo`: `Apache-2.0`
  - `github.com/opencontainers/runtime-spec`, `github.com/opencontainers/image-spec`: `Apache-2.0`
  - `github.com/pelletier/go-toml/v2`: `MIT`
  - `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go`: `Apache-2.0`
  - `sigs.k8s.io/yaml`: `MIT` and `BSD-3-Clause`
- **Distribution Packages (Ubuntu 24.04 Archive)**:
  - `util-linux` (`nsenter`): `GPL-2.0-or-later`, `LGPL-2.1-or-later`, `BSD-3-Clause`, `Public Domain`
  - `ca-certificates`: `MPL-2.0`, `GPL-2.0-or-later`
  - `coreutils`, `bash`: `GPL-3.0-or-later`
  - `libc6` (glibc runtime): `LGPL-2.1-or-later`
- **Bundled Pre-compiled Binaries & Libraries (`/opt/criu-bundle/` from Debian forky, pinned to a snapshot.debian.org timestamp)**:
  - `criu.bin` (version >= 4.2.1; `/opt/criu-bundle/licenses/package-versions.txt` lists the exact version of every bundled package): `GPL-2.0-only` (with Linux syscall exception)
  - `ld-linux-x86-64.so.2` (dynamic linker): `LGPL-2.1-or-later`
  - `libc.so.6`: `LGPL-2.1-or-later`
  - `libnet.so.9`: `BSD-2-Clause` / `GPL-2.0-or-later`
  - `libnl-3.so.200`: `LGPL-2.1-only`
  - `libprotobuf-c.so.1`: `BSD-2-Clause`
  - `libbsd.so.0`: `BSD-3-Clause`, `BSD-4-Clause`, `MIT`
  - `libmd.so.0`: `BSD-2-Clause`, `BSD-3-Clause`, `ISC`, `Beerware`, `Public Domain`
  - `libgnutls.so.30`: `LGPL-2.1-or-later`
  - `libp11-kit.so.0`: `BSD-3-Clause`, `ISC`
  - `libffi.so.8`: `MIT`, `Public Domain`
  - `libnftables.so.1`, `libnftnl.so.11`: `GPL-2.0-only`
  - `libxtables.so.12` (iptables): `GPL-2.0-only`
  - `libjansson.so.4`: `MIT` (its `dtoa.c` under David M. Gay's permissive notice)
  - `libgmp.so.10`: `LGPL-3.0-or-later` / `GPL-2.0-or-later`
  - `libnettle.so.8`, `libhogweed.so.6`: `LGPL-3.0-or-later` / `GPL-2.0-or-later`
  - `libtasn1.so.6`: `LGPL-2.1-or-later`
  - `libunistring.so.5`: `LGPL-3.0-or-later` / `GPL-2.0-or-later`
  - `libidn2.so.0`: `LGPL-2.1-or-later` / `GPL-2.0-or-later`
  - `libmnl.so.0`: `LGPL-2.1-or-later`
  - `libuuid.so.1` (util-linux): `BSD-3-Clause`
- **In-Image License Disclosures**:
  - Go runtime and all compiled third-party Go module `LICENSE` / `NOTICE` files (plus `go-modules.txt`): `/THIRD_PARTY_NOTICES/`
  - Reciprocal Go dependency source (unmodified `github.com/cyphar/filepath-securejoin` `MPL-2.0` files compiled transitively via `containerd/v2`): `/THIRD_PARTY_NOTICES/reciprocal-source/`
  - Distribution packages: `/usr/share/doc/*/copyright`
  - Bundled CRIU and shared libraries: `/opt/criu-bundle/licenses/*.copyright`
  - Build-time package version manifest: `/opt/criu-bundle/licenses/package-versions.txt`
  - Runtime base package version manifest: `/licenses/runtime-package-versions.txt`

---

## 3. CRIU Subprocess Boundary & License Separation

The CRIU snapshot engine adheres strictly to open-source licensing boundaries:

1. **Subprocess Isolation**: The Go node agent (`cmd/agent`) does not link against CRIU headers, source code, or libraries. CRIU is bundled as an unmodified standalone executable (`/opt/criu-bundle/criu.bin`).
2. **Standard Interfaces**: The node agent invokes CRIU solely as an external out-of-process subprocess (`exec.Command`) through `runc restore` or via `nsenter` in host namespaces.
3. **Distribution Compliance**: Because the agent image redistributes pre-compiled GPL-2.0 (`criu`, `nftables`) and LGPL-2.1 (`glibc`, `libnl-3`, `gnutls`) binaries, matching machine-readable source code is made available for each published release per GPL-2.0 Section 3.

---

## 4. GPL and LGPL Source Code Availability

In compliance with the GNU General Public License Version 2.0 (GPL-2.0) and GNU Lesser General Public License Version 2.1 (LGPL-2.1), the matching source code for all GPL and LGPL binaries redistributed in the `pod-migration-criu-agent` container image is made available to users through the following channels:

### 4.1 Upstream Project Repositories

- **CRIU (Checkpoint/Restore In Userspace)**:
  - Project Home: [https://criu.org](https://criu.org)
  - Source Repository: [https://github.com/checkpoint-restore/criu](https://github.com/checkpoint-restore/criu)
- **libnl (Netlink Protocol Library)**:
  - Source Repository: [https://github.com/thom311/libnl](https://github.com/thom311/libnl)
- **nftables / libnftnl**:
  - Project Home: [https://netfilter.org/projects/nftables/](https://netfilter.org/projects/nftables/)
- **GNU C Library (glibc)**:
  - Project Home: [https://www.gnu.org/software/libc/](https://www.gnu.org/software/libc/)
- **util-linux**:
  - Project Home: [https://github.com/util-linux/util-linux](https://github.com/util-linux/util-linux)

### 4.2 Distribution Package Source Repositories

The bundled binaries are built from Debian forky distribution packages at the snapshot.debian.org timestamp that `engines/criu/build/agent/Dockerfile` pins (`DEBIAN_SNAPSHOT`), while runtime utilities derive from Ubuntu 24.04:

- **Debian Forky Archive, at the pinned snapshot**: `http://snapshot.debian.org/archive/debian/<DEBIAN_SNAPSHOT>/pool/`
- **Ubuntu 24.04 Archive**: `http://archive.ubuntu.com/ubuntu/pool/`

_Note on package archival_: Live distribution pools periodically drop superseded package versions when minor updates are published. For superseded Debian package versions, exact matching source archives (`.dsc`, `.orig.tar.xz`, and `.debian.tar.xz`) are permanently archived and downloadable from the Debian Snapshot service:
`https://snapshot.debian.org/`

To fetch a matching Debian source package with standard distribution tools, ask for the exact version that the image's `/opt/criu-bundle/licenses/package-versions.txt` lists (`tools/release/package_gpl_sources.sh --package-versions <file>` does this for every bundled package):

```bash
apt-get source -d criu=<version>
```

### 4.3 Release Source Bundles

For each tagged project release publishing container images (e.g. `v0.1.0-alpha`), a complete standalone source code archive (`criu-gpl-source-<version>.tar.gz`) containing the exact upstream source code and distribution patches corresponding to the bundled CRIU binary and libraries is packaged and attached directly to the GitHub Release.

A script (`tools/release/package_gpl_sources.sh`) is provided in this repository to automate the generation and cryptographic checksum verification of this archive, supporting build-time package version manifests (`package-versions.txt`).

### 4.4 Written Offer for Source Code

In accordance with Section 3(b) of the GNU General Public License Version 2.0 and Section 4 of the GNU Lesser General Public License Version 2.1, the pod-migration project hereby makes a formal written offer:

> **Written Offer**: Any recipient of binary container images distributed by the pod-migration project may request a machine-readable copy of the corresponding source code for all GPL/LGPL software included in those images, for a period of at least three years from distribution.
>
> To request source code under this offer, please open an issue at:
> https://github.com/gke-labs/pod-migration/issues
> or contact the project maintainers.
