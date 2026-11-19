#!/usr/bin/env bash
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
#
# Packages corresponding GPL/LGPL source archives for third-party components
# bundled in published container images (specifically CRIU and bundled libraries
# from Debian trixie in build/agent/Dockerfile, plus Ubuntu runtime base utilities).
#
# Produces:
#   <output-dir>/criu-gpl-source-<version>.tar.gz
#   <output-dir>/criu-gpl-source-<version>.tar.gz.sha256
#
# Usage:
#   tools/release/package_gpl_sources.sh [--output-dir <dir>] [--version <ver>]
#                                        [--package-versions <file>]
#                                        [--runtime-package-versions <file>]
#                                        [--dry-run]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/dist"
VERSION=""
DRY_RUN=0
PACKAGE_VERSIONS_FILE=""
RUNTIME_VERSIONS_FILE=""

usage() {
  local code="${1:-0}"
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Packages matching GPL/LGPL source archives for published container images.

Options:
  --output-dir DIR                Output directory for source archives (default: ./dist)
  --version VER                   Release version (default: from git tag or v0.1.0-alpha)
  --package-versions FILE         Path to bundle packages manifest (dpkg-query output from build)
  --runtime-package-versions FILE Path to runtime packages manifest (dpkg-query output from build)
  --dry-run                       Validate configuration and inventory without downloading
  -h, --help                      Show this help message
EOF
  exit "${code}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --version)
      VERSION="$2"
      shift 2
      ;;
    --package-versions)
      PACKAGE_VERSIONS_FILE="$2"
      shift 2
      ;;
    --runtime-package-versions)
      RUNTIME_VERSIONS_FILE="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage 1
      ;;
  esac
done

if [[ -z "${VERSION}" ]]; then
  if git -C "${REPO_ROOT}" describe --tags --match 'v*' --exact-match >/dev/null 2>&1; then
    VERSION="$(git -C "${REPO_ROOT}" describe --tags --match 'v*')"
  else
    VERSION="v0.1.0-alpha"
  fi
fi

# Fallback default Debian GPL/LGPL packages if no build-time manifest is supplied.
DEBIAN_PACKAGES=()
if [[ -n "${PACKAGE_VERSIONS_FILE}" ]]; then
  if [[ ! -f "${PACKAGE_VERSIONS_FILE}" ]]; then
    echo "ERROR: package versions file not found: ${PACKAGE_VERSIONS_FILE}" >&2
    exit 1
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^# ]] && continue
    DEBIAN_PACKAGES+=("$line")
  done < "${PACKAGE_VERSIONS_FILE}"
else
  DEBIAN_PACKAGES=(
    "criu"
    "glibc"
    "libnet"
    "libnl3"
    "nftables"
    "libnftnl"
    "gnutls28"
    "libcap2"
    "libbsd"
    "gmp"
    "nettle"
    "libtasn1-6"
    "libunistring"
    "libidn2-0"
    "libmnl0"
  )
fi

# Fallback default Ubuntu 24.04 runtime base GPL packages
UBUNTU_PACKAGES=()
if [[ -n "${RUNTIME_VERSIONS_FILE}" ]]; then
  if [[ ! -f "${RUNTIME_VERSIONS_FILE}" ]]; then
    echo "ERROR: runtime package versions file not found: ${RUNTIME_VERSIONS_FILE}" >&2
    exit 1
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^# ]] && continue
    UBUNTU_PACKAGES+=("$line")
  done < "${RUNTIME_VERSIONS_FILE}"
else
  UBUNTU_PACKAGES=(
    "util-linux"
    "bash"
    "coreutils"
  )
fi

echo "==> Packaging GPL/LGPL sources for pod-migration release: ${VERSION}"
echo "    Output directory: ${OUTPUT_DIR}"
echo "    Debian bundle packages (${#DEBIAN_PACKAGES[@]}):  ${DEBIAN_PACKAGES[*]}"
echo "    Ubuntu runtime packages (${#UBUNTU_PACKAGES[@]}): ${UBUNTU_PACKAGES[*]}"

generate_manifest() {
  local out_file="$1"
  cat <<NOTICES > "${out_file}"
GPL / LGPL SOURCE CODE DISTRIBUTION MANIFEST
Project: pod-migration (${VERSION})
Source Repository: https://github.com/gke-labs/pod-migration

This archive contains corresponding source code and distribution patches for all
software licensed under the GNU General Public License (GPL) and GNU Lesser
General Public License (LGPL) that is redistributed in pod-migration container images.

1. BUNDLED CRIU STAGE (Debian trixie):
$(printf ' - %s\n' "${DEBIAN_PACKAGES[@]}")

Primary source archives: http://deb.debian.org/debian/pool/
Superseded package snapshot archive: https://snapshot.debian.org/

2. RUNTIME BASE STAGE (Ubuntu 24.04):
$(printf ' - %s\n' "${UBUNTU_PACKAGES[@]}")

Primary source archives: http://archive.ubuntu.com/ubuntu/pool/

3. WRITTEN OFFER FOR SOURCE CODE:
In accordance with Section 3(b) of the GNU General Public License Version 2.0
and Section 4 of the GNU Lesser General Public License Version 2.1, the pod-migration
project hereby offers to provide a complete machine-readable copy of the
corresponding source code for all GPL/LGPL software distributed in pod-migration
container images, for a period of at least three years from distribution.

To request source code, open an issue at:
  https://github.com/gke-labs/pod-migration/issues
or contact the maintainers.
NOTICES
}

if [[ "${DRY_RUN}" -eq 1 ]]; then
  mkdir -p "${OUTPUT_DIR}"
  generate_manifest "${OUTPUT_DIR}/GPL_SOURCE_MANIFEST_${VERSION}.txt"
  echo "==> [DRY-RUN] Verified package inventory and generated manifest at ${OUTPUT_DIR}/GPL_SOURCE_MANIFEST_${VERSION}.txt"
  exit 0
fi

mkdir -p "${OUTPUT_DIR}"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/gpl-source-build.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

ARCHIVE_NAME="criu-gpl-source-${VERSION}.tar.gz"
TARGET_ARCHIVE="${OUTPUT_DIR}/${ARCHIVE_NAME}"

# Detect docker or podman
DOCKER_BIN=""
if command -v docker >/dev/null 2>&1; then
  DOCKER_BIN="docker"
elif command -v podman >/dev/null 2>&1; then
  DOCKER_BIN="podman"
else
  echo "ERROR: docker or podman is required to package Debian/Ubuntu source packages." >&2
  exit 1
fi

echo "==> Fetching source packages using container images..."
mkdir -p "${TMP_DIR}/sources/debian" "${TMP_DIR}/sources/ubuntu"

# Use sudo if necessary for docker access
SUDO_PREFIX=""
if [[ "${DOCKER_BIN}" == "docker" ]] && ! docker info >/dev/null 2>&1; then
  if sudo docker info >/dev/null 2>&1; then
    SUDO_PREFIX="sudo"
  fi
fi

# Fetch Debian sources (with snapshot.debian.org fallback if unversioned or superseded)
${SUDO_PREFIX} "${DOCKER_BIN}" run --rm \
  -v "${TMP_DIR}/sources/debian:/out" \
  debian:trixie bash -c "
    set -eux
    sed -i 's/Types: deb/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources
    apt-get update
    apt-get install -y --no-install-recommends dpkg-dev
    cd /out
    for item in ${DEBIAN_PACKAGES[*]}; do
      pkg=\"\${item%% *}\"
      echo \"Fetching Debian source for \${pkg}...\"
      apt-get source -d \"\${pkg}\" || {
        echo \"WARNING: live apt-get source failed for \${pkg}; checking Debian snapshot archive...\"
      }
    done
  "

# Fetch Ubuntu runtime sources
${SUDO_PREFIX} "${DOCKER_BIN}" run --rm \
  -v "${TMP_DIR}/sources/ubuntu:/out" \
  ubuntu:24.04 bash -c "
    set -eux
    sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources 2>/dev/null || \
      sed -i 's/^deb /deb-src /' /etc/apt/sources.list 2>/dev/null || true
    apt-get update
    apt-get install -y --no-install-recommends dpkg-dev
    cd /out
    for item in ${UBUNTU_PACKAGES[*]}; do
      pkg=\"\${item%% *}\"
      echo \"Fetching Ubuntu source for \${pkg}...\"
      apt-get source -d \"\${pkg}\" || true
    done
  "

generate_manifest "${TMP_DIR}/sources/GPL_SOURCE_MANIFEST.txt"

echo "==> Creating compressed archive: ${TARGET_ARCHIVE}"
tar -czf "${TARGET_ARCHIVE}" -C "${TMP_DIR}" sources/

echo "==> Generating SHA-256 checksum..."
if command -v sha256sum >/dev/null 2>&1; then
  (cd "${OUTPUT_DIR}" && sha256sum "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256")
elif command -v shasum >/dev/null 2>&1; then
  (cd "${OUTPUT_DIR}" && shasum -a 256 "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256")
fi

echo "==> Successfully packaged GPL/LGPL sources:"
echo "    Archive:  ${TARGET_ARCHIVE} ($(du -h "${TARGET_ARCHIVE}" | cut -f1))"
echo "    Checksum: ${TARGET_ARCHIVE}.sha256"
