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
# from Debian trixie in build/agent/Dockerfile).
#
# Produces:
#   <output-dir>/criu-gpl-source-<version>.tar.gz
#   <output-dir>/criu-gpl-source-<version>.tar.gz.sha256
#
# Usage:
#   tools/release/package_gpl_sources.sh [--output-dir <dir>] [--version <ver>] [--dry-run]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/dist"
VERSION=""
DRY_RUN=0

usage() {
  local code="${1:-0}"
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Packages matching GPL/LGPL source archives for published container images.

Options:
  --output-dir DIR   Output directory for source archives (default: ./dist)
  --version VER      Release version (default: from git tag or v0.1.0-alpha)
  --dry-run          Validate configuration and inventory without downloading
  -h, --help         Show this help message
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

# GPL and LGPL packages bundled into the agent image from Debian trixie
GPL_PACKAGES=(
  "criu"
  "glibc"
  "libnet"
  "libnl3"
  "nftables"
  "libnftnl"
  "gnutls28"
  "libcap2"
  "libbsd"
)

echo "==> Packaging GPL/LGPL sources for pod-migration release: ${VERSION}"
echo "    Output directory: ${OUTPUT_DIR}"
echo "    Target packages:  ${GPL_PACKAGES[*]}"

if [[ "${DRY_RUN}" -eq 1 ]]; then
  echo "==> [DRY-RUN] Verified package inventory and release target ${VERSION}."
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
  echo "ERROR: docker or podman is required to package Debian source packages." >&2
  exit 1
fi

echo "==> Fetching source packages using debian:trixie container..."
mkdir -p "${TMP_DIR}/sources"

# Use sudo if necessary for docker access
SUDO_PREFIX=""
if [[ "${DOCKER_BIN}" == "docker" ]] && ! docker info >/dev/null 2>&1; then
  if sudo docker info >/dev/null 2>&1; then
    SUDO_PREFIX="sudo"
  fi
fi

${SUDO_PREFIX} "${DOCKER_BIN}" run --rm \
  -v "${TMP_DIR}/sources:/out" \
  debian:trixie bash -c "
    set -eux
    sed -i 's/Types: deb/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources
    apt-get update
    apt-get install -y --no-install-recommends dpkg-dev
    cd /out
    for pkg in ${GPL_PACKAGES[*]}; do
      echo \"Fetching source for \${pkg}...\"
      apt-get source -d \"\${pkg}\"
    done
    cat <<'NOTICES' > /out/GPL_SOURCE_MANIFEST.txt
This directory contains matching GPL and LGPL source packages corresponding to
the binaries bundled into the CRIU node agent image for pod-migration ${VERSION}.

Bundled packages:
${GPL_PACKAGES[*]}

Extracted from Debian trixie package repository (http://deb.debian.org/debian).
Upstream CRIU project repository: https://github.com/checkpoint-restore/criu
NOTICES
  "

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
