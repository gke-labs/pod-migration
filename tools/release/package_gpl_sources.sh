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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/dist"
VERSION=""
DRY_RUN=0
PACKAGE_VERSIONS_FILE=""
RUNTIME_VERSIONS_FILE=""

INTERNAL_FETCH_DISTRO=""
INTERNAL_MANIFEST=""
INTERNAL_OUT_DIR=""

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

fetch_debian_snapshot() {
  local pkg="$1"
  local ver="$2"
  local out_dir="$3"
  echo "Attempting Debian snapshot download for ${pkg} ${ver}..."

  python3 -c '
import sys, json, urllib.request, os

pkg = sys.argv[1]
ver = sys.argv[2]
out_dir = sys.argv[3]

versions_to_try = [ver]
if ":" in ver:
    versions_to_try.append(ver.split(":", 1)[1])

fileinfo = {}
for v in versions_to_try:
    url = f"https://snapshot.debian.org/mr/package/{pkg}/{v}/srcfiles?fileinfo=1"
    req = urllib.request.Request(url, headers={"User-Agent": "pod-migration-gpl-source-packager"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
            fi = data.get("fileinfo", {})
            if fi:
                fileinfo = fi
                break
    except Exception:
        continue

if not fileinfo:
    print(f"ERROR: no files found on snapshot.debian.org for {pkg} {ver}", file=sys.stderr)
    sys.exit(1)

downloaded = 0
for h, entries in fileinfo.items():
    if not entries:
        continue
    name = entries[0]["name"]
    file_url = f"https://snapshot.debian.org/file/{h}"
    dest = os.path.join(out_dir, name)
    print(f"Downloading {name} from {file_url}...")
    req_file = urllib.request.Request(file_url, headers={"User-Agent": "pod-migration-gpl-source-packager"})
    with urllib.request.urlopen(req_file, timeout=60) as f_in, open(dest, "wb") as f_out:
        f_out.write(f_in.read())
    downloaded += 1

if downloaded == 0:
    sys.exit(1)
print(f"Successfully downloaded {downloaded} files for {pkg} {ver} from snapshot.debian.org")
' "${pkg}" "${ver}" "${out_dir}"
}

fetch_ubuntu_snapshot() {
  local pkg="$1"
  local ver="$2"
  local out_dir="$3"
  echo "Attempting Ubuntu snapshot/Launchpad download for ${pkg} ${ver}..."

  python3 -c '
import sys, json, urllib.request, os

pkg = sys.argv[1]
ver = sys.argv[2]
out_dir = sys.argv[3]

url = f"https://api.launchpad.net/1.0/ubuntu/+archive/primary?ws.op=getPublishedSources&source_name={pkg}&version={ver}"
req = urllib.request.Request(url, headers={"User-Agent": "pod-migration-gpl-source-packager"})

entries = []
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)
        entries = data.get("entries", [])
except Exception as e:
    print(f"Launchpad API request failed for {pkg} {ver}: {e}", file=sys.stderr)

file_urls = []
for entry in entries:
    pub_url = entry.get("self_link")
    if not pub_url:
        continue
    req_files = urllib.request.Request(f"{pub_url}?ws.op=sourceFileUrls", headers={"User-Agent": "pod-migration-gpl-source-packager"})
    try:
        with urllib.request.urlopen(req_files, timeout=30) as f_resp:
            urls = json.load(f_resp)
            if urls:
                file_urls = urls
                break
    except Exception:
        continue

if not file_urls:
    print(f"ERROR: no source files found on Launchpad for {pkg} {ver}", file=sys.stderr)
    sys.exit(1)

downloaded = 0
for u in file_urls:
    name = u.rstrip("/").split("/")[-1]
    dest = os.path.join(out_dir, name)
    print(f"Downloading {name} from {u}...")
    req_dl = urllib.request.Request(u, headers={"User-Agent": "pod-migration-gpl-source-packager"})
    with urllib.request.urlopen(req_dl, timeout=60) as f_in, open(dest, "wb") as f_out:
        f_out.write(f_in.read())
    downloaded += 1

if downloaded == 0:
    sys.exit(1)
print(f"Successfully downloaded {downloaded} files for {pkg} {ver} from Launchpad")
' "${pkg}" "${ver}" "${out_dir}"
}

fetch_manifest_sources() {
  local distro="$1"
  local manifest="$2"
  local out_dir="$3"
  local missing=()

  mkdir -p "${out_dir}"
  cd "${out_dir}"

  while read -r pkg ver extra || [[ -n "${pkg}" ]]; do
    [[ -z "${pkg}" || "${pkg}" =~ ^# ]] && continue
    local target="${pkg}"
    if [[ -n "${ver}" ]]; then
      target="${pkg}=${ver}"
    fi

    echo "Fetching ${distro} source for ${target}..."
    if apt-get source -d "${target}"; then
      echo "Successfully fetched ${target} via apt-get"
    else
      echo "WARNING: apt-get source -d ${target} failed; trying snapshot fallback..."
      local fetched=0
      if [[ -n "${ver}" ]]; then
        if [[ "${distro}" == "debian" ]]; then
          if fetch_debian_snapshot "${pkg}" "${ver}" "${out_dir}"; then
            fetched=1
          fi
        elif [[ "${distro}" == "ubuntu" ]]; then
          if fetch_ubuntu_snapshot "${pkg}" "${ver}" "${out_dir}"; then
            fetched=1
          fi
        fi
      fi
      if [[ "${fetched}" -eq 0 ]]; then
        echo "ERROR: failed to obtain corresponding source for ${target}" >&2
        missing+=("${target}")
      fi
    fi
  done < "${manifest}"

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: Failed to obtain corresponding source for ${#missing[@]} package(s):" >&2
    for m in "${missing[@]}"; do
      echo "  - ${m}" >&2
    done
    return 1
  fi
  return 0
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
    --internal-fetch-distro)
      INTERNAL_FETCH_DISTRO="$2"
      shift 2
      ;;
    --manifest)
      INTERNAL_MANIFEST="$2"
      shift 2
      ;;
    --out-dir)
      INTERNAL_OUT_DIR="$2"
      shift 2
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

if [[ -n "${INTERNAL_FETCH_DISTRO}" ]]; then
  if [[ -z "${INTERNAL_MANIFEST}" || -z "${INTERNAL_OUT_DIR}" ]]; then
    echo "ERROR: --manifest and --out-dir required with --internal-fetch-distro" >&2
    exit 1
  fi
  fetch_manifest_sources "${INTERNAL_FETCH_DISTRO}" "${INTERNAL_MANIFEST}" "${INTERNAL_OUT_DIR}"
  exit $?
fi

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
printf '%s\n' "${DEBIAN_PACKAGES[@]}" > "${TMP_DIR}/sources/debian/packages.txt"

${SUDO_PREFIX} "${DOCKER_BIN}" run --rm \
  -v "${TMP_DIR}/sources/debian:/out" \
  -v "${SCRIPT_DIR}:/scripts:ro" \
  debian:trixie bash -c "
    set -eux
    sed -i 's/Types: deb/Types: deb deb-src/' /etc/apt/sources.list.d/debian.sources
    apt-get update
    apt-get install -y --no-install-recommends dpkg-dev curl ca-certificates python3
    /scripts/package_gpl_sources.sh --internal-fetch-distro debian --manifest /out/packages.txt --out-dir /out
  "
rm -f "${TMP_DIR}/sources/debian/packages.txt"

# Fetch Ubuntu runtime sources
printf '%s\n' "${UBUNTU_PACKAGES[@]}" > "${TMP_DIR}/sources/ubuntu/packages.txt"

${SUDO_PREFIX} "${DOCKER_BIN}" run --rm \
  -v "${TMP_DIR}/sources/ubuntu:/out" \
  -v "${SCRIPT_DIR}:/scripts:ro" \
  ubuntu:24.04 bash -c "
    set -eux
    sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources 2>/dev/null || \
      sed -i 's/^deb /deb-src /' /etc/apt/sources.list 2>/dev/null || true
    apt-get update
    apt-get install -y --no-install-recommends dpkg-dev curl ca-certificates python3
    /scripts/package_gpl_sources.sh --internal-fetch-distro ubuntu --manifest /out/packages.txt --out-dir /out
  "
rm -f "${TMP_DIR}/sources/ubuntu/packages.txt"

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
