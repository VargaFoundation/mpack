#!/usr/bin/env bash
#
# Build script for the Varga Ambari Management Pack (Kirka + Tarn).
#
# Produces build/target/varga-mpack-<version>.tar.gz, ready to be installed on Ambari with:
#   ambari-server install-mpack --mpack=varga-mpack-<version>.tar.gz --verbose
#   ambari-server restart
#
# Source layout (relative to this script):
#   ../../kirka/    : Kirka project (the JAR to embed will be looked up in target/)
#   ../../tarn/     : Tarn project    (same convention)
#   ../mpack-varga/ : the mpack source tree we copy from
#
# Set REBUILD=1 (default) to invoke `mvn package` on each project; REBUILD=0 reuses an
# existing JAR.

set -euo pipefail

VERSION="1.0.0.0"
MPACK_NAME="varga-mpack"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${SCRIPT_DIR}/../mpack-varga"
OUTPUT_DIR="${SCRIPT_DIR}/target"
PROJECT_ROOT="${SCRIPT_DIR}/../.."
REBUILD="${REBUILD:-1}"

KIRKA_DIR="${PROJECT_ROOT}/kirka"
TARN_DIR="${PROJECT_ROOT}/tarn"
KIRKA_FILES="${SOURCE_DIR}/common-services/KIRKA/1.0.0/package/files"
TARN_FILES="${SOURCE_DIR}/common-services/TARN/1.0.0/package/files"

log() { printf '\033[1;32m[mpack]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[mpack]\033[0m %s\n' "$*" >&2; }

ensure_jar() {
  local name="$1" project_dir="$2" target_jar
  if [[ ! -d "${project_dir}" ]]; then
    err "Missing source tree: ${project_dir}"
    err "Expected ${name} to be checked out next to the mpack repo."
    exit 1
  fi

  if [[ "${REBUILD}" == "1" ]]; then
    log "Building ${name} (${project_dir})..."
    (cd "${project_dir}" && mvn -B -q -DskipTests package)
  fi

  # shellcheck disable=SC2012
  target_jar=$(ls -1 "${project_dir}/target/${name}-"*.jar 2>/dev/null | grep -v 'sources.jar\|javadoc.jar' | head -n1 || true)
  if [[ -z "${target_jar}" ]]; then
    err "Could not locate ${name}-*.jar under ${project_dir}/target/. Re-run with REBUILD=1."
    exit 1
  fi
  printf '%s' "${target_jar}"
}

log "Building ${MPACK_NAME} ${VERSION}"

# 1. Validate the mpack source layout
required=(
  "${SOURCE_DIR}/mpack.json"
  "${SOURCE_DIR}/common-services/KIRKA/1.0.0/metainfo.xml"
  "${SOURCE_DIR}/common-services/KIRKA/1.0.0/configuration/kirka-site.xml"
  "${SOURCE_DIR}/common-services/KIRKA/1.0.0/package/scripts/kirka_server.py"
  "${SOURCE_DIR}/common-services/KIRKA/1.0.0/package/scripts/params.py"
  "${SOURCE_DIR}/common-services/KIRKA/1.0.0/package/templates/application.properties.j2"
)
for f in "${required[@]}"; do
  if [[ ! -f "${f}" ]]; then
    err "Missing required file: ${f}"
    exit 1
  fi
done

# 2. Build & embed the JARs
KIRKA_JAR="$(ensure_jar kirka "${KIRKA_DIR}")"
TARN_JAR="$(ensure_jar tarn "${TARN_DIR}")"

log "Embedding kirka.jar  <- ${KIRKA_JAR}"
mkdir -p "${KIRKA_FILES}" "${TARN_FILES}"
rm -f "${KIRKA_FILES}/kirka.jar" "${TARN_FILES}/tarn.jar"
cp "${KIRKA_JAR}" "${KIRKA_FILES}/kirka.jar"
log "Embedding tarn.jar   <- ${TARN_JAR}"
cp "${TARN_JAR}"  "${TARN_FILES}/tarn.jar"

# 3. Stage and tar.gz with the mandatory top-level directory
mkdir -p "${OUTPUT_DIR}"
ARCHIVE="${OUTPUT_DIR}/${MPACK_NAME}-${VERSION}.tar.gz"
log "Creating ${ARCHIVE}"
tar -C "$(dirname "${SOURCE_DIR}")" \
    --transform "s|^mpack-varga|${MPACK_NAME}-${VERSION}|" \
    -czf "${ARCHIVE}" mpack-varga

# 4. Sanity check the archive — read the full listing so `set -o pipefail` doesn't kill us
#    on a SIGPIPE when `awk`/`head` exits early.
log "Verifying archive..."
first_entry=$(tar -tzf "${ARCHIVE}" 2>/dev/null | head -n1 ; true)
case "${first_entry}" in
  ${MPACK_NAME}-${VERSION}/*) : ;;
  *)
    err "Archive root directory mismatch (got '${first_entry}') — Ambari install will fail."
    exit 1
    ;;
esac
size=$(du -h "${ARCHIVE}" | cut -f1)
log "Done. Archive: ${ARCHIVE} (${size})"
log "Install with:"
log "  ambari-server install-mpack --mpack=${ARCHIVE} --verbose"
log "  ambari-server restart"
