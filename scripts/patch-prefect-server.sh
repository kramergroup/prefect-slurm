#!/usr/bin/env bash
# patch-prefect-server.sh — Download a specific Prefect version, apply the
# X-Remote-User identity patch, build a Docker image, and optionally push it.
#
# Usage:
#   ./scripts/patch-prefect-server.sh \
#       --version 2.20.25 \
#       --registry registry.example.org/iscc \
#       --image-name prefect-server \
#       [--push]
#
# The patch file scripts/prefect-server-x-remote-user.patch must exist.
# It is version-specific: if the target Prefect version differs from the one
# the patch was generated against, 'patch' will report a failure and the
# script will exit non-zero.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_FILE="${SCRIPT_DIR}/prefect-server-x-remote-user.patch"

VERSION=""
REGISTRY=""
IMAGE_NAME="prefect-server"
PUSH=false

usage() {
    echo "Usage: $0 --version VERSION --registry REGISTRY [--image-name NAME] [--push]"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)    VERSION="$2";    shift 2 ;;
        --registry)   REGISTRY="$2";  shift 2 ;;
        --image-name) IMAGE_NAME="$2"; shift 2 ;;
        --push)       PUSH=true;       shift   ;;
        *) usage ;;
    esac
done

[[ -z "$VERSION"  ]] && { echo "ERROR: --version is required";  usage; }
[[ -z "$REGISTRY" ]] && { echo "ERROR: --registry is required"; usage; }
[[ -f "$PATCH_FILE" ]] || { echo "ERROR: patch file not found: $PATCH_FILE"; exit 1; }

BASE_IMAGE_VERSION="${VERSION}-python${PYTHON_VERSION:-3.14}-kubernetes"

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker is not installed or not in PATH"; exit 1; }

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "==> Downloading prefect==${VERSION} source..."
pip download "prefect==${VERSION}" --no-deps --no-binary prefect -d "$WORKDIR" -q
TARBALLS=( "$WORKDIR"/prefect-*.tar.gz )
[[ -f "${TARBALLS[0]}" ]] || { echo "ERROR: source tarball not found in $WORKDIR"; exit 1; }
TARBALL="${TARBALLS[0]}"
tar -xzf "$TARBALL" -C "$WORKDIR"
SRC_DIRS=( "$WORKDIR"/prefect-*/ )
SRC_DIR="${SRC_DIRS[0]}"

DEPS_FILE="${SRC_DIR}src/prefect/server/api/dependencies.py"
[[ -f "$DEPS_FILE" ]] || { echo "ERROR: dependencies.py not found at $DEPS_FILE"; exit 1; }

echo "==> Applying patch..."
patch --forward "$DEPS_FILE" < "$PATCH_FILE" \
    || { echo "ERROR: patch failed — is the patch generated against Prefect version ${VERSION}?"; exit 1; }

FULL_TAG="${REGISTRY}/${IMAGE_NAME}:${VERSION}-patched"
LATEST_TAG="${REGISTRY}/${IMAGE_NAME}:latest-patched"

echo "==> Writing Dockerfile..."
cat > "$WORKDIR/Dockerfile" <<DOCKERFILE
FROM prefecthq/prefect:${BASE_IMAGE_VERSION}

# Replace dependencies.py with the patched version
COPY dependencies.py /tmp/patched_dependencies.py
RUN SITE=\$(python -c "import prefect, os; print(os.path.dirname(prefect.__file__))") && \\
    cp /tmp/patched_dependencies.py "\${SITE}/server/api/dependencies.py"

# Runtime-configurable — override in your container environment
ENV PREFECT_SERVER_USER_HEADER=x-remote-user
ENV PREFECT_SERVER_USER_HEADER_REGEX=
DOCKERFILE

cp "$DEPS_FILE" "$WORKDIR/dependencies.py"

echo "==> Building image ${FULL_TAG}..."
docker build \
    --tag "$FULL_TAG" \
    --tag "$LATEST_TAG" \
    --platform linux/amd64 \
    "$WORKDIR"

echo "==> Built: ${FULL_TAG}"
echo "==> Tagged: ${LATEST_TAG}"

if [[ "$PUSH" == true ]]; then
    echo "==> Pushing ${FULL_TAG}..."
    docker push "$FULL_TAG"
    echo "==> Pushing ${LATEST_TAG}..."
    docker push "$LATEST_TAG"
    echo "==> Push complete."
fi
