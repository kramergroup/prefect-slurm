#!/usr/bin/env bash
set -euo pipefail

REPO="materialsfoundry.io/iscc/prefect-slurm-worker"
TAG="$(date +%Y-%m-%d)"
IMAGE="${REPO}:${TAG}"

echo "Building ${IMAGE}"
docker build --platform linux/amd64 -t "${IMAGE}" "$(dirname "$0")/.."

echo "Pushing ${IMAGE}"
docker push "${IMAGE}"

echo "Done: ${IMAGE}"
