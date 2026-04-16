#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="jmr-wc-submitter:local"
NETWORK_NAME="jmr-network"
SUBMITTER_CONTAINER="jmr-submitter-wc"
RESULT_DIR="${REPO_ROOT}/docker-output"
RESULT_FILE="${RESULT_DIR}/wordcount-result.ser"

usage() {
  cat <<'EOF'
Usage: scripts/scriptExecuteWc.sh <master-port> [--skip-build]

Example:
  scripts/scriptExecuteWc.sh 50051
EOF
}

log_step() {
  printf '==> %s\n' "$1"
}

docker_run_raw() {
  MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker run "$@"
}

build_submitter_artifacts() {
  log_step "Building word count job"
  (
    cd "${REPO_ROOT}/my-mapreduce-job"
    mvn -q package -DskipTests
  )

  log_step "Building submitter Docker image"
  docker build -t "${IMAGE_NAME}" -f "${REPO_ROOT}/docker/jmr-submitter/Dockerfile" "${REPO_ROOT}" >/dev/null
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

MASTER_PORT="$1"
if ! [[ "${MASTER_PORT}" =~ ^[0-9]+$ ]]; then
  printf 'master-port must be an integer\n' >&2
  exit 1
fi

require_command docker

SKIP_BUILD="false"
if [[ $# -eq 2 ]]; then
  if [[ "$2" == "--skip-build" ]]; then
    SKIP_BUILD="true"
  else
    printf 'Unknown argument: %s\n' "$2" >&2
    usage
    exit 1
  fi
fi

if ! docker info >/dev/null 2>&1; then
  printf 'Docker engine is not available. Start Docker Desktop or the Docker daemon first.\n' >&2
  exit 1
fi

if [[ "${SKIP_BUILD}" != "true" ]]; then
  build_submitter_artifacts
elif ! docker image inspect "${IMAGE_NAME}" >/dev/null 2>&1; then
  printf 'Docker image %s not found. Re-run without --skip-build.\n' "${IMAGE_NAME}" >&2
  exit 1
fi

if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  printf 'Docker network %s not found. Start the cluster first.\n' "${NETWORK_NAME}" >&2
  exit 1
fi

mkdir -p "${RESULT_DIR}"
rm -f "${RESULT_FILE}"
docker rm -f "${SUBMITTER_CONTAINER}" >/dev/null 2>&1 || true

log_step "Submitting word count job to master on port ${MASTER_PORT}"
docker_run_raw --rm \
  --name "${SUBMITTER_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  -v "${RESULT_DIR}:/outputs" \
  "${IMAGE_NAME}" \
  -Dlog4j.configurationFile=file:/opt/jmr/config/submitter-log4j2.xml \
  -Dlog4j2.statusLoggerLevel=WARN \
  -cp /opt/jmr/my-mapreduce-job.jar \
  com.example.MyWcJob \
  /opt/jmr/data/serialized_data \
  /opt/jmr/my-mapreduce-job.jar \
  jmr-master \
  "${MASTER_PORT}" \
  "${SUBMITTER_CONTAINER}" \
  /outputs/wordcount-result.ser

if [[ ! -f "${RESULT_FILE}" ]]; then
  printf 'Word count completed but result file was not produced at %s\n' "${RESULT_FILE}" >&2
  exit 1
fi

printf '\nWord count completed successfully.\n'
printf 'Serialized result: %s\n' "${RESULT_FILE}"
