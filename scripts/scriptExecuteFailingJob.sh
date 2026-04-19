#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME="jmr-wc-submitter:local"
NETWORK_NAME="jmr-network"

usage() {
  cat <<'EOF'
Usage: scripts/scriptExecuteFailingJob.sh <master-port> <failure-phase: map|reduce> [--skip-build]

Example:
  scripts/scriptExecuteFailingJob.sh 50051 map
EOF
}

log_step() {
  printf '==> %s\n' "$1"
}

docker_run_raw() {
  MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker run "$@"
}

build_submitter_artifacts() {
  log_step "Building failing job jar"
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

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

MASTER_PORT="$1"
FAILURE_PHASE="$2"
SKIP_BUILD="false"

shift 2
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      SKIP_BUILD="true"
      shift
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! [[ "${MASTER_PORT}" =~ ^[0-9]+$ ]]; then
  printf 'master-port must be an integer\n' >&2
  exit 1
fi

case "${FAILURE_PHASE}" in
  map|reduce)
    ;;
  *)
    printf 'failure-phase must be map or reduce\n' >&2
    exit 1
    ;;
esac

require_command docker

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

RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
SUBMITTER_CONTAINER="jmr-submitter-failing-${FAILURE_PHASE}-${RUN_ID}"

log_step "Submitting failing job (${FAILURE_PHASE}) to master on port ${MASTER_PORT}"
docker_run_raw --rm \
  --name "${SUBMITTER_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  "${IMAGE_NAME}" \
  -Dlog4j.configurationFile=file:/opt/jmr/config/submitter-log4j2.xml \
  -Dlog4j2.statusLoggerLevel=WARN \
  -cp /opt/jmr/my-mapreduce-job.jar \
  com.example.MyFailingJob \
  /opt/jmr/data/serialized_data \
  /opt/jmr/my-mapreduce-job.jar \
  jmr-master \
  "${MASTER_PORT}" \
  "${FAILURE_PHASE}" \
  "${SUBMITTER_CONTAINER}"

printf '\nFailing job completed with expected FAILED status.\n'
