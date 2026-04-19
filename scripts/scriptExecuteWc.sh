#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAME_DEFAULT="leo02n/jmr-wc"
IMAGE_NAME_DEV="jmr-wc:dev"
NETWORK_NAME="jmr-network"
RESULT_DIR="${REPO_ROOT}/docker-output"
DATA_DIR_DEFAULT="${REPO_ROOT}/data/serialized_data"

usage() {
  cat <<'EOF'
Usage: scripts/scriptExecuteWc.sh <master-port> [--data-dir <path>] [--result-file <path>] [--memory-gb <gb>] [--dev] [--skip-build]

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

cleanup_data_volume() {
  if [[ -n "${DATA_VOLUME_NAME:-}" ]]; then
    docker volume rm -f "${DATA_VOLUME_NAME}" >/dev/null 2>&1 || true
  fi
}

stage_data_into_volume() {
  local source_dir="$1"
  DATA_VOLUME_NAME="jmr-wc-data-${RUN_ID}"
  trap cleanup_data_volume EXIT

  log_step "Copying input data into Docker volume ${DATA_VOLUME_NAME}"
  docker volume create "${DATA_VOLUME_NAME}" >/dev/null
  docker pull alpine:3.20 >/dev/null
  docker_run_raw --rm \
    -v "${DATA_VOLUME_NAME}:/data-volume" \
    -v "${source_dir}:/host-data:ro" \
    alpine:3.20 \
    sh -lc "cp -a /host-data/. /data-volume/"
}

build_submitter_artifacts() {
  log_step "Installing shared MapReduce modules"
  (
    cd "${REPO_ROOT}/mapreduce-parent"
    mvn -q install -DskipTests
  )

  log_step "Building word count job"
  (
    cd "${REPO_ROOT}/my-mapreduce-job"
    mvn -q package -DskipTests
  )

  log_step "Building local word count image"
  docker build -t "${IMAGE_NAME_DEV}" -f "${REPO_ROOT}/docker/jmr-wc/Dockerfile" "${REPO_ROOT}" >/dev/null
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

if [[ $# -lt 1 ]]; then
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
DEV_MODE="false"
CUSTOM_RESULT_FILE=""
DATA_DIR="${DATA_DIR_DEFAULT}"
MEMORY_GB="2"

shift 1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev)
      DEV_MODE="true"
      shift
      ;;
    --skip-build)
      SKIP_BUILD="true"
      shift
      ;;
    --data-dir)
      if [[ $# -lt 2 ]]; then
        printf 'Missing value for --data-dir\n' >&2
        usage
        exit 1
      fi
      DATA_DIR="$2"
      shift 2
      ;;
    --memory-gb)
      if [[ $# -lt 2 ]]; then
        printf 'Missing value for --memory-gb\n' >&2
        usage
        exit 1
      fi
      MEMORY_GB="$2"
      shift 2
      ;;
    --result-file)
      if [[ $# -lt 2 ]]; then
        printf 'Missing value for --result-file\n' >&2
        usage
        exit 1
      fi
      CUSTOM_RESULT_FILE="$2"
      shift 2
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! docker info >/dev/null 2>&1; then
  printf 'Docker engine is not available. Start Docker Desktop or the Docker daemon first.\n' >&2
  exit 1
fi
if ! [[ "${MEMORY_GB}" =~ ^[0-9]+$ ]] || (( MEMORY_GB < 1 )); then
  printf 'memory-gb must be a positive integer\n' >&2
  exit 1
fi

if [[ "${DATA_DIR}" != /* ]]; then
  DATA_DIR="${REPO_ROOT}/${DATA_DIR}"
fi
if [[ ! -d "${DATA_DIR}" ]]; then
  printf 'Data directory not found: %s\n' "${DATA_DIR}" >&2
  exit 1
fi

if [[ "${DEV_MODE}" == "true" && "${SKIP_BUILD}" != "true" ]]; then
  build_submitter_artifacts
elif [[ "${DEV_MODE}" == "true" ]]; then
  if ! docker image inspect "${IMAGE_NAME_DEV}" >/dev/null 2>&1; then
    printf 'Docker image %s not found. Re-run with --dev without --skip-build.\n' "${IMAGE_NAME_DEV}" >&2
    exit 1
  fi
else
  docker pull "${IMAGE_NAME_DEFAULT}" >/dev/null
fi

IMAGE_NAME="${IMAGE_NAME_DEFAULT}"
if [[ "${DEV_MODE}" == "true" ]]; then
  IMAGE_NAME="${IMAGE_NAME_DEV}"
fi

if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  printf 'Docker network %s not found. Start the cluster first.\n' "${NETWORK_NAME}" >&2
  exit 1
fi

RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
SUBMITTER_CONTAINER="jmr-submitter-wc-${RUN_ID}"
DATA_VOLUME_NAME=""
if [[ -n "${CUSTOM_RESULT_FILE}" ]]; then
  if [[ "${CUSTOM_RESULT_FILE}" = /* ]]; then
    RESULT_FILE="${CUSTOM_RESULT_FILE}"
  else
    RESULT_FILE="${REPO_ROOT}/${CUSTOM_RESULT_FILE}"
  fi
else
  RESULT_FILE="${RESULT_DIR}/wordcount-result-${RUN_ID}.csv"
fi
RESULT_FILE_NAME="$(basename "${RESULT_FILE}")"
RESULT_FILE_DIR="$(dirname "${RESULT_FILE}")"

mkdir -p "${RESULT_FILE_DIR}"
RESULT_FILE_DIR="$(cd "${RESULT_FILE_DIR}" && pwd)"
RESULT_FILE="${RESULT_FILE_DIR}/${RESULT_FILE_NAME}"
rm -f "${RESULT_FILE}"
stage_data_into_volume "${DATA_DIR}"

log_step "Submitting word count job to master on port ${MASTER_PORT} with run id ${RUN_ID}"
docker_run_raw --rm \
  --name "${SUBMITTER_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --memory "${MEMORY_GB}g" \
  -v "${DATA_VOLUME_NAME}:/data:ro" \
  -v "${RESULT_FILE_DIR}:/outputs" \
  -e "JAVA_TOOL_OPTIONS=-Xms1g -Xmx$((MEMORY_GB * 1024 - 256))m" \
  "${IMAGE_NAME}" \
  /data \
  /opt/jmr/my-mapreduce-job.jar \
  jmr-master \
  "${MASTER_PORT}" \
  "${SUBMITTER_CONTAINER}" \
  "/outputs/${RESULT_FILE_NAME}"

if [[ ! -f "${RESULT_FILE}" ]]; then
  printf 'Word count completed but result file was not produced at %s\n' "${RESULT_FILE}" >&2
  exit 1
fi

printf '\nWord count completed successfully.\n'
printf 'Submitter container: %s\n' "${SUBMITTER_CONTAINER}"
printf 'Submitter memory: %s GB\n' "${MEMORY_GB}"
printf 'CSV result: %s\n' "${RESULT_FILE}"
cleanup_data_volume
trap - EXIT
