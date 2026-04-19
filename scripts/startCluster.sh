#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MASTER_IMAGE_DEFAULT="leo02n/jmr-master"
WORKER_IMAGE_DEFAULT="leo02n/jmr-worker"
MASTER_IMAGE_DEV="jmr-master:dev"
WORKER_IMAGE_DEV="jmr-worker:dev"

# tenere allineate in startCluster.sh e stopCluster.sh
NETWORK_NAME="jmr-network"
CLUSTER_LABEL="jmr.cluster=wordcount-demo"
MASTER_CONTAINER="jmr-master"

usage() {
  cat <<'EOF'
Usage: scripts/startCluster.sh <num-workers> <starting-worker-port> [--dev] [--skip-build] [--master-memory-gb <gb>] [--worker-memory-gb <gb>]

Example:
  scripts/startCluster.sh 3 50051
  scripts/startCluster.sh 3 50051 --dev

The script only starts the cluster infrastructure:
  - master on port <starting-worker-port>
  - workers on ports <starting-worker-port + 1> .. <starting-worker-port + num-workers>
  - dashboards on the same ports +1000
  - no submitter container
  - no word count job build or execution
  - default images: leo02n/jmr-master and leo02n/jmr-worker
  - with --dev: build/use local images instead
EOF
}

log_step() {
  printf '==> %s\n' "$1"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

open_url() {
  local url="$1"
  if command -v cmd.exe >/dev/null 2>&1; then
    cmd.exe /c start "" "$url" >/dev/null 2>&1 &
  elif command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command "Start-Process '$url'" >/dev/null 2>&1 &
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$url" >/dev/null 2>&1 &
  elif command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command "Start-Process '$url'" >/dev/null 2>&1 &
  elif command -v open >/dev/null 2>&1; then
    open "$url" >/dev/null 2>&1 &
  fi
}

docker_run_raw() {
  MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker run "$@"
}

wait_for_port() {
  local port="$1"
  local timeout_seconds="${2:-60}"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if python - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
try:
    with socket.create_connection(("127.0.0.1", port), timeout=1):
        sys.exit(0)
except OSError:
    sys.exit(1)
PY
    then
      return 0
    fi
    sleep 2
  done

  printf 'Timed out waiting for localhost:%s\n' "$port" >&2
  exit 1
}

wait_for_http() {
  local url="$1"
  local timeout_seconds="${2:-60}"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if python - "$url" <<'PY'
import sys
import urllib.request

url = sys.argv[1]
try:
    with urllib.request.urlopen(url, timeout=2) as response:
        sys.exit(0 if response.status == 200 else 1)
except Exception:
    sys.exit(1)
PY
    then
      return 0
    fi
    sleep 2
  done

  printf 'Timed out waiting for %s\n' "$url" >&2
  exit 1
}

cleanup_previous_cluster() {
  local existing_ids
  existing_ids="$(docker ps -aq --filter "label=${CLUSTER_LABEL}")"
  if [[ -n "${existing_ids}" ]]; then
    docker rm -f ${existing_ids} >/dev/null 2>&1 || true
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
      existing_ids="$(docker ps -aq --filter "label=${CLUSTER_LABEL}")"
      if [[ -z "${existing_ids}" ]]; then
        break
      fi
      sleep 1
    done
  fi

  if docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
    docker network rm "${NETWORK_NAME}" >/dev/null || true
  fi
}

build_artifacts() {
  log_step "Building MapReduce modules"
  (
    cd "${REPO_ROOT}/mapreduce-parent"
    mvn -q install -DskipTests
  )
}

build_dev_images() {
  log_step "Building local master image"
  docker build -t "${MASTER_IMAGE_DEV}" -f "${REPO_ROOT}/docker/jmr-master/Dockerfile" "${REPO_ROOT}" >/dev/null

  log_step "Building local worker image"
  docker build -t "${WORKER_IMAGE_DEV}" -f "${REPO_ROOT}/docker/jmr-worker/Dockerfile" "${REPO_ROOT}" >/dev/null
}

assert_port_range() {
  local port="$1"
  if (( port < 1 || port > 64535 )); then
    printf 'Port %s is outside the supported range 1..64535\n' "$port" >&2
    exit 1
  fi
}

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

NUM_WORKERS="$1"
STARTING_PORT="$2"
SKIP_BUILD="false"
DEV_MODE="false"
MASTER_MEMORY_GB="2"
WORKER_MEMORY_GB="2"

shift 2
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dev)
      DEV_MODE="true"
      shift
      ;;
    --master-memory-gb)
      if [[ $# -lt 2 ]]; then
        printf 'Missing value for --master-memory-gb\n' >&2
        usage
        exit 1
      fi
      MASTER_MEMORY_GB="$2"
      shift 2
      ;;
    --worker-memory-gb)
      if [[ $# -lt 2 ]]; then
        printf 'Missing value for --worker-memory-gb\n' >&2
        usage
        exit 1
      fi
      WORKER_MEMORY_GB="$2"
      shift 2
      ;;
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

if ! [[ "${NUM_WORKERS}" =~ ^[0-9]+$ ]] || (( NUM_WORKERS < 1 )); then
  printf 'num-workers must be a positive integer\n' >&2
  exit 1
fi

if ! [[ "${STARTING_PORT}" =~ ^[0-9]+$ ]]; then
  printf 'starting-port must be an integer\n' >&2
  exit 1
fi
if ! [[ "${MASTER_MEMORY_GB}" =~ ^[0-9]+$ ]] || (( MASTER_MEMORY_GB < 1 )); then
  printf 'master-memory-gb must be a positive integer\n' >&2
  exit 1
fi
if ! [[ "${WORKER_MEMORY_GB}" =~ ^[0-9]+$ ]] || (( WORKER_MEMORY_GB < 1 )); then
  printf 'worker-memory-gb must be a positive integer\n' >&2
  exit 1
fi

require_command docker
require_command python

if ! docker info >/dev/null 2>&1; then
  printf 'Docker engine is not available. Start Docker Desktop or the Docker daemon first.\n' >&2
  exit 1
fi

MASTER_PORT="${STARTING_PORT}"
MASTER_DASHBOARD_PORT=$((MASTER_PORT + 1000))

assert_port_range "${STARTING_PORT}"
assert_port_range "${MASTER_PORT}"
assert_port_range "${MASTER_DASHBOARD_PORT}"
for ((i = 0; i < NUM_WORKERS; i++)); do
  assert_port_range "$((STARTING_PORT + i + 1))"
  assert_port_range "$((STARTING_PORT + i + 1001))"
done

mkdir -p "${REPO_ROOT}/docker-output"

if [[ "${DEV_MODE}" == "true" && "${SKIP_BUILD}" != "true" ]]; then
  require_command mvn
  build_artifacts
  build_dev_images
elif [[ "${DEV_MODE}" == "true" ]]; then
  if ! docker image inspect "${MASTER_IMAGE_DEV}" >/dev/null 2>&1; then
    printf 'Docker image %s not found. Re-run with --dev without --skip-build.\n' "${MASTER_IMAGE_DEV}" >&2
    exit 1
  fi
  if ! docker image inspect "${WORKER_IMAGE_DEV}" >/dev/null 2>&1; then
    printf 'Docker image %s not found. Re-run with --dev without --skip-build.\n' "${WORKER_IMAGE_DEV}" >&2
    exit 1
  fi
fi

if [[ "${DEV_MODE}" == "true" ]]; then
  MASTER_IMAGE="${MASTER_IMAGE_DEV}"
  WORKER_IMAGE="${WORKER_IMAGE_DEV}"
else
  MASTER_IMAGE="${MASTER_IMAGE_DEFAULT}"
  WORKER_IMAGE="${WORKER_IMAGE_DEFAULT}"
  log_step "Pulling published images"
  docker pull "${MASTER_IMAGE}" >/dev/null
  docker pull "${WORKER_IMAGE}" >/dev/null
fi

log_step "Cleaning previous cluster"
cleanup_previous_cluster

log_step "Creating Docker network ${NETWORK_NAME}"
docker network create "${NETWORK_NAME}" >/dev/null

WORKER_DEFINITIONS=()
for ((i = 0; i < NUM_WORKERS; i++)); do
  worker_index=$((i + 1))
  worker_port=$((STARTING_PORT + i + 1))
  worker_dashboard_port=$((worker_port + 1000))
  worker_name="jmr-worker-${worker_index}"
  worker_storage="jmr-worker-${worker_index}-storage"

  log_step "Starting ${worker_name} on port ${worker_port}"
  docker_run_raw -d \
    --name "${worker_name}" \
    --label "${CLUSTER_LABEL}" \
    --network "${NETWORK_NAME}" \
    --memory "${WORKER_MEMORY_GB}g" \
    -p "${worker_port}:${worker_port}" \
    -p "${worker_dashboard_port}:${worker_dashboard_port}" \
    -v "${worker_storage}:/var/lib/jmr" \
    -e "JAVA_TOOL_OPTIONS=-Xms512m -Xmx$((WORKER_MEMORY_GB * 1024 - 256))m" \
    "${WORKER_IMAGE}" \
    --workerId "${worker_name}" \
    --port "${worker_port}" \
    --storageDirectory /var/lib/jmr >/dev/null

  WORKER_DEFINITIONS+=("--worker" "${worker_name}:${worker_name}:${worker_port}")
done

log_step "Starting master on port ${MASTER_PORT}"
docker_run_raw -d \
  --name "${MASTER_CONTAINER}" \
  --label "${CLUSTER_LABEL}" \
  --network "${NETWORK_NAME}" \
  --memory "${MASTER_MEMORY_GB}g" \
  -p "${MASTER_PORT}:${MASTER_PORT}" \
  -p "${MASTER_DASHBOARD_PORT}:${MASTER_DASHBOARD_PORT}" \
  -v jmr-master-storage:/var/lib/jmr \
  -e "JAVA_TOOL_OPTIONS=-Xms512m -Xmx$((MASTER_MEMORY_GB * 1024 - 256))m" \
  "${MASTER_IMAGE}" \
  --port "${MASTER_PORT}" \
  --storageDirectory /var/lib/jmr \
  "${WORKER_DEFINITIONS[@]}" >/dev/null

log_step "Waiting for master port ${MASTER_PORT}"
wait_for_port "${MASTER_PORT}" 60

MASTER_DASHBOARD_URL="http://localhost:${MASTER_DASHBOARD_PORT}"
log_step "Waiting for master dashboard ${MASTER_DASHBOARD_URL}"
wait_for_http "${MASTER_DASHBOARD_URL}" 60

log_step "Opening master dashboard"
open_url "${MASTER_DASHBOARD_URL}"

printf '\nCluster started successfully.\n'
printf 'Master gRPC:      localhost:%s\n' "${MASTER_PORT}"
printf 'Master dashboard: http://localhost:%s\n' "${MASTER_DASHBOARD_PORT}"
printf 'Master memory:    %s GB\n' "${MASTER_MEMORY_GB}"
printf 'Run word count:   scripts/scriptExecuteWc.sh %s\n' "${MASTER_PORT}"
for ((i = 0; i < NUM_WORKERS; i++)); do
  worker_port=$((STARTING_PORT + i + 1))
  printf 'Worker %d gRPC:   localhost:%s\n' "$((i + 1))" "${worker_port}"
  printf 'Worker %d UI:     http://localhost:%s\n' "$((i + 1))" "$((worker_port + 1000))"
done
printf 'Worker memory:    %s GB each\n' "${WORKER_MEMORY_GB}"
