#!/usr/bin/env bash
set -euo pipefail

NETWORK_NAME="jmr-network"
CLUSTER_LABEL="jmr.cluster=wordcount-demo"

usage() {
  cat <<'EOF'
Usage: scripts/stopCluster.sh

Stops the dynamic Docker cluster started by scripts/startCluster.sh.

Options:
  --remove-volumes   Also remove the named Docker volumes used by master/workers.
EOF
}

log_step() {
  printf '==> %s\n' "$1"
}

if ! command -v docker >/dev/null 2>&1; then
  printf 'Missing required command: docker\n' >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  printf 'Docker engine is not available. Start Docker Desktop or the Docker daemon first.\n' >&2
  exit 1
fi

container_ids="$(docker ps -aq --filter "label=${CLUSTER_LABEL}")"
if [[ -n "${container_ids}" ]]; then
  log_step "Stopping and removing cluster containers"
  docker rm -f ${container_ids} >/dev/null
else
  log_step "No cluster containers found"
fi

if docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  log_step "Removing Docker network ${NETWORK_NAME}"
  docker network rm "${NETWORK_NAME}" >/dev/null
else
  log_step "Docker network ${NETWORK_NAME} not found"
fi

volume_names="$(docker volume ls -q --filter "name=^jmr-(master|worker-[0-9]+)-storage$")"
if [[ -n "${volume_names}" ]]; then
  log_step "Removing cluster volumes"
  docker volume rm ${volume_names} >/dev/null
else
  log_step "No cluster volumes found"
fi

printf '\nCluster stopped successfully.\n'
