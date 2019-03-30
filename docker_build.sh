#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly CONTAINER_ID="${1}"
readonly MYPATH=$(realpath "${0}")
readonly AGENT_PATH=$(dirname "${MYPATH}")

set -x
docker exec -it "${CONTAINER_ID}" rm -rf /tmp/agent
docker cp "${AGENT_PATH}" "${CONTAINER_ID}:/tmp"
readonly CMD="python3.7 -m agent.make_arch --standalone --python-version 3.7 /tmp/agent/agent /tmp/agent.sh"
echo "cd /tmp/agent ; ${CMD}" | docker exec -i "${CONTAINER_ID}" bash
docker cp "${CONTAINER_ID}:/tmp/agent.sh" "${AGENT_PATH}/arch/agent.sh"
