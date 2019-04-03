#!/usr/bin/env bash
set -x
set -o errexit
set -o pipefail
set -o nounset

readonly AGENT_PATH=$(dirname $(realpath "${0}"))
readonly CONTAINER_ID="${1}"

readonly INSTALL_TO="/opt/mirantis/agent"
readonly ARCH_PATH="${AGENT_PATH}/arch/agent.sh"
readonly DEPLOY_SH="${INSTALL_TO}/deploy.sh"


function build {
    local -r container="${1}"
    local -r agent_path="${2}"
    local -r arch_path="${3}"

    local -r copy_to="/tmp/agent"
    local -r in_container_arch="/tmp/agent.sh"

    docker exec -it "${container}" rm -rf "${copy_to}"
    docker cp "${agent_path}" "${container}:${copy_to}"

    local cmd="python3.7 -m agent.make_arch --config ${copy_to}/arch_config.txt "
    cmd="${cmd} --standalone ${copy_to} ${in_container_arch}"

    echo "cd ${copy_to}; ${cmd}" | docker exec -i "${container}" bash
    docker cp "${container}:${in_container_arch}" "${arch_path}"
}


function redeploy {
    local -r inventory="${1}"

    # install locally
    rm -rf "${INSTALL_TO}"
    bash "${ARCH_PATH}" "${INSTALL_TO}"

    # redeploy
    bash "${DEPLOY_SH}" remove "${inventory}"
    bash "${DEPLOY_SH}" deploy "${inventory}"

    # relink
    #rm -rf "${INSTALL_TO}/agent"
    #ln -s "${AGENT_PATH}" "${INSTALL_TO}/agent"

    # show status
    sleep 1
    bash "${DEPLOY_SH}" status --certs-folder "${INSTALL_TO}/agent_client_keys" "${inventory}"
}

build "${CONTAINER_ID}" "${AGENT_PATH}" "${ARCH_PATH}"
#redeploy "${2}"
