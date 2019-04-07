#!/usr/bin/env bash
set -x
set -o errexit
set -o pipefail
set -o nounset

readonly AGENT_PATH=$(dirname $(realpath "${0}"))
readonly CONTAINER_ID="${1}"
readonly INSTALL_TO="${2}"
readonly INVENTORY="${3}"

readonly ARCH_PATH="${AGENT_PATH}/arch/agent.sh"


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
    local -r install_path="${1}"
    local -r inventory="${2}"
    local -r arch_path="${3}"

    local -r deploy_cmd="${install_path}/run.sh ssh_deploy"

    # install locally
    rm -rf "${install_path}"
    bash "${arch_path}" "${install_path}"

    # redeploy
    bash "${deploy_cmd}" uninstall "${inventory}"
    bash "${deploy_cmd}" install "${inventory}"

    # show status
    sleep 1
    bash "${deploy_cmd}" status --certs-folder "${install_path}/agent_client_keys" "${inventory}"
}

build "${CONTAINER_ID}" "${AGENT_PATH}" "${ARCH_PATH}"
redeploy "${INSTALL_TO}" "${INVENTORY}" "${ARCH_PATH}"
