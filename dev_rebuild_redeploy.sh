#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset


if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "-h" ]] ; then
    echo "Usage: ${0} BUILD_CONTAINER_ID LOCAL_INSTALL_FOLDER INVENTORY_FILE"
    exit 0
fi

readonly AGENT_PATH=$(dirname $(realpath "${0}"))
readonly MAKE_ARCH_LIB=$(python -c "import koder_utils ; print(koder_utils.__file__)")
readonly MAKE_ARCH_LIB_PATH=$(dirname $(realpath "${MAKE_ARCH_LIB}"))
readonly CONTAINER_ID="${1}"
readonly ARCH_PATH="${AGENT_PATH}/arch/agent.sh"


function build {
    local -r container="${1}"
    local -r agent_path="${2}"
    local -r libpath="${3}"
    local -r arch_path="${4}"

    local -r copy_to="/tmp/agent"
    local -r in_container_arch="/tmp/agent.sh"

    docker exec -it "${container}" rm -rf "${copy_to}"
    docker cp "${agent_path}" "${container}:${copy_to}"
    docker cp "${libpath}" "${container}:${copy_to}"

    local -r cmd="python3.7 -m koder_utils.tools.make_arch --standalone ${copy_to} ${in_container_arch}"
    echo "cd ${copy_to}; ${cmd}" | docker exec -i "${container}" bash
    docker cp "${container}:${in_container_arch}" "${arch_path}"
}


function redeploy {
    local -r install_path="${1}"
    local -r inventory="${2}"
    local -r arch_path="${3}"

    # install locally
    rm -rf "${install_path}/*"
    bash "${arch_path}" --install "${install_path}"

    local -r ctl="${install_path}/python/python3.7 -m agent.ctl"

    pushd "${install_path}"

    # redeploy
    ${ctl} uninstall --inventory "${inventory}"
    ${ctl} install --inventory "${inventory}"

    # show status
    sleep 1
    ${ctl} status
    popd >/dev/null 2>&1
}

build "${CONTAINER_ID}" "${AGENT_PATH}" "${MAKE_ARCH_LIB_PATH}" "${ARCH_PATH}"

readonly INSTALL_TO="${2}"
readonly INVENTORY="${3}"
redeploy "${INSTALL_TO}" "${INVENTORY}" "${ARCH_PATH}"
