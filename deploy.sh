#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly MYPATH="${0}"
readonly MY_ABS_PATH=$(realpath "${MYPATH}")
readonly INSTALL_DIR=$(dirname "${MY_ABS_PATH}")

export PYTHONHOME="${INSTALL_DIR}/python"
export PYTHONPATH="${INSTALL_DIR}:${INSTALL_DIR}/libs:${PYTHONPATH:- }"
readonly PYTHONBIN="${PYTHONHOME}/python3.7"

exec "${PYTHONBIN}" -m agent.ssh_deploy "$@"
