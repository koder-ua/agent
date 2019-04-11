#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly MYPATH="${0}"
readonly MODULE="${1}"
shift

readonly PYTHONVER=3.7
readonly MY_ABS_PATH=$(realpath "${MYPATH}")
readonly INSTALL_DIR=$(dirname "${MY_ABS_PATH}")

export AGENT_NO_VAR_MARK="<<empty>>"
export ORIGIN_PYTHONHOME="${PYTHONHOME:-$AGENT_NO_VAR_MARK}"
export ORIGIN_PYTHONPATH="${PYTHONPATH:-$AGENT_NO_VAR_MARK}"

export PYTHONHOME="${INSTALL_DIR}/python"
export PYTHONPATH="${INSTALL_DIR}:${INSTALL_DIR}/libs:${PYTHONPATH:- }"
readonly PYTHONBIN="${PYTHONHOME}/python${PYTHONVER}"

exec "${PYTHONBIN}" -m "agent.${MODULE}" "${@}"
