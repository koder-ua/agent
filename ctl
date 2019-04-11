#/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly MYDIR=$(dirname "${0}")
bash "${MYDIR}/run.sh" ctl "${@}"
