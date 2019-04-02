#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly archname="${0}"
readonly unpack_folder="${1}"

readonly archbasename=$(basename "${archname}")
readonly arch_content_pos=$(awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' "${archname}")

if [[ "${unpack_folder}" == "--list" ]] ; then
    tail "-n+${arch_content_pos}" "${archname}" | tar --gzip --list
    exit 0
fi

mkdir --parents "${unpack_folder}"
tail "-n+${arch_content_pos}" "${archname}" | tar -zx -C "${unpack_folder}"
tail "-n+${arch_content_pos}" "${archname}" > "${unpack_folder}/distribution.tar.gz"
exit 0

__ARCHIVE_BELOW__
