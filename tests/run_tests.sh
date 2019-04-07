#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

readonly MYPATH=$(realpath "${0}")
readonly CPATH=$(dirname "${MYPATH}")
readonly ROOT_DIR=$(dirname "${CPATH}")

pushd "${ROOT_DIR}"
python -m agent.server server --cert tests/test_cert.crt --key tests/test_key.key --api-key tests/api_key.enc >/dev/null &
readonly SERVER_PID=$!
popd

pushd "${CPATH}"
set +o errexit
pytest -s -v --duration=0 test_agent.py
set -o errexit
popd

kill -INT "${SERVER_PID}"
