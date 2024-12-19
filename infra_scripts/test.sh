#!/bin/bash

# This script is a dummy example to run a couple of test cases.

REL_TEST_DIR="generated_data"
ABS_TEST_DIR="/cs165/${REL_TEST_DIR}"
STUDENT_OUTPUT_DIR="/cs165/student_outputs"

DATA_SIZE=10000
RAND_SEED=42

SRCDIR=/cs165/src
SERVER_BIN="${SRCDIR}/server"
CLIENT_BIN="${SRCDIR}/client"

if [[ "${CS165_RUST}" == "1" ]]; then
  SRCDIR=/cs165/rustsrc
  SERVER_BIN="${SRCDIR}/target/release/server"
  CLIENT_BIN="${SRCDIR}/target/release/client"
fi

cd project_tests/data_generation_scripts
python milestone1.py "${DATA_SIZE}" "${RAND_SEED}" "${ABS_TEST_DIR}" "${ABS_TEST_DIR}"

cd "${SRCDIR}"
make distclean
make clean
make

SLEEP_TIME=1

echo "Running test 1"
"${SERVER_BIN}" > "${STUDENT_OUTPUT_DIR}/test01gen.server.debug.out" &
sleep "${SLEEP_TIME}"
"${CLIENT_BIN}" < "../${REL_TEST_DIR}/test01gen.dsl"

echo "Running test 2"
"${SERVER_BIN}" > "${STUDENT_OUTPUT_DIR}/test02gen.server.debug.out" &
sleep "${SLEEP_TIME}"
"${CLIENT_BIN}" < "../${REL_TEST_DIR}/test02gen.dsl"

sleep "${SLEEP_TIME}"
if pgrep server; then
  pkill server;
fi
