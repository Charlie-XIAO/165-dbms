#!/bin/bash

# This script runs all the tests up to a certain milestone.

UPTOMILE="${1:-5}"

# The number of seconds you need to wait for your server to go from shutdown to
# ready to receive queries from client. The default is 5s.
#
# Note: if you see segfaults when running this test suite for the test cases
# which expect a fresh server restart, this may be because you are not waiting
# long enough between client tests. Once you know how long your server takes,
# you can cut this time down and provide it on your command line.
WAIT_SECONDS_TO_RECOVER_DATA="${2:-5}"

MAX_TEST=65
if [[ "${UPTOMILE}" -eq "1" ]]; then
  MAX_TEST=09
elif [[ "${UPTOMILE}" -eq "2" ]]; then
  MAX_TEST=19
elif [[ "${UPTOMILE}" -eq "3" ]]; then
  MAX_TEST=44
elif [[ "${UPTOMILE}" -eq "4" ]]; then
  MAX_TEST=59
elif [[ "${UPTOMILE}" -eq "5" ]]; then
  MAX_TEST=65
fi
TEST_IDS=$(seq -w 1 "${MAX_TEST}")

SRCDIR=/cs165/src
SERVER_BIN="${SRCDIR}/server"
if [[ "${CS165_RUST}" == "1" ]]; then
  SRCDIR=/cs165/rustsrc
  SERVER_BIN="${SRCDIR}/target/release/server"
fi

# Function to kill all running servers
function killserver {
  SERVER_NUM_RUNNING=$(ps aux | grep server | wc -l)
  if [[ $((SERVER_NUM_RUNNING)) -ne 0 ]]; then
    if pgrep server; then
      pkill -9 server
    fi
  fi
}

FIRST_SERVER_START=0
for TEST_ID in ${TEST_IDS}; do
  if [[ "${FIRST_SERVER_START}" -eq 0 ]]; then
    # Start the server before the first case we test
    cd "${SRCDIR}"
    "${SERVER_BIN}" > last_server.out &
    FIRST_SERVER_START=1
  elif [[ "${TEST_ID}" == "02" ]] || \
       [[ "${TEST_ID}" == "05" ]] || \
       [[ "${TEST_ID}" == "11" ]] || \
       [[ "${TEST_ID}" == "21" ]] || \
       [[ "${TEST_ID}" == "22" ]] || \
       [[ "${TEST_ID}" == "31" ]] || \
       [[ "${TEST_ID}" == "46" ]] || \
       [[ "${TEST_ID}" == "63" ]]; then
    # We restart the server after tests 1,4,10,20,21,30,45,62 (i.e., before
    # tests 2,3,11,21,22,31,46,63)
    killserver
    cd "${SRCDIR}"
    "${SERVER_BIN}" >> last_server.out &
    sleep "${WAIT_SECONDS_TO_RECOVER_DATA}"
  fi

  SERVER_NUM_RUNNING=$(ps aux | grep server | wc -l)
  if [[ $((SERVER_NUM_RUNNING)) -lt 1 ]]; then
    echo "Warning: no server running at this point. Your server may have crashed early."
  fi

  echo "========== #${TEST_ID} ==========" >> last_server.out
  /cs165/infra_scripts/run_test.sh "${TEST_ID}"
  sleep 1
done

echo "Milestone run is complete up to Milestone #${UPTOMILE}"
killserver
