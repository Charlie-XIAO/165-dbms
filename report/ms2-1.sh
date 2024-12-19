#!/bin/bash

BASE_DIR=$(dirname "$(realpath "$0")")
DATA_DIR="${BASE_DIR}/data"
LOGS_DIR="${BASE_DIR}/logs"

if [[ ! -d "${LOGS_DIR}" ]]; then
  mkdir -p "${LOGS_DIR}"
fi

SRC_DIR="${BASE_DIR}/../src"
SERVER_BIN="${SRC_DIR}/server"
CLIENT_BIN="${SRC_DIR}/client"

cd "${SRC_DIR}"
make distclean
make clean
make

"${SERVER_BIN}" &
sleep 2

echo -e "$(cat "${DATA_DIR}/ms2-1-pre.in")\nshutdown" | "${CLIENT_BIN}"
sleep 2

echo "n_jobs,time" > "${LOGS_DIR}/ms2-1.csv"
n_rows=$((2 ** 24))

# Single-core
"${SERVER_BIN}" &
sleep 2

start=$(date +%s%N)
"${CLIENT_BIN}" < "${DATA_DIR}/ms2-1-single-core.in"
end=$(date +%s%N)
echo "0,$((end-start))" >> "${LOGS_DIR}/ms2-1.csv"
echo "shutdown" | "${CLIENT_BIN}"
sleep 2

for n_jobs in $(seq 1 20); do
  # Multi-core
  "${SERVER_BIN}" -j "${n_jobs}" &
  sleep 2

  start=$(date +%s%N)
  "${CLIENT_BIN}" < "${DATA_DIR}/ms2-1-multi-core.in"
  end=$(date +%s%N)
  echo "${n_jobs},$((end-start))" >> "${LOGS_DIR}/ms2-1.csv"
  echo "shutdown" | "${CLIENT_BIN}"
  sleep 2
done

"${BASE_DIR}/ms2-1-plot.py"
