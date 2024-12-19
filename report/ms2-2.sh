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

"${CLIENT_BIN}" < "${DATA_DIR}/ms2-2-pre.in"
sleep 1

echo "batch_size,time" > "${LOGS_DIR}/ms2-2.csv"

start=$(date +%s%N)
"${CLIENT_BIN}" < "${DATA_DIR}/ms2-2-unbatched.in"
end=$(date +%s%N)
echo "1,$((end-start))" >> "${LOGS_DIR}/ms2-2.csv"
sleep 1

for batch_size in 2 4 8 16 32 64 128; do
  start=$(date +%s%N)
  "${CLIENT_BIN}" < "${DATA_DIR}/ms2-2-batched-${batch_size}.in"
  end=$(date +%s%N)
  echo "${batch_size},$((end-start))" >> "${LOGS_DIR}/ms2-2.csv"
  sleep 1
done

echo "shutdown" | "${CLIENT_BIN}"

"${BASE_DIR}/ms2-2-plot.py"
