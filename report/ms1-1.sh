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

"${CLIENT_BIN}" < "${DATA_DIR}/ms1-1-pre.in"
sleep 1

echo "n_rows,time" > "${LOGS_DIR}/ms1-1.csv"
for exponent in $(seq 15 24); do
  n_rows=$((2 ** exponent))
  start=$(date +%s%N)
  "${CLIENT_BIN}" < "${DATA_DIR}/ms1-1-${n_rows}.in"
  end=$(date +%s%N)
  echo "${n_rows},$((end-start))" >> "${LOGS_DIR}/ms1-1.csv"
  sleep 1
done

echo "shutdown" | "${CLIENT_BIN}"

"${BASE_DIR}/ms1-1-plot.py"
