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

"${CLIENT_BIN}" < "${DATA_DIR}/ms1-2-pre.in"
sleep 1

echo "n_rows,n_cols,time" > "${LOGS_DIR}/ms1-2.csv"
for exponent in $(seq 15 24); do
  n_rows=$((2 ** exponent))
  for n_cols in $(seq 1 4); do
    start=$(date +%s%N)
    "${CLIENT_BIN}" < "${DATA_DIR}/ms1-2-${n_rows}-${n_cols}.in"
    end=$(date +%s%N)
    echo "${n_rows},${n_cols},$((end-start))" >> "${LOGS_DIR}/ms1-2.csv"
    sleep 1
  done
done

echo "shutdown" | "${CLIENT_BIN}"

"${BASE_DIR}/ms1-2-plot.py"
