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

echo "n_cols,type,time" > "${LOGS_DIR}/ms3-1.csv"
n_rows=$((2 ** 24))

for index_type in "btree" "sorted"; do
  for index_metatype in "unclustered" "clustered"; do
    for n_cols in $(seq 1 4); do
      make distclean
      "${SERVER_BIN}" &
      sleep 2

      "${CLIENT_BIN}" < "${DATA_DIR}/ms3-1-pre.in"
      sleep 1

      start=$(date +%s%N)
      "${CLIENT_BIN}" < "${DATA_DIR}/ms3-1-${index_type}-${index_metatype}-${n_cols}.in"
      end=$(date +%s%N)
      echo "${n_cols},${index_type}-${index_metatype},$((end-start))" >> "${LOGS_DIR}/ms3-1.csv"
      echo "shutdown" | "${CLIENT_BIN}"
      sleep 2
    done
  done
done

"${BASE_DIR}/ms3-1-plot.py"
