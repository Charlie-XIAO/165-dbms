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

n_rows=$((2 ** 24))

make distclean
"${SERVER_BIN}" &
sleep 2

"${CLIENT_BIN}" < "${DATA_DIR}/ms3-2-pre.in"
sleep 1

echo "selectivity,time" > "${LOGS_DIR}/ms3-2.csv"
for selectivity in 0.01 0.02 0.05 0.1 0.2 0.5; do
  start=$(date +%s%N)
  "${CLIENT_BIN}" < "${DATA_DIR}/ms3-2-${selectivity}.in"
  end=$(date +%s%N)
  echo "${selectivity},$((end-start))" >> "${LOGS_DIR}/ms3-2.csv"
  sleep 1
done

echo "shutdown" | "${CLIENT_BIN}"

for index_type in "btree" "sorted"; do
  for index_metatype in "unclustered" "clustered"; do
    make distclean
    "${SERVER_BIN}" &
    sleep 2

    "${CLIENT_BIN}" < "${DATA_DIR}/ms3-2-pre-${index_type}-${index_metatype}.in"
    sleep 1

    echo "selectivity,time" > "${LOGS_DIR}/ms3-2-${index_type}-${index_metatype}.csv"
    for selectivity in 0.01 0.02 0.05 0.1 0.2 0.5; do
      start=$(date +%s%N)
      "${CLIENT_BIN}" < "${DATA_DIR}/ms3-2-${selectivity}.in"
      end=$(date +%s%N)
      echo "${selectivity},$((end-start))" >> "${LOGS_DIR}/ms3-2-${index_type}-${index_metatype}.csv"
      sleep 1
    done

    echo "shutdown" | "${CLIENT_BIN}"
  done
done

# "${BASE_DIR}/ms3-2-plot.py"
