#!/bin/bash

# This script interprets test results and prints a human-readable summary. It
# looks for the test cases in the `test_data` directory and test outputs in the
# `outputs` directory. If there is no diff between the output and the expected
# output it considers the case to pass. Otherwise it prints the diff in yellow.
# If there are stderr messages, it prints them in red.

OUTPUTS_DIR="outputs"
TESTS_DIR="test_data"

GRN="\e[32m"
RED="\e[31m"
YEL="\e[33m"
RST="\e[0m"
GRY="\e[90m"
BLD="\e[1m"

if [[ $# -eq 1 ]]; then
  start=$1
  end=$1
elif [[ $# -eq 2 ]]; then
  start=$1
  end=$2
else
  echo "Usage: $0 <start> [<end>]"
  exit 1
fi

for i in $(seq -f "%02g" "${start}" "${end}"); do
  out_file="${OUTPUTS_DIR}/test${i}gen.out"
  cln_file="${OUTPUTS_DIR}/test${i}gen.cleaned.out"
  err_file="${OUTPUTS_DIR}/test${i}gen.out.err"
  dsl_file="${TESTS_DIR}/test${i}gen.dsl"
  exp_file="${TESTS_DIR}/test${i}gen.exp"

  if [[ -f "${out_file}" && \
        -f "${err_file}" && \
        -f "${dsl_file}" && \
        -f "${exp_file}" ]]; then
    echo -e "\n${BLD}========== Test #${i} ==========${RST}"

    # Print the DSL file dimmed, without comment lines and squeezing newlines
    dsl_content=$(sed '/^--/d' "${dsl_file}" | tr -s '\n' | sed '/^$/d')
    dsl_line_cnt=$(echo "${dsl_content}" | wc -l)
    if [[ "${dsl_line_cnt}" -gt 20 ]]; then
      echo -e "\n${GRY}$(echo "${dsl_content}" | head -n 10)${RST}"
      echo -e "${GRY}...${RST}"
      echo -e "${GRY}$(echo "${dsl_content}" | tail -n 10)${RST}"
    else
      echo -e "\n${GRY}${dsl_content}${RST}"
    fi

    # Print out diff between output and expected output if any
    diff_output=$(diff -B -w "${out_file}" "${exp_file}")
    if [[ -z "${diff_output}" ]]; then
      echo -e "\n${GRN}PASS${RST}"
    else
      # Try sorting and comparing again
      out_file_sorted=$(mktemp)
      exp_file_sorted=$(mktemp)
      sort -n "${out_file}" > "${out_file_sorted}"
      sort -n "${exp_file}" > "${exp_file_sorted}"

      diff_cln_output=$(diff -B -w "${out_file_sorted}" "${exp_file_sorted}")
      if [[ -z "${diff_cln_output}" ]]; then
        echo -e "\n${GRN}PASS (SORTED)${RST}"
      else
        echo -e "\n${YEL}${diff_output}${RST}"
      fi
    fi

    # Print out stderr messages if they exist
    if [[ -f "${err_file}" && -s "${err_file}" ]]; then
      # Trim off ANSI color codes in the original error file
      echo -e "\n${RED}$(sed -e 's/\x1b\[[0-9;]*m//g' "${err_file}")${RST}"
    fi
  fi
done

echo ""
