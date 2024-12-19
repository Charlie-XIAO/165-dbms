#!/bin/bash

# ./ss.sh     Compile all and run the server
#   -d        Compile as development build (production build by default)
#   -o level  Compile with the specified optimization level
#   -j jobs   Run the server with the specified number of jobs
#   -m        Run the server with valgrind (overrides -h)
#   -h        Run the server with helgrind (overrides -m)

CLN_COMMAND="make clean"
BLD_COMMAND="make"
RUN_COMMAND="./server"

VALGRIND_COMMAND=""

while getopts "do:j:mh" flag; do
  case "${flag}" in
    d) BLD_COMMAND="${BLD_COMMAND} DEV=1" ;;
    o) BLD_COMMAND="${BLD_COMMAND} O=${OPTARG}" ;;
    j) RUN_COMMAND="${RUN_COMMAND} -j ${OPTARG}" ;;
    m) VALGRIND_COMMAND="valgrind -s --leak-check=full --show-leak-kinds=all --track-origins=yes --track-fds=all" ;;
    h) VALGRIND_COMMAND="valgrind --tool=helgrind" ;;
    *) echo "Usage: $0 [-o] [-m] [-h] [-j jobs]"
       exit 1 ;;
  esac
done

${CLN_COMMAND} && ${BLD_COMMAND} && ${VALGRIND_COMMAND} ${RUN_COMMAND}
