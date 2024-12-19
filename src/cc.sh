#!/bin/bash

# Examples:
# ./cc.sh  Run the client
#   -m     Run the client with valgrind
#   -h     Run the client with helgrind

RUN_COMMAND="./client"

VALGRIND_COMMAND=""

while getopts "mh" flag; do
  case "${flag}" in
    m) VALGRIND_COMMAND="valgrind -s --leak-check=full --show-leak-kinds=all --track-origins=yes --track-fds=all" ;;
    h) VALGRIND_COMMAND="valgrind --tool=helgrind" ;;
    *) echo "Usage: $0 [-m] [-h]"
       exit 1 ;;
  esac
done

${VALGRIND_COMMAND} ${RUN_COMMAND}
