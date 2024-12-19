#!/bin/bash

SRCDIR=/cs165/src
if [[ "${CS165_RUST}" == "1" ]]; then
  SRCDIR=/cs165/rustsrc
fi

cd "${SRCDIR}"
make distclean
make clean
make
