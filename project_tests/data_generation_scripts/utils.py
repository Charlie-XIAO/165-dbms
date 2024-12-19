#!/usr/bin/python

import sys

import numpy as np


def nantozero(val):
    if val is None:
        return 0
    return val if not np.isnan(val) else 0


def open_files(test_num, test_dir=""):
    # If a directory base specified, we want to add the trailing separator `/`
    if test_dir != "":
        test_dir += "/"

    output_file = open(f"{test_dir}test{test_num:02d}gen.dsl", "w")
    exp_output_file = open(f"{test_dir}test{test_num:02d}gen.exp", "w")
    return output_file, exp_output_file


def close_files(output_file, exp_output_file):
    output_file.flush()
    exp_output_file.flush()
    output_file.close()
    exp_output_file.close()


def generate_header(db_name, table_name, n_cols):
    return [f"{db_name}.{table_name}.col{i + 1}" for i in range(n_cols)]


def print_output(pl_series, precision):
    if pl_series.shape[0] == 0:
        return ""

    return np.array2string(
        pl_series.to_numpy(),
        floatmode="fixed",
        precision=precision,
        separator="\n",
        threshold=sys.maxsize,  # Always print the entire array
    )[1:-1]  # Remove the square brackets
