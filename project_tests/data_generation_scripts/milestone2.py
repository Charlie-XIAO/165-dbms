#!/usr/bin/python

import argparse

import numpy as np
import polars as pl

import utils

# Base path to store the generated data files
TEST_BASE_DIR = "/cs165/generated_data"

# Base path that points to the generated data files
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

# Precision for average aggregation results
PREC = 2


def generate_data_milestone_2(size):
    output_file = f"{TEST_BASE_DIR}/data3_batch.csv"
    header = utils.generate_header("db1", "tbl3_batch", 4)
    df = pl.DataFrame(
        {
            "col1": np.random.randint(0, 1000, size=size),
            "col2": np.random.randint(0, size // 5, size=size),
            "col3": np.random.randint(0, size // 5, size=size),
            "col4": np.random.randint(0, 10000, size=size),
        }
    )
    df = df.with_columns((df["col4"] + df["col1"]).alias("col4"))
    df.rename({f"col{i + 1}": header[i] for i in range(4)}).write_csv(output_file)
    return df


def create_test_10():
    output_file, exp_output_file = utils.open_files(10, TEST_BASE_DIR)
    output_file.write(
        "-- Load Test Data 2\n"
        "-- Create a table to run batch queries on\n"
        "--\n"
        "-- Loads data from: data3_batch.csv\n"
        "--\n"
        "-- Create Table\n"
        'create(tbl,"tbl3_batch",db1,4)\n'
        'create(col,"col1",db1.tbl3_batch)\n'
        'create(col,"col2",db1.tbl3_batch)\n'
        'create(col,"col3",db1.tbl3_batch)\n'
        'create(col,"col4",db1.tbl3_batch)\n'
        "--\n"
        "-- Load data immediately\n"
        f'load("{DOCKER_TEST_BASE_DIR}/data3_batch.csv")\n'
        "--\n"
        "-- Testing that the data is durable on disk.\n"
        "shutdown\n"
    )
    utils.close_files(output_file, exp_output_file)


def create_test_11(df):
    output_file, exp_output_file = utils.open_files(11, TEST_BASE_DIR)
    low1, high1 = 10, 20
    low2, high2 = 800, 830

    output_file.write(
        "--\n"
        "-- Testing for batching queries\n"
        "-- 2 queries with NO overlap\n"
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT col4 FROM tbl3_batch WHERE col1 >= {low1} AND col1 < {high1};\n"
        f"-- SELECT col4 FROM tbl3_batch WHERE col1 >= {low2} AND col1 < {high2};\n"
        "--\n"
        "--\n"
        "batch_queries()\n"
        f"s1=select(db1.tbl3_batch.col1,{low1},{high1})\n"
        f"s2=select(db1.tbl3_batch.col1,{low2},{high2})\n"
        "batch_execute()\n"
        "f1=fetch(db1.tbl3_batch.col4,s1)\n"
        "f2=fetch(db1.tbl3_batch.col4,s2)\n"
        "print(f1)\n"
        "print(f2)\n"
    )

    df_select1 = df.filter((pl.col("col1") >= low1) & (pl.col("col1") < high1))
    df_select2 = df.filter((pl.col("col1") >= low2) & (pl.col("col1") < high2))
    exp_output_file.write(utils.print_output(df_select1["col4"], PREC) + "\n\n")
    exp_output_file.write(utils.print_output(df_select2["col4"], PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_12(df):
    output_file, exp_output_file = utils.open_files(12, TEST_BASE_DIR)
    low1, high1 = 600, 820
    low2, high2 = 800, 830
    output_file.write(
        "--\n"
        "-- Testing for batching queries\n"
        "-- 2 queries with partial overlap\n"
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT col4 FROM tbl3_batch WHERE col1 >= {low1} AND col1 < {high1};\n"
        f"-- SELECT col4 FROM tbl3_batch WHERE col1 >= {low2} AND col1 < {high2};\n"
        "--\n"
        "--\n"
        "batch_queries()\n"
        f"s1=select(db1.tbl3_batch.col1,{low1},{high1})\n"
        f"s2=select(db1.tbl3_batch.col1,{low2},{high2})\n"
        "batch_execute()\n"
        "f1=fetch(db1.tbl3_batch.col4,s1)\n"
        "f2=fetch(db1.tbl3_batch.col4,s2)\n"
        "print(f1)\n"
        "print(f2)\n"
    )

    df_select1 = df.filter((pl.col("col1") >= low1) & (pl.col("col1") < high1))
    df_select2 = df.filter((pl.col("col1") >= low2) & (pl.col("col1") < high2))
    exp_output_file.write(utils.print_output(df_select1["col4"], PREC) + "\n\n")
    exp_output_file.write(utils.print_output(df_select2["col4"], PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_13(df):
    output_file, exp_output_file = utils.open_files(13, TEST_BASE_DIR)
    low1, high1 = 810, 820
    low2, high2 = 800, 830
    output_file.write(
        "--\n"
        "-- Testing for batching queries\n"
        "-- 2 queries with full overlap (subsumption)\n"
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT col4 FROM tbl3_batch WHERE col1 >= {low1} AND col1 < {high1};\n"
        f"-- SELECT col4 FROM tbl3_batch WHERE col1 >= {low2} AND col1 < {high2};\n"
        "--\n"
        "--\n"
        "batch_queries()\n"
        f"s1=select(db1.tbl3_batch.col1,{low1},{high1})\n"
        f"s2=select(db1.tbl3_batch.col1,{low2},{high2})\n"
        "batch_execute()\n"
        "f1=fetch(db1.tbl3_batch.col4,s1)\n"
        "f2=fetch(db1.tbl3_batch.col4,s2)\n"
        "print(f1)\n"
        "print(f2)\n"
    )

    df_select1 = df.filter((pl.col("col1") >= low1) & (pl.col("col1") < high1))
    df_select2 = df.filter((pl.col("col1") >= low2) & (pl.col("col1") < high2))
    exp_output_file.write(utils.print_output(df_select1["col4"], PREC) + "\n\n")
    exp_output_file.write(utils.print_output(df_select2["col4"], PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_14(df):
    output_file, exp_output_file = utils.open_files(14, TEST_BASE_DIR)
    output_file.write(
        "--\n"
        "-- Testing for batching queries\n"
        "-- Queries with no overlap\n"
        "--\n"
        "-- Query in SQL:\n"
        "-- 10 Queries of the type:\n"
        "-- SELECT col1 FROM tbl3_batch WHERE col4 >= _ AND col4 < _;\n"
        "--\n"
        "--\n"
    )

    output_file.write("batch_queries()\n")
    for i in range(10):
        output_file.write(
            f"s{i}=select(db1.tbl3_batch.col4,{1000 * i},{1000 * i + 30})\n"
        )
    output_file.write("batch_execute()\n")
    for i in range(10):
        output_file.write(f"f{i}=fetch(db1.tbl3_batch.col1,s{i})\n")
    for i in range(10):
        output_file.write(f"print(f{i})\n")

    for i in range(10):
        df_select = df.filter(
            (pl.col("col4") >= 1000 * i) & (pl.col("col4") < 1000 * i + 30)
        )
        exp_output_file.write(utils.print_output(df_select["col1"], PREC) + "\n\n")
    utils.close_files(output_file, exp_output_file)


def create_test_15(df):
    output_file, exp_output_file = utils.open_files(15, TEST_BASE_DIR)
    output_file.write(
        "--\n"
        "-- Testing for batching queries\n"
        "-- Queries with full overlap (subsumption)\n"
        "--\n"
        "-- Query in SQL:\n"
        "-- 10 Queries of the type:\n"
        "-- SELECT col1 FROM tbl3_batch WHERE col4 >= _ AND col4 < _;\n"
        "--\n"
        "--\n"
    )

    val = np.random.randint(1000, 9900)
    output_file.write("batch_queries()\n")
    for i in range(10):
        output_file.write(
            f"s{i}=select(db1.tbl3_batch.col4,{val + 2 * i},{val + 60 - 2 * i})\n"
        )
    output_file.write("batch_execute()\n")
    for i in range(10):
        output_file.write(f"f{i}=fetch(db1.tbl3_batch.col1,s{i})\n")
    for i in range(10):
        output_file.write(f"print(f{i})\n")

    for i in range(10):
        df_select = df.filter(
            (pl.col("col4") >= val + 2 * i) & (pl.col("col4") < val + 60 - 2 * i)
        )
        exp_output_file.write(utils.print_output(df_select["col1"], PREC) + "\n\n")
    utils.close_files(output_file, exp_output_file)


def create_tests_16_17(df, size):
    # 1 / 1000 tuples should qualify on average. This is so that most time is spent on
    # scans and not fetches or prints
    n_queries = 100
    offset = np.max([1, int(size / 5000)])
    lowers = np.random.randint(0, size / 8, size=n_queries)

    output_file16, exp_output_file16 = utils.open_files(16, TEST_BASE_DIR)
    output_file17, exp_output_file17 = utils.open_files(17, TEST_BASE_DIR)
    output_file16.write(
        "--\n"
        "-- Control timing for without batching\n"
        "-- Queries for 16 and 17 are identical.\n"
        "-- Query in SQL:\n"
        "-- 100 Queries of the type:\n"
        "-- SELECT col3 FROM tbl3_batch WHERE col2 >= _ AND col2 < _;\n"
        "--\n"
    )
    output_file17.write(
        "--\n"
        "-- Same queries with batching\n"
        "-- Queries for 16 and 17 are identical.\n"
        "--\n"
    )

    output_file17.write("batch_queries()\n")
    for i in range(n_queries):
        output_file16.write(
            f"s{i}=select(db1.tbl3_batch.col2,{lowers[i]},{lowers[i] + offset})\n"
        )
        output_file17.write(
            f"s{i}=select(db1.tbl3_batch.col2,{lowers[i]},{lowers[i] + offset})\n"
        )
    output_file17.write("batch_execute()\n")
    for i in range(n_queries):
        output_file16.write(f"f{i}=fetch(db1.tbl3_batch.col3,s{i})\n")
        output_file17.write(f"f{i}=fetch(db1.tbl3_batch.col3,s{i})\n")
    for i in range(n_queries):
        output_file16.write(f"print(f{i})\n")
        output_file17.write(f"print(f{i})\n")

    for i in range(n_queries):
        df_select = df.filter(
            (pl.col("col2") >= lowers[i]) & (pl.col("col2") < (lowers[i] + offset))
        )
        exp_output_file16.write(utils.print_output(df_select["col3"], PREC) + "\n\n")
        exp_output_file17.write(utils.print_output(df_select["col3"], PREC) + "\n\n")
    utils.close_files(output_file16, exp_output_file16)
    utils.close_files(output_file17, exp_output_file17)

    return lowers


def create_tests_18_19(df, size, lowers):
    # 1 / 1000 tuples should qualify on average. This is so that most time is spent on
    # scans and not fetches or prints
    n_queries = len(lowers)
    offset = np.max([1, int(size / 5000)])

    output_file18, exp_output_file18 = utils.open_files(18, TEST_BASE_DIR)
    output_file19, exp_output_file19 = utils.open_files(19, TEST_BASE_DIR)
    output_file18.write(
        "--\n"
        "-- Queries for 18 and 19 are single-core versions of Queries for 16 and 17.\n"
        "-- Query in SQL:\n"
        "-- 100 Queries of the type:\n"
        "-- SELECT col3 FROM tbl3_batch WHERE col2 >= _ AND col2 < _;\n"
        "--\n"
    )
    output_file19.write(
        "--\n"
        "-- Same queries with single-core execution\n"
        "-- Queries for 18 and 19 are single-core versions of Queries for 16 and 17.\n"
        "--\n"
    )

    output_file18.write("single_core()\n")
    output_file19.write("single_core()\n")
    output_file19.write("batch_queries()\n")
    for i in range(n_queries):
        output_file18.write(
            f"s{i}=select(db1.tbl3_batch.col2,{lowers[i]},{lowers[i] + offset})\n"
        )
        output_file19.write(
            f"s{i}=select(db1.tbl3_batch.col2,{lowers[i]},{lowers[i] + offset})\n"
        )
    output_file19.write("batch_execute()\n")
    for i in range(n_queries):
        output_file18.write(f"f{i}=fetch(db1.tbl3_batch.col3,s{i})\n")
        output_file19.write(f"f{i}=fetch(db1.tbl3_batch.col3,s{i})\n")
    output_file19.write("single_core_execute()\n")
    output_file18.write("single_core_execute()\n")
    for i in range(n_queries):
        output_file18.write(f"print(f{i})\n")
        output_file19.write(f"print(f{i})\n")

    for i in range(n_queries):
        df_select = df.filter(
            (pl.col("col2") >= lowers[i]) & (pl.col("col2") < (lowers[i] + offset))
        )
        exp_output_file18.write(utils.print_output(df_select["col3"], PREC) + "\n\n")
        exp_output_file19.write(utils.print_output(df_select["col3"], PREC) + "\n\n")
    utils.close_files(output_file18, exp_output_file18)
    utils.close_files(output_file19, exp_output_file19)


def generate_milestone_2_files(size, seed):
    np.random.seed(seed)
    df = generate_data_milestone_2(size)
    create_test_10()
    create_test_11(df)
    create_test_12(df)
    create_test_13(df)
    create_test_14(df)
    create_test_15(df)
    lowers = create_tests_16_17(df, size)
    create_tests_18_19(df, size, lowers)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("size", type=int)
    parser.add_argument("seed", type=int)
    parser.add_argument("test_base_dir", type=str, nargs="?", default="")
    parser.add_argument("docker_test_base_dir", type=str, nargs="?", default="")
    args = parser.parse_args()

    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR

    if args.test_base_dir:
        TEST_BASE_DIR = args.test_base_dir
    if args.docker_test_base_dir:
        DOCKER_TEST_BASE_DIR = args.docker_test_base_dir

    generate_milestone_2_files(args.size, args.seed)


if __name__ == "__main__":
    main()
