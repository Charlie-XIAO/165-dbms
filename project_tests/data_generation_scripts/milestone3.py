#!/usr/bin/python

import argparse
from itertools import product

import numpy as np
import polars as pl

import utils

# Base path to store the generated data files
TEST_BASE_DIR = "/cs165/generated_data"

# Base path that points to the generated data files
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

# Precision for average aggregation results
PREC = 2


def generate_data_milestone_3(size):
    output_file_ctrl = f"{TEST_BASE_DIR}/data4_ctrl.csv"
    output_file_bt = f"{TEST_BASE_DIR}/data4_btree.csv"
    output_file_cbt = f"{TEST_BASE_DIR}/data4_clustered_btree.csv"

    header_ctrl = utils.generate_header("db1", "tbl4_ctrl", 4)
    header_bt = utils.generate_header("db1", "tbl4", 4)
    header_cbt = utils.generate_header("db1", "tbl4_clustered_btree", 4)

    df = pl.DataFrame(
        {
            "col1": np.random.randint(0, 1000, size=size),
            "col2": np.random.randint(0, size // 5, size=size),
            "col3": np.random.randint(0, size // 5, size=size),
            "col4": np.random.randint(0, 10000, size=size),
        }
    )
    # Make ~5% of values in col2 a single value
    freq_val1 = np.random.randint(0, int(size / 5))
    mask_start = np.random.uniform(0.0, 1.0, size)
    df = df.with_columns(
        pl.when(mask_start < 0.05).then(freq_val1).otherwise(df["col2"]).alias("col2")
    )
    # Make ~2% of values in col2 a different value
    freq_val2 = np.random.randint(0, int(size / 5))
    mask_start = np.random.uniform(0.0, 1.0, size)
    df = df.with_columns(
        pl.when(mask_start < 0.02).then(freq_val2).otherwise(df["col2"]).alias("col2")
    )
    df = df.with_columns((df["col1"] + df["col4"]).alias("col4"))

    df.rename({f"col{i + 1}": header_ctrl[i] for i in range(4)}).write_csv(
        output_file_ctrl
    )
    df.rename({f"col{i + 1}": header_bt[i] for i in range(4)}).write_csv(output_file_bt)
    df.rename({f"col{i + 1}": header_cbt[i] for i in range(4)}).write_csv(
        output_file_cbt
    )
    return freq_val1, freq_val2, df


def create_test_20():
    output_file, exp_output_file = utils.open_files(20, TEST_BASE_DIR)
    output_file.write(
        "-- Create a control table that is identical to the one in test21.dsl, but\n"
        "-- without any indexes\n"
        "--\n"
        "-- Loads data from: data4_ctrl.csv\n"
        "--\n"
        "-- Create Table\n"
        'create(tbl,"tbl4_ctrl",db1,4)\n'
        'create(col,"col1",db1.tbl4_ctrl)\n'
        'create(col,"col2",db1.tbl4_ctrl)\n'
        'create(col,"col3",db1.tbl4_ctrl)\n'
        'create(col,"col4",db1.tbl4_ctrl)\n'
        "--\n"
        "-- Load data immediately\n"
        f'load("{DOCKER_TEST_BASE_DIR}/data4_ctrl.csv")\n'
        "--\n"
        "-- Testing that the data and their indexes are durable on disk.\n"
        "shutdown\n"
    )
    utils.close_files(output_file, exp_output_file)


def create_test_21():
    output_file, exp_output_file = utils.open_files(21, TEST_BASE_DIR)
    output_file.write(
        "-- Test for creating table with indexes\n"
        "--\n"
        "-- Table tbl4 has a clustered index with col3 being the leading column.\n"
        "-- The clustered index has the form of a sorted column.\n"
        "-- The table also has an unclustered btree index on col2.\n"
        "--\n"
        "-- Loads data from: data4_btree.csv\n"
        "--\n"
        "-- Create Table\n"
        'create(tbl,"tbl4",db1,4)\n'
        'create(col,"col1",db1.tbl4)\n'
        'create(col,"col2",db1.tbl4)\n'
        'create(col,"col3",db1.tbl4)\n'
        'create(col,"col4",db1.tbl4)\n'
        "-- Create a clustered index on col3\n"
        "create(idx,db1.tbl4.col3,sorted,clustered)\n"
        "-- Create an unclustered btree index on col2\n"
        "create(idx,db1.tbl4.col2,btree,unclustered)\n"
        "--\n"
        "--\n"
        "-- Load data immediately in the form of a clustered index\n"
        f'load("{DOCKER_TEST_BASE_DIR}/data4_btree.csv")\n'
        "--\n"
        "-- Testing that the data and their indexes are durable on disk.\n"
        "shutdown\n"
    )
    utils.close_files(output_file, exp_output_file)


def create_tests_22_23(df, size):
    output_file22, exp_output_file22 = utils.open_files(22, TEST_BASE_DIR)
    output_file23, exp_output_file23 = utils.open_files(23, TEST_BASE_DIR)
    offset1 = np.max([1, int(size / 5000)])
    offset2 = np.max([2, int(size / 2500)])
    val1 = np.random.randint(0, int(size / 5 - offset1))
    val2 = np.random.randint(0, int(size / 5 - offset2))

    # Generate test 22
    output_file22.write(
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= {val1} and col3 < {val1 + offset1};\n"
        f"-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= {val2} and col3 < {val2 + offset2};\n"
        "--\n"
        f"s1=select(db1.tbl4_ctrl.col3,{val1},{val1 + offset1})\n"
        "f1=fetch(db1.tbl4_ctrl.col1,s1)\n"
        "print(f1)\n"
        f"s2=select(db1.tbl4_ctrl.col3,{val2},{val2 + offset2})\n"
        "f2=fetch(db1.tbl4_ctrl.col1,s2)\n"
        "print(f2)\n"
    )

    # Generate test 23
    output_file23.write(
        "--\n"
        "-- tbl3 has a secondary b-tree tree index on col2, and a clustered index on col3 with the form of a sorted column\n"
        "-- testing for correctness\n"
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT col1 FROM tbl4 WHERE col3 >= {val1} and col3 < {val1 + offset1};\n"
        f"-- SELECT col1 FROM tbl4 WHERE col3 >= {val2} and col3 < {val2 + offset2};\n"
        "--\n"
        "-- since col3 has a clustered index, the index is expected to be used by the select operator\n"
        f"s1=select(db1.tbl4.col3,{val1},{val1 + offset1})\n"
        "f1=fetch(db1.tbl4.col1,s1)\n"
        "print(f1)\n"
        f"s2=select(db1.tbl4.col3,{val2},{val2 + offset2})\n"
        "f2=fetch(db1.tbl4.col1,s2)\n"
        "print(f2)\n"
    )

    # Generate expected results
    df_select1 = df.filter((pl.col("col3") >= val1) & (pl.col("col3") < val1 + offset1))
    df_select2 = df.filter((pl.col("col3") >= val2) & (pl.col("col3") < val2 + offset2))
    for exp_output_file in [exp_output_file22, exp_output_file23]:
        exp_output_file.write(utils.print_output(df_select1["col1"], PREC) + "\n\n")
        exp_output_file.write(utils.print_output(df_select2["col1"], PREC) + "\n")
    utils.close_files(output_file22, exp_output_file22)
    utils.close_files(output_file23, exp_output_file23)


def create_test_24(df, size):
    output_file, exp_output_file = utils.open_files(24, TEST_BASE_DIR)
    offset1 = np.max([1, int(size / 10)])
    offset2 = 2000
    val1 = np.random.randint(0, int(size / 5 - offset1))
    val2 = np.random.randint(0, 8000)

    output_file.write(
        "-- Test for a clustered index select followed by a second predicate\n"
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT sum(col1) FROM tbl4 WHERE (col3 >= {val1} and col3 < {val1 + offset1}) AND (col2 >= {val2} and col2 < {val2 + offset2});\n"
        "--\n"
        f"s1=select(db1.tbl4.col3,{val1},{val1 + offset1})\n"
        "f1=fetch(db1.tbl4.col2,s1)\n"
        f"s2=select(s1,f1,{val2},{val2 + offset2})\n"
        "f2=fetch(db1.tbl4.col1,s2)\n"
        "print(f2)\n"
        "a1=sum(f2)\n"
        "print(a1)\n"
    )

    df_select = df.filter(
        (pl.col("col3") >= val1)
        & (pl.col("col3") < val1 + offset1)
        & (pl.col("col2") >= val2)
        & (pl.col("col2") < val2 + offset2)
    )
    exp_output_file.write(utils.print_output(df_select["col1"], PREC) + "\n\n")
    exp_output_file.write(str(int(df_select["col1"].sum())) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_tests_25_26(df, size):
    output_file25, exp_output_file25 = utils.open_files(25, TEST_BASE_DIR)
    output_file26, exp_output_file26 = utils.open_files(26, TEST_BASE_DIR)
    offset = np.max([2, int(size / 1000)])
    output_file25.write(
        "-- Test for a non-clustered index select followed by an aggregate (control-test)\n"
        "--\n"
        "-- Query form in SQL:\n"
        "-- SELECT avg(col3) FROM tbl4_ctrl WHERE (col2 >= _ and col2 < _);\n"
        "--\n"
    )
    output_file26.write(
        "-- Test for a non-clustered index select followed by an aggregate (control-test)\n"
        "--\n"
        "-- Query form in SQL:\n"
        "-- SELECT sum(col3) FROM tbl4 WHERE (col2 >= _ and col2 < _);\n"
        "--\n"
    )

    for i in range(10):
        val = np.random.randint(0, int(size / 5 - offset))
        output_file25.write(
            f"s{i}=select(db1.tbl4_ctrl.col2,{val},{val + offset})\n"
            f"f{i}=fetch(db1.tbl4_ctrl.col3,s{i})\n"
            f"a{i}=sum(f{i})\n"
            f"print(a{i})\n"
        )
        output_file26.write(
            f"s{i}=select(db1.tbl4.col2,{val},{val + offset})\n"
            f"f{i}=fetch(db1.tbl4.col3,s{i})\n"
            f"a{i}=sum(f{i})\n"
            f"print(a{i})\n"
        )

        df_select = df.filter((pl.col("col2") >= val) & (pl.col("col2") < val + offset))
        exp_output_file25.write(str(int(df_select["col3"].sum())) + "\n")
        exp_output_file26.write(str(int(df_select["col3"].sum())) + "\n")

    utils.close_files(output_file25, exp_output_file25)
    utils.close_files(output_file26, exp_output_file26)


def create_test_27(df, freq_val1, freq_val2):
    output_file, exp_output_file = utils.open_files(27, TEST_BASE_DIR)
    output_file.write(
        "-- Test for a clustered index select followed by a second predicate\n"
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= {freq_val1 - 1} and col2 < {freq_val1 + 1});\n"
        f"-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= {freq_val2 - 1} and col2 < {freq_val2 + 1});\n"
        "--\n"
        f"s1=select(db1.tbl4.col2,{freq_val1 - 1},{freq_val1 + 1})\n"
        "f1=fetch(db1.tbl4.col1,s1)\n"
        "a1=sum(f1)\n"
        "print(a1)\n"
        f"s2=select(db1.tbl4.col2,{freq_val2 - 1},{freq_val2 + 1})\n"
        "f2=fetch(db1.tbl4.col1,s2)\n"
        "a2=sum(f2)\n"
        "print(a2)\n"
    )

    df_select1 = df.filter(
        (pl.col("col2") >= freq_val1 - 1) & (pl.col("col2") < freq_val1 + 1)
    )
    df_select2 = df.filter(
        (pl.col("col2") >= freq_val2 - 1) & (pl.col("col2") < freq_val2 + 1)
    )
    exp_output_file.write(str(int(df_select1["col1"].sum())) + "\n")
    exp_output_file.write(str(int(df_select2["col1"].sum())) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_tests_28_29(df, size):
    output_file28, exp_output_file28 = utils.open_files(28, TEST_BASE_DIR)
    output_file29, exp_output_file29 = utils.open_files(29, TEST_BASE_DIR)
    offset = np.max([2, int(size / 500)])
    output_file28.write(
        "-- Test for a non-clustered index select followed by an aggregate (control-test, many queries)\n"
        "-- Compare to test 29 for timing differences between B-tree and scan for highly selective queries\n"
        "--\n"
        "-- Query form in SQL:\n"
        "-- SELECT avg(col3) FROM tbl4_ctrl WHERE (col2 >= _ and col2 < _);\n"
        "--\n"
    )
    output_file29.write(
        "-- Test for a non-clustered index select followed by an aggregate (many queries)\n"
        "--\n"
        "-- Query form in SQL:\n"
        "-- SELECT avg(col3) FROM tbl4 WHERE (col2 >= _ and col2 < _);\n"
        "--\n"
    )

    for i in range(100):
        val = np.random.randint(0, int(size / 5 - offset))
        output_file28.write(
            f"s{i}=select(db1.tbl4_ctrl.col2,{val},{val + offset})\n"
            f"f{i}=fetch(db1.tbl4_ctrl.col3,s{i})\n"
            f"a{i}=avg(f{i})\n"
            f"print(a{i})\n"
        )
        output_file29.write(
            f"s{i}=select(db1.tbl4.col2,{val},{val + offset})\n"
            f"f{i}=fetch(db1.tbl4.col3,s{i})\n"
            f"a{i}=avg(f{i})\n"
            f"print(a{i})\n"
        )

        df_select = df.filter((pl.col("col2") >= val) & (pl.col("col2") < val + offset))
        mean_result = utils.nantozero(df_select["col3"].mean())
        exp_output_file28.write(f"{mean_result:0.{PREC}f}\n")
        exp_output_file29.write(f"{mean_result:0.{PREC}f}\n")

    utils.close_files(output_file28, exp_output_file28)
    utils.close_files(output_file29, exp_output_file29)


def create_test_30():
    output_file, exp_output_file = utils.open_files(30, TEST_BASE_DIR)
    output_file.write(
        "-- Test for creating table with indexes\n"
        "--\n"
        "-- Table tbl4_clustered_btree has a clustered index with col3 being the leading column.\n"
        "-- The clustered index has the form of a B-Tree.\n"
        "-- The table also has a secondary sorted index.\n"
        "--\n"
        "-- Loads data from: data4_clustered_btree.csv\n"
        "--\n"
        "-- Create Table\n"
        'create(tbl,"tbl4_clustered_btree",db1,4)\n'
        'create(col,"col1",db1.tbl4_clustered_btree)\n'
        'create(col,"col2",db1.tbl4_clustered_btree)\n'
        'create(col,"col3",db1.tbl4_clustered_btree)\n'
        'create(col,"col4",db1.tbl4_clustered_btree)\n'
        "-- Create a clustered index on col3\n"
        "create(idx,db1.tbl4_clustered_btree.col3,btree,clustered)\n"
        "-- Create an unclustered btree index on col2\n"
        "create(idx,db1.tbl4_clustered_btree.col2,sorted,unclustered)\n"
        "--\n"
        "--\n"
        "-- Load data immediately in the form of a clustered index\n"
        f'load("{DOCKER_TEST_BASE_DIR}/data4_clustered_btree.csv")\n'
        "--\n"
        "-- Testing that the data and their indexes are durable on disk.\n"
        "shutdown\n"
    )
    utils.close_files(output_file, exp_output_file)


def create_test_31(df, size):
    output_file, exp_output_file = utils.open_files(31, TEST_BASE_DIR)
    offset1 = np.max([1, int(size / 5000)])
    offset2 = np.max([2, int(size / 2500)])
    val1 = np.random.randint(0, int(size / 5 - offset1))
    val2 = np.random.randint(0, int(size / 5 - offset2))

    output_file.write(
        "--\n"
        "-- tbl4_clustered_btree has a secondary sorted index on col2, and a clustered b-tree index on col3\n"
        "-- testing for correctness\n"
        "--\n"
        "-- Query in SQL:\n"
        f"-- SELECT col1 FROM tbl4_clustered_btree WHERE col3 >= {val1} and col3 < {val1 + offset1};\n"
        f"-- SELECT col1 FROM tbl4_clustered_btree WHERE col3 >= {val2} and col3 < {val2 + offset2};\n"
        "--\n"
        "-- since col3 has a clustered index, the index is expected to be used by the select operator\n"
        f"s1=select(db1.tbl4_clustered_btree.col3,{val1},{val1 + offset1})\n"
        "f1=fetch(db1.tbl4_clustered_btree.col1,s1)\n"
        "print(f1)\n"
        f"s2=select(db1.tbl4_clustered_btree.col3,{val2},{val2 + offset2})\n"
        "f2=fetch(db1.tbl4_clustered_btree.col1,s2)\n"
        "print(f2)\n"
    )

    df_select1 = df.filter((pl.col("col3") >= val1) & (pl.col("col3") < val1 + offset1))
    df_select2 = df.filter((pl.col("col3") >= val2) & (pl.col("col3") < val2 + offset2))
    exp_output_file.write(utils.print_output(df_select1["col1"], PREC) + "\n\n")
    exp_output_file.write(utils.print_output(df_select2["col1"], PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_32(df, size):
    output_file, exp_output_file = utils.open_files(32, TEST_BASE_DIR)
    offset = np.max([2, int(size / 1000)])
    output_file.write(
        "-- Test for a non-clustered index select followed by an aggregate\n"
        "--\n"
        "-- Query form in SQL:\n"
        "-- SELECT sum(col3) FROM tbl4_clustered_btree WHERE (col2 >= _ and col2 < _);\n"
        "--\n"
    )

    for i in range(5):
        val = np.random.randint(0, int(size / 5 - offset))
        output_file.write(
            f"s{i}=select(db1.tbl4_clustered_btree.col2,{val},{val + offset})\n"
            f"f{i}=fetch(db1.tbl4_clustered_btree.col3,s{i})\n"
            f"a{i}=sum(f{i})\n"
            f"print(a{i})\n"
        )

        df_select = df.filter((pl.col("col2") >= val) & (pl.col("col2") < val + offset))
        exp_output_file.write(str(int(df_select["col3"].sum())) + "\n")

    utils.close_files(output_file, exp_output_file)


def create_tests_33_to_38(df, size):
    table_names = ["tbl4_ctrl", "tbl4", "tbl4_clustered_btree"]
    selectivities = ["0.1%", "1%"]
    offsets = [np.max([1, int(size / 5000)]), np.max([2, int(size / 500)])]

    for num, ((offset, selectivity), table_name) in enumerate(
        product(zip(offsets, selectivities), table_names), start=33
    ):
        output_file, exp_output_file = utils.open_files(num, TEST_BASE_DIR)
        output_file.write(
            "--\n"
            f"-- selectivity={selectivity}\n"
            "-- Query in SQL:\n"
            f"-- SELECT avg(col1) FROM {table_name} WHERE col3 >= _ and col3 < _;\n"
            "--\n"
        )

        for i in range(20):
            val = np.random.randint(0, int(size / 5 - offset))
            output_file.write(
                f"s{i}=select(db1.{table_name}.col3,{val},{val + offset})\n"
                f"f{i}=fetch(db1.{table_name}.col1,s{i})\n"
                f"a{i}=avg(f{i})\n"
                f"print(a{i})\n"
            )

            df_select = df.filter(
                (pl.col("col3") >= val) & (pl.col("col3") < val + offset)
            )
            mean_result = utils.nantozero(df_select["col1"].mean())
            exp_output_file.write(f"{mean_result:0.{PREC}f}\n")

        utils.close_files(output_file, exp_output_file)


def create_tests_39_to_44(df, size):
    table_names = ["tbl4_ctrl", "tbl4", "tbl4_clustered_btree"]
    selectivities = ["0.1%", "1%"]
    offsets = [np.max([1, int(size / 5000)]), np.max([2, int(size / 500)])]

    for num, ((offset, selectivity), table_name) in enumerate(
        product(zip(offsets, selectivities), table_names), start=39
    ):
        output_file, exp_output_file = utils.open_files(num, TEST_BASE_DIR)
        output_file.write(
            "--\n"
            f"-- selectivity={selectivity}\n"
            "-- Query in SQL:\n"
            f"-- SELECT avg(col3) FROM {table_name} WHERE col2 >= _ and col2 < _;\n"
            "--\n"
        )

        for i in range(20):
            val = np.random.randint(0, int(size / 5 - offset))
            output_file.write(
                f"s{i}=select(db1.{table_name}.col2,{val},{val + offset})\n"
                f"f{i}=fetch(db1.{table_name}.col3,s{i})\n"
                f"a{i}=avg(f{i})\n"
                f"print(a{i})\n"
            )

            df_select = df.filter(
                (pl.col("col2") >= val) & (pl.col("col2") < val + offset)
            )
            mean_result = utils.nantozero(df_select["col3"].mean())
            exp_output_file.write(f"{mean_result:0.{PREC}f}\n")

        utils.close_files(output_file, exp_output_file)


def generate_milestone_3_files(size, seed):
    np.random.seed(seed)
    freq_val1, freq_val2, df = generate_data_milestone_3(size)
    create_test_20()
    create_test_21()
    create_tests_22_23(df, size)
    create_test_24(df, size)
    create_tests_25_26(df, size)
    create_test_27(df, freq_val1, freq_val2)
    create_tests_28_29(df, size)
    create_test_30()
    create_test_31(df, size)
    create_test_32(df, size)
    create_tests_33_to_38(df, size)
    create_tests_39_to_44(df, size)


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

    generate_milestone_3_files(args.size, args.seed)


if __name__ == "__main__":
    main()
