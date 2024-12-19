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


def generate_data_milestone_5(size):
    output_file = f"{TEST_BASE_DIR}/data5.csv"
    header = utils.generate_header("db1", "tbl5", 4)
    df = pl.DataFrame(
        {
            "col1": np.random.randint(0, 1000, size=size),
            "col2": np.random.randint(0, 1000, size=size),
            "col3": np.random.randint(0, 10000, size=size),
            "col4": np.random.randint(0, 10000, size=size),
        }
    )
    df.rename({f"col{i + 1}": header[i] for i in range(4)}).write_csv(output_file)
    return df


def create_test_60(df):
    output_file, exp_output_file = utils.open_files(60, TEST_BASE_DIR)
    output_file.write(
        "-- Correctness test: Do inserts in tbl5.\n"
        "--\n"
        "-- Let table tbl5 have a secondary index (col2) and a clustered index (col3), so, all should be maintained when we insert new data.\n"
        "-- This means that the table should be always sorted on col3 and the secondary indexes on col2 should be updated\n"
        "--\n"
        "-- Create Table\n"
        'create(tbl,"tbl5",db1,4)\n'
        'create(col,"col1",db1.tbl5)\n'
        'create(col,"col2",db1.tbl5)\n'
        'create(col,"col3",db1.tbl5)\n'
        'create(col,"col4",db1.tbl5)\n'
        "-- Create a clustered index on col3\n"
        "create(idx,db1.tbl5.col3,sorted,clustered)\n"
        "-- Create an unclustered btree index on col2\n"
        "create(idx,db1.tbl5.col2,btree,unclustered)\n"
        "--\n"
        "--\n"
        "-- Load data immediately in the form of a clustered index\n"
        f'load("{DOCKER_TEST_BASE_DIR}/data5.csv")\n'
        "--\n"
        "-- INSERT INTO tbl5 VALUES (-1,-11,-111,-1111);\n"
        "-- INSERT INTO tbl5 VALUES (-2,-22,-222,-2222);\n"
        "-- INSERT INTO tbl5 VALUES (-3,-33,-333,-2222);\n"
        "-- INSERT INTO tbl5 VALUES (-4,-44,-444,-2222);\n"
        "-- INSERT INTO tbl5 VALUES (-5,-55,-555,-2222);\n"
        "--\n"
        "relational_insert(db1.tbl5,-1,-11,-111,-1111)\n"
        "relational_insert(db1.tbl5,-2,-22,-222,-2222)\n"
        "relational_insert(db1.tbl5,-3,-33,-333,-2222)\n"
        "relational_insert(db1.tbl5,-4,-44,-444,-2222)\n"
        "relational_insert(db1.tbl5,-5,-55,-555,-2222)\n"
    )

    df_delta = pl.DataFrame(
        [
            [-1, -11, -111, -1111],
            [-2, -22, -222, -2222],
            [-3, -33, -333, -3333],
            [-4, -44, -444, -4444],
            [-5, -55, -555, -5555],
        ],
        schema=["col1", "col2", "col3", "col4"],
        orient="row",
    )
    df = pl.concat([df, df_delta], how="vertical")
    utils.close_files(output_file, exp_output_file)
    return df


def create_test_61(df, selectivity):
    output_file, exp_output_file = utils.open_files(61, TEST_BASE_DIR)
    offset = int(selectivity * len(df))
    select_val_less1 = np.random.randint(-55, -11)
    select_val_greater1 = select_val_less1 + offset
    select_val_less2 = np.random.randint(-10, 0)
    select_val_greater2 = select_val_less2 + offset

    output_file.write(
        "-- Correctness test: Test for updates on columns with index\n"
        "--\n"
        f"-- SELECT col1 FROM tbl5 WHERE col2 >= {select_val_less1} AND col2 < {select_val_greater1};\n"
        "--\n"
        f"s1=select(db1.tbl5.col2,{select_val_less1},{select_val_greater1})\n"
        "f1=fetch(db1.tbl5.col1,s1)\n"
        "print(f1)\n"
        "--\n"
        f"-- SELECT col3 FROM tbl5 WHERE col1 >= {select_val_less2} AND col1 < {select_val_greater2};\n"
        "--\n"
        f"s2=select(db1.tbl5.col1,{select_val_less2},{select_val_greater2})\n"
        "f2=fetch(db1.tbl5.col3,s2)\n"
        "print(f2)\n"
    )

    df_select1 = df.filter(
        (pl.col("col2") >= select_val_less1) & (pl.col("col2") < select_val_greater1)
    )
    df_select2 = df.filter(
        (pl.col("col1") >= select_val_less2) & (pl.col("col1") < select_val_greater2)
    )
    exp_output_file.write(utils.print_output(df_select1["col1"], PREC) + "\n\n")
    exp_output_file.write(utils.print_output(df_select2["col3"], PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_62(df):
    output_file, exp_output_file = utils.open_files(62, TEST_BASE_DIR)
    output_file.write(
        "-- Correctness test: Update values\n"
        "--\n"
        "-- UPDATE tbl5 SET col1 = -10 WHERE col1 = -1;\n"
        "-- UPDATE tbl5 SET col1 = -20 WHERE col2 = -22;\n"
        "-- UPDATE tbl5 SET col1 = -30 WHERE col1 = -3;\n"
        "-- UPDATE tbl5 SET col1 = -40 WHERE col3 = -444;\n"
        "-- UPDATE tbl5 SET col1 = -50 WHERE col1 = -5;\n"
        "--\n"
        "u1=select(db1.tbl5.col1,-1,0)\n"
        "relational_update(db1.tbl5.col1,u1,-10)\n"
        "u2=select(db1.tbl5.col2,-22,-21)\n"
        "relational_update(db1.tbl5.col1,u2,-20)\n"
        "u3=select(db1.tbl5.col1,-3,-2)\n"
        "relational_update(db1.tbl5.col1,u3,-30)\n"
        "u4=select(db1.tbl5.col3,-444,-443)\n"
        "relational_update(db1.tbl5.col1,u4,-40)\n"
        "u5=select(db1.tbl5.col1,-5,-4)\n"
        "relational_update(db1.tbl5.col1,u5,-50)\n"
        "shutdown\n"
    )

    df = df.with_columns(
        pl.when(pl.col("col1") == -1)
        .then(-10)
        .when(pl.col("col2") == -22)
        .then(-20)
        .when(pl.col("col1") == -3)
        .then(-30)
        .when(pl.col("col3") == -444)
        .then(-40)
        .when(pl.col("col1") == -5)
        .then(-50)
        .otherwise(pl.col("col1"))
        .alias("col1")
    )
    utils.close_files(output_file, exp_output_file)
    return df


def create_test_63(df):
    output_file, exp_output_file = utils.open_files(63, TEST_BASE_DIR)
    select_val_less = np.random.randint(-200, -100)
    select_val_greater = np.random.randint(10, 100)
    output_file.write(
        "-- Correctness test: Run query after inserts and updates\n"
        "--\n"
        f"-- SELECT col1 FROM tbl5 WHERE col2 >= {select_val_less} AND col2 < {select_val_greater};\n"
        "--\n"
        f"s1=select(db1.tbl5.col2,{select_val_less},{select_val_greater})\n"
        "f1=fetch(db1.tbl5.col1,s1)\n"
        "print(f1)\n"
    )

    df_select = df.filter(
        (pl.col("col2") >= select_val_less) & (pl.col("col2") < select_val_greater)
    )
    exp_output_file.write(utils.print_output(df_select["col1"], PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_64(df):
    output_file, exp_output_file = utils.open_files(64, TEST_BASE_DIR)
    output_file.write(
        "-- Correctness test: Delete values and run queries after inserts, updates, and deletes\n"
        "--\n"
        "-- DELETE FROM tbl5 WHERE col1 = -10;\n"
        "-- DELETE FROM tbl5 WHERE col2 = -22;\n"
        "-- DELETE FROM tbl5 WHERE col1 = -30;\n"
        "-- DELETE FROM tbl5 WHERE col3 = -444;\n"
        "-- DELETE FROM tbl5 WHERE col1 = -50;\n"
        "-- SELECT col1 FROM tbl5 WHERE col2 >= -100 AND col2 < 20;\n"
        "--\n"
        "d1=select(db1.tbl5.col1,-10,-9)\n"
        "relational_delete(db1.tbl5,d1)\n"
        "d2=select(db1.tbl5.col2,-22,-21)\n"
        "relational_delete(db1.tbl5,d2)\n"
        "d3=select(db1.tbl5.col1,-30,-29)\n"
        "relational_delete(db1.tbl5,d3)\n"
        "d4=select(db1.tbl5.col3,-444,-443)\n"
        "relational_delete(db1.tbl5,d4)\n"
        "d5=select(db1.tbl5.col1,-50,-49)\n"
        "relational_delete(db1.tbl5,d5)\n"
        "s1=select(db1.tbl5.col2,-100,20)\n"
        "f1=fetch(db1.tbl5.col1,s1)\n"
        "print(f1)\n"
    )

    df = df.filter(
        (pl.col("col1") != -10)
        & (pl.col("col2") != -22)
        & (pl.col("col1") != -30)
        & (pl.col("col3") != -444)
        & (pl.col("col1") != -50)
    )
    df_select = df.filter((pl.col("col2") >= -100) & (pl.col("col2") < 20))
    exp_output_file.write(utils.print_output(df_select["col1"], PREC) + "\n")
    utils.close_files(output_file, exp_output_file)
    return df


def create_random_updates(df, n_updates, output_file):
    for _ in range(n_updates):
        update_pos = np.random.randint(1, len(df) - 1)
        col1_val, col2_val = df[update_pos, 0], df[update_pos, 1]
        output_file.write(
            f"-- UPDATE tbl5 SET col1 = {col1_val + 1} WHERE col2 = {col2_val};\n"
            f"u1=select(db1.tbl5.col2,{col2_val},{col2_val + 1})\n"
            f"relational_update(db1.tbl5.col1,u1,{col1_val + 1})\n"
            "--\n"
        )
        df = df.with_columns(
            pl.when(pl.col("col2") == col2_val)
            .then(col1_val + 1)
            .otherwise(pl.col("col1"))
            .alias("col1")
        )
    return df


def create_random_deletes(df, n_deletes, output_file):
    for _ in range(n_deletes):
        delete_pos = np.random.randint(1, len(df) - 1)
        col1_val = df[delete_pos, 0]
        output_file.write(
            f"-- DELETE FROM tbl5 WHERE col1 = {col1_val};\n"
            f"d1=select(db1.tbl5.col1,{col1_val},{col1_val + 1})\n"
            f"relational_delete(db1.tbl5,d1)\n"
            "--\n"
        )
        df = df.filter(pl.col("col1") != col1_val)
    return df


def create_random_inserts(df, n_inserts, output_file):
    for _ in range(n_inserts):
        col1_val = np.random.randint(0, 1000)
        col2_val = np.random.randint(0, 1000)
        col3_val = np.random.randint(0, 10000)
        col4_val = np.random.randint(0, 10000)
        output_file.write(
            f"-- INSERT INTO tbl5 VALUES ({col1_val},{col2_val},{col3_val},{col4_val});\n"
            f"relational_insert(db1.tbl5,{col1_val},{col2_val},{col3_val},{col4_val})\n"
            "--\n"
        )
        new_row = pl.DataFrame(
            [[col1_val, col2_val, col3_val, col4_val]],
            schema=["col1", "col2", "col3", "col4"],
            orient="row",
        )
        df = pl.concat([df, new_row], how="vertical")
    return df


def create_random_selects(df, n_queries, output_file, exp_output_file):
    low, high = df["col2"].min(), df["col2"].max()
    for _ in range(n_queries):
        select_val_less = np.random.randint(low - 1, high - 1)
        select_val_greater = np.random.randint(select_val_less, high)
        output_file.write(
            f"-- SELECT col1 FROM tbl5 WHERE col2 >= {select_val_less} AND col2 < {select_val_greater};\n"
            f"s1=select(db1.tbl5.col2,{select_val_less},{select_val_greater})\n"
            "f1=fetch(db1.tbl5.col1,s1)\n"
            "print(f1)\n"
        )
        df_select = df.filter(
            (pl.col("col2") >= select_val_less) & (pl.col("col2") < select_val_greater)
        )
        exp_output_file.write(utils.print_output(df_select["col1"], PREC) + "\n")


def create_test_65(df):
    output_file, exp_output_file = utils.open_files(65, TEST_BASE_DIR)
    output_file.write(
        "-- Scalability test: A large number of inserts, deletes and updates, followed by a number of queries\n"
        "--\n"
    )
    df = create_random_inserts(df, 100, output_file)
    df = create_random_updates(df, 100, output_file)
    df = create_random_deletes(df, 100, output_file)
    create_random_selects(df, 5, output_file, exp_output_file)
    utils.close_files(output_file, exp_output_file)


def generate_milestone_5_files(size, seed):
    np.random.seed(seed)
    df = generate_data_milestone_5(size)
    df = create_test_60(df)
    create_test_61(df, 0.1)
    df = create_test_62(df)
    create_test_63(df)
    df = create_test_64(df)
    create_test_65(df)


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

    generate_milestone_5_files(args.size, args.seed)


if __name__ == "__main__":
    main()
