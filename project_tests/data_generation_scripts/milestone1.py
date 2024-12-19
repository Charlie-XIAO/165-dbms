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


def generate_data_file_midway_checkin():
    output_file = f"{TEST_BASE_DIR}/data1_generated.csv"
    header = utils.generate_header("db1", "tbl1", 2)
    column1 = list(range(0, 1000))
    column2 = list(range(10, 1010))
    np.random.seed(47)  # Exact same seed as on the server
    np.random.shuffle(column2)
    df = pl.DataFrame({"col1": column1, "col2": column2})
    df.rename({"col1": header[0], "col2": header[1]}).write_csv(output_file)
    return df


def create_test_1():
    output_file, exp_output_file = utils.open_files(1, TEST_BASE_DIR)
    output_file.write(
        "-- Load+create Data and shut down of tbl1 which has 1 attribute only\n"
        'create(db,"db1")\n'
        'create(tbl,"tbl1",db1,2)\n'
        'create(col,"col1",db1.tbl1)\n'
        'create(col,"col2",db1.tbl1)\n'
        f'load("{DOCKER_TEST_BASE_DIR}/data1_generated.csv")\n'
        "shutdown\n"
    )
    utils.close_files(output_file, exp_output_file)


def create_test_2(df):
    output_file, exp_output_file = utils.open_files(2, TEST_BASE_DIR)
    output_file.write("-- Test Select + Fetch\n")

    # Query 1
    select_val_less = 20
    output_file.write(
        "--\n"
        f"-- SELECT col1 FROM tbl1 WHERE col1 < {select_val_less};\n"
        f"s1=select(db1.tbl1.col1,null,{select_val_less})\n"
        "f1=fetch(db1.tbl1.col1,s1)\n"
        "print(f1)\n"
    )
    df_select = df.filter(pl.col("col1") < select_val_less)
    exp_output_file.write(utils.print_output(df_select["col1"], PREC) + "\n")

    # Query 2
    select_val_greater = 987
    output_file.write(
        "--\n"
        f"-- SELECT col2 FROM tbl1 WHERE col1 >= {select_val_greater};\n"
        f"s2=select(db1.tbl1.col1,{select_val_greater},null)\n"
        "f2=fetch(db1.tbl1.col2,s2)\n"
        "print(f2)\n"
    )
    df_select = df.filter(pl.col("col1") >= select_val_greater)
    exp_output_file.write(utils.print_output(df_select["col2"], PREC) + "\n")

    utils.close_files(output_file, exp_output_file)


def create_test_3(df):
    output_file, exp_output_file = utils.open_files(3, TEST_BASE_DIR)
    output_file.write("-- Test Multiple Selects + Average\n")

    # Query
    select_val_less, select_val_greater = 956, 972
    output_file.write(
        "--\n"
        f"-- SELECT avg(col2) FROM tbl1 WHERE col1 >= {select_val_less} and col1 < {select_val_greater};\n"
        f"s1=select(db1.tbl1.col1,{select_val_less},{select_val_greater})\n"
        "f1=fetch(db1.tbl1.col2,s1)\n"
        "a1=avg(f1)\n"
        "print(a1)\n"
    )
    df_select = df.filter(
        (pl.col("col1") >= select_val_less) & (pl.col("col1") < select_val_greater)
    )
    output = utils.nantozero(df_select["col2"].mean())
    exp_output_file.write(f"{output:0.{PREC}f}\n")

    utils.close_files(output_file, exp_output_file)


def generate_data_file_2(size):
    output_file = f"{TEST_BASE_DIR}/data2_generated.csv"
    header = utils.generate_header("db1", "tbl2", 4)
    df = pl.DataFrame(
        {
            "col1": np.random.randint(-size // 2, size // 2, size=size),
            "col2": np.random.randint(-size // 2, size // 2, size=size),
            "col3": np.random.randint(0, 100, size=size),  # Many duplicates
            "col4": np.random.randint(2**31 - 10000, 2**31, size=size),
        }
    )
    df = df.with_columns((df["col2"] + df["col1"]).alias("col2"))
    df.rename({f"col{i + 1}": header[i] for i in range(4)}).write_csv(output_file)
    return df


def create_test_4(df):
    output_file, exp_output_file = utils.open_files(4, TEST_BASE_DIR)
    output_file.write(
        "-- Load Test Data 2\n"
        "--\n"
        "-- Load+create+insert Data and shut down of tbl2 which has 4 attributes\n"
        'create(tbl,"tbl2",db1,4)\n'
        'create(col,"col1",db1.tbl2)\n'
        'create(col,"col2",db1.tbl2)\n'
        'create(col,"col3",db1.tbl2)\n'
        'create(col,"col4",db1.tbl2)\n'
        f'load("{DOCKER_TEST_BASE_DIR}/data2_generated.csv")\n'
        "relational_insert(db1.tbl2,-1,-11,-111,-1111)\n"
        "relational_insert(db1.tbl2,-2,-22,-222,-2222)\n"
        "relational_insert(db1.tbl2,-3,-33,-333,-2222)\n"
        "relational_insert(db1.tbl2,-4,-44,-444,-2222)\n"
        "relational_insert(db1.tbl2,-5,-55,-555,-2222)\n"
        "relational_insert(db1.tbl2,-6,-66,-666,-2222)\n"
        "relational_insert(db1.tbl2,-7,-77,-777,-2222)\n"
        "relational_insert(db1.tbl2,-8,-88,-888,-2222)\n"
        "relational_insert(db1.tbl2,-9,-99,-999,-2222)\n"
        "relational_insert(db1.tbl2,-10,-11,0,-34)\n"
        "shutdown\n"
    )

    df_delta = pl.DataFrame(
        [
            [-1, -11, -111, -1111],
            [-2, -22, -222, -2222],
            [-3, -33, -333, -2222],
            [-4, -44, -444, -2222],
            [-5, -55, -555, -2222],
            [-6, -66, -666, -2222],
            [-7, -77, -777, -2222],
            [-8, -88, -888, -2222],
            [-9, -99, -999, -2222],
            [-10, -11, 0, -34],
        ],
        schema=["col1", "col2", "col3", "col4"],
        orient="row",
    )
    df = pl.concat([df, df_delta], how="vertical")
    utils.close_files(output_file, exp_output_file)
    return df


def create_test_5(df, size, selectivity):
    output_file, exp_output_file = utils.open_files(5, TEST_BASE_DIR)
    offset = int(selectivity * size)
    highest_high_val = int(size / 2 - offset)
    select_val_less = np.random.randint(int(-size / 2), highest_high_val)
    select_val_greater = select_val_less + offset
    output_file.write(
        "-- Summation\n"
        "--\n"
        f"-- SELECT SUM(col3) FROM tbl2 WHERE col1 >= {select_val_less} AND col1 < {select_val_greater};\n"
        f"s1=select(db1.tbl2.col1,{select_val_less},{select_val_greater})\n"
        "f1=fetch(db1.tbl2.col3,s1)\n"
        "a1=sum(f1)\n"
        "print(a1)\n"
        "--\n"
        "-- SELECT SUM(col1) FROM tbl2;\n"
        "a2=sum(db1.tbl2.col1)\n"
        "print(a2)\n"
    )

    df_select = df.filter(
        (pl.col("col1") >= select_val_less) & (pl.col("col1") < select_val_greater)
    )
    exp_output_file.write(str(int(df_select["col3"].sum())) + "\n")
    exp_output_file.write(str(int(df["col1"].sum())) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_6(df, size, n_outputs):
    output_file, exp_output_file = utils.open_files(6, TEST_BASE_DIR)
    offset = n_outputs
    highest_high_val = int(size / 2 - offset)
    select_val_less = np.random.randint(int(-size / 2), highest_high_val)
    select_val_greater = select_val_less + offset
    output_file.write(
        "-- Addition\n"
        "--\n"
        f"-- SELECT col2+col3 FROM tbl2 WHERE col1 >= {select_val_less} AND col1 < {select_val_greater};\n"
        f"s11=select(db1.tbl2.col1,{select_val_less},{select_val_greater})\n"
        "f11=fetch(db1.tbl2.col2,s11)\n"
        "f12=fetch(db1.tbl2.col3,s11)\n"
        "a11=add(f11,f12)\n"
        "print(a11)\n"
    )

    df_select = df.filter(
        (pl.col("col1") >= select_val_less) & (pl.col("col1") < select_val_greater)
    )
    output = df_select["col2"] + df_select["col3"]
    exp_output_file.write(utils.print_output(output, PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_7(df, size, n_outputs):
    output_file, exp_output_file = utils.open_files(7, TEST_BASE_DIR)
    offset = n_outputs
    highest_high_val = int(size / 2 - offset)
    select_val_less = np.random.randint(int(-size / 2), highest_high_val)
    select_val_greater = select_val_less + offset
    output_file.write(
        "-- Subtraction\n"
        "--\n"
        f"-- SELECT col3-col2 FROM tbl2 WHERE col1 >= {select_val_less} AND col1 < {select_val_greater};\n"
        f"s21=select(db1.tbl2.col1,{select_val_less},{select_val_greater})\n"
        "f21=fetch(db1.tbl2.col2,s21)\n"
        "f22=fetch(db1.tbl2.col3,s21)\n"
        "s21=sub(f22,f21)\n"
        "print(s21)\n"
    )

    df_select = df.filter(
        (pl.col("col1") >= select_val_less) & (pl.col("col1") < select_val_greater)
    )
    output = df_select["col3"] - df_select["col2"]
    exp_output_file.write(utils.print_output(output, PREC) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_8(df, size, selectivity):
    output_file, exp_output_file = utils.open_files(8, TEST_BASE_DIR)
    offset = int(selectivity * size)
    highest_high_val = int(size / 2 - offset)
    select_val_less = np.random.randint(int(-size / 2), highest_high_val)
    select_val_greater = select_val_less + offset
    output_file.write(
        "-- Min,Max\n"
        "--\n"
        "-- Min\n"
        "-- SELECT min(col1) FROM tbl2;\n"
        "a1=min(db1.tbl2.col1)\n"
        "print(a1)\n"
        "--\n"
        "--\n"
        f"-- SELECT min(col1) FROM tbl2 WHERE col1 >= {select_val_less} AND col1 < {select_val_greater};\n"
        f"s1=select(db1.tbl2.col1,{select_val_less},{select_val_greater})\n"
        "f1=fetch(db1.tbl2.col1,s1)\n"
        "m1=min(f1)\n"
        "print(m1)\n"
        "--\n"
        f"-- SELECT min(col2) FROM tbl2 WHERE col1 >= {select_val_less} AND col1 < {select_val_greater};\n"
        "f2=fetch(db1.tbl2.col2,s1)\n"
        "m2=min(f2)\n"
        "print(m2)\n"
        "--\n"
        "--\n"
        "-- Max\n"
        "-- SELECT max(col1) FROM tbl2;\n"
        "a2=max(db1.tbl2.col1)\n"
        "print(a2)\n"
        "--\n"
        "--\n"
        f"-- SELECT max(col1) FROM tbl2 WHERE col1 >= {select_val_less} AND col1 < {select_val_greater};\n"
        f"s21=select(db1.tbl2.col1,{select_val_less},{select_val_greater})\n"
        "f21=fetch(db1.tbl2.col1,s21)\n"
        "m21=max(f21)\n"
        "print(m21)\n"
        "--\n"
        f"-- SELECT max(col2) FROM tbl2 WHERE col1 >= {select_val_less} AND col1 < {select_val_greater};\n"
        "f22=fetch(db1.tbl2.col2,s21)\n"
        "m22=max(f22)\n"
        "print(m22)\n"
    )

    df_select = df.filter(
        (pl.col("col1") >= select_val_less) & (pl.col("col1") < select_val_greater)
    )
    exp_output_file.write(str(int(df["col1"].min())) + "\n")
    exp_output_file.write(str(int(df_select["col1"].min())) + "\n")
    exp_output_file.write(str(int(df_select["col2"].min())) + "\n")
    exp_output_file.write(str(int(df["col1"].max())) + "\n")
    exp_output_file.write(str(int(df_select["col1"].max())) + "\n")
    exp_output_file.write(str(int(df_select["col2"].max())) + "\n")
    utils.close_files(output_file, exp_output_file)


def create_test_9(df, size, selectivity):
    output_file, exp_output_file = utils.open_files(9, TEST_BASE_DIR)
    selectivity_each_subclause = np.sqrt(selectivity)
    offset = int(selectivity_each_subclause * size)
    highest_high_val = int(size / 2 - offset)
    select_val_less1 = np.random.randint(int(-size / 2), highest_high_val)
    select_val_less2 = np.random.randint(int(-size / 2), highest_high_val)
    select_val_greater1 = select_val_less1 + offset
    select_val_greater2 = select_val_less2 + offset
    output_file.write(
        "-- Big Bad Boss Test! Milestone 1\n"
        "-- It's basically just the previous tests put together\n"
        "-- But also, its.... Boss test!\n\n"
        # First query
        f"-- SELECT min(col2), max(col3), sum(col3-col2) FROM tbl2 WHERE (col1 >= {select_val_less1} AND col1 < {select_val_greater1}) AND (col2 >= {select_val_less2} AND col2 < {select_val_greater2});\n"
        f"s1=select(db1.tbl2.col1,{select_val_less1},{select_val_greater1})\n"
        "sf1=fetch(db1.tbl2.col2,s1)\n"
        f"s2=select(s1,sf1,{select_val_less2},{select_val_greater2})\n"
        "f2=fetch(db1.tbl2.col2,s2)\n"
        "f3=fetch(db1.tbl2.col3,s2)\n"
        "out11=min(f2)\n"
        "out12=max(f3)\n"
        "sub32=sub(f3,f2)\n"
        "out13=sum(sub32)\n"
        "print(out11,out12,out13)\n\n\n"
        # Second query
        f"-- SELECT avg(col1+col2), min(col2), max(col3), avg(col3-col2), sum(col3-col2) FROM tbl2 WHERE (col1 >= {select_val_less1} AND col1 < {select_val_greater1}) AND (col2 >= {select_val_less2} AND col2 < {select_val_greater2});\n"
        f"s1=select(db1.tbl2.col1,{select_val_less1},{select_val_greater1})\n"
        "sf1=fetch(db1.tbl2.col2,s1)\n"
        f"s2=select(s1,sf1,{select_val_less2},{select_val_greater2})\n"
        "f1=fetch(db1.tbl2.col1,s2)\n"
        "f2=fetch(db1.tbl2.col2,s2)\n"
        "f3=fetch(db1.tbl2.col3,s2)\n"
        "add12=add(f1,f2)\n"
        "out1=avg(add12)\n"
        "out2=min(f2)\n"
        "out3=max(f3)\n"
        "sub32=sub(f3,f2)\n"
        "out4=avg(sub32)\n"
        "out5=sum(sub32)\n"
        "print(out1,out2,out3,out4,out5)\n"
    )

    df_select = df.filter(
        (pl.col("col1") >= select_val_less1)
        & (pl.col("col1") < select_val_greater1)
        & (pl.col("col2") >= select_val_less2)
        & (pl.col("col2") < select_val_greater2)
    )
    col1pluscol2 = df_select["col1"] + df_select["col2"]
    col3minuscol2 = df_select["col3"] - df_select["col2"]

    # First query
    exp_output_file.write(str(int(df_select["col2"].min())) + ",")
    exp_output_file.write(str(int(df_select["col3"].max())) + ",")
    exp_output_file.write(str(int(col3minuscol2.sum())) + "\n")

    # Second query
    exp_output_file.write(f"{utils.nantozero(col1pluscol2.mean()):0.{PREC}f},")
    exp_output_file.write(str(int(df_select["col2"].min())) + ",")
    exp_output_file.write(str(int(df_select["col3"].max())) + ",")
    exp_output_file.write(f"{utils.nantozero(col3minuscol2.mean()):0.{PREC}f},")
    exp_output_file.write(str(int(col3minuscol2.sum())) + "\n")

    utils.close_files(output_file, exp_output_file)


def generate_milestone_1_files(size, seed):
    # Midway check-in
    df1 = generate_data_file_midway_checkin()
    create_test_1()
    create_test_2(df1)
    create_test_3(df1)

    # Other tests for milestone 1
    np.random.seed(seed)
    df2 = generate_data_file_2(size)
    df2 = create_test_4(df2)
    create_test_5(df2, size, 0.8)
    create_test_6(df2, size, 20)
    create_test_7(df2, size, 20)
    create_test_8(df2, size, 0.1)
    create_test_9(df2, size, 0.1)


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

    generate_milestone_1_files(args.size, args.seed)


if __name__ == "__main__":
    main()
