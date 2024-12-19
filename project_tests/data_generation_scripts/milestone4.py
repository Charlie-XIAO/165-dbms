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


class ZipfianDistribution:
    def __init__(self, zipfian_param, n_elements):
        self.zipfian_param = zipfian_param
        self.n_elements = n_elements

        # Compute the harmonic number
        self.h_s = np.sum(1.0 / np.arange(1, n_elements + 1) ** zipfian_param)

    def single_sample(self, unifSample):
        i, total = 0, 0.0
        while unifSample >= total:
            i += 1
            total += 1.0 / i**self.zipfian_param / self.h_s
        return i

    def sample(self, size):
        arr = np.random.uniform(size=size)
        return np.vectorize(self.single_sample)(arr)


def generate_data_milestone_4(
    size_fact, size_dim1, size_dim2, size_sel, zipfian_param, n_distinct
):
    output_file1 = f"{TEST_BASE_DIR}/data5_fact.csv"
    output_file2 = f"{TEST_BASE_DIR}/data5_dimension1.csv"
    output_file3 = f"{TEST_BASE_DIR}/data5_dimension2.csv"
    output_file4 = f"{TEST_BASE_DIR}/data5_selectivity1.csv"
    output_file5 = f"{TEST_BASE_DIR}/data5_selectivity2.csv"

    header_fact = utils.generate_header("db1", "tbl5_fact", 4)
    header_dim1 = utils.generate_header("db1", "tbl5_dim1", 3)
    header_dim2 = utils.generate_header("db1", "tbl5_dim2", 2)
    header_sel1 = utils.generate_header("db1", "tbl5_sel1", 2)
    header_sel2 = utils.generate_header("db1", "tbl5_sel2", 2)

    zipf_dist = ZipfianDistribution(zipfian_param, n_distinct)

    df_fact = pl.DataFrame(
        {
            header_fact[0]: zipf_dist.sample(size_fact),
            header_fact[1]: np.random.randint(0, size_fact // 5, size=size_fact),
            header_fact[2]: np.full(size_fact, 1),
            header_fact[3]: np.random.randint(1, size_dim2, size=size_fact),
        }
    )
    df_dim1 = pl.DataFrame(
        {
            header_dim1[0]: zipf_dist.sample(size_dim1),
            header_dim1[1]: np.random.randint(1, size_dim2, size=size_dim1),
            header_dim1[2]: np.random.randint(0, size_dim1 // 5, size=size_dim1),
        }
    )
    df_dim2 = pl.DataFrame(
        {
            header_dim2[0]: np.arange(1, size_dim2 + 1),
            header_dim2[1]: np.random.randint(0, size_dim2 // 5, size=size_dim2),
        }
    )
    df_sel1 = pl.DataFrame(
        {
            header_sel1[0]: np.random.randint(0, size_sel // 5, size=size_sel),
            header_sel1[1]: np.random.randint(0, size_sel // 5, size=size_sel),
        }
    )
    df_sel2 = pl.DataFrame(
        {
            header_sel2[0]: np.random.randint(0, size_sel // 5, size=size_sel),
            header_sel2[1]: np.random.randint(0, size_sel // 5, size=size_sel),
        }
    )

    df_fact.write_csv(output_file1)
    df_dim1.write_csv(output_file2)
    df_dim2.write_csv(output_file3)
    df_sel1.write_csv(output_file4)
    df_sel2.write_csv(output_file5)

    return (
        df_fact.rename({header_fact[i]: f"col{i + 1}" for i in range(4)}),
        df_dim1.rename({header_dim1[i]: f"col{i + 1}" for i in range(3)}),
        df_dim2.rename({header_dim2[i]: f"col{i + 1}" for i in range(2)}),
        df_sel1.rename({header_sel1[i]: f"col{i + 1}" for i in range(2)}),
        df_sel2.rename({header_sel2[i]: f"col{i + 1}" for i in range(2)}),
    )


def create_test_45():
    output_file, exp_output_file = utils.open_files(45, TEST_BASE_DIR)
    output_file.write(
        "-- Creates tables for join tests\n"
        "-- without any indexes\n"
        'create(tbl,"tbl5_fact",db1,4)\n'
        'create(col,"col1",db1.tbl5_fact)\n'
        'create(col,"col2",db1.tbl5_fact)\n'
        'create(col,"col3",db1.tbl5_fact)\n'
        'create(col,"col4",db1.tbl5_fact)\n'
        f'load("{DOCKER_TEST_BASE_DIR}/data5_fact.csv")\n'
        "--\n"
        'create(tbl,"tbl5_dim1",db1,3)\n'
        'create(col,"col1",db1.tbl5_dim1)\n'
        'create(col,"col2",db1.tbl5_dim1)\n'
        'create(col,"col3",db1.tbl5_dim1)\n'
        f'load("{DOCKER_TEST_BASE_DIR}/data5_dimension1.csv")\n'
        "--\n"
        'create(tbl,"tbl5_dim2",db1,2)\n'
        'create(col,"col1",db1.tbl5_dim2)\n'
        'create(col,"col2",db1.tbl5_dim2)\n'
        f'load("{DOCKER_TEST_BASE_DIR}/data5_dimension2.csv")\n'
        "--\n"
        'create(tbl,"tbl5_sel1",db1,2)\n'
        'create(col,"col1",db1.tbl5_sel1)\n'
        'create(col,"col2",db1.tbl5_sel1)\n'
        f'load("{DOCKER_TEST_BASE_DIR}/data5_selectivity1.csv")\n'
        "--\n"
        'create(tbl,"tbl5_sel2",db1,2)\n'
        'create(col,"col1",db1.tbl5_sel2)\n'
        'create(col,"col2",db1.tbl5_sel2)\n'
        f'load("{DOCKER_TEST_BASE_DIR}/data5_selectivity2.csv")\n'
        "--\n"
        "-- Testing that the data and their indexes are durable on disk.\n"
        "shutdown\n"
    )
    utils.close_files(output_file, exp_output_file)


def create_test_46(
    df_fact, df_dim2, size_fact, size_dim2, selectivity_fact, selectivity_dim2
):
    output_file, exp_output_file = utils.open_files(46, TEST_BASE_DIR)
    upper_fact = int((size_fact / 5) * selectivity_fact)
    upper_dim2 = int(size_dim2 * selectivity_dim2)
    output_file.write(
        "-- First join test - nested-loop. Select + Join + aggregation\n"
        "-- Performs the join using nested loops\n"
        "-- Do this only on reasonable sized tables! (O(n^2))\n"
        "-- Query in SQL:\n"
        f"-- SELECT avg(tbl5_fact.col2), sum(tbl5_fact.col3) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < {upper_fact} AND tbl5_dim2.col1 < {upper_dim2};\n"
        "--\n"
        "--\n"
        f"p1=select(db1.tbl5_fact.col2,null,{upper_fact})\n"
        f"p2=select(db1.tbl5_dim2.col1,null,{upper_dim2})\n"
        "f1=fetch(db1.tbl5_fact.col4,p1)\n"
        "f2=fetch(db1.tbl5_dim2.col1,p2)\n"
        "t1,t2=join(f1,p1,f2,p2,nested-loop)\n"
        "col2joined=fetch(db1.tbl5_fact.col2,t1)\n"
        "col3joined=fetch(db1.tbl5_fact.col3,t2)\n"
        "a1=avg(col2joined)\n"
        "a2=sum(col3joined)\n"
        "print(a1,a2)\n"
    )

    df_select_fact = df_fact.filter(pl.col("col2") < upper_fact)
    df_select_dim2 = df_dim2.filter(pl.col("col1") < upper_dim2)
    df_join = df_select_fact.join(df_select_dim2, left_on="col4", right_on="col1")
    col2_mean = utils.nantozero(df_join["col2"].mean())
    col3_sum = df_join["col3"].sum()
    exp_output_file.write(f"{col2_mean:.{PREC}f},{col3_sum}\n")
    utils.close_files(output_file, exp_output_file)


def create_test_47(
    df_fact, df_dim2, size_fact, size_dim2, selectivity_fact, selectivity_dim2
):
    output_file, exp_output_file = utils.open_files(47, TEST_BASE_DIR)
    upper_fact = int((size_fact / 5) * selectivity_fact)
    upper_dim2 = int(size_dim2 * selectivity_dim2)
    output_file.write(
        "-- First join test - hash. Select + Join + aggregation\n"
        "-- Performs the join using hashing\n"
        "-- Query in SQL:\n"
        f"-- SELECT avg(tbl5_fact.col2), sum(tbl5_fact.col3) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < {upper_fact} AND tbl5_dim2.col1 < {upper_dim2};\n"
        "--\n"
        "--\n"
        f"p1=select(db1.tbl5_fact.col2,null,{upper_fact})\n"
        f"p2=select(db1.tbl5_dim2.col1,null,{upper_dim2})\n"
        "f1=fetch(db1.tbl5_fact.col4,p1)\n"
        "f2=fetch(db1.tbl5_dim2.col1,p2)\n"
        "t1,t2=join(f1,p1,f2,p2,hash)\n"
        "col2joined=fetch(db1.tbl5_fact.col2,t1)\n"
        "col3joined=fetch(db1.tbl5_fact.col3,t2)\n"
        "a1=avg(col2joined)\n"
        "a2=sum(col3joined)\n"
        "print(a1,a2)\n"
    )

    df_select_fact = df_fact.filter(pl.col("col2") < upper_fact)
    df_select_dim2 = df_dim2.filter(pl.col("col1") < upper_dim2)
    df_join = df_select_fact.join(df_select_dim2, left_on="col4", right_on="col1")
    col2_mean = utils.nantozero(df_join["col2"].mean())
    col3_sum = df_join["col3"].sum()
    exp_output_file.write(f"{col2_mean:.{PREC}f},{col3_sum}\n")
    utils.close_files(output_file, exp_output_file)


def create_test_48(
    df_fact, df_dim1, size_fact, size_dim1, selectivity_fact, selectivity_dim1
):
    output_file, exp_output_file = utils.open_files(48, TEST_BASE_DIR)
    upper_fact = int((size_fact / 5) * selectivity_fact)
    upper_dim1 = int((size_dim1 / 5) * selectivity_dim1)
    output_file.write(
        "-- Second join test - nested-loop. Select + Join + aggregation\n"
        "-- Performs the join using nested loops\n"
        "-- Do this only on reasonable sized tables! (O(n^2))\n"
        "-- Query in SQL:\n"
        f"-- SELECT sum(tbl5_fact.col2), avg(tbl5_dim1.col1) FROM tbl5_fact,tbl5_dim1 WHERE tbl5_fact.col1=tbl5_dim1.col1 AND tbl5_fact.col2 < {upper_fact} AND tbl5_dim1.col3 < {upper_dim1};\n"
        "--\n"
        "--\n"
        f"p1=select(db1.tbl5_fact.col2,null,{upper_fact})\n"
        f"p2=select(db1.tbl5_dim1.col3,null,{upper_dim1})\n"
        "f1=fetch(db1.tbl5_fact.col1,p1)\n"
        "f2=fetch(db1.tbl5_dim1.col1,p2)\n"
        "t1,t2=join(f1,p1,f2,p2,nested-loop)\n"
        "col2joined=fetch(db1.tbl5_fact.col2,t1)\n"
        "col1joined=fetch(db1.tbl5_dim1.col1,t2)\n"
        "a1=sum(col2joined)\n"
        "a2=avg(col1joined)\n"
        "print(a1,a2)\n"
    )

    df_select_fact = df_fact.filter(pl.col("col2") < upper_fact)
    df_select_dim1 = df_dim1.filter(pl.col("col3") < upper_dim1)
    df_join = df_select_fact.join(df_select_dim1, left_on="col1", right_on="col1")
    col2_sum = df_join["col2"].sum()
    col1_mean = utils.nantozero(df_join["col1"].mean())
    exp_output_file.write(f"{col2_sum},{col1_mean:.{PREC}f}\n")
    utils.close_files(output_file, exp_output_file)


def create_test_49(
    df_fact, df_dim1, size_fact, size_dim1, selectivity_fact, selectivity_dim1
):
    output_file, exp_output_file = utils.open_files(49, TEST_BASE_DIR)
    upper_fact = int((size_fact / 5) * selectivity_fact)
    upper_dim1 = int((size_dim1 / 5) * selectivity_dim1)
    output_file.write(
        "-- join test 2 - hash. Select + Join + aggregation\n"
        "-- Performs the join using hashing\n"
        "-- Query in SQL:\n"
        f"-- SELECT sum(tbl5_fact.col2), avg(tbl5_dim1.col1) FROM tbl5_fact,tbl5_dim1 WHERE tbl5_fact.col1=tbl5_dim1.col1 AND tbl5_fact.col2 < {upper_fact} AND tbl5_dim1.col3 < {upper_dim1};\n"
        "--\n"
        "--\n"
        f"p1=select(db1.tbl5_fact.col2,null,{upper_fact})\n"
        f"p2=select(db1.tbl5_dim1.col3,null,{upper_dim1})\n"
        "f1=fetch(db1.tbl5_fact.col1,p1)\n"
        "f2=fetch(db1.tbl5_dim1.col1,p2)\n"
        "t1,t2=join(f1,p1,f2,p2,hash)\n"
        "col2joined=fetch(db1.tbl5_fact.col2,t1)\n"
        "col1joined=fetch(db1.tbl5_dim1.col1,t2)\n"
        "a1=sum(col2joined)\n"
        "a2=avg(col1joined)\n"
        "print(a1,a2)\n"
    )

    df_select_fact = df_fact.filter(pl.col("col2") < upper_fact)
    df_select_dim1 = df_dim1.filter(pl.col("col3") < upper_dim1)
    df_join = df_select_fact.join(df_select_dim1, left_on="col1", right_on="col1")
    col2_sum = df_join["col2"].sum()
    col1_mean = utils.nantozero(df_join["col1"].mean())
    exp_output_file.write(f"{col2_sum},{col1_mean:.{PREC}f}\n")
    utils.close_files(output_file, exp_output_file)


def create_test_50(
    df_fact, df_dim2, size_fact, size_dim2, selectivity_fact, selectivity_dim2
):
    output_file, exp_output_file = utils.open_files(50, TEST_BASE_DIR)
    upper_fact = int((size_fact / 5) * selectivity_fact)
    upper_dim2 = int(size_dim2 * selectivity_dim2)
    output_file.write(
        "-- join test 3 - hashing many-one with larger selectivities.\n"
        "-- Select + Join + aggregation\n"
        "-- Performs the join using hashing\n"
        "-- Query in SQL:\n"
        f"-- SELECT avg(tbl5_fact.col2), sum(tbl5_dim2.col2) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < {upper_fact} AND tbl5_dim2.col1 < {upper_dim2};\n"
        "--\n"
        "--\n"
        f"p1=select(db1.tbl5_fact.col2,null,{upper_fact})\n"
        f"p2=select(db1.tbl5_dim2.col1,null,{upper_dim2})\n"
        "f1=fetch(db1.tbl5_fact.col4,p1)\n"
        "f2=fetch(db1.tbl5_dim2.col1,p2)\n"
        "t1,t2=join(f1,p1,f2,p2,hash)\n"
        "col2joined=fetch(db1.tbl5_fact.col2,t1)\n"
        "col2t2joined=fetch(db1.tbl5_dim2.col2,t2)\n"
        "a1=avg(col2joined)\n"
        "a2=sum(col2t2joined)\n"
        "print(a1,a2)\n"
    )

    df_select_fact = df_fact.filter(pl.col("col2") < upper_fact)
    df_select_dim2 = df_dim2.filter(pl.col("col1") < upper_dim2)
    df_join = df_select_fact.join(df_select_dim2, left_on="col4", right_on="col1")
    col2_mean = utils.nantozero(df_join["col2"].mean())
    col2_sum = df_join["col2_right"].sum()
    exp_output_file.write(f"{col2_mean:.{PREC}f},{col2_sum}\n")
    utils.close_files(output_file, exp_output_file)


def create_test_51(
    df_fact, df_dim1, size_fact, size_dim1, selectivity_fact, selectivity_dim1
):
    output_file, exp_output_file = utils.open_files(51, TEST_BASE_DIR)
    upper_fact = int((size_fact / 5) * selectivity_fact)
    upper_dim1 = int((size_dim1 / 5) * selectivity_dim1)
    output_file.write(
        "-- join test 4 - hashing many-many with larger selectivities.\n"
        "-- Select + Join + aggregation\n"
        "-- Query in SQL:\n"
        f"-- SELECT sum(tbl5_fact.col2), avg(tbl5_dim1.col1) FROM tbl5_fact,tbl5_dim1 WHERE tbl5_fact.col1=tbl5_dim1.col1 AND tbl5_fact.col2 < {upper_fact} AND tbl5_dim1.col3 < {upper_dim1};\n"
        "--\n"
        "--\n"
        f"p1=select(db1.tbl5_fact.col2,null,{upper_fact})\n"
        f"p2=select(db1.tbl5_dim1.col3,null,{upper_dim1})\n"
        "f1=fetch(db1.tbl5_fact.col1,p1)\n"
        "f2=fetch(db1.tbl5_dim1.col1,p2)\n"
        "t1,t2=join(f1,p1,f2,p2,hash)\n"
        "col2joined=fetch(db1.tbl5_fact.col2,t1)\n"
        "col1joined=fetch(db1.tbl5_dim1.col1,t2)\n"
        "a1=sum(col2joined)\n"
        "a2=avg(col1joined)\n"
        "print(a1,a2)\n"
    )

    df_select_fact = df_fact.filter(pl.col("col2") < upper_fact)
    df_select_dim1 = df_dim1.filter(pl.col("col3") < upper_dim1)
    df_join = df_select_fact.join(df_select_dim1, left_on="col1", right_on="col1")
    col2_sum = df_join["col2"].sum()
    col1_mean = utils.nantozero(df_join["col1"].mean())
    exp_output_file.write(f"{col2_sum},{col1_mean:.{PREC}f}\n")
    utils.close_files(output_file, exp_output_file)


def _perf_test_helper(output_file, size_select, selectivity1, selectivity2, join_type):
    upper1 = int(selectivity1 * (size_select / 5))
    upper2 = int(selectivity2 * (size_select / 5))
    output_file.write(
        f"-- join performance test - {join_type} with selectivities {selectivity1} and {selectivity2}.\n"
        "-- Select + Join + aggregation\n"
        "-- Query in SQL:\n"
        f"-- SELECT sum(tbl5_sel1.col1), avg(tbl5_sel2.col2) FROM tbl5_sel1, tbl5_sel2 WHERE tbl5_sel1.col1=tbl5_sel2.col1 AND tbl5_sel1.col1 < {upper1} AND tbl5_sel2.col2< {upper2};\n"
        "--\n"
        "--\n"
        f"p1=select(db1.tbl5_sel1.col1,null,{upper1})\n"
        f"p2=select(db1.tbl5_sel2.col2,null,{upper2})\n"
        "f1=fetch(db1.tbl5_sel1.col1,p1)\n"
        "f2=fetch(db1.tbl5_sel2.col1,p2)\n"
        f"t1,t2=join(f1,p1,f2,p2,{join_type})\n"
    )


def create_join_correctness_test(
    df_select1,
    df_select2,
    size,
    selectivity1,
    selectivity2,
    join_type1,
    join_type2,
    test_num,
):
    output_file_1, exp_output_file_1 = utils.open_files(test_num, TEST_BASE_DIR)
    _perf_test_helper(output_file_1, size, selectivity1, selectivity2, join_type1)
    output_file_1.write(
        "col1joined=fetch(db1.tbl5_sel1.col1,t1)\n"
        "col2joined=fetch(db1.tbl5_sel2.col2,t2)\n"
        "a1=sum(col1joined)\n"
        "a2=avg(col2joined)\n"
        "print(a1,a2)\n"
    )

    output_file_2, exp_output_file_2 = utils.open_files(test_num + 1, TEST_BASE_DIR)
    _perf_test_helper(output_file_2, size, selectivity1, selectivity2, join_type2)
    output_file_2.write(
        "col1joined=fetch(db1.tbl5_sel1.col1,t1)\n"
        "col2joined=fetch(db1.tbl5_sel2.col2,t2)\n"
        "a1=sum(col1joined)\n"
        "a2=avg(col2joined)\n"
        "print(a1,a2)\n"
    )

    upper1 = int(selectivity1 * (size / 5))
    upper2 = int(selectivity2 * (size / 5))
    df_select1 = df_select1.filter(pl.col("col1") < upper1)
    df_select2 = df_select2.filter(pl.col("col2") < upper2)
    df_join = df_select1.join(df_select2, left_on="col1", right_on="col1")
    col1_sum = df_join["col1"].sum()
    col2_mean = utils.nantozero(df_join["col2_right"].mean())
    exp_output_file_1.write(f"{col1_sum},{col2_mean:.{PREC}f}\n")
    exp_output_file_2.write(f"{col1_sum},{col2_mean:.{PREC}f}\n")
    utils.close_files(output_file_1, exp_output_file_1)
    utils.close_files(output_file_2, exp_output_file_2)


def create_join_perf_test(
    size, selectivity1, selectivity2, join_type1, join_type2, test_num
):
    output_file_1, exp_output_file_1 = utils.open_files(test_num, TEST_BASE_DIR)
    _perf_test_helper(output_file_1, size, selectivity1, selectivity2, join_type1)
    utils.close_files(output_file_1, exp_output_file_1)

    # Second join
    output_file_2, exp_output_file_2 = utils.open_files(test_num + 1, TEST_BASE_DIR)
    _perf_test_helper(output_file_2, size, selectivity1, selectivity2, join_type2)
    utils.close_files(output_file_2, exp_output_file_2)


def create_tests_52_to_55(df_select1, df_select2, size):
    join_type1, join_type2 = "nested-loop", "naive-hash"
    selectivity1, selectivity2 = 0.1, 0.1

    create_join_correctness_test(
        df_select1,
        df_select2,
        size,
        selectivity1,
        selectivity2,
        join_type1,
        join_type2,
        52,
    )
    create_join_perf_test(size, selectivity1, selectivity2, join_type1, join_type2, 54)


def create_tests_56_to_59(df_select1, df_select2, size):
    join_type1, join_type2 = "naive-hash", "grace-hash"
    selectivity1, selectivity2 = 0.8, 0.8

    create_join_correctness_test(
        df_select1,
        df_select2,
        size,
        selectivity1,
        selectivity2,
        join_type1,
        join_type2,
        56,
    )
    create_join_perf_test(size, selectivity1, selectivity2, join_type1, join_type2, 58)


def generate_milestone_4_files(
    size_fact, size_dim1, size_dim2, size_sel, zipfian_param, n_distinct, seed
):
    np.random.seed(seed)
    df_fact, df_dim1, df_dim2, df_sel1, df_sel2 = generate_data_milestone_4(
        size_fact,
        size_dim1,
        size_dim2,
        size_sel,
        zipfian_param,
        n_distinct,
    )

    create_test_45()
    # Test many to 1 joins
    create_test_46(df_fact, df_dim2, size_fact, size_dim2, 0.15, 0.15)
    create_test_47(df_fact, df_dim2, size_fact, size_dim2, 0.15, 0.15)
    # Test many to many joins
    create_test_48(df_fact, df_dim1, size_fact, size_dim1, 0.15, 0.15)
    create_test_49(df_fact, df_dim1, size_fact, size_dim1, 0.15, 0.15)
    # Test both joins with much larger selectivities. This should mostly test speed.
    create_test_50(df_fact, df_dim2, size_fact, size_dim2, 0.8, 0.8)
    create_test_51(df_fact, df_dim1, size_fact, size_dim1, 0.8, 0.8)

    del df_fact, df_dim1, df_dim2  # Free up memory
    assert len(df_sel1) == len(df_sel2)
    create_tests_52_to_55(df_sel1, df_sel2, len(df_sel1))
    create_tests_56_to_59(df_sel1, df_sel2, len(df_sel1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("size_fact", type=int)
    parser.add_argument("size_dim1", type=int)
    parser.add_argument("size_dim2", type=int)
    parser.add_argument("size_sel", type=int)
    parser.add_argument("seed", type=int)
    parser.add_argument("zipfian_param", type=float)
    parser.add_argument("n_distinct", type=int)
    parser.add_argument("test_base_dir", type=str, nargs="?", default="")
    parser.add_argument("docker_test_base_dir", type=str, nargs="?", default="")
    args = parser.parse_args()

    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR

    if args.test_base_dir:
        TEST_BASE_DIR = args.test_base_dir
    if args.docker_test_base_dir:
        DOCKER_TEST_BASE_DIR = args.docker_test_base_dir

    generate_milestone_4_files(
        args.size_fact,
        args.size_dim1,
        args.size_dim2,
        args.size_sel,
        args.zipfian_param,
        args.n_distinct,
        args.seed,
    )


if __name__ == "__main__":
    main()
