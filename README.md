# CS165 Column-Store DBMS

> - This is the course project of [Harvard CS165 (Fall 2024)](http://daslab.seas.harvard.edu/classes/cs165/index.html) Data Systems by Yao Xiao @Charlie-XIAO.
> - Original (non-public): [code.harvard.edu/yax892/cs165-column-store](https://code.harvard.edu/yax892/cs165-column-store) @ [fa8df73](https://code.harvard.edu/yax892/cs165-column-store/commit/fa8df7360362c00d0062f391283c29fab88d1815)

## Table of Contents

- [Overview](#overview) [ [Highlights](#highlights) ]
- [Running Instructions](#running-instructions)
- [Test Server Benchmarks](#test-server-benchmarks) [ [Full](#full-benchmark) | [B1](#benchmark-1) | [B2](#benchmark-2) | [B3](#benchmark-3) | [B4](#benchmark-4) | [B5](#benchmark-5) | [B6](#benchmark-6) | [B7](#benchmark-7) ]

## Overview

This project supports all queries specified in the [CS165 domain-specific language](http://daslab.seas.harvard.edu/classes/cs165/project.html). Essentially most basic SQL queries (create, load, insert, delete, update, select, aggregate, join, etc.) are supported with proper translation into the CS165 DSL.

<details>
<summary>Some examples of SQL queries and their equivalent in CS165 DSL</summary>
<p>

**Example 1:** Creating database objects, column indexes, and loading data into the table.

```sql
CREATE DATABASE awesomebase;

CREATE TABLE grades (
  grades INT,
  project INT,
  midterm1 INT,
  midterm2 INT,
  class INT,
  quizzes INT,
  student_id INT
);

-- Note that SQL expects no header, but CS165 DSL expects a
-- proper header to identify which table to load data into;
-- see the CS165 DSL specifications for more details
LOAD DATA LOCAL INFILE "client-side-data.csv";

INSERT INTO grades VALUES (107, 80, 75, 95, 93, 1);

CREATE CLUSTERED INDEX idx_grades_clustered ON grades (student_id);
CREATE INDEX idx_project_unclustered ON grades (project);
```

```
create(db,"awesomebase")
create(tbl,"grades",awesomebase,6)
create(col,"project",awesomebase.grades)
create(col,"midterm1",awesomebase.grades)
create(col,"midterm2",awesomebase.grades)
create(col,"class",awesomebase.grades)
create(col,"quizzes",awesomebase.grades)
create(col,"student_id",awesomebase.grades)

load("./client-side-data.csv")

relational_insert(awesomebase.grades,107,80,75,95,93,1)

create(idx,awesomebase.grades.student_id,btree,clustered)
create(idx,awesomebase.grades.project,btree,unclustered)
```

**Example 2:** Select with range-based predicates and aggregation.

```sql
SELECT
  avg(col1 + col2),
  min(col2),
  max(col3),
  avg(col3 - col2),
  sum(col3 - col2)
FROM
  tbl2
WHERE
  (col1 >= -360812 AND col1 < -44585)
  AND
  (col2 >= -163196 AND col2 < 153031);
```

```
s1=select(db1.tbl2.col1,-360812,-44585)
sf1=fetch(db1.tbl2.col2,s1)
s2=select(s1,sf1,-163196,153031)
f1=fetch(db1.tbl2.col1,s2)
f2=fetch(db1.tbl2.col2,s2)
f3=fetch(db1.tbl2.col3,s2)
add12=add(f1,f2)
out1=avg(add12)
out2=min(f2)
out3=max(f3)
sub32=sub(f3,f2)
out4=avg(sub32)
out5=sum(sub32)
print(out1,out2,out3,out4,out5)
```

**Example 3:** Select, join, and aggregation.

```sql
SELECT
  sum(tbl5_sel1.col1),
  avg(tbl5_sel2.col2)
FROM
  tbl5_sel1, tbl5_sel2
WHERE
  tbl5_sel1.col1 = tbl5_sel2.col1
  AND tbl5_sel1.col1 < 160000
  AND tbl5_sel2.col2 < 160000;
```

```
p1=select(db1.tbl5_sel1.col1,null,160000)
p2=select(db1.tbl5_sel2.col2,null,160000)
f1=fetch(db1.tbl5_sel1.col1,p1)
f2=fetch(db1.tbl5_sel2.col1,p2)
t1,t2=join(f1,p1,f2,p2,grace-hash)
col1joined=fetch(db1.tbl5_sel1.col1,t1)
col2joined=fetch(db1.tbl5_sel2.col2,t2)
a1=sum(col1joined)
a2=avg(col2joined)
print(a1,a2)
```

</p>
</details>

### Highlights

- Streamlined CSV parsing cache-aware chunked loading, achieving over **4x** speedup than the non-optimized version for 100M rows and 4 columns of data.
- Cache-aware shared scan for batchable queries and parallelized processing between chunks (supported for select and aggregate), achieving over **20x** speedup for 100M rows of data and 100 batchable select queries.
- Column indexes (sorted and B+ tree, clustered and unclustered). B+ tree index creation has minimal overhead (less than 20ms on column with 100M rows of data) by presorting and bulk loading. Select queries with can achieve over **25x** speedup when low selectivity (~5%) on 100M rows of data.
- Parallelized radix hash join, achieving over **8x** speedup over naive hash join on 100M x 100M data with 80% selectivity. Note that the naive hash join is already nearly **200x** faster than nested loop join with just 10% selectivity (the latter takes too long with 80% selectivity and is not measured).
- SIMD/AVX2 optimizations (not included in this repository).

Disclaimer: The above speedups are only comparing my own optimized implementation versus non-optimized implementation. They may not be (and are not meant to be) fast in the absolute sense, especially when compared with well-developed industry-level database systems.

## Running Instructions

The project is originally implemened in C (under `/src/`) and partially reimplemented in Rust (under `/rustsrc/`, mainly for learning purposes). The Rust version is slightly different from the C version and does not implement full features (yet).

To run the full test suite, make sure you have Docker installed:

```bash
make build                   # Build the container
make devgen                  # Generate test data (10K rows)
make devgen size=1000000     # Generate larger test data
make devtest                 # Run full test suite (C version, passes all)
make devtest CS165_RUST=1    # Run full test suite (Rust version, passes some)
```

When running the `make devtest` target, do not care about the final error after running the full test suite - that is just about generating a report which relies on the local development environment.

For local development, make sure you have the required dependencies as specified in the root `Dockerfile`. Note that the project only works on **Linux** systems (including WSL2 for Windows, not including macOS). To run the C version on the host machine (server and client need to be started in two different terminals):

```bash
cd src
make                      # Build the binaries
./server                  # Run the server
./client                  # Run the client (interactive)
./client < something.dsl  # Run the client
```

Similarly, to run the Rust version:

```bash
cd rustsrc
make                                     # Build the binaries
./target/release/server                  # Run the server
./target/release/client                  # Run the client (interactive)
./target/release/client < something.dsl  # Run the client
```

## Test Server Benchmarks

The leaderboard is copied from CS165 (Fall 2024) test server results. It shows the best recorded time for the top 10 users for the CS165 testing suite. The initial dataset is comprised of 100M rows for each table. The raw data size of the three tables that are loaded is 10 GB. The users are anonymized for privacy - look for **Old Lace** (bolded in the tables) for my perfomance (C version).

Disclaimer: The following is copied from the [CS165 course website (public)](http://daslab.seas.harvard.edu/classes/cs165/leaderboard/) as of the last serer run in the Fall 2024 semester (Thu Dec 19 8 PM EST 2024). If those information are not allowed for use in this way, please let me know and I will remove relevant contents (and history).

### Full Benchmark

Aggregate time of all benchmarks.

| User                    | Performance    |
|-------------------------|----------------|
| **Old Lace**            | **666.78 ms**  |
| Olivine                 | 683.29 ms      |
| Orchid                  | 1212.65 ms     |
| Pale Cornflower Blue    | 1215.55 ms     |
| Onyx                    | 1389.08 ms     |
| Palatinate Purple       | 1698.44 ms     |
| Old Heliotrope          | 1883.82 ms     |

### Benchmark 1

Load a table of 4 columns.

| User              | Performance   |
|-------------------|---------------|
| Old Moss Green    | 91.26 ms      |
| Olivine           | 113.33 ms     |
| **Old Lace**      | **127.86 ms** |
| Olive Drab #7     | 165.38 ms     |
| Opera Mauve       | 180.46 ms     |
| Pale Copper       | 228.70 ms     |
| Orange Peel       | 233.39 ms     |
| Old Heliotrope    | 242.45 ms     |
| Old Lavender      | 267.02 ms     |
| Orchid            | 267.19 ms     |

### Benchmark 2

Execute a single aggregate query.

| User                    | Performance   |
|-------------------------|---------------|
| Olive Drab #7           | 6.82 ms       |
| **Old Lace**            | **6.99 ms**   |
| Olivine                 | 7.58 ms       |
| Pale Cornflower Blue    | 8.38 ms       |
| Olive Drab (#3)         | 8.60 ms       |
| Orange (RYB)            | 9.10 ms       |
| Pakistan Green          | 9.43 ms       |
| Old Silver              | 11.24 ms      |
| Opera Mauve             | 11.35 ms      |
| Old Moss Green          | 12.89 ms      |

### Benchmark 3

Load a table of 4 columns, while bulk loading a tree and maintaining a clustered column.

| User                    | Performance   |
|-------------------------|---------------|
| **Old Lace**            | **144.58 ms** |
| Olivine                 | 180.27 ms     |
| Old Moss Green          | 219.07 ms     |
| Orange Peel             | 245.79 ms     |
| Pale Copper             | 245.79 ms     |
| Olive Drab #7           | 253.97 ms     |
| Onyx                    | 286.45 ms     |
| Pakistan Green          | 322.08 ms     |
| Pale Cornflower Blue    | 364.12 ms     |
| Pale Green              | 395.30 ms     |

### Benchmark 4

Index probes in both indexes.

| User                    | Performance   |
|-------------------------|---------------|
| **Old Lace**            | **4.25 ms**   |
| Olivine                 | 5.39 ms       |
| Orchid                  | 5.60 ms       |
| Citrine                 | 6.03 ms       |
| Old Heliotrope          | 6.04 ms       |
| Olive Drab (#3)         | 6.06 ms       |
| Pakistan Green          | 6.19 ms       |
| Palatinate Purple       | 6.20 ms       |
| Olive Drab #7           | 6.81 ms       |
| Pale Cornflower Blue    | 7.44 ms       |

### Benchmark 5

Executing shared scans.

| User                    | Performance   |
|-------------------------|---------------|
| Pale Cornflower Blue    | 39.17 ms      |
| Citrine                 | 52.59 ms      |
| Palatinate Purple       | 91.29 ms      |
| Olivine                 | 111.39 ms     |
| **Old Lace**            | **116.33 ms** |
| Orchid                  | 116.46 ms     |
| Olive Drab (#3)         | 127.61 ms     |
| Onyx                    | 148.27 ms     |
| Orange (Crayola)        | 173.17 ms     |
| Old Heliotrope          | 175.76 ms     |

### Benchmark 6

Load a table of 4 columns, while bulk loading a tree (data used for the join benchmark).

| User                    | Performance   |
|-------------------------|---------------|
| Olivine                 | 147.88 ms     |
| **Old Lace**            | **200.74 ms** |
| Orchid                  | 243.98 ms     |
| Pale Cornflower Blue    | 338.03 ms     |
| Onyx                    | 420.06 ms     |
| Orange (Crayola)        | 489.57 ms     |
| Citrine                 | 497.18 ms     |
| Old Heliotrope          | 504.45 ms     |
| Palatinate Purple       | 602.73 ms     |
| Olive Drab (#3)         | 671.59 ms     |

### Benchmark 7

Performing a join.

| User                    | Performance   |
|-------------------------|---------------|
| **Old Lace**            | **31.09 ms**  |
| Pale Cornflower Blue    | 34.67 ms      |
| Orange (RYB)            | 48.46 ms      |
| Old Silver              | 54.26 ms      |
| Citrine                 | 54.58 ms      |
| Onyx                    | 61.18 ms      |
| Olivine                 | 61.47 ms      |
| Orchid                  | 63.11 ms      |
| Pakistan Green          | 73.09 ms      |
| Palatinate Purple       | 182.25 ms     |
