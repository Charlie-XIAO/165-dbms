#!/usr/bin/env python3

from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


if __name__ == "__main__":
    # MS1-1
    with (DATA_DIR / "ms1-1-pre.in").open("w", encoding="utf-8") as f:
        f.write('create(db,"db")\n')
        for exponent in range(15, 25):
            n_rows = 2**exponent
            path = DATA_DIR / f"tbl_{n_rows}_1.csv"
            f.write(f'create(tbl,"tbl_{n_rows}_1",db,1)\n')
            f.write(f'create(col,"col0",db.tbl_{n_rows}_1)\n')
            f.write(f'load("{path}")\n')
    for exponent in range(15, 25):
        n_rows = 2**exponent
        with (DATA_DIR / f"ms1-1-{n_rows}.in").open("w", encoding="utf-8") as f:
            for i in range(100):
                f.write(f"a{i}=sum(db.tbl_{n_rows}_1.col0)\n")

    # MS1-2
    with (DATA_DIR / "ms1-2-pre.in").open("w", encoding="utf-8") as f:
        f.write('create(db,"db")\n')
        for exponent in range(15, 25):
            n_rows = 2**exponent
            for n_cols in range(1, 5):
                f.write(f'create(tbl,"tbl_{n_rows}_{n_cols}",db,{n_cols})\n')
                for i in range(n_cols):
                    f.write(f'create(col,"col{i}",db.tbl_{n_rows}_{n_cols})\n')
    for exponent in range(15, 25):
        n_rows = 2**exponent
        for n_cols in range(1, 5):
            with (DATA_DIR / f"ms1-2-{n_rows}-{n_cols}.in").open(
                "w", encoding="utf-8"
            ) as f:
                path = DATA_DIR / f"tbl_{n_rows}_{n_cols}.csv"
                f.write(f'load("{path}")\n')

    # MS2-1
    with (DATA_DIR / "ms2-1-pre.in").open("w", encoding="utf-8") as f:
        n_rows = 2**24
        f.write('create(db,"db")\n')
        f.write(f'create(tbl,"tbl_{n_rows}_1",db,1)\n')
        f.write(f'create(col,"col0",db.tbl_{n_rows}_1)\n')
        path = DATA_DIR / f"tbl_{n_rows}_1.csv"
        f.write(f'load("{path}")\n')
    with (DATA_DIR / "ms2-1-single-core.in").open("w", encoding="utf-8") as f:
        f.write("single_core()\n")
        for i in range(100):
            f.write(f"s{i}=select(db.tbl_{n_rows}_1.col0,0,100)\n")
        f.write("single_core_execute()\n")
    with (DATA_DIR / "ms2-1-multi-core.in").open("w", encoding="utf-8") as f:
        for i in range(100):
            f.write(f"s{i}=select(db.tbl_{n_rows}_1.col0,0,100)\n")

    # MS2-2
    with (DATA_DIR / "ms2-2-pre.in").open("w", encoding="utf-8") as f:
        n_rows = 2**24
        f.write('create(db,"db")\n')
        f.write(f'create(tbl,"tbl_{n_rows}_1",db,1)\n')
        f.write(f'create(col,"col0",db.tbl_{n_rows}_1)\n')
        path = DATA_DIR / f"tbl_{n_rows}_1.csv"
        f.write(f'load("{path}")\n')
    with (DATA_DIR / "ms2-2-unbatched.in").open("w", encoding="utf-8") as f:
        f.write("single_core()\n")
        for i in range(128):
            f.write(f"s{i}=select(db.tbl_{n_rows}_1.col0,0,100)\n")
        f.write("single_core_execute()\n")
    for batch_size in [2, 4, 8, 16, 32, 64, 128]:
        with (DATA_DIR / f"ms2-2-batched-{batch_size}.in").open(
            "w", encoding="utf-8"
        ) as f:
            f.write("single_core()\n")
            f.write("batch_queries()\n")
            for i in range(128):
                f.write(f"s{i}=select(db.tbl_{n_rows}_1.col0,0,100)\n")
                if (i + 1) % batch_size == 0 and i != 127:
                    f.write("batch_execute()\n")
                    f.write("batch_queries()\n")
            f.write("batch_execute()\n")
            f.write("single_core_execute()\n")

    # MS3-1
    with (DATA_DIR / "ms3-1-pre.in").open("w", encoding="utf-8") as f:
        n_rows = 2**24
        f.write('create(db,"db")\n')
        for n_cols in range(1, 5):
            f.write(f'create(tbl,"tbl_{n_rows}_{n_cols}",db,{n_cols})\n')
            for i in range(n_cols):
                f.write(f'create(col,"col{i}",db.tbl_{n_rows}_{n_cols})\n')
            path = DATA_DIR / f"tbl_{n_rows}_{n_cols}.csv"
            f.write(f'load("{path}")\n')
    for index_type in ["btree", "sorted"]:
        for index_metatype in ["unclustered", "clustered"]:
            for n_cols in range(1, 5):
                with (
                    DATA_DIR / f"ms3-1-{index_type}-{index_metatype}-{n_cols}.in"
                ).open("w", encoding="utf-8") as f:
                    f.write(
                        f"create(idx,db.tbl_{n_rows}_{n_cols}.col0,{index_type},{index_metatype})\n"
                    )

    # MS3-2
    with (DATA_DIR / f"ms3-2-pre.in").open("w", encoding="utf-8") as f:
        n_rows = 2**24
        f.write('create(db,"db")\n')
        f.write(f'create(tbl,"tbl_{n_rows}_1",db,1)\n')
        f.write(f'create(col,"col0",db.tbl_{n_rows}_1)\n')
        path = DATA_DIR / f"tbl_{n_rows}_1.csv"
        f.write(f'load("{path}")\n')
    for index_type in ["btree", "sorted"]:
        for index_metatype in ["unclustered", "clustered"]:
            with (DATA_DIR / f"ms3-2-pre-{index_type}-{index_metatype}.in").open(
                "w", encoding="utf-8"
            ) as f:
                n_rows = 2**24
                f.write('create(db,"db")\n')
                f.write(f'create(tbl,"tbl_{n_rows}_1",db,1)\n')
                f.write(f'create(col,"col0",db.tbl_{n_rows}_1)\n')
                path = DATA_DIR / f"tbl_{n_rows}_1.csv"
                f.write(f'load("{path}")\n')
                f.write(
                    f"create(idx,db.tbl_{n_rows}_1.col0,{index_type},{index_metatype})\n"
                )
    for selectivity in [0.01, 0.02, 0.05, 0.1, 0.2, 0.5]:
        with (DATA_DIR / f"ms3-2-{selectivity}.in").open("w", encoding="utf-8") as f:
            for i in range(100):
                f.write(
                    f"s{i}=select(db.tbl_{n_rows}_1.col0,0,{int(selectivity * 1000000)})\n"
                )
