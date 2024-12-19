#!/usr/bin/env python3

from pathlib import Path

import numpy as np
import polars as pl

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


if __name__ == "__main__":
    rng = np.random.default_rng(0)

    for exponent in range(15, 25):
        n_rows = 2**exponent
        for n_cols in range(1, 5):
            data = rng.integers(0, 1000000, (n_rows, n_cols))
            df = pl.DataFrame(
                data, schema=[f"db.tbl_{n_rows}_{n_cols}.col{i}" for i in range(n_cols)]
            )
            df.write_csv(DATA_DIR / f"tbl_{n_rows}_{n_cols}.csv")
            print(f"Generated: tbl_{n_rows}_{n_cols}.csv")
