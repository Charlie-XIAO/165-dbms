#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

LOGS_DIR = Path(__file__).parent / "logs"
PLOTS_DIR = Path(__file__).parent / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

sns.set_theme()


if __name__ == "__main__":
    df = pl.read_csv(LOGS_DIR / "ms1-2.csv")

    for n_cols in range(1, 5):
        df_n_cols = df.filter(pl.col("n_cols") == n_cols)
        plt.plot(
            df_n_cols["n_rows"] * 4 / 1024 / 1024,
            df_n_cols["time"] / 1_000_000,
            marker="o",
            label=f"{n_cols} columns",
        )
    plt.xlabel("Column size (MiB)")
    plt.ylabel("Time (ms)")
    plt.title("Load CSV data")
    plt.xscale("log", base=2)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ms1-2.png")
    plt.close()
