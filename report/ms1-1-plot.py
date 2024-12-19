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
    df = pl.read_csv(LOGS_DIR / "ms1-1.csv")

    plt.plot(df["n_rows"] * 4 / 1024 / 1024, df["time"] / 1_000_000, marker="o")
    plt.xlabel("Column size (MiB)")
    plt.ylabel("Time (ms)")
    plt.title("Sum aggregate repeated 100 times")
    plt.xscale("log", base=2)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ms1-1.png")
    plt.close()
