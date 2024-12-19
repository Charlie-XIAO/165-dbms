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
    df = pl.read_csv(LOGS_DIR / "ms2-2.csv")

    plt.plot(df["batch_size"], df["time"] / 1_000_000, marker="o")
    plt.xlabel("Batch size")
    plt.ylabel("Time (ms)")
    plt.title("Select repeated 128 times (single-core)")
    plt.xscale("log", base=2)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ms2-2.png")
    plt.close()
