#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns

LOGS_DIR = Path(__file__).parent / "logs"
PLOTS_DIR = Path(__file__).parent / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

sns.set_theme()


if __name__ == "__main__":
    df = pl.read_csv(LOGS_DIR / "ms3-1.csv")
    df_pivot = df.pivot(values="time", index="n_cols", on="type")

    n_cols = df_pivot["n_cols"]
    types = df_pivot.columns[1:]
    x = np.arange(len(n_cols))
    bar_width = 0.2

    offsets = {
        "btree-unclustered": -1.5,
        "btree-clustered": -0.5,
        "sorted-unclustered": 0.5,
        "sorted-clustered": 1.5,
    }

    for type_name, offset in offsets.items():
        y_values = df_pivot[type_name]
        plt.bar(
            x + offset * bar_width,
            y_values / 1_000_000,
            width=bar_width,
            label=type_name,
            edgecolor="black",
            alpha=0.6,
        )

    plt.xlabel("Number of columns")
    plt.ylabel("Time (ms)")
    plt.xticks(x, n_cols)
    plt.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ms3-1.png")
    plt.close()
