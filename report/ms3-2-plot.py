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
    df = pl.read_csv(LOGS_DIR / "ms3-2.csv")
    plt.plot(df["selectivity"], df["time"] / 1_000_000, marker="o", label="none")

    for index_type in ["btree", "sorted"]:
        for index_metatype in ["unclustered", "clustered"]:
            df = pl.read_csv(LOGS_DIR / f"ms3-2-{index_type}-{index_metatype}.csv")
            plt.plot(
                df["selectivity"],
                df["time"] / 1_000_000,
                marker="o",
                label=f"{index_type}-{index_metatype}",
            )

    plt.xlabel("Selectivity")
    plt.ylabel("Time (ms)")
    plt.title("Select query repeated 100 times with index")
    plt.xscale("log", base=2)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "ms3-2.png")
    plt.close()
