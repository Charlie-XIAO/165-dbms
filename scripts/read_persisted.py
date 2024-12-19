#!/usr/bin/env python3

import sys
from enum import Enum
from pathlib import Path
from struct import calcsize, unpack

import numpy as np
import polars as pl
import rich

MAX_SIZE_NAME = 64
DB_PERSIST_DIR = ".cs165_db"
DB_PERSIST_CATALOG_FILE = "__catalog__"

pl.Config.set_tbl_rows(20)


class ColumnIndexType(Enum):
    NONE = 0
    UNCLUSTERED_SORTED = 1
    UNCLUSTERED_BTREE = 2
    CLUSTERED_SORTED = 3
    CLUSTERED_BTREE = 4


def read_object_name(file):
    return file.read(MAX_SIZE_NAME).decode("utf-8").rstrip("\x00")


def main():
    rich.print()
    persisted_dir = Path(__file__).parent.parent / "src" / DB_PERSIST_DIR
    catalog_path = persisted_dir / DB_PERSIST_CATALOG_FILE

    if not catalog_path.exists():
        rich.print("[bold yellow]Database not persisted.[/bold yellow]")
        rich.print()
        return

    with catalog_path.open("rb") as catalog:
        # Read the database metadata
        db_name = read_object_name(catalog)
        (db_n_tables,) = unpack("N", catalog.read(calcsize("N")))
        (db_capacity,) = unpack("N", catalog.read(calcsize("N")))

        rich.print(f"[bold green]DATABASE [ {db_name} ][/bold green]")
        rich.print(f"  n_tables={db_n_tables}, capacity={db_capacity}")

        for _ in range(db_n_tables):
            # Read each table metadata
            table_name = read_object_name(catalog)
            (table_n_cols,) = unpack("N", catalog.read(calcsize("N")))
            (table_n_inited_cols,) = unpack("N", catalog.read(calcsize("N")))
            (table_n_rows,) = unpack("N", catalog.read(calcsize("N")))
            (table_capacity,) = unpack("N", catalog.read(calcsize("N")))
            (table_primary,) = unpack("N", catalog.read(calcsize("N")))
            if table_primary == 2 * sys.maxsize + 1:  # __SIZE_MAX__
                table_primary = None

            rich.print()
            rich.get_console().rule()
            rich.print()
            rich.print(f"[bold green]TABLE [ {table_name} ][/bold green]")
            rich.print(f"  n_cols={table_n_cols}, n_inited_cols={table_n_inited_cols}")
            rich.print(f"  n_rows={table_n_rows}, capacity={table_capacity}")
            rich.print(f"  primary={table_primary}")

            # Read the initialized columns of each table and construct dataframe
            columns_data = {}
            for _ in range(table_n_inited_cols):
                column_name = read_object_name(catalog)
                (column_index_type,) = unpack("i", catalog.read(calcsize("i")))
                rich.print(f"  [ {column_name} ] {ColumnIndexType(column_index_type)}")

                column_data = np.empty(table_n_rows, dtype=np.int32)
                column_path = persisted_dir / f"{table_name}.{column_name}"
                with column_path.open("rb") as column_file:
                    for j in range(table_n_rows):
                        (col_value,) = unpack("i", column_file.read(calcsize("i")))
                        column_data[j] = col_value

                columns_data[column_name] = column_data

            rich.print()
            df = pl.DataFrame(columns_data)
            print(df)

    rich.print()


if __name__ == "__main__":
    main()
