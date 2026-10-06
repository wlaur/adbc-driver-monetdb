#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["pyarrow"]
# ///
"""Write small Parquet probe files for the native-reader checks.

usage: make-parquet-probe.py OUTPUT_DIR
"""

import datetime as dt
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 8


def ints(count: int) -> list[tuple[str, pa.Array]]:
    return [(f"c{i:02d}", pa.array(list(range(ROWS)), pa.int64())) for i in range(count)]


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)

    timestamps = pa.array(
        [dt.datetime(2026, 1, 1) + dt.timedelta(seconds=j) for j in range(ROWS)],
        pa.timestamp("us"),
    )
    dates = pa.array(
        [dt.date(2026, 1, 1) + dt.timedelta(days=j) for j in range(ROWS)],
        pa.date32(),
    )
    strings = pa.array([f"row{j}" for j in range(ROWS)], pa.string())
    decimals = [Decimal(f"{j + 1}.25") for j in range(ROWS)]

    files: dict[str, list[tuple[str, pa.Array]]] = {
        # column index 14 exists only here: parquet.c renames it to "e1"
        "p_int18.parquet": ints(18),
        # control for the rename: no column index 14
        "p_int3.parquet": ints(3),
        "p_str.parquet": [*ints(2), ("s", strings)],
        "p_ts.parquet": [*ints(2), ("ts", timestamps)],
        "p_date.parquet": [*ints(2), ("d", dates)],
        # precision 15 is outside the decoder's supported set
        "p_dec15.parquet": [*ints(2), ("v", pa.array(decimals, pa.decimal128(15, 2)))],
        # precision 8 is inside it, and fails too
        "p_dec8.parquet": [*ints(2), ("v", pa.array(decimals, pa.decimal128(8, 2)))],
    }

    for name, columns in files.items():
        pq.write_table(pa.table(dict(columns)), out / name)
        print(out / name)


if __name__ == "__main__":
    main()
