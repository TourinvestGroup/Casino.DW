"""Validate a generated daily-visits xlsx against a legacy SSMS-exported xlsx.

Normalizes both files to comparable Python types ("NULL" -> None, numeric Decimal
-> rounded float, times to second precision), then surfaces:
  - column header mismatches
  - row count delta
  - rows present in legacy but not generated
  - rows present in generated but not legacy

Run:
    python compare_to_legacy.py legacy.xlsx generated.xlsx

Exit code 0 = full match, 1 = differences found, 2 = structural error.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, time
from pathlib import Path

import pandas as pd

COLS = [
    "GamingDay", "Membership", "VisitNo",
    "CasinoEntryTime", "CasinoExitTime",
    "SessionStart", "SessionFinish", "averBet",
]
TIME_COLS = ("CasinoEntryTime", "CasinoExitTime", "SessionStart", "SessionFinish")


def _is_null(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    if isinstance(v, str) and v.strip().upper() == "NULL":
        return True
    return False


def _to_time(v):
    if _is_null(v):
        return None
    if isinstance(v, time):
        return v.replace(microsecond=0)
    if isinstance(v, datetime):
        return v.time().replace(microsecond=0)
    if isinstance(v, str):
        parts = v.split(":")
        return time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) >= 3 else 0)
    raise ValueError(f"cannot convert {v!r} ({type(v).__name__}) to time")


def _to_float4(v):
    if _is_null(v):
        return None
    return round(float(v), 4)


def load_normalize(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=object)
    if list(df.columns) != COLS:
        raise SystemExit(
            f"[STRUCTURE] {path.name}: column mismatch\n"
            f"  expected: {COLS}\n"
            f"  got:      {list(df.columns)}"
        )

    df["GamingDay"] = pd.to_datetime(df["GamingDay"]).dt.date
    df["Membership"] = df["Membership"].astype("Int64")
    df["VisitNo"] = df["VisitNo"].astype("Int64")
    for col in TIME_COLS:
        df[col] = df[col].apply(_to_time)
    df["averBet"] = df["averBet"].apply(_to_float4)
    return df


def _row_repr(row: tuple) -> str:
    return " | ".join("NULL" if v is None else str(v) for v in row)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("legacy", help="Legacy SSMS-exported xlsx (e.g. sample.xlsx)")
    parser.add_argument("generated", help="Output of export_visit_sessions.py")
    parser.add_argument(
        "--max-show", type=int, default=15,
        help="Max diff rows to print per side (default: 15)",
    )
    args = parser.parse_args()

    legacy_path = Path(args.legacy)
    generated_path = Path(args.generated)

    if not legacy_path.exists():
        print(f"[ERROR] legacy file not found: {legacy_path}")
        return 2
    if not generated_path.exists():
        print(f"[ERROR] generated file not found: {generated_path}")
        return 2

    a = load_normalize(legacy_path)
    b = load_normalize(generated_path)

    print(f"legacy    : {len(a):,} rows  ({legacy_path})")
    print(f"generated : {len(b):,} rows  ({generated_path})")

    if len(a) != len(b):
        print(f"\n[ROW COUNT] delta = {len(b) - len(a):+,} (generated minus legacy)")

    a_tuples = [tuple(r) for r in a[COLS].itertuples(index=False, name=None)]
    b_tuples = [tuple(r) for r in b[COLS].itertuples(index=False, name=None)]
    a_ct = Counter(a_tuples)
    b_ct = Counter(b_tuples)

    only_in_legacy = a_ct - b_ct
    only_in_generated = b_ct - a_ct

    legacy_extra = sum(only_in_legacy.values())
    generated_extra = sum(only_in_generated.values())

    if legacy_extra == 0 and generated_extra == 0:
        print("\n[OK] FULL MATCH — every row in legacy is reproduced exactly in generated.")
        return 0

    print(f"\n[DIFF] only in legacy   : {legacy_extra:,} row(s)")
    for i, (row, n) in enumerate(only_in_legacy.most_common()):
        if i >= args.max_show:
            print(f"  ... and {len(only_in_legacy) - args.max_show} more unique row(s)")
            break
        prefix = f"  ({n}x) " if n > 1 else "        "
        print(prefix + _row_repr(row))

    print(f"\n[DIFF] only in generated: {generated_extra:,} row(s)")
    for i, (row, n) in enumerate(only_in_generated.most_common()):
        if i >= args.max_show:
            print(f"  ... and {len(only_in_generated) - args.max_show} more unique row(s)")
            break
        prefix = f"  ({n}x) " if n > 1 else "        "
        print(prefix + _row_repr(row))

    return 1


if __name__ == "__main__":
    sys.exit(main())
