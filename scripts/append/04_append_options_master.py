#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | OPTIONS MASTER BUILDER (FINAL LOCKED)

✔ Consumes STANDARDIZED options_daily output
✔ Dates already YYYYMMDD → NO parsing
✔ STRIKE_PRICE enforced
✔ Append-safe & idempotent
✔ CSV + Parquet (same schema)
✔ ZERO warnings
"""

from pathlib import Path
import pandas as pd

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

SRC_ROOT = ROOT / "data" / "processed" / "options_daily"
OUT_ROOT = ROOT / "data" / "master" / "option_master"
STATE_DIR = OUT_ROOT / "_state"

SRC_MAP = {
    "STOCKS": SRC_ROOT / "STOCKS",
    "INDICES": SRC_ROOT / "INDICES",
}

OUT_MAP = {
    "STOCKS": OUT_ROOT / "STOCKS",
    "INDICES": OUT_ROOT / "INDICES",
}

for p in OUT_MAP.values():
    p.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

print("\n MarketForge | OPTIONS MASTER BUILD STARTED")

# ==================================================
# HARD CONTRACT (FINAL)
# ==================================================
FINAL_COLS = [
    "INSTRUMENT",
    "SYMBOL",
    "TRADE_DATE",
    "EXP_DATE",
    "STRIKE_PRICE",
    "OPT_TYPE",
    "OPEN_PRICE",
    "HI_PRICE",
    "LO_PRICE",
    "CLOSE_PRICE",
    "OPEN_INT",
    "TRD_QTY",
    "NO_OF_CONT",
    "NO_OF_TRADE",
    "NOTION_VAL",
    "PR_VAL",
]

DEDUP_KEYS = [
    "SYMBOL",
    "TRADE_DATE",
    "EXP_DATE",
    "STRIKE_PRICE",
    "OPT_TYPE",
]

SORT_KEYS = DEDUP_KEYS


def load_processed(state_file: Path) -> set[str]:
    if not state_file.exists():
        return set()

    return {
        line.strip()
        for line in state_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def save_processed(state_file: Path, processed: set[str]) -> None:
    state_file.write_text(
        "\n".join(sorted(processed)) + ("\n" if processed else ""),
        encoding="utf-8",
    )


def append_option_daily_file(daily_file: Path, out_dir: Path):
    df = pd.read_csv(daily_file, low_memory=False)

    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.upper()
    )

    missing = set(FINAL_COLS) - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns in {daily_file.name}: {sorted(missing)}")

    df = df[FINAL_COLS]

    df["TRADE_DATE"] = pd.to_numeric(df["TRADE_DATE"], errors="coerce").astype("Int64")
    df["EXP_DATE"] = pd.to_numeric(df["EXP_DATE"], errors="coerce").astype("Int64")
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce").astype("int64")

    float_cols = [
        "OPEN_PRICE", "HI_PRICE", "LO_PRICE",
        "CLOSE_PRICE", "PR_VAL"
    ]
    for c in float_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    int_cols = [
        "OPEN_INT", "TRD_QTY",
        "NO_OF_CONT", "NO_OF_TRADE",
        "NOTION_VAL"
    ]
    for c in int_cols:
        df[c] = (
            pd.to_numeric(df[c], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()
    df["OPT_TYPE"] = df["OPT_TYPE"].astype(str).str.strip()

    df = df[
        df["TRADE_DATE"].notna() &
        df["EXP_DATE"].notna() &
        df["SYMBOL"].notna()
    ]

    for symbol, g in df.groupby("SYMBOL", sort=False):
        g = g.sort_values(SORT_KEYS)

        csv_out = out_dir / f"{symbol}.csv"
        pq_out = out_dir / f"{symbol}.parquet"

        if csv_out.exists():
            old = pd.read_csv(csv_out, low_memory=False)

            old["TRADE_DATE"] = pd.to_numeric(old["TRADE_DATE"], errors="coerce").astype("Int64")
            old["EXP_DATE"] = pd.to_numeric(old["EXP_DATE"], errors="coerce").astype("Int64")
            old["STRIKE_PRICE"] = pd.to_numeric(old["STRIKE_PRICE"], errors="coerce").astype("int64")

            merged = (
                pd.concat([old, g], ignore_index=True)
                .drop_duplicates(subset=DEDUP_KEYS, keep="last")
                .sort_values(SORT_KEYS)
            )
        else:
            merged = g

        merged.to_csv(csv_out, index=False)
        merged.to_parquet(pq_out, index=False)

# ==================================================
# PROCESS
# ==================================================
for seg, src_dir in SRC_MAP.items():
    out_dir = OUT_MAP[seg]
    state_file = STATE_DIR / f"processed_{seg.lower()}_files.txt"
    processed = load_processed(state_file)

    files = sorted(src_dir.glob("*.csv"))
    print(f"\n Processing {seg} | Files: {len(files)}")

    if not files:
        continue

    updated = False
    for daily_file in files:
        if daily_file.name in processed:
            print(f"  Skipping already appended {daily_file.name}")
            continue
        print(f"  Appending {daily_file.name}")
        append_option_daily_file(daily_file, out_dir)
        processed.add(daily_file.name)
        updated = True

    if updated:
        save_processed(state_file, processed)

    print(f" {seg} OPTIONS MASTER UPDATED → {out_dir}")

# ==================================================
# DONE
# ==================================================
print("\n OPTIONS MASTER BUILD COMPLETED (LOCKED & STANDARD)")
print(f" Output root: {OUT_ROOT}")
