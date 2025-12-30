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

# ==================================================
# PROCESS
# ==================================================
for seg, src_dir in SRC_MAP.items():
    out_dir = OUT_MAP[seg]

    files = sorted(src_dir.glob("*.csv"))
    print(f"\n Processing {seg} | Files: {len(files)}")

    if not files:
        continue

    # ---------- LOAD ALL DAILY FILES ----------
    df = pd.concat(
        (pd.read_csv(f, low_memory=False) for f in files),
        ignore_index=True
    )

    # ---------- NORMALIZE COLUMNS ----------
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.upper()
    )

    # ---------- VALIDATE CONTRACT ----------
    missing = set(FINAL_COLS) - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns in {seg}: {sorted(missing)}")

    df = df[FINAL_COLS]

    # ---------- STRICT TYPE ENFORCEMENT ----------
    df["TRADE_DATE"] = pd.to_numeric(df["TRADE_DATE"], errors="coerce").astype("Int64")
    df["EXP_DATE"]   = pd.to_numeric(df["EXP_DATE"], errors="coerce").astype("Int64")
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

    # ---------- PER SYMBOL APPEND ----------
    for symbol, g in df.groupby("SYMBOL", sort=False):
        g = g.sort_values(SORT_KEYS)

        csv_out = out_dir / f"{symbol}.csv"
        pq_out  = out_dir / f"{symbol}.parquet"

        if csv_out.exists():
            old = pd.read_csv(csv_out, low_memory=False)

            old["TRADE_DATE"] = pd.to_numeric(old["TRADE_DATE"], errors="coerce").astype("Int64")
            old["EXP_DATE"]   = pd.to_numeric(old["EXP_DATE"], errors="coerce").astype("Int64")
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

    print(f" {seg} OPTIONS MASTER UPDATED → {out_dir}")

# ==================================================
# DONE
# ==================================================
print("\n OPTIONS MASTER BUILD COMPLETED (LOCKED & STANDARD)")
print(f" Output root: {OUT_ROOT}")
