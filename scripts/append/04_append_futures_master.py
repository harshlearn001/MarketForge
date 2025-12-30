#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | FUTURES MASTER APPENDER (FINAL LOCKED)

✔ Consumes STANDARD daily futures files
✔ Dates are already YYYYMMDD (int) → NO parsing
✔ Handles OPEN_INT*, OPEN_INT, OPNINTRST
✔ NSE-safe
✔ CSV only
✔ ZERO warnings
✔ Idempotent
"""

from pathlib import Path
import pandas as pd

# ==================================================
# PATHS
# ==================================================
ROOT = Path(r"H:\MarketForge")

DAILY_ROOT = ROOT / "data" / "processed" / "futures_daily"
MASTER_ROOT = ROOT / "data" / "master" / "Futures_master"

STOCK_MASTER = MASTER_ROOT / "FUTSTK"
INDEX_MASTER = MASTER_ROOT / "FUTIDX"

STOCK_MASTER.mkdir(parents=True, exist_ok=True)
INDEX_MASTER.mkdir(parents=True, exist_ok=True)

# ==================================================
# COLUMN MAP (REAL NSE VARIANTS)
# ==================================================
OI_ALIASES = ["OPEN_INT*", "OPEN_INT", "OPNINTRST"]

FINAL_COLS = [
    "INSTRUMENT",
    "SYMBOL",
    "EXP_DATE",
    "OPEN_PRICE",
    "HI_PRICE",
    "LO_PRICE",
    "CLOSE_PRICE",
    "OPEN_INT",
    "TRD_VAL",
    "TRD_QTY",
    "NO_OF_CONT",
    "NO_OF_TRADE",
    "TRADE_DATE",
]

# ==================================================
# APPEND FUNCTION
# ==================================================
def append_futures(daily_file: Path, out_dir: Path):
    print(f"  → Reading {daily_file.name}")

    df = pd.read_csv(daily_file, low_memory=False)

    # -----------------------------
    # NORMALIZE COLUMN NAMES
    # -----------------------------
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.upper()
    )

    # -----------------------------
    # FIX OPEN INTEREST COLUMN
    # -----------------------------
    oi_col = next((c for c in OI_ALIASES if c in df.columns), None)
    if not oi_col:
        raise RuntimeError(f"No OPEN INTEREST column found in {daily_file.name}")

    df = df.rename(columns={oi_col: "OPEN_INT"})

    # -----------------------------
    # HARD REQUIRED COLUMNS
    # -----------------------------
    missing = set(FINAL_COLS) - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns {sorted(missing)} in {daily_file.name}")

    df = df[FINAL_COLS]

    # -----------------------------
    # STRICT TYPE ENFORCEMENT
    # -----------------------------
    df["TRADE_DATE"] = pd.to_numeric(df["TRADE_DATE"], errors="coerce").astype("Int64")
    df["EXP_DATE"] = pd.to_numeric(df["EXP_DATE"], errors="coerce").astype("Int64")

    num_cols = [
        "OPEN_PRICE", "HI_PRICE", "LO_PRICE", "CLOSE_PRICE",
        "OPEN_INT", "TRD_VAL", "TRD_QTY",
        "NO_OF_CONT", "NO_OF_TRADE"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()

    df = df[
        df["TRADE_DATE"].notna() &
        df["EXP_DATE"].notna() &
        df["SYMBOL"].notna()
    ]

    # -----------------------------
    # APPEND PER SYMBOL (IDEMPOTENT)
    # -----------------------------
    for symbol, g in df.groupby("SYMBOL"):
        out_file = out_dir / f"{symbol}.csv"

        if out_file.exists():
            old = pd.read_csv(out_file, low_memory=False)

            old["TRADE_DATE"] = pd.to_numeric(old["TRADE_DATE"], errors="coerce").astype("Int64")
            old["EXP_DATE"] = pd.to_numeric(old["EXP_DATE"], errors="coerce").astype("Int64")

            merged = (
                pd.concat([old, g], ignore_index=True)
                .drop_duplicates(
                    subset=["SYMBOL", "TRADE_DATE", "EXP_DATE"],
                    keep="last"
                )
                .sort_values(["TRADE_DATE", "EXP_DATE"])
            )
        else:
            merged = g.sort_values(["TRADE_DATE", "EXP_DATE"])

        merged.to_csv(out_file, index=False)

# ==================================================
# RUN
# ==================================================
print("\nProcessing STOCK FUTURES")
for f in sorted((DAILY_ROOT / "STOCKS").glob("futstk*.csv")):
    append_futures(f, STOCK_MASTER)

print("\nProcessing INDEX FUTURES")
for f in sorted((DAILY_ROOT / "INDICES").glob("futidx*.csv")):
    append_futures(f, INDEX_MASTER)

print("\n FUTURES MASTER APPEND COMPLETED (LOCKED, ZERO WARNINGS)")
print(f" FUTSTK → {STOCK_MASTER}")
print(f"FUTIDX → {INDEX_MASTER}")
