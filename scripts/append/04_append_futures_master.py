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
STATE_DIR = MASTER_ROOT / "_state"

STOCK_MASTER = MASTER_ROOT / "FUTSTK"
INDEX_MASTER = MASTER_ROOT / "FUTIDX"

STOCK_MASTER.mkdir(parents=True, exist_ok=True)
INDEX_MASTER.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

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

STOCK_STATE = STATE_DIR / "processed_futstk_files.txt"
INDEX_STATE = STATE_DIR / "processed_futidx_files.txt"


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
processed_stock = load_processed(STOCK_STATE)
stock_updated = False
for f in sorted((DAILY_ROOT / "STOCKS").glob("futstk*.csv")):
    if f.name in processed_stock:
        print(f"  → Skipping already appended {f.name}")
        continue
    append_futures(f, STOCK_MASTER)
    processed_stock.add(f.name)
    stock_updated = True

if stock_updated:
    save_processed(STOCK_STATE, processed_stock)

print("\nProcessing INDEX FUTURES")
processed_index = load_processed(INDEX_STATE)
index_updated = False
for f in sorted((DAILY_ROOT / "INDICES").glob("futidx*.csv")):
    if f.name in processed_index:
        print(f"  → Skipping already appended {f.name}")
        continue
    append_futures(f, INDEX_MASTER)
    processed_index.add(f.name)
    index_updated = True

if index_updated:
    save_processed(INDEX_STATE, processed_index)

print("\n FUTURES MASTER APPEND COMPLETED (LOCKED, ZERO WARNINGS)")
print(f" FUTSTK → {STOCK_MASTER}")
print(f"FUTIDX → {INDEX_MASTER}")
