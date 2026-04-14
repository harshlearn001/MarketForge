#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | MASTER LAST-ROW CHECKER (READ-ONLY)

✔ Checks all master datasets
✔ Prints last row per file
✔ Date-range aware
✔ CSV only
✔ ZERO side effects
"""

from pathlib import Path
import pandas as pd

ROOT = Path(r"H:\MarketForge\data\master")

MASTER_PATHS = {
    "EQUITY_STOCK": ROOT / "Equity_stock_master",
    "EQUITY_MTO": next(
        (
            p for p in [
                ROOT / "EquityDat_master",
                ROOT / "EqiutyDat_master",
            ]
            if p.exists()
        ),
        ROOT / "EquityDat_master",
    ),
    "FUTURES_STK": ROOT / "Futures_master" / "FUTSTK",
    "FUTURES_IDX": ROOT / "Futures_master" / "FUTIDX",
    "OPTIONS_STK": ROOT / "option_master" / "STOCKS",
    "OPTIONS_IDX": ROOT / "option_master" / "INDICES",
    "NIFTY_INDEX": ROOT / "Indices_master" / "master_nifty.csv",
}

def show_last_row(label, file):
    try:
        df = pd.read_csv(file, low_memory=False)
        if df.empty:
            print(f"[{label}] {file.name} → EMPTY")
            return

        last = df.iloc[-1].to_dict()

        date_col = next(
            (c for c in ["TRADE_DATE", "DATE"] if c in df.columns),
            None
        )

        print(f"\n[{label}] {file.name}")
        if date_col:
            print(f"  Date Range : {df[date_col].min()} → {df[date_col].max()}")
        print(f"  Rows       : {len(df)}")
        print(f"  Last Row   : {last}")

    except Exception as e:
        print(f"[{label}] {file.name} ❌ ERROR → {e}")

print("\n🔍 MarketForge | MASTER LAST ROW CHECK\n")

for label, path in MASTER_PATHS.items():

    # Single master file (NIFTY)
    if path.is_file():
        show_last_row(label, path)
        continue

    # Folder-based masters
    if path.is_dir():
        files = sorted(path.glob("*.csv"))
        if not files:
            print(f"[{label}] No CSV files found")
            continue

        for f in files:
            show_last_row(label, f)

print("\n✅ MASTER CHECK COMPLETED")
