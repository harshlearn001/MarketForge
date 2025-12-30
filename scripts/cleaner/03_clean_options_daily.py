#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | FO DAILY → OPTIONS ONLY (FINAL LOCKED & STANDARDIZED)

✔ Works with opDDMMYYYY.csv
✔ Auto-detects instrument column
✔ OPTSTK / OPTIDX robust detection
✔ TRADE_DATE from filename → YYYYMMDD (int)
✔ EXP_DATE → YYYYMMDD (Int64)
✔ STRIKE_PRICE → int
✔ OPEN_INT* normalized
✔ Numeric columns → strict int / float
✔ STOCKS / INDICES separation
✔ CE + PE together
✔ ZERO warnings
"""

from pathlib import Path
import pandas as pd
import re
from datetime import datetime

# =================================================
# PATHS
# =================================================
ROOT = Path(__file__).resolve().parents[2]

SRC_DIR = ROOT / "data" / "unzip_daily" / "future_daily_unzip"
OUT_ROOT = ROOT / "data" / "processed" / "options_daily"

OUT_OPT_STK = OUT_ROOT / "STOCKS"
OUT_OPT_IDX = OUT_ROOT / "INDICES"

OUT_OPT_STK.mkdir(parents=True, exist_ok=True)
OUT_OPT_IDX.mkdir(parents=True, exist_ok=True)

print(f"Scanning source dir : {SRC_DIR}")

# =================================================
# DISCOVER OPTION DAILY FILES
# =================================================
files = sorted(
    f for f in SRC_DIR.glob("op*.csv")
    if re.fullmatch(r"op\d{8}\.csv", f.name)
)

print(f" Found {len(files)} option daily CSV files")

if not files:
    raise RuntimeError(" No opDDMMYYYY.csv files found")

# =================================================
# POSSIBLE INSTRUMENT COLUMN NAMES
# =================================================
INSTRUMENT_COLS = {
    "INSTRUMENT",
    "FININSTRMTP",
    "FIN_INSTRM_TP",
    "INSTRUMENT_TYPE",
}

# =================================================
# COLUMN STANDARDIZATION
# =================================================
RENAME_MAP = {
    "STR_PRICE": "STRIKE_PRICE",
    "STRK_PRICE": "STRIKE_PRICE",
    "STRKPRIC": "STRIKE_PRICE",
    "OPEN_INT*": "OPEN_INT",
}

FLOAT_COLS = {
    "OPEN_PRICE", "HI_PRICE", "LO_PRICE",
    "CLOSE_PRICE", "PR_VAL"
}

INT_COLS = {
    "STRIKE_PRICE",
    "OPEN_INT", "TRD_QTY",
    "NO_OF_CONT", "NO_OF_TRADE",
    "NOTION_VAL"
}

# =================================================
# DATE STANDARDIZER (YYYYMMDD | ZERO WARNINGS)
# =================================================
def standardize_date(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    out = pd.Series(pd.NA, index=s.index, dtype="Int64")

    iso = s.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    if iso.any():
        out.loc[iso] = (
            pd.to_datetime(
                s.loc[iso],
                format="%Y-%m-%d",
                errors="coerce"
            )
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )

    non_iso = ~iso
    if non_iso.any():
        out.loc[non_iso] = (
            pd.to_datetime(
                s.loc[non_iso],
                dayfirst=True,
                errors="coerce"
            )
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )

    return out

# =================================================
# PROCESS
# =================================================
for file in files:
    print(f"\n Processing: {file.name}")

    # ---------------------------------------------
    # TRADE DATE FROM FILENAME (AUTHORITATIVE)
    # ---------------------------------------------
    m = re.search(r"(\d{8})", file.stem)
    if not m:
        print(" Cannot extract date — skipped")
        continue

    trade_date_int = int(
        datetime.strptime(m.group(1), "%d%m%Y").strftime("%Y%m%d")
    )

    # ---------------------------------------------
    # LOAD
    # ---------------------------------------------
    df = pd.read_csv(file, low_memory=False)

    # ---------------------------------------------
    # NORMALIZE COLUMN NAMES
    # ---------------------------------------------
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.upper()
    )

    df = df.rename(columns=RENAME_MAP)

    # ---------------------------------------------
    # AUTO-DETECT INSTRUMENT COLUMN
    # ---------------------------------------------
    instr_col = next(
        (c for c in INSTRUMENT_COLS if c in df.columns),
        None
    )

    if not instr_col:
        print(" No instrument column found — skipped")
        continue

    df["INSTRUMENT"] = df[instr_col].astype(str).str.strip()

    # ---------------------------------------------
    # OPTIONS FILTER
    # ---------------------------------------------
    opt = df[df["INSTRUMENT"].str.startswith("OPT", na=False)].copy()

    if opt.empty:
        print(" No options data found")
        continue

    # ---------------------------------------------
    # ADD TRADE DATE (INT)
    # ---------------------------------------------
    opt["TRADE_DATE"] = trade_date_int

    # ---------------------------------------------
    # EXP_DATE STANDARDIZATION
    # ---------------------------------------------
    if "EXP_DATE" in opt.columns:
        opt["EXP_DATE"] = standardize_date(opt["EXP_DATE"])

    # ---------------------------------------------
    # NUMERIC STANDARDIZATION
    # ---------------------------------------------
    for col in FLOAT_COLS & set(opt.columns):
        opt[col] = pd.to_numeric(opt[col], errors="coerce").astype("float64")

    for col in INT_COLS & set(opt.columns):
        opt[col] = (
            pd.to_numeric(opt[col], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    # ---------------------------------------------
    # TEXT CLEAN
    # ---------------------------------------------
    for col in ("SYMBOL", "OPT_TYPE", "INSTRUMENT"):
        if col in opt.columns:
            opt[col] = opt[col].astype(str).str.strip()

    # ---------------------------------------------
    # SPLIT & SAVE
    # ---------------------------------------------
    tag = m.group(1)

    optstk = opt[opt["INSTRUMENT"].str.startswith("OPTSTK")]
    optidx = opt[opt["INSTRUMENT"].str.startswith("OPTIDX")]

    if not optstk.empty:
        out = OUT_OPT_STK / f"optstk{tag}.csv"
        optstk.sort_values(
            ["SYMBOL", "EXP_DATE", "STRIKE_PRICE", "OPT_TYPE"]
        ).to_csv(out, index=False)
        print(f"Saved → {out}")

    if not optidx.empty:
        out = OUT_OPT_IDX / f"optidx{tag}.csv"
        optidx.sort_values(
            ["SYMBOL", "EXP_DATE", "STRIKE_PRICE", "OPT_TYPE"]
        ).to_csv(out, index=False)
        print(f" Saved → {out}")

print("\n OPTIONS DAILY SPLIT COMPLETED (LOCKED, STANDARD & ZERO WARNINGS)")
