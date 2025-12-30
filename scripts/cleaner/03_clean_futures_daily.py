#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | FO DAILY → FUTURES ONLY (FINAL • ZERO WARNINGS)

✔ Works with ALL NSE FO formats
✔ Filename date = authoritative
✔ FUTSTK / FUTIDX safe split
✔ OI column normalized (OPEN_INT*)
✔ Standard column names
✔ EXP_DATE → YYYYMMDD (Int64) (mixed formats safe)
✔ TRADE_DATE → YYYYMMDD (int)
✔ Numeric columns enforced
✔ CSV ONLY (master-safe)
✔ Production hardened
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
OUT_ROOT = ROOT / "data" / "processed" / "futures_daily"

OUT_FUT_STK = OUT_ROOT / "STOCKS"
OUT_FUT_IDX = OUT_ROOT / "INDICES"

OUT_FUT_STK.mkdir(parents=True, exist_ok=True)
OUT_FUT_IDX.mkdir(parents=True, exist_ok=True)

print(f" Scanning source dir : {SRC_DIR}")

# =================================================
# DISCOVER FO MASTER FILES
# =================================================
files = sorted(
    f for f in SRC_DIR.glob("fo*.csv")
    if not f.name.startswith("fo_")
)

print(f" Found {len(files)} FO master CSV files")

if not files:
    raise RuntimeError(" No foDDMMYYYY.csv files found")

# =================================================
# NSE REALITY — POSSIBLE INSTRUMENT COLUMNS
# =================================================
INSTRUMENT_COLS = {
    "INSTRUMENT",
    "FININSTRMTP",
    "FIN_INSTRM_TP",
    "INSTRUMENT_TYPE",
}

# =================================================
# NSE COLUMN STANDARDIZATION
# =================================================
RENAME_MAP = {
    "OPNPRIC": "OPEN_PRICE",
    "HGHPRIC": "HI_PRICE",
    "LWPRIC": "LO_PRICE",
    "CLSPRIC": "CLOSE_PRICE",
    "OPNINTRST": "OPEN_INT",
    "CHNGINOPNINTRST": "CHG_IN_OI",
    "TTLTRADGVOL": "TRD_QTY",
    "TTLTRFVAL": "TRD_VAL",
    "TTLNBOFTXSEXCTD": "NO_OF_TRADE",
    "NOOFCONTRACTS": "NO_OF_CONT",
}

FLOAT_COLS = {
    "OPEN_PRICE", "HI_PRICE", "LO_PRICE",
    "CLOSE_PRICE", "TRD_VAL"
}

INT_COLS = {
    "OPEN_INT", "CHG_IN_OI",
    "TRD_QTY", "NO_OF_TRADE",
    "NO_OF_CONT"
}

# =================================================
# NSE-SAFE EXP_DATE PARSER (ZERO WARNINGS)
# =================================================
def standardize_exp_date(series: pd.Series) -> pd.Series:
    """
    Handles:
    - YYYY-MM-DD
    - DD-MM-YYYY
    - DD/MM/YYYY
    Output:
    - YYYYMMDD (Int64)
    """
    s = series.astype(str).str.strip()
    out = pd.Series(pd.NA, index=s.index, dtype="Int64")

    # ISO format
    iso_mask = s.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    if iso_mask.any():
        out.loc[iso_mask] = (
            pd.to_datetime(
                s.loc[iso_mask],
                format="%Y-%m-%d",
                errors="coerce"
            )
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )

    # NSE formats
    non_iso = ~iso_mask
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
    print(f"\n▶ Processing: {file.name}")

    # ---------------------------------------------
    # DATE FROM FILENAME (DDMMYYYY — AUTHORITATIVE)
    # ---------------------------------------------
    m = re.search(r"(\d{8})", file.stem)
    if not m:
        print("Cannot extract date — skipped")
        continue

    trade_date = datetime.strptime(m.group(1), "%d%m%Y")
    trade_date_int = int(trade_date.strftime("%Y%m%d"))

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

    # ---------------------------------------------
    # DETECT INSTRUMENT COLUMN
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
    # FUTURES ONLY
    # ---------------------------------------------
    fut = df[df["INSTRUMENT"].str.startswith("FUT", na=False)].copy()

    if fut.empty:
        print(" No futures rows found")
        continue

    # ---------------------------------------------
    # ADD TRADE DATE (STANDARD INT)
    # ---------------------------------------------
    fut["TRADE_DATE"] = trade_date_int

    # ---------------------------------------------
    # FIX OPEN_INT*
    # ---------------------------------------------
    if "OPEN_INT*" in fut.columns:
        fut = fut.rename(columns={"OPEN_INT*": "OPNINTRST"})

    # ---------------------------------------------
    # STANDARDIZE COLUMN NAMES
    # ---------------------------------------------
    fut = fut.rename(
        columns={k: v for k, v in RENAME_MAP.items() if k in fut.columns}
    )

    # ---------------------------------------------
    # EXP_DATE STANDARDIZATION (ZERO WARNINGS)
    # ---------------------------------------------
    if "EXP_DATE" in fut.columns:
        fut["EXP_DATE"] = standardize_exp_date(fut["EXP_DATE"])

    # ---------------------------------------------
    # TYPE ENFORCEMENT
    # ---------------------------------------------
    for col in FLOAT_COLS & set(fut.columns):
        fut[col] = pd.to_numeric(fut[col], errors="coerce").astype("float64")

    for col in INT_COLS & set(fut.columns):
        fut[col] = (
            pd.to_numeric(fut[col], errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    # ---------------------------------------------
    # SYMBOL CLEAN
    # ---------------------------------------------
    if "SYMBOL" in fut.columns:
        fut["SYMBOL"] = fut["SYMBOL"].astype(str).str.strip()

    # ---------------------------------------------
    # SPLIT & SAVE
    # ---------------------------------------------
    tag = trade_date.strftime("%d%m%Y")

    futstk = fut[fut["INSTRUMENT"].str.startswith("FUTSTK")]
    futidx = fut[fut["INSTRUMENT"].str.startswith("FUTIDX")]

    if not futstk.empty:
        out = OUT_FUT_STK / f"futstk{tag}.csv"
        futstk.sort_values(["SYMBOL", "EXP_DATE"]).to_csv(out, index=False)
        print(f" Saved → {out}")

    if not futidx.empty:
        out = OUT_FUT_IDX / f"futidx{tag}.csv"
        futidx.sort_values(["SYMBOL", "EXP_DATE"]).to_csv(out, index=False)
        print(f" Saved → {out}")

print("\n FUTURES DAILY SPLIT COMPLETED (LOCKED, STANDARD & ZERO WARNINGS)")
