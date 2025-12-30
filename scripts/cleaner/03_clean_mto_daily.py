#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NSE DELIVERY MTO CLEANER (FINAL LOCKED)

✔ Reads NSE MTO_*.DAT (binary safe)
✔ RECORD_TYPE = 20 (AUTHORITATIVE)
✔ Date from filename → YYYYMMDD (int)
✔ CSV / space delimiter auto-detect
✔ Garbage / footer safe
✔ Strict int / float dtypes
✔ CSV + Parquet output
✔ ZERO warnings
"""

from pathlib import Path
import pandas as pd
import re
from datetime import datetime
import sys

# ==================================================
# PATHS
# ==================================================
ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = ROOT / "data" / "raw" / "equityDat"
OUT_DIR = ROOT / "data" / "processed" / "equityDat_daily"
OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f" Scanning source dir : {RAW_DIR}")

# ==================================================
# DISCOVER LATEST MTO FILE
# ==================================================
files = sorted(
    RAW_DIR.glob("MTO_*.DAT"),
    key=lambda f: f.stat().st_mtime,
    reverse=True
)

print(f" Found {len(files)} MTO DAT files")

if not files:
    print("⚠ No MTO files found")
    sys.exit(0)

dat_file = files[0]
print(f"\n Processing: {dat_file.name}")

# ==================================================
# DATE FROM FILENAME (AUTHORITATIVE)
# ==================================================
m = re.search(r"MTO_(\d{8})", dat_file.name)
if not m:
    raise RuntimeError(" Cannot extract date from filename")

trade_date_int = int(
    datetime.strptime(m.group(1), "%d%m%Y").strftime("%Y%m%d")
)

# ==================================================
# READ FILE (BINARY SAFE)
# ==================================================
raw_lines = []

with open(dat_file, "rb") as f:
    for raw in f:
        raw = raw.replace(b"\x00", b"")
        line = raw.decode("utf-8", errors="ignore")
        line = line.replace("\t", " ").strip()
        if line:
            raw_lines.append(line)

print(f" Total raw lines: {len(raw_lines)}")

# ==================================================
# FILTER DELIVERY ROWS (RECORD_TYPE = 20)
# ==================================================
data_lines = [l for l in raw_lines if l.lstrip().startswith("20")]

print(f"Delivery rows found: {len(data_lines)}")

if not data_lines:
    print(" No delivery rows found — aborting")
    sys.exit(0)

# ==================================================
# SPLIT ROWS (CSV OR SPACE)
# ==================================================
if "," in data_lines[0]:
    rows = [l.split(",") for l in data_lines]
else:
    rows = [l.split() for l in data_lines]

rows = [r[:7] for r in rows if len(r) >= 7]

if not rows:
    print(" No usable delivery rows after split")
    sys.exit(0)

# ==================================================
# BUILD DATAFRAME
# ==================================================
df = pd.DataFrame(
    rows,
    columns=[
        "RECORD_TYPE",
        "SR_NO",
        "SYMBOL",
        "SERIES",
        "TRADED_QTY",
        "DELIVERABLE_QTY",
        "DELIVERY_PCT",
    ]
)

# ==================================================
# STANDARDIZE & TYPE ENFORCEMENT
# ==================================================
df.insert(0, "TRADE_DATE", trade_date_int)

df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper().str.strip()
df["SERIES"] = df["SERIES"].astype(str).str.upper().str.strip()

int_cols = [
    "RECORD_TYPE",
    "SR_NO",
    "TRADED_QTY",
    "DELIVERABLE_QTY",
]

for c in int_cols:
    df[c] = (
        pd.to_numeric(df[c], errors="coerce")
        .fillna(0)
        .astype("int64")
    )

df["DELIVERY_PCT"] = pd.to_numeric(
    df["DELIVERY_PCT"], errors="coerce"
).astype("float64")

df = df[df["SYMBOL"].notna() & (df["TRADED_QTY"] > 0)]

# ==================================================
# FINAL COLUMN ORDER (LOCKED)
# ==================================================
FINAL_COLS = [
    "TRADE_DATE",
    "RECORD_TYPE",
    "SR_NO",
    "SYMBOL",
    "SERIES",
    "TRADED_QTY",
    "DELIVERABLE_QTY",
    "DELIVERY_PCT",
]

df = df[FINAL_COLS]

# ==================================================
# SAVE OUTPUT
# ==================================================
out_csv = OUT_DIR / f"mto_{trade_date_int}.csv"
out_parquet = OUT_DIR / f"mto_{trade_date_int}.parquet"

df.to_csv(out_csv, index=False)
df.to_parquet(out_parquet, index=False)

print("\n MTO DELIVERY CLEANED & STANDARDIZED")
print(f" Date    : {trade_date_int}")
print(f" Rows    : {len(df)}")
print(f" Symbols : {df['SYMBOL'].nunique()}")
print(f" CSV     : {out_csv}")
print(f" Parquet : {out_parquet}")
