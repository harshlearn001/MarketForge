#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NIFTY 500 SYMBOL LIST DOWNLOADER (NSE SAFE)

✔ Browser headers
✔ Session-based
✔ Retry-safe
✔ Saves clean CSV
"""

import requests
import pandas as pd
from io import StringIO
from pathlib import Path
import time

# --------------------------------------------------
# OUTPUT PATH
# --------------------------------------------------
OUT_DIR = Path(r"H:\MarketForge\data\master")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "nifty_500_symbols.csv"

# --------------------------------------------------
# URL (OFFICIAL)
# --------------------------------------------------
URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"

# --------------------------------------------------
# NSE SAFE HEADERS
# --------------------------------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.niftyindices.com/",
    "Connection": "keep-alive",
}

# --------------------------------------------------
# DOWNLOAD USING SESSION
# --------------------------------------------------
session = requests.Session()
session.headers.update(HEADERS)

# warm-up request (CRITICAL for NSE)
session.get("https://www.niftyindices.com", timeout=10)
time.sleep(1)

resp = session.get(URL, timeout=15)
resp.raise_for_status()

# --------------------------------------------------
# LOAD CSV
# --------------------------------------------------
df = pd.read_csv(StringIO(resp.text))

# normalize column names
df.columns = (
    df.columns
      .str.strip()
      .str.upper()
)

# keep only symbol column (safe)
symbol_col = [c for c in df.columns if "SYMBOL" in c][0]
out = df[[symbol_col]].rename(columns={symbol_col: "SYMBOL"})

# --------------------------------------------------
# SAVE
# --------------------------------------------------
out.to_csv(OUT_FILE, index=False)

print("✅ NIFTY 500 SYMBOL LIST DOWNLOADED")
print(f"📄 Symbols : {len(out)}")
print(f"📁 Saved  : {OUT_FILE}")
