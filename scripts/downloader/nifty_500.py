#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge | NIFTY 500 SYMBOL LIST DOWNLOADER (PRO)

✔ NSE safe
✔ Clean symbols
✔ Deduplicated
✔ Ready for system integration
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
# URL
# --------------------------------------------------
URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"

# --------------------------------------------------
# HEADERS
# --------------------------------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv,*/*",
    "Referer": "https://www.niftyindices.com/",
}

# --------------------------------------------------
# SESSION
# --------------------------------------------------
session = requests.Session()
session.headers.update(HEADERS)

# warm-up
session.get("https://www.niftyindices.com", timeout=10)
time.sleep(1)

# download
resp = session.get(URL, timeout=15)
resp.raise_for_status()

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
df = pd.read_csv(StringIO(resp.text))

# normalize columns
df.columns = df.columns.str.strip().str.upper()

# detect symbol column
symbol_col = [c for c in df.columns if "SYMBOL" in c][0]

symbols = (
    df[symbol_col]
    .astype(str)
    .str.strip()
    .str.upper()
)

# --------------------------------------------------
# CLEAN SYMBOLS
# --------------------------------------------------
symbols = symbols[
    (symbols != "") &
    (symbols != "NAN") &
    (symbols.str.len() > 0)
]

# remove duplicates
symbols = sorted(symbols.unique())

# --------------------------------------------------
# SAVE
# --------------------------------------------------
out = pd.DataFrame(symbols, columns=["SYMBOL"])
out.to_csv(OUT_FILE, index=False)

print("✅ NIFTY 500 SYMBOL LIST DOWNLOADED (CLEAN)")
print(f"📄 Symbols : {len(out)}")
print(f"📁 Saved  : {OUT_FILE}")