#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MarketForge PRO
Historical Participant Data Batch Cleaner (v2.0 - Vertical Stack Engine)

✔ Scans and loops through ALL historical raw files automatically
✔ Stacks Volume and OI records vertically row-upon-row inside single clean sheets
✔ Forces standard clean integer casting to remove trailing decimals (.0)
✔ Standardizes date tags to uniform YYYY-MM-DD strings
✔ Generates clean individual snapshots plus a master time-series database
"""

import pandas as pd
import re
from pathlib import Path
from collections import defaultdict
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn

console = Console()

console.print(Panel.fit(
    "[bold cyan]HISTORICAL DATA BATCH CLEANER[/bold cyan]\n[dim]MarketForge PRO Engine v2.0[/dim]",
    border_style="cyan"
))

# ==========================================
# PATHS
# ==========================================
RAW_DIR     = Path(r"H:\MarketForge\data\raw\participant_historical")
CLEAN_DIR   = Path(r"H:\MarketForge\data\processed\participant_historical")
MASTER_FILE = CLEAN_DIR / "participant_master.csv"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================
# FILE PAIRING & DATE EXTRACTION
# ==========================================
all_files = list(RAW_DIR.glob("participant_*.csv"))

if not all_files:
    console.print("[red]❌ No raw historical files found in:[/red]", RAW_DIR)
    exit()

# Group files by date so we process VOL and OI files for the same day together
date_groups = defaultdict(list)
for f in all_files:
    match = re.search(r'(\d{8})', f.name)
    if match:
        date_str = match.group(1)
        # Convert DDMMYYYY filename string to standard YYYY-MM-DD
        date_iso = pd.to_datetime(date_str, format="%d%m%Y").strftime("%Y-%m-%d")
        date_groups[date_iso].append(f)

sorted_dates = sorted(date_groups.keys())
console.print(f"📦 Found [bold green]{len(all_files)}[/bold green] raw files spanning [bold cyan]{len(sorted_dates)}[/bold cyan] unique trading dates.\n")

# ==========================================
# SENTIMENT CONFIGURATION
# ==========================================
def calculate_sentiment(net_position):
    if net_position > 50000:   return "STRONG LONG"
    if net_position > 0:       return "LONG"
    if net_position < -50000:  return "STRONG SHORT"
    if net_position < 0:       return "SHORT"
    return "NEUTRAL"

# ==========================================
# PARSING ENGINE FOR A SINGLE DATE
# ==========================================
def process_date_files(date_iso: str, files: list) -> pd.DataFrame:
    """Processes all files for a date and stacks them vertically (row-upon-row)."""
    processed_frames = []

    for file_path in files:
        # Determine tracking type based on file name token markers
        label_type = "VOLUME" if "_vol_" in file_path.name else "OI"
        
        try:
            # Skip NSE text header line safely
            df = pd.read_csv(file_path, skiprows=1)
            if df.empty or "Participant wise" in df.columns[0]:
                df = pd.read_csv(file_path)
        except Exception:
            continue

        if df.empty:
            continue

        # Clean columns to standard lower_snake_case
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace(r"[^a-z0-9_]", "", regex=True)
        )

        if "client_type" not in df.columns:
            continue

        df["client_type"] = df["client_type"].astype(str).str.upper().str.strip()
        valid_types = ["FII", "DII", "PRO", "CLIENT", "TOTAL"]
        df = df[df["client_type"].isin(valid_types)].copy()

        numeric_targets = [
            "future_index_long",   "future_index_short",
            "future_stock_long",   "future_stock_short",
            "option_index_call_long",  "option_index_put_long",
            "option_index_call_short", "option_index_put_short",
            "option_stock_call_long",  "option_stock_put_long",
            "option_stock_call_short", "option_stock_put_short",
            "total_long_contracts",    "total_short_contracts",
        ]
        
        # Clean comma strings and cast variables to clean standard integers
        for col in numeric_targets:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            else:
                df[col] = 0

        # Calculate math transforms explicitly using native integers
        df["net_index_futures"] = (df.get("future_index_long", 0) - df.get("future_index_short", 0)).astype(int)
        df["net_stock_futures"] = (df.get("future_stock_long", 0) - df.get("future_stock_short", 0)).astype(int)
        
        df["net_index_options"] = (
            df.get("option_index_call_long", 0) + df.get("option_index_put_long", 0) -
            df.get("option_index_call_short", 0) - df.get("option_index_put_short", 0)
        ).astype(int)
        df["net_total"] = (df.get("total_long_contracts", 0) - df.get("total_short_contracts", 0)).astype(int)

        df["sentiment_index"] = df["net_index_futures"].apply(calculate_sentiment)
        df["sentiment_stock"] = df["net_stock_futures"].apply(calculate_sentiment)
        
        # Assign categorization tracking indices
        df["data_type"] = label_type
        df["date"] = str(date_iso)
        
        processed_frames.append(df)

    if not processed_frames:
        return pd.DataFrame()

    # Concatenate the elements matching this calendar date into a single frame
    combined_df = pd.concat(processed_frames, ignore_index=True)

    final_schema = [
        "date", "data_type", "client_type",
        "future_index_long",    "future_index_short",   "net_index_futures",
        "future_stock_long",    "future_stock_short",   "net_stock_futures",
        "option_index_call_long","option_index_put_long",
        "option_index_call_short","option_index_put_short","net_index_options",
        "total_long_contracts", "total_short_contracts", "net_total",
        "sentiment_index",      "sentiment_stock",
    ]
    
    existing_cols = [c for c in final_schema if c in combined_df.columns]
    return combined_df[existing_cols]

# ==========================================
# MASTER PROCESSING LOOP
# ==========================================
master_frames = []

# Using Rich progress tracker to avoid console text flooding
with Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}"),
    BarColumn(bar_width=40, complete_style="cyan"),
    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
) as progress:

    task = progress.add_task("[cyan]Processing history...", total=len(sorted_dates))

    for date_iso in sorted_dates:
        files = date_groups[date_iso]
        day_df = process_date_files(date_iso, files)

        if not day_df.empty:
            master_frames.append(day_df)
            # Save the individual optimized day file containing stacked rows
            day_output = CLEAN_DIR / f"participant_clean_{date_iso}.csv"
            day_df.to_csv(day_output, index=False)

        progress.advance(task)

# ==========================================
# EXPORT UNIFIED TIME-SERIES
# ==========================================
if master_frames:
    console.print("\n[yellow]⏳ Assembling time-series master registry database...[/yellow]")
    master_df = pd.concat(master_frames, ignore_index=True)
    
    # Sort logically so tracking indices group predictably
    master_df = master_df.sort_values(["date", "data_type", "client_type"]).reset_index(drop=True)
    master_df.to_csv(MASTER_FILE, index=False)
    
    console.print(f"[green]✅ Process complete![/green]")
    console.print(f"📁 [bold white]{len(master_frames)}[/bold white] clean calendar day snapshot files generated.")
    console.print(f"📊 Global unified master tracker built at: [cyan]{MASTER_FILE.name}[/cyan] ([bold white]{len(master_df)}[/bold white] data matrix rows).")
else:
    console.print("[red]❌ Error: No valid historical structures could be extracted.[/red]")