# MarketForge Architecture

MarketForge is a deterministic, append-only NSE market data engine built around staged daily ingestion.

## Core Principles

- Code-first repository; large data stays outside git
- Reproducible downloader → cleaner → append flow
- Append-only master datasets
- Deduplicated reruns
- Safe handling of NSE delay / partial publication behavior
- Scheduler-friendly operation

## Pipeline Model

```text
Downloader
  ↓
Cleaner
  ↓
Append
  ↓
Master Datasets
```

## Modules

### 1. Downloader

Responsibility:
- Fetch raw NSE data

Active download domains:
- CM bhavcopy
- FO zip
- MTO DAT
- Indices OHLC
- FII/DII activity
- Participant data

Design notes:
- automatic backtracking when current-day files are not yet published
- archive-first behavior where possible
- scheduler-safe logging and exits

### 2. Cleaner

Responsibility:
- Normalize raw NSE files into stable daily outputs

Active cleaner behaviors:
- unzip CM and FO archives
- normalize old/new NSE schema variants
- split futures and options by segment
- standardize dates and numeric fields
- filter equity daily outputs to `SERIES = EQ`

Daily cleaner outputs live under:
- `data/processed/equity_daily/`
- `data/processed/futures_daily/`
- `data/processed/options_daily/`
- `data/processed/equityDat_daily/`
- `data/processed/indices_daily/`
- `data/processed/participant/`
- `data/processed/fii_dii/`

### 3. Append

Responsibility:
- Convert daily cleaned outputs into long-lived masters

Append patterns:
- per-symbol equity masters
- per-symbol MTO masters
- per-symbol futures masters
- per-symbol options masters
- per-index OHLC masters
- participant and FII/DII master files

Append guarantees:
- idempotent by design
- duplicate-safe on reruns
- contract/date-key aware where needed

### 4. Master Layer

Responsibility:
- Provide stable research-ready historical datasets

Current populated master areas:
- `Equity_stock_master/`
- `Indices_master/`
- `Futures_master/FUTSTK`
- `Futures_master/FUTIDX`
- `option_master/STOCKS`
- `option_master/INDICES`
- `EquityDat_master/`
- `participant/participant_master.csv`
- `fii_dii/fii_dii_master.csv`

## Daily Alignment Rule

The pipeline now treats the latest usable market day as the latest fully published date shared across:
- CM bhavcopy
- FO zip
- MTO

Index processing is forced to respect that aligned date. This avoids the earlier failure mode where index OHLC could advance to a newer date than the rest of the dataset universe.

## Incremental Rerun Design

To keep repeated scheduled runs cheap, the system now skips already-processed work in the major heavy paths.

Cleaner-side skip behavior:
- equity daily cleaner
- futures daily cleaner
- options daily cleaner
- MTO daily cleaner

Appender-side state tracking:
- `data/master/Equity_stock_master/_state/`
- `data/master/Indices_master/_state/`
- `data/master/Futures_master/_state/`
- `data/master/option_master/_state/`

These state files record which cleaned daily inputs have already been appended.

## Data Contracts

### Equity
- One CSV per symbol
- Daily OHLCV-style cash-market history
- Deduped by `DATE`

### Indices
- One CSV per tracked index
- Deduped by `TRADE_DATE`

### Futures
- One CSV per symbol/index under `FUTSTK` and `FUTIDX`
- Deduped by contract-aware keys

### Options
- One CSV per symbol/index under `STOCKS` and `INDICES`
- Deduped by trade date, expiry, strike, and option type

### MTO
- One CSV per symbol in `EquityDat_master`
- Delivery statistics appended by `TRADE_DATE`

## Compatibility Notes

The architecture still supports a few legacy path names for compatibility, while newer code prefers canonical names such as:
- `EquityDat_master`
- `equity_daily_unzip`

## What MarketForge Is Not

- Not a strategy engine
- Not a backtester
- Not a signal generator
- Not a portfolio management system

It is the data foundation those systems can build on.
