# MarketForge

MarketForge is a production-grade NSE market data engine for Indian markets.

It currently maintains active pipelines and master datasets for:
- Equities
- Indices
- Futures
- Options
- Equity delivery / MTO
- Participant data
- FII/DII activity

## What It Does

MarketForge runs a deterministic:

```text
Download → Clean → Append → Master
```

workflow that turns raw NSE files into symbol-wise and index-wise master datasets.

The daily pipeline is designed to be:
- Append-only
- Idempotent
- Scheduler-friendly
- Safe against partial NSE publication windows
- Incremental on reruns for the major heavy steps

## Current State

As of April 14, 2026, the repository has populated masters for:
- `Equity_stock_master/`
- `Indices_master/`
- `Futures_master/FUTSTK`
- `Futures_master/FUTIDX`
- `option_master/STOCKS`
- `option_master/INDICES`
- `EquityDat_master/`
- `participant/participant_master.csv`
- `fii_dii/fii_dii_master.csv`

For the latest detailed counts and coverage, see [MARKETFORGE_DATA_SUMMARY.md](h:\MarketForge\MARKETFORGE_DATA_SUMMARY.md).

## Daily Pipeline

Main scheduler entrypoint:

```powershell
.\scripts\daily_run_equity.ps1
```

That run currently performs:
- CM bhavcopy download
- FO zip download
- MTO download
- Index OHLC download
- FII/DII and participant download
- Unzip steps
- Daily cleaners
- Master append/build steps

## Alignment Behavior

Index processing is aligned to the latest fully published market date across:
- CM bhavcopy
- FO zip
- MTO

This prevents indices from moving ahead of the rest of the market data when NSE has only partially published the current trading day.

## Incremental Reruns

Recent improvements make reruns much lighter:
- already-cleaned equity/futures/options/MTO daily files are skipped
- already-appended equity/futures/options/index daily files are skipped
- futures/options/equity/index state is tracked under `_state/` folders in the relevant master directories

## Repository Layout

```text
H:\MarketForge\
├── data/
│   ├── raw/
│   ├── processed/
│   ├── master/
│   └── unzip_daily/
├── scripts/
│   ├── downloader/
│   ├── cleaner/
│   ├── append/
│   ├── master_merge/
│   ├── 99_check_master_last_rows.py
│   └── daily_run_equity.ps1
├── config/
├── README.md
├── ARCHITECTURE.md
└── MARKETFORGE_DATA_SUMMARY.md
```

## Key Principles

- Deterministic outputs
- Append-only masters
- Deduplicated appends
- NSE-safe handling of delayed publication
- Practical production behavior over one-off scripting

## Notes

Some legacy folder names may still exist on disk in older environments, but the code now supports compatibility fallbacks while moving toward canonical names such as:
- `data/master/EquityDat_master`
- `data/unzip_daily/equity_daily_unzip`
