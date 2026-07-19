# 📊 MarketForge Master Data Dictionary

Generated dynamically from: `H:\MarketForge`

---

## 📁 Directory Branch: `data`
Total Data Objects Tracked: **1 files**

### 🛠️ Table Schema Model (Sample Object: `nifty_500_symbols.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **Company Name** | 🔤 Text / Categorical Key | `360 ONE WAM Ltd.` |
| **Industry** | 🔤 Text / Categorical Key | `Financial Services` |
| **Symbol** | 🔤 Text / Categorical Key | `360ONE` |
| **Series** | 🔤 Text / Categorical Key | `EQ` |
| **ISIN Code** | 🔤 Text / Categorical Key | `INE466L01038` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `nifty_500_symbols.csv`
</details>

---

## 📁 Directory Branch: `data\master`
Total Data Objects Tracked: **2 files**

### 🛠️ Table Schema Model (Sample Object: `fno_213_symbols.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `fno_213_symbols.csv`
- `nifty_500_symbols.csv`
</details>

---

## 📁 Directory Branch: `data\master\EquityDat_master`
Total Data Objects Tracked: **501 files**

### 🛠️ Table Schema Model (Sample Object: `360ONE.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20230123` |
| **RECORD_TYPE** | 🔢 Integer (Volume / OI) | `20` |
| **SR_NO** | 🔢 Integer (Volume / OI) | `7` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **SERIES** | 🔤 Text / Categorical Key | `EQ` |
| **TRADED_QTY** | 🔢 Integer (Volume / OI) | `73219` |
| **DELIVERABLE_QTY** | 🔢 Integer (Volume / OI) | `37553` |
| **DELIVERY_PCT** | 💵 Float (Price / Metric) | `51.29` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `360ONE.csv`
- `3MINDIA.csv`
- `AADHARHFC.csv`
- `AARTIIND.csv`
- `AAVAS.csv`
</details>

---

## 📁 Directory Branch: `data\master\Equity_stock_master`
Total Data Objects Tracked: **2720 files**

### 🛠️ Table Schema Model (Sample Object: `20MICRONS.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **DATE** | 📅 Standardised Date / Time | `2025-12-22` |
| **SYMBOL** | 🔤 Text / Categorical Key | `20MICRONS` |
| **SERIES** | 🔤 Text / Categorical Key | `EQ` |
| **OPEN** | 💵 Float (Price / Metric) | `194.24` |
| **HIGH** | 💵 Float (Price / Metric) | `196.64` |
| **LOW** | 💵 Float (Price / Metric) | `193.58` |
| **CLOSE** | 💵 Float (Price / Metric) | `194.53` |
| **LAST** | 💵 Float (Price / Metric) | `194.05` |
| **PREVCLOSE** | 💵 Float (Price / Metric) | `192.33` |
| **TOTTRDQTY** | 🔢 Integer (Volume / OI) | `56691` |
| **TOTTRDVAL** | 💵 Float (Price / Metric) | `11058459.23` |
| **TOTALTRADES** | 🔢 Integer (Volume / OI) | `1917` |
| **ISIN** | 🔤 Text / Categorical Key | `INE144J01027` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `20MICRONS.csv`
- `21STCENMGM.csv`
- `360ONE.csv`
- `3BBLACKBIO.csv`
- `3IINFOLTD.csv`
</details>

---

## 📁 Directory Branch: `data\master\fii_dii`
Total Data Objects Tracked: **1 files**

### 🛠️ Table Schema Model (Sample Object: `fii_dii_master.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **participant** | 🔤 Text / Categorical Key | `DII` |
| **date** | 📅 Standardised Date / Time | `2026-04-13` |
| **buy** | 💵 Float (Price / Metric) | `16612.03` |
| **sell** | 💵 Float (Price / Metric) | `14179.73` |
| **net** | 💵 Float (Price / Metric) | `2432.3` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `fii_dii_master.csv`
</details>

---

## 📁 Directory Branch: `data\master\Futures_master\FUTIDX`
Total Data Objects Tracked: **13 files**

### 🛠️ Table Schema Model (Sample Object: `BANKNIFTY.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTIDX` |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20160128` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `16909.85` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `17144.85` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `16823.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `17096.8` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `1753500` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `34041180115.5` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `2001210` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `66707` |
| **NO_OF_TRADE** | 💵 Float (Price / Metric) | `38751.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20160101` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BANKNIFTY.csv`
- `DJIA.csv`
- `FINNIFTY.csv`
- `FTSE100.csv`
- `MIDCPNIFTY.csv`
</details>

---

## 📁 Directory Branch: `data\master\Futures_master\FUTSTK`
Total Data Objects Tracked: **215 files**

### 🛠️ Table Schema Model (Sample Object: `360ONE.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20250627` |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTSTK` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20250731` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `1190.55` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `1216.6` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `1178.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `1205.1` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `334500` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `828058750.0` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `691500` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `1383` |
| **NO_OF_TRADE** | 💵 Float (Price / Metric) | `1356.0` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `360ONE.csv`
- `ABB.csv`
- `ABCAPITAL.csv`
- `ADANIENSOL.csv`
- `ADANIENT.csv`
</details>

---

## 📁 Directory Branch: `data\master\Futures_master_three_expiries\FUTIDX`
Total Data Objects Tracked: **13 files**

### 🛠️ Table Schema Model (Sample Object: `BANKNIFTY.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTIDX` |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20160128` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `16909.85` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `17144.85` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `16823.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `17096.8` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `1753500` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `34041180115.5` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `2001210` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `66707` |
| **NO_OF_TRADE** | 💵 Float (Price / Metric) | `38751.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20160101` |
| **EXPIRY_TYPE** | 📅 Standardised Date / Time | `NEAR` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BANKNIFTY.csv`
- `DJIA.csv`
- `FINNIFTY.csv`
- `FTSE100.csv`
- `MIDCPNIFTY.csv`
</details>

---

## 📁 Directory Branch: `data\master\Futures_master_three_expiries\FUTSTK`
Total Data Objects Tracked: **215 files**

### 🛠️ Table Schema Model (Sample Object: `360ONE.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20250627` |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTSTK` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20250731` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `1190.55` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `1216.6` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `1178.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `1205.1` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `334500` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `828058750.0` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `691500` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `1383` |
| **NO_OF_TRADE** | 💵 Float (Price / Metric) | `1356.0` |
| **EXPIRY_TYPE** | 📅 Standardised Date / Time | `NEAR` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `360ONE.csv`
- `ABB.csv`
- `ABCAPITAL.csv`
- `ADANIENSOL.csv`
- `ADANIENT.csv`
</details>

---

## 📁 Directory Branch: `data\master\Indices_master`
Total Data Objects Tracked: **19 files**

### 🛠️ Table Schema Model (Sample Object: `BANKNIFTY.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20200101` |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **OPEN** | 💵 Float (Price / Metric) | `32237.9` |
| **HIGH** | 💵 Float (Price / Metric) | `32348.0` |
| **LOW** | 💵 Float (Price / Metric) | `32057.2` |
| **CLOSE** | 💵 Float (Price / Metric) | `32102.9` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BANKNIFTY.csv`
- `FINNIFTY.csv`
- `NIFTY.csv`
- `NIFTY100.csv`
- `NIFTY200.csv`
</details>

---

## 📁 Directory Branch: `data\master\option_master\INDICES`
Total Data Objects Tracked: **10 files**

### 🛠️ Table Schema Model (Sample Object: `BANKNIFTY.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `OPT` |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20201228` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20201231` |
| **STRIKE_PRICE** | 🔢 Integer (Volume / OI) | `17500` |
| **OPT_TYPE** | 🔤 Text / Categorical Key | `PE` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `3.0` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `3.0` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `1.45` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `1.6` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `46525` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `50050` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `2002` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `439` |
| **NOTION_VAL** | 🔢 Integer (Volume / OI) | `875971305` |
| **PR_VAL** | 💵 Float (Price / Metric) | `96305.0` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BANKNIFTY.csv`
- `BANKNIFTY.parquet`
- `FINNIFTY.csv`
- `FINNIFTY.parquet`
- `MIDCPNIFTY.csv`
</details>

---

## 📁 Directory Branch: `data\master\option_master\STOCKS`
Total Data Objects Tracked: **518 files**

### 🛠️ Table Schema Model (Sample Object: `360ONE.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `OPT` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20250627` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20250731` |
| **STRIKE_PRICE** | 🔢 Integer (Volume / OI) | `1000` |
| **OPT_TYPE** | 🔤 Text / Categorical Key | `CE` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `210.0` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `212.0` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `210.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `210.0` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `1500` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `1500` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `3` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `3` |
| **NOTION_VAL** | 🔢 Integer (Volume / OI) | `1816000` |
| **PR_VAL** | 💵 Float (Price / Metric) | `316000.0` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `360ONE.csv`
- `360ONE.parquet`
- `AARTIIND.csv`
- `ABB.csv`
- `ABB.parquet`
</details>

---

## 📁 Directory Branch: `data\master\participant`
Total Data Objects Tracked: **1 files**

### 🛠️ Table Schema Model (Sample Object: `participant_master.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **date** | 📅 Standardised Date / Time | `2023-01-02` |
| **client_type** | 🔤 Text / Categorical Key | `DII` |
| **net_index_futures** | 💵 Float (Price / Metric) | `-348.0` |
| **net_stock_futures** | 💵 Float (Price / Metric) | `-5314.0` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `participant_master.csv`
</details>

---

## 📁 Directory Branch: `data\master\processed_merged_data`
Total Data Objects Tracked: **1 files**

### 🛠️ Table Schema Model (Sample Object: `WIPRO_processed.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **Date** | 📅 Standardised Date / Time | `2016-01-01` |
| **Symbol** | 🔤 Text / Categorical Key | `WIPRO` |
| **Spot_Close** | 💵 Float (Price / Metric) | `556.45` |
| **Futures_Close** | 💵 Float (Price / Metric) | `555.15` |
| **Basis_Spread** | 💵 Float (Price / Metric) | `-1.3000000000000682` |
| **Open_Interest** | 🔢 Integer (Volume / OI) | `5962000` |
| **OI_Change** | 🔢 Integer (Volume / OI) | `0` |
| **OI_Pct_Change** | 🔤 Text / Categorical Key | `0.00%` |
| **Futures_Volume** | 🔢 Integer (Volume / OI) | `1467000` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `WIPRO_processed.csv`
</details>

---

## 📁 Directory Branch: `data\processed\equityDat_daily`
Total Data Objects Tracked: **18 files**

### 🛠️ Table Schema Model (Sample Object: `mto_20260622.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20260622` |
| **RECORD_TYPE** | 🔢 Integer (Volume / OI) | `20` |
| **SR_NO** | 🔢 Integer (Volume / OI) | `1` |
| **SYMBOL** | 🔤 Text / Categorical Key | `0MOFSL27` |
| **SERIES** | 🔤 Text / Categorical Key | `N3` |
| **TRADED_QTY** | 🔢 Integer (Volume / OI) | `10` |
| **DELIVERABLE_QTY** | 🔢 Integer (Volume / OI) | `10` |
| **DELIVERY_PCT** | 💵 Float (Price / Metric) | `100.0` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `mto_20260622.csv`
- `mto_20260622.parquet`
- `mto_20260623.csv`
- `mto_20260623.parquet`
- `mto_20260624.csv`
</details>

---

## 📁 Directory Branch: `data\processed\equity_daily`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `BhavCopy_NSE_CM_0_0_0_20260615_F_0000.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **DATE** | 📅 Standardised Date / Time | `2026-06-15` |
| **SYMBOL** | 🔤 Text / Categorical Key | `20MICRONS` |
| **SERIES** | 🔤 Text / Categorical Key | `EQ` |
| **OPEN** | 💵 Float (Price / Metric) | `184.45` |
| **HIGH** | 💵 Float (Price / Metric) | `198.0` |
| **LOW** | 💵 Float (Price / Metric) | `183.05` |
| **CLOSE** | 💵 Float (Price / Metric) | `188.82` |
| **LAST** | 💵 Float (Price / Metric) | `188.52` |
| **PREVCLOSE** | 💵 Float (Price / Metric) | `182.37` |
| **TOTTRDQTY** | 🔢 Integer (Volume / OI) | `428997` |
| **TOTTRDVAL** | 💵 Float (Price / Metric) | `82359091.03` |
| **TOTALTRADES** | 🔢 Integer (Volume / OI) | `6577` |
| **ISIN** | 🔤 Text / Categorical Key | `INE144J01027` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BhavCopy_NSE_CM_0_0_0_20260615_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260616_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260617_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260618_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260619_F_0000.csv`
</details>

---

## 📁 Directory Branch: `data\processed\fii_dii`
Total Data Objects Tracked: **12 files**

### 🛠️ Table Schema Model (Sample Object: `fii_dii_clean_2026-06-15.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **buy** | 💵 Float (Price / Metric) | `21080.9` |
| **participant** | 🔤 Text / Categorical Key | `DII` |
| **date** | 📅 Standardised Date / Time | `2026-06-15` |
| **net** | 💵 Float (Price / Metric) | `3189.26` |
| **sell** | 💵 Float (Price / Metric) | `17891.64` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `fii_dii_clean_2026-06-15.csv`
- `fii_dii_clean_2026-06-16.csv`
- `fii_dii_clean_2026-06-17.csv`
- `fii_dii_clean_2026-06-18.csv`
- `fii_dii_clean_2026-06-19.csv`
</details>

---

## 📁 Directory Branch: `data\processed\fii_dii\clean`
Total Data Objects Tracked: **5 files**

### 🛠️ Table Schema Model (Sample Object: `fii_dii_clean_2026-06-15.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **buy** | 💵 Float (Price / Metric) | `21080.9` |
| **participant** | 🔤 Text / Categorical Key | `DII` |
| **date** | 📅 Standardised Date / Time | `2026-06-15` |
| **net** | 💵 Float (Price / Metric) | `3189.26` |
| **sell** | 💵 Float (Price / Metric) | `17891.64` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `fii_dii_clean_2026-06-15.csv`
- `fii_dii_clean_2026-06-16.csv`
- `fii_dii_clean_2026-06-17.csv`
- `fii_dii_clean_2026-06-18.csv`
- `fii_dii_clean_2026-06-19.csv`
</details>

---

## 📁 Directory Branch: `data\processed\futures_daily\INDICES`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `futidx01072026.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTIDX` |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20260728` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `57991.0` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `58390.0` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `57859.8` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `58309.8` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `2210370` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `34383773094.0` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `590820` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `19694` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `13251` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20260701` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `futidx01072026.csv`
- `futidx02072026.csv`
- `futidx03072026.csv`
- `futidx15062026.csv`
- `futidx16062026.csv`
</details>

---

## 📁 Directory Branch: `data\processed\futures_daily\STOCKS`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `futstk01072026.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTSTK` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20260728` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `1087.4` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `1089.9` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `1067.1` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `1076.0` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `7363000` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `2020258700.0` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `1875500` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `3751` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `2464` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20260701` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `futstk01072026.csv`
- `futstk02072026.csv`
- `futstk03072026.csv`
- `futstk15062026.csv`
- `futstk16062026.csv`
</details>

---

## 📁 Directory Branch: `data\processed\indices_daily`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `indices_ohlc_clean_20260615.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20260615` |
| **INDEX_NAME** | 🔤 Text / Categorical Key | `INDIA VIX` |
| **OPEN** | 💵 Float (Price / Metric) | `14.72` |
| **HIGH** | 💵 Float (Price / Metric) | `14.72` |
| **LOW** | 💵 Float (Price / Metric) | `13.56` |
| **CLOSE** | 💵 Float (Price / Metric) | `14.35` |
| **PCT_CHANGE** | 💵 Float (Price / Metric) | `-2.48` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `indices_ohlc_clean_20260615.csv`
- `indices_ohlc_clean_20260616.csv`
- `indices_ohlc_clean_20260617.csv`
- `indices_ohlc_clean_20260618.csv`
- `indices_ohlc_clean_20260619.csv`
</details>

---

## 📁 Directory Branch: `data\processed\options_daily\INDICES`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `optidx01072026.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `OPTIDX` |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20260728` |
| **STRIKE_PRICE** | 🔢 Integer (Volume / OI) | `46000` |
| **OPT_TYPE** | 🔤 Text / Categorical Key | `PE` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `10.0` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `10.0` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `5.8` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `6.35` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `52140` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `49800` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `1660` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `660` |
| **NOTION_VAL** | 🔢 Integer (Volume / OI) | `2291174538` |
| **PR_VAL** | 💵 Float (Price / Metric) | `374538.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20260701` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `optidx01072026.csv`
- `optidx02072026.csv`
- `optidx03072026.csv`
- `optidx15062026.csv`
- `optidx16062026.csv`
</details>

---

## 📁 Directory Branch: `data\processed\options_daily\STOCKS`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `optstk01072026.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `OPTSTK` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **EXP_DATE** | 📅 Standardised Date / Time | `20260728` |
| **STRIKE_PRICE** | 🔢 Integer (Volume / OI) | `880` |
| **OPT_TYPE** | 🔤 Text / Categorical Key | `PE` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `1.75` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `1.75` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `1.55` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `1.65` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `4500` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `5000` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `10` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `7` |
| **NOTION_VAL** | 🔢 Integer (Volume / OI) | `4408375` |
| **PR_VAL** | 💵 Float (Price / Metric) | `8375.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `20260701` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `optstk01072026.csv`
- `optstk02072026.csv`
- `optstk03072026.csv`
- `optstk15062026.csv`
- `optstk16062026.csv`
</details>

---

## 📁 Directory Branch: `data\processed\participant`
Total Data Objects Tracked: **1 files**

### 🛠️ Table Schema Model (Sample Object: `participant_clean_2026-06-15.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **date** | 📅 Standardised Date / Time | `2026-06-15` |
| **client_type** | 🔤 Text / Categorical Key | `DII` |
| **net_index_futures** | 💵 Float (Price / Metric) | `12163.0` |
| **net_stock_futures** | 💵 Float (Price / Metric) | `41219.0` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `participant_clean_2026-06-15.csv`
</details>

---

## 📁 Directory Branch: `data\raw\equit_historical`
Total Data Objects Tracked: **499 files**

### 🛠️ Table Schema Model (Sample Object: `360ONE.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **DATE** | 📅 Standardised Date / Time | `2023-01-23` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **SERIES** | 🔤 Text / Categorical Key | `EQ` |
| **OPEN** | 💵 Float (Price / Metric) | `1950.0` |
| **HIGH** | 💵 Float (Price / Metric) | `1960.0` |
| **LOW** | 💵 Float (Price / Metric) | `1911.0` |
| **CLOSE** | 💵 Float (Price / Metric) | `1950.1` |
| **LAST** | 💵 Float (Price / Metric) | `1950.0` |
| **PREVCLOSE** | 💵 Float (Price / Metric) | `1922.25` |
| **TOTTRDQTY** | 🔢 Integer (Volume / OI) | `73219` |
| **TOTTRDVAL** | 💵 Float (Price / Metric) | `142513575.85` |
| **TOTALTRADES** | 🔢 Integer (Volume / OI) | `6733` |
| **ISIN** | 🔤 Text / Categorical Key | `INE466L01020` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `360ONE.csv`
- `3MINDIA.csv`
- `AADHARHFC.csv`
- `AARTIIND.csv`
- `AAVAS.csv`
</details>

---

## 📁 Directory Branch: `data\raw\fii_dii`
Total Data Objects Tracked: **12 files**

### 🛠️ Table Schema Model (Sample Object: `fii_dii_raw_2026-06-15.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **buy** | 💵 Float (Price / Metric) | `21080.9` |
| **participant** | 🔤 Text / Categorical Key | `DII` |
| **date** | 📅 Standardised Date / Time | `2026-06-15` |
| **net** | 💵 Float (Price / Metric) | `3189.26` |
| **sell** | 💵 Float (Price / Metric) | `17891.64` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `fii_dii_raw_2026-06-15.csv`
- `fii_dii_raw_2026-06-16.csv`
- `fii_dii_raw_2026-06-17.csv`
- `fii_dii_raw_2026-06-18.csv`
- `fii_dii_raw_2026-06-19.csv`
</details>

---

## 📁 Directory Branch: `data\raw\futures_historical\FUTIDX`
Total Data Objects Tracked: **13 files**

### 🛠️ Table Schema Model (Sample Object: `BANKNIFTY.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTIDX` |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **EXP_DATE** | 📅 Standardised Date / Time | `28/01/2016` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `16909.85` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `17144.85` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `16823.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `17096.8` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `1753500` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `34041180115.5` |
| **TRD_QTY** | 💵 Float (Price / Metric) | `2001210.0` |
| **NO_OF_CONT** | 💵 Float (Price / Metric) | `66707.0` |
| **NO_OF_TRADE** | 💵 Float (Price / Metric) | `38751.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `2016-01-01` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BANKNIFTY.csv`
- `DJIA.csv`
- `FINNIFTY.csv`
- `FTSE100.csv`
- `MIDCPNIFTY.csv`
</details>

---

## 📁 Directory Branch: `data\raw\futures_historical\FUTSTK`
Total Data Objects Tracked: **351 files**

### 🛠️ Table Schema Model (Sample Object: `360ONE.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTSTK` |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **EXP_DATE** | 📅 Standardised Date / Time | `30/09/2025` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `1210.0` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `1210.0` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `1210.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `1210.0` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `500` |
| **TRD_VAL** | 💵 Float (Price / Metric) | `605000.0` |
| **TRD_QTY** | 💵 Float (Price / Metric) | `500.0` |
| **NO_OF_CONT** | 💵 Float (Price / Metric) | `1.0` |
| **NO_OF_TRADE** | 💵 Float (Price / Metric) | `1.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `2025-06-27` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `360ONE.csv`
- `AARTIIND.csv`
- `ABB.csv`
- `ABBOTINDIA.csv`
- `ABCAPITAL.csv`
</details>

---

## 📁 Directory Branch: `data\raw\indices`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `indices_ohlc_eod_20260615.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TRADE_DATE** | 📅 Standardised Date / Time | `2026-06-15` |
| **INDEX_NAME** | 🔤 Text / Categorical Key | `NIFTY 50` |
| **OPEN** | 💵 Float (Price / Metric) | `23984.85` |
| **HIGH** | 💵 Float (Price / Metric) | `24011.4` |
| **LOW** | 💵 Float (Price / Metric) | `23817.8` |
| **CLOSE** | 💵 Float (Price / Metric) | `23853.9` |
| **PCT_CHANGE** | 💵 Float (Price / Metric) | `0.98` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `indices_ohlc_eod_20260615.csv`
- `indices_ohlc_eod_20260616.csv`
- `indices_ohlc_eod_20260617.csv`
- `indices_ohlc_eod_20260618.csv`
- `indices_ohlc_eod_20260619.csv`
</details>

---

## 📁 Directory Branch: `data\raw\option_hitorical\INDICES`
Total Data Objects Tracked: **10 files**

### 🛠️ Table Schema Model (Sample Object: `BANKNIFTY.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **SYMBOL** | 🔤 Text / Categorical Key | `BANKNIFTY` |
| **EXP_DATE** | 📅 Standardised Date / Time | `2020-12-31` |
| **STR_PRICE** | 💵 Float (Price / Metric) | `17500.0` |
| **OPT_TYPE** | 🔤 Text / Categorical Key | `PE` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `3.0` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `3.0` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `1.45` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `1.6` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `46525` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `50050` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `2002` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `439` |
| **NOTION_VAL** | 🔢 Integer (Volume / OI) | `875971305` |
| **PR_VAL** | 💵 Float (Price / Metric) | `96305.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `2020-12-28` |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `OPT` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BANKNIFTY.csv`
- `BANKNIFTY.parquet`
- `FINNIFTY.csv`
- `FINNIFTY.parquet`
- `MIDCPNIFTY.csv`
</details>

---

## 📁 Directory Branch: `data\raw\option_hitorical\STOCKS`
Total Data Objects Tracked: **562 files**

### 🛠️ Table Schema Model (Sample Object: `360ONE.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **SYMBOL** | 🔤 Text / Categorical Key | `360ONE` |
| **EXP_DATE** | 📅 Standardised Date / Time | `2025-07-31` |
| **STR_PRICE** | 💵 Float (Price / Metric) | `1000.0` |
| **OPT_TYPE** | 🔤 Text / Categorical Key | `CE` |
| **OPEN_PRICE** | 💵 Float (Price / Metric) | `210.0` |
| **HI_PRICE** | 💵 Float (Price / Metric) | `212.0` |
| **LO_PRICE** | 💵 Float (Price / Metric) | `210.0` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `210.0` |
| **OPEN_INT** | 🔢 Integer (Volume / OI) | `1500` |
| **TRD_QTY** | 🔢 Integer (Volume / OI) | `1500` |
| **NO_OF_CONT** | 🔢 Integer (Volume / OI) | `3` |
| **NO_OF_TRADE** | 🔢 Integer (Volume / OI) | `3` |
| **NOTION_VAL** | 🔢 Integer (Volume / OI) | `1816000` |
| **PR_VAL** | 💵 Float (Price / Metric) | `316000.0` |
| **TRADE_DATE** | 📅 Standardised Date / Time | `2025-06-27` |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `OPT` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `360ONE.csv`
- `360ONE.parquet`
- `AARTIIND.csv`
- `AARTIIND.parquet`
- `ABB.csv`
</details>

---

## 📁 Directory Branch: `data\raw\participant`
Total Data Objects Tracked: **3 files**

### 🛠️ Table Schema Model (Sample Object: `participant_raw_02072026.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **Participant wise Trading Volume (no. of contracts) in Equity Derivatives as on Jul 02** | 🔤 Text / Categorical Key | `Client Type` |
| ** 2026""** | 🔤 Text / Categorical Key | `Future Index Long` |
| **** | 🔤 Text / Categorical Key | `Future Index Short` |
| **** | 🔤 Text / Categorical Key | `Future Stock Long` |
| **** | 🔤 Text / Categorical Key | `Future Stock Short       ` |
| **** | 🔤 Text / Categorical Key | `Option Index Call Long` |
| **** | 🔤 Text / Categorical Key | `Option Index Put Long` |
| **** | 🔤 Text / Categorical Key | `Option Index Call Short` |
| **** | 🔤 Text / Categorical Key | `Option Index Put Short` |
| **** | 🔤 Text / Categorical Key | `Option Stock Call Long` |
| **** | 🔤 Text / Categorical Key | `Option Stock Put Long` |
| **** | 🔤 Text / Categorical Key | `Option Stock Call Short` |
| **** | 🔤 Text / Categorical Key | `Option Stock Put Short` |
| **** | 🔤 Text / Categorical Key | `Total Long Contracts      ` |
| **** | 🔤 Text / Categorical Key | `Total Short Contracts` |
| **** | 🔤 Text / Categorical Key | `N/A` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `participant_raw_02072026.csv`
- `participant_raw_03072026.csv`
- `participant_raw_15062026.csv`
</details>

---

## 📁 Directory Branch: `data\raw\symbolwise_equitydatcleaned_4`
Total Data Objects Tracked: **2824 files**

### 🛠️ Table Schema Model (Sample Object: `20MICRONS.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **DATE** | 📅 Standardised Date / Time | `2022-01-03` |
| **RECORD_TYPE** | 🔢 Integer (Volume / OI) | `20` |
| **SR_NO** | 🔢 Integer (Volume / OI) | `2` |
| **SYMBOL** | 🔤 Text / Categorical Key | `20MICRONS` |
| **SERIES** | 🔤 Text / Categorical Key | `EQ` |
| **TRADED_QTY** | 🔢 Integer (Volume / OI) | `451601` |
| **DELIVERABLE_QTY** | 🔢 Integer (Volume / OI) | `248467` |
| **DELIVERY_PCT** | 💵 Float (Price / Metric) | `55.02` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `20MICRONS.csv`
- `21STCENMGM.csv`
- `360ONE.csv`
- `3IINFOLTD.csv`
- `3MINDIA.csv`
</details>

---

## 📁 Directory Branch: `data\unzip_daily\equity_daily_unzip`
Total Data Objects Tracked: **14 files**

### 🛠️ Table Schema Model (Sample Object: `BhavCopy_NSE_CM_0_0_0_20260615_F_0000.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **TradDt** | 🔤 Text / Categorical Key | `2026-06-15` |
| **BizDt** | 🔤 Text / Categorical Key | `2026-06-15` |
| **Sgmt** | 🔤 Text / Categorical Key | `CM` |
| **Src** | 🔤 Text / Categorical Key | `NSE` |
| **FinInstrmTp** | 🔤 Text / Categorical Key | `STK` |
| **FinInstrmId** | 🔢 Integer (Volume / OI) | `19078` |
| **ISIN** | 🔤 Text / Categorical Key | `IN0020200104` |
| **TckrSymb** | 🔤 Text / Categorical Key | `SGBJUN28` |
| **SctySrs** | 🔤 Text / Categorical Key | `GB` |
| **XpryDt** | Empty / Null Data | `` |
| **FininstrmActlXpryDt** | Empty / Null Data | `` |
| **StrkPric** | Empty / Null Data | `` |
| **OptnTp** | Empty / Null Data | `` |
| **FinInstrmNm** | 🔤 Text / Categorical Key | `2.5%GOLDBONDS2028SR-III` |
| **OpnPric** | 💵 Float (Price / Metric) | `14650.01` |
| **HghPric** | 💵 Float (Price / Metric) | `14984.99` |
| **LwPric** | 💵 Float (Price / Metric) | `14640.53` |
| **ClsPric** | 💵 Float (Price / Metric) | `14954.42` |
| **LastPric** | 💵 Float (Price / Metric) | `14955.68` |
| **PrvsClsgPric** | 💵 Float (Price / Metric) | `14640.53` |
| **UndrlygPric** | Empty / Null Data | `` |
| **SttlmPric** | 💵 Float (Price / Metric) | `15046.69` |
| **OpnIntrst** | Empty / Null Data | `` |
| **ChngInOpnIntrst** | Empty / Null Data | `` |
| **TtlTradgVol** | 🔢 Integer (Volume / OI) | `76` |
| **TtlTrfVal** | 💵 Float (Price / Metric) | `1130958.51` |
| **TtlNbOfTxsExctd** | 🔢 Integer (Volume / OI) | `28` |
| **SsnId** | 🔤 Text / Categorical Key | `F1` |
| **NewBrdLotQty** | 🔢 Integer (Volume / OI) | `1` |
| **Rmks** | Empty / Null Data | `` |
| **Rsvd1** | Empty / Null Data | `` |
| **Rsvd2** | Empty / Null Data | `` |
| **Rsvd3** | Empty / Null Data | `` |
| **Rsvd4** | Empty / Null Data | `` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `BhavCopy_NSE_CM_0_0_0_20260615_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260616_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260617_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260618_F_0000.csv`
- `BhavCopy_NSE_CM_0_0_0_20260619_F_0000.csv`
</details>

---

## 📁 Directory Branch: `data\unzip_daily\future_daily_unzip`
Total Data Objects Tracked: **140 files**

### 🛠️ Table Schema Model (Sample Object: `fo01072026.csv`):
| Field | Detected Type | Sample Value |
| :--- | :--- | :--- |
| **INSTRUMENT** | 🔤 Text / Categorical Key | `FUTIDX    ` |
| **SYMBOL    ** | 🔤 Text / Categorical Key | `BANKNIFTY ` |
| **EXP_DATE  ** | 📅 Standardised Date / Time | `28/07/2026` |
| **OPEN_PRICE ** | 💵 Float (Price / Metric) | `00057991.00` |
| **HI_PRICE   ** | 💵 Float (Price / Metric) | `00058390.00` |
| **LO_PRICE   ** | 💵 Float (Price / Metric) | `00057859.80` |
| **CLOSE_PRICE** | 💵 Float (Price / Metric) | `00058309.80` |
| **OPEN_INT*      ** | 🔢 Integer (Volume / OI) | `000000002210370` |
| **TRD_VAL           ** | 💵 Float (Price / Metric) | `    34383773094.00` |
| **TRD_QTY          ** | 🔢 Integer (Volume / OI) | `           590820` |
| **NO_OF_CONT       ** | 🔢 Integer (Volume / OI) | `            19694` |
| **NO_OF_TRADE      ** | 🔢 Integer (Volume / OI) | `            13251` |

<details><summary>📦 Click to view inventory (Top 5)</summary>

- `fo01072026.csv`
- `fo02072026.csv`
- `fo03072026.csv`
- `fo15062026.csv`
- `fo16062026.csv`
</details>

---

