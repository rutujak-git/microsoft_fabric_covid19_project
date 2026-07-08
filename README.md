# COVID-19 Analytics Pipeline — Microsoft Fabric

An end-to-end data engineering project built on Microsoft Fabric, following a medallion architecture (bronze → silver → gold), culminating in a Direct Lake semantic model and Power BI report.

**Data source:** [Google COVID-19 Open Data](https://health.google.com/covid-19/open-data/raw-data) — historical, static dataset (real-time updates stopped September 2022).

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Bronze Layer — Ingestion](#bronze-layer--ingestion)
3. [Silver Layer — Cleaning & Transformation](#silver-layer--cleaning--transformation)
4. [Gold Layer — Star Schema](#gold-layer--star-schema)
5. [Semantic Model](#semantic-model)
6. [Report](#report)
7. [Known Limitations & Data Quality Decisions](#known-limitations--data-quality-decisions)
8. [Tech Stack](#tech-stack)

---

## Architecture Overview

```
Google Cloud Storage (public dataset)
        │
        ▼
  Data Pipeline (ForEach + Copy activity, config-driven via table_config.csv)
        │
        ▼
  Files/raw_files/*.csv  (bronze landing zone)
        │
        ▼
  Notebook: NB_Bronze_RawToDelta  →  raw.* (Delta tables)
        │
        ▼
  Notebook: NB_Silver_CleanTransform  →  silver.* (cleaned, flagged, quarantine split)
        │
        ▼
  Notebook: NB_Gold_StarSchema  →  gold.FactCovidDaily, gold.DimLocation, gold.DimDate
        │
        ▼
  Semantic Model (Direct Lake) — relationships, DAX measures, calculation groups, field parameters
        │
        ▼
  Power BI Report
```

<img width="1440" height="1240" alt="image" src="https://github.com/user-attachments/assets/22fd8bfb-4d71-4f16-981d-2b55855192c8" />

<img width="1440" height="732" alt="image" src="https://github.com/user-attachments/assets/f272170b-0030-4103-91d1-d35cb89579a3" />



---

## Bronze Layer — Ingestion

**Pipeline:** `raw_data_ingestion_pipeline`

Config-driven ingestion — table names are read from `Files/config/table_config.csv` via a Lookup activity, then a ForEach loop pulls each table's CSV from Google Cloud Storage using an HTTP connector, landing raw files in `Files/raw_files/`.

**Why config-driven:** adding a new source table only requires editing the CSV config, not modifying the pipeline itself.

### table_config.csv

<img width="1438" height="806" alt="image" src="https://github.com/user-attachments/assets/db87ef63-159e-4028-be35-5b80f997df60" />

### Lookup activity settings

<img width="1440" height="808" alt="image" src="https://github.com/user-attachments/assets/94e634bd-1294-4890-a5d0-b15a5a325f72" />


### ForEach + Copy data activity (dynamic source URL, dynamic destination path)

<img width="1440" height="806" alt="image" src="https://github.com/user-attachments/assets/0464ec2f-d256-40fd-9d5d-b083acdc348c" />

<img width="1440" height="810" alt="image" src="https://github.com/user-attachments/assets/25624fcb-f818-4fe9-9c2c-0512a039b543" />

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/f6e63bf4-2a41-42ce-a301-16542b141230" />

<img width="1440" height="808" alt="image" src="https://github.com/user-attachments/assets/749c4695-d029-47ce-b662-43fdf6fe6763" />


### Notebook: NB_Bronze_RawToDelta
Converts raw CSVs into Delta tables under the `raw` schema.

<img width="1440" height="808" alt="image" src="https://github.com/user-attachments/assets/7ad7578d-a342-42da-a68a-d7c4b10414e7" />

<img width="1440" height="802" alt="image" src="https://github.com/user-attachments/assets/f9c70f3a-0b85-46a1-84a3-391ad6201f1f" />


**Tables created:** `raw.epidemiology`, `raw.hospitalizations`, `raw.vaccinations`, `raw.index`, `raw.demographics`

---

## Silver Layer — Cleaning & Transformation

**Notebook:** `NB_Silver_CleanTransform`

### Cleaning pattern applied to fact tables (epidemiology, hospitalizations, vaccinations)
For each table:
1. Cast types (string → date/int/long)
2. Filter to country-level locations only (via `raw.index.aggregation_level = 0`)
3. Flag invalid dates (outside 2019–today), negative values, and missing values on core reliable columns
4. Deduplicate on (location_key, date)
5. Split into clean table + quarantine table

<img width="1440" height="806" alt="image" src="https://github.com/user-attachments/assets/ab61af0d-26c5-464f-9b7a-fcaa3bac83a8" />

<img width="1440" height="808" alt="image" src="https://github.com/user-attachments/assets/2922133d-7941-4af8-b257-16c09dd84ac4" />

### Dimension tables (index, demographics)
Filtered to country-level, deduplicated on `location_key`, minimal column selection.

<img width="1440" height="808" alt="image" src="https://github.com/user-attachments/assets/7633a364-3f41-40c8-a9cf-619f0bcddbe1" />

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/abc094c7-8f2d-4e2a-93df-77a3c83b1376" />

### Investigated and dropped: vaccinations_by_manufacturer
Checked manufacturer-level vaccination data (Pfizer, Moderna, Janssen, Sinovac) across all 56,031 rows — only 569 rows (~1%) had any data, all belonging to a single country (Malaysia). Dropped as not viable for reporting.

<img width="1440" height="806" alt="image" src="https://github.com/user-attachments/assets/39173c80-de11-43da-b5a2-e02debabf348" />


---

## Gold Layer — Star Schema

**Notebook:** `NB_Gold_StarSchema`

### DimDate
Generated calendar dimension (2019-01-01 through current date).

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/14803b74-06e1-4edc-a070-e672983f3682" />

### DimLocation
Joins `silver.index` + `silver.demographics` on `location_key`.

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/1e018d0b-da68-40b8-9c45-58a141c82d69" />

### FactCovidDaily
Left join: `silver.epidemiology` (base) + `silver.hospitalizations` + `silver.vaccinations`, on `(location_key, date)`.

**Grain:** one row per country per day.

<img width="1440" height="806" alt="image" src="https://github.com/user-attachments/assets/42c11167-9da2-478a-9c9d-ce5081f83a18" />

### Validation
Confirmed zero orphaned LocationKeys between fact and dimension tables.

<img width="1440" height="806" alt="image" src="https://github.com/user-attachments/assets/169e43ea-3530-4921-b6fb-5f3a84da0ca6" />


**Final row counts:** FactCovidDaily (227,519 rows, 232 countries) · DimLocation (246 rows) · DimDate (2,745 rows)

---

## Semantic Model

**Storage mode:** Direct Lake, with one Import-mode calculated table (composite model demo)

### Relationships
`FactCovidDaily[LocationKey]` → `DimLocation[LocationKey]` · `FactCovidDaily[DateKey]` → `DimDate[DateKey]`
Referential integrity assumed (verified zero orphaned keys beforehand).

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/e594621e-326f-423d-a99c-2b1b38832255" />


### Key DAX measures

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/94d85f40-389d-4683-be2c-706aaabc512d" />

<img width="1440" height="810" alt="image" src="https://github.com/user-attachments/assets/a9f7792a-ffdf-4a13-8be8-5c148e9fd40a" />

<img width="1440" height="812" alt="image" src="https://github.com/user-attachments/assets/5f693a0b-5caa-4931-abb5-09ca5df3c047" />

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/de4bb855-48f9-4a48-9d63-711b02d494c7" />

<img width="1440" height="806" alt="image" src="https://github.com/user-attachments/assets/8b7cb8dc-b025-48a4-ae4d-52cfa94188ef" />


### Calculation group — Time Calculation
Reusable time-intelligence items: `current_period`, `previous_period`, `pct_change` — applies to any measure via `SELECTEDMEASURE()`.

<img width="1440" height="810" alt="image" src="https://github.com/user-attachments/assets/8517570e-e9ec-4aa1-ad8a-a65a273d47c2" />

<img width="1440" height="806" alt="image" src="https://github.com/user-attachments/assets/9b761453-1bab-49b2-a6c6-0da97f4e9f76" />

<img width="1440" height="808" alt="image" src="https://github.com/user-attachments/assets/21ca820b-7aa7-4ca1-a61a-3778a6f4ab72" />


### Field parameter — Metric Selector
Lets report users switch a single chart between four metrics without duplicating visuals.

<img width="1440" height="812" alt="image" src="https://github.com/user-attachments/assets/69934b3b-7a16-40f2-92c8-76eea462bcda" />


### Composite model demo
`DataSourceNote` — a DAX calculated table (Import mode) coexisting with Direct Lake fact/dimension tables.

<img width="1440" height="812" alt="image" src="https://github.com/user-attachments/assets/cce6a5dd-2616-4e4f-bce5-4c6db5517ea3" />

---

## Report

Single-page overview: metric cards (driven by field parameter), trend line, country comparison bar chart.

<img width="1440" height="808" alt="image" src="https://github.com/user-attachments/assets/f064ed6b-16a5-4615-b622-a30e88da4e3b" />


---

## Known Limitations & Data Quality Decisions

| Issue | Decision |
|---|---|
| `new_recovered` / `new_tested` sparsely reported globally | Excluded from negative/missing-value flagging logic (would have quarantined 233/246 countries) |
| ICU/ventilator columns sparse in hospitalizations | Excluded from flagging; core hospitalization columns only |
| Manufacturer-level vaccination data (Pfizer/Moderna/Janssen/Sinovac) | Investigated, found ~1% coverage (single country) — table dropped |
| Some countries' vaccinated counts exceed population (AU, CL, PE, WS, PW, GI, AE, NU, BN) | Likely dose-count vs. person-count reporting inconsistency at source. Capped at 100% per country before aggregation |
| Country vs. state/county granularity | Scoped to country-level only (`aggregation_level = 0`) — country-level figures are the most complete/authoritative per-source; sub-national data available in `raw.*` if needed later |

---

## Tech Stack

Microsoft Fabric (Data Pipelines, Lakehouse, Notebooks/PySpark, Direct Lake Semantic Model, Power BI) · Delta Lake · DAX

---
