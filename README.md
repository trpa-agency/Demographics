# Demographics — TRPA Census Data Utility

This repository contains tools for downloading US Census Bureau data and filtering it to the **Lake Tahoe basin** (TRPA jurisdiction), along with post-processing helpers for summarizing and calculating medians from binned census data.

---

## Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            utils.py                                     │
│                                                                         │
│  DATA SOURCES                CORE PIPELINE              OUTPUT          │
│                                                                         │
│  ┌──────────────────┐        ┌─────────────────────┐                   │
│  │  Census API      │───────▶│  get_variable_data  │                   │
│  │  api.census.gov  │        │  (one variable,      │                   │
│  └──────────────────┘        │   4 counties,        │                   │
│                              │   2 states)          │                   │
│  ┌──────────────────┐        └────────┬────────────┘                   │
│  │  TRPA ArcGIS     │                 │  filter to Tahoe                │
│  │  Feature Service │───────▶tahoe_geometry (TRPAID join)              │
│  └──────────────────┘                 │                                 │
│                                       ▼                                 │
│                        ┌─────────────────────────────┐                 │
│                        │  census_download_wrapper    │                 │
│                        │  ────────────────────────── │                 │
│                        │  loops over variables_df    │  ──▶  DataFrame │
│                        │  calls get_variable_data    │                 │
│                        │  for each row               │                 │
│                        └────────────┬────────────────┘                 │
│                                     │  (with checkpointing)            │
│                        ┌────────────▼────────────────┐                 │
│                        │  census_download_wrapper    │                 │
│                        │  _checkpointed              │  ──▶  CSV files │
│                        │  skips completed rows,      │      + DataFrame│
│                        │  saves per-variable CSVs    │                 │
│                        └─────────────────────────────┘                 │
│                                                                         │
│  POST-PROCESSING                                                        │
│                                                                         │
│  categorize_values ──▶ groups raw variables into named categories       │
│  sum_across_levels ──▶ aggregates by Basin / County / North-South /    │
│                         State                                           │
│  calculate_median_value ──▶ interpolated median from binned data        │
│  median_across_levels  ──▶ runs calculate_median_value at each         │
│                             geographic level                            │
│                                                                         │
│  HELPERS                                                                │
│  make_session ──▶ requests.Session with retry/backoff (429, 5xx)       │
│  get_fs_as_df / get_fs_data ──▶ ArcGIS FeatureLayer → DataFrame        │
│  get_tahoe_geometry ──▶ TRPA geometry + TRPAID lookup table             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Key Functions

### `make_session()`
Creates an HTTP session with automatic retry logic (up to 5 retries, exponential backoff) for rate-limit (429) and server errors (500–504). Should be passed into any function making Census API calls.

### `get_tahoe_geometry()`
Pulls the Tahoe basin geometry from TRPA's ArcGIS server. Returns a lookup table keyed on `TRPAID` (a combo of census GEOID + geometry year). This is the spatial filter used throughout.

### `get_variable_data()`
The core function. For a single census variable, it:
1. Loops over 4 counties across CA (El Dorado, Placer) and NV (Douglas, Washoe)
2. Builds and fires a Census API request per county
3. Normalizes the JSON response into a DataFrame
4. Strips the `US` prefix from `GEO_ID` and builds `TRPAID`
5. Filters rows to only those within the Tahoe basin (`tahoe_geometry`)
6. Handles ACS-specific Margin of Error columns

### `census_download_wrapper_checkpointed()`
Production-grade wrapper over `get_variable_data`. Saves each variable's result to a CSV checkpoint file so that if the download is interrupted, already-completed variables are skipped on re-run.

### `calculate_median_value()`
Interpolates a median from binned census data (e.g., income or age brackets). It parses lower/upper bounds from category label strings using regex, computes a cumulative sum, finds the bin containing the 50th percentile, then interpolates within that bin.

### `categorize_values()`
Joins a raw census DataFrame to a user-supplied CSV mapping variable codes to category labels, then aggregates values by category.

### `sum_across_levels()` / `median_across_levels()`
Roll up data to four geographic levels: whole Basin, by County, by North/South shore, and by State.
