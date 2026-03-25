# ET-Applications — Pan-India ET Downscaling (Pixel-Wise CSV Edition)

**Version:** 1.0  
**Platform:** Google Earth Engine (GEE) + Python 3.x  
**Output:** Per-pixel 30 m resolution CSV files for any tehsil in India

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Prerequisites and Access Requirements](#3-prerequisites-and-access-requirements)
4. [Environment Setup](#4-environment-setup)
5. [Step 1 — Create the Tehsil Boundary Asset in GEE](#5-step-1--create-the-tehsil-boundary-asset-in-gee)
6. [Step 2 — Configure config.yaml](#6-step-2--configure-configyaml)
7. [Step 3 — Authenticate Earth Engine in Python](#7-step-3--authenticate-earth-engine-in-python)
8. [Step 4 — Run the Python Application](#8-step-4--run-the-python-application)
9. [Application Modes and Output Files](#9-application-modes-and-output-files)
10. [Output CSV Column Reference](#10-output-csv-column-reference)
11. [config.yaml Parameter Reference](#11-configyaml-parameter-reference)
12. [Changing to a Different Tehsil](#12-changing-to-a-different-tehsil)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Project Overview

ET-Applications downscales MODIS-resolution evapotranspiration data to the native 30 m Landsat 8 grid using a pre-trained Random Forest model. For any selected tehsil in India, the tool generates pixel-level CSV files containing:

- Monthly mean daily Actual Evapotranspiration (AET)
- Annual total AET
- Monthly mean daily Potential Evapotranspiration (PET) from MODIS MOD16A2
- Relative Water Deficit Index (RWDI) per month
- Water Stress ratio (AET/PET) per month

All output CSVs share a common `pixel_id`, `longitude`, and `latitude` key, making them directly joinable. The pixel grid is always defined by the AET (Landsat 8) stack, so pixel positions are consistent across separate runs for the same tehsil and year.

---

## 2. Repository Structure

```
et-applications/
├── et_applications.py             Main Python CLI application
├── config.yaml                    User configuration file (edit this)
├── requirements.txt               Python package dependencies
├── 1_Check_Tehsil.js              GEE Script 1 — verify tehsil before export
├── 2_Generate_Tehsil_Boundary.js  GEE Script 2 — export tehsil asset
├── README.md
└── results/                       Output directory (created automatically when run)
```

**Workflow at a glance:**

```
GEE Code Editor                         Local Machine
─────────────────────────              ──────────────────────────────
1_Check_Tehsil.js   (verify)
        |
2_Generate_Tehsil_Boundary.js  ──>  GEE Asset created
        |
        └── copy asset path into config.yaml
                                            |
                                    python et_applications.py
                                            |
                                    results/ CSV + PNG files
```

---

## 3. Prerequisites and Access Requirements

### 3.1 Google Earth Engine Account

- A Google Earth Engine account is required. Register at: https://code.earthengine.google.com
- You must have a **GEE Cloud Project** set up. If you do not have one, create a project at https://console.cloud.google.com and enable the Earth Engine API.
- Note your **Cloud Project ID** (e.g., `shuvamdownscalinget`). This goes into `config.yaml` and the GEE scripts.

### 3.2 GEE Asset Access — What Must Be Public or Shared

Two GEE assets are required before running the Python script:

| Asset | Path in config.yaml | Who Owns It | What You Need to Do |
|---|---|---|---|
| Tehsil boundary asset | `assets.tehsil_asset` | You (created by Script 2) | Created automatically — no action needed after export |
| RF model asset | `assets.model_aez` | Project team | Must be shared with your GEE account (at minimum Reader access) |
| India block boundaries | `users/mtpictd/india_block_boundaries` | External dataset | Must be set to **Public** or shared with your account |

**To verify or set an asset as public in GEE:**

1. Open the GEE Code Editor at https://code.earthengine.google.com
2. In the left panel, click the **Assets** tab.
3. Locate the asset, click the three-dot menu next to it, and select **Share**.
4. Under "Add people", type `allUsers` and set permission to **Reader**.
5. Click **Save**.

If you do not have permission to make `users/mtpictd/india_block_boundaries` public, contact the dataset owner to request Reader access for your Google account.

### 3.3 Python Requirements

- Python 3.9 or higher
- pip

---

## 4. Environment Setup

### 4.1 Install Python Dependencies

```bash
pip install -r requirements.txt
```

The `requirements.txt` installs:

| Package | Minimum Version | Purpose |
|---|---|---|
| earthengine-api | 0.1.370 | GEE Python client |
| pandas | 2.0 | CSV assembly and output |
| numpy | 1.24 | Numerical operations |
| matplotlib | 3.7 | Optional plots |
| pyyaml | 6.0 | config.yaml parsing |

### 4.2 Verify Installation

```bash
python -c "import ee, pandas, numpy, yaml; print('All packages OK')"
```

---

## 5. Step 1 — Create the Tehsil Boundary Asset in GEE

This step is performed entirely in the **GEE Code Editor** (https://code.earthengine.google.com). You will run two JavaScript scripts in sequence.

### 5.1 Open Script 1: 1_Check_Tehsil.js

1. Open the GEE Code Editor.
2. Create a new script and paste the full contents of `1_Check_Tehsil.js`.
3. Edit the `CONFIG` block at the top of the script:

```javascript
var CONFIG = {
  state     : 'UTTAR PRADESH',     // Full state name in UPPER CASE
  district  : 'BANDA',             // District name in UPPER CASE
  tehsil    : 'TELYANI',           // Tehsil name in UPPER CASE
  geeProject: 'shuvamdownscalinget' // Your GEE Cloud Project ID
};
```

**Important:** The `state`, `district`, and `tehsil` values must match the spelling used in the `users/mtpictd/india_block_boundaries` dataset exactly. Use UPPER CASE throughout.

4. Click **Run**.

### 5.2 Reading the Check Report

After running Script 1, inspect the **Console** panel on the right:

- If the tehsil is found, the console prints the matched state, district, tehsil name, area in km², centroid coordinates, and bounding box. The map shows the tehsil filled in green with a red centroid marker.
- If nothing is found, the console lists all available tehsil names for that district, or all districts for that state. Use those printed values to correct your CONFIG spelling.

Do not proceed to Script 2 until the Console shows a confirmed match.

### 5.3 Open Script 2: 2_Generate_Tehsil_Boundary.js

1. Create a new script and paste the full contents of `2_Generate_Tehsil_Boundary.js`.
2. Copy the exact same `CONFIG` block from Script 1 — no changes are needed.
3. Click **Run**.
4. The Console will print the full **asset path** in this format:

```
projects/<your-project-id>/assets/tehsil_<state>__<district>__<tehsil>
```

Example:
```
projects/shuvamdownscalinget/assets/tehsil_uttar_pradesh__banda__telyani
```

5. Switch to the **Tasks** tab (top-right in the Code Editor).
6. Click **RUN** next to the export task.
7. Wait for the task to complete. A small tehsil typically completes in under 2 minutes. Monitor progress in the Tasks tab — the status will change from "RUNNING" to "COMPLETED".

### 5.4 Copy the Asset Path

Once the task shows COMPLETED, copy the full asset path printed by the Console. You will paste this into `config.yaml` in the next step.

---

## 6. Step 2 — Configure config.yaml

Open `config.yaml` in a text editor and update the following fields:

### Required Fields

```yaml
gee_project: "shuvamdownscalinget"    # Your GEE Cloud Project ID

tehsil:
  state    : "UTTAR PRADESH"          # For output file labelling only
  district : "BANDA"
  name     : "TELYANI"

assets:
  tehsil_asset : "projects/shuvamdownscalinget/assets/tehsil_uttar_pradesh__banda__telyani"
  model_aez    : "projects/shuvamdownscalinget/assets/rf_aez4_final"

time:
  year : 2022                         # Year to process (Landsat 8 + MODIS)
```

### Optional Fields

```yaml
application: "all"      # Which outputs to generate (see Section 9)

compute:
  chunk_size: 3000      # Pixels per download tile; lower if memory errors occur

output:
  directory: "./results"
  plot      : true      # Set to false to skip matplotlib charts
```

### Parameter Notes

- `gee_project`: Must match the project ID used in the GEE scripts and in your authenticated session.
- `tehsil.name`, `tehsil.state`, `tehsil.district`: These are used only for naming output files. They do not affect which area is processed — the geometry always comes from `assets.tehsil_asset`.
- `assets.model_aez`: This is a shared Random Forest model asset. Do not change this unless you are substituting a different trained model.
- `time.year`: Landsat 8 (Collection 2, Tier 1 L2) is used for AET. Coverage is available from 2013 onward. MODIS MOD16A2 PET is available from 2000 onward.
- `compute.chunk_size`: If you receive `User memory limit exceeded` errors from GEE, reduce this value (e.g., to `1000` or `500`). If downloads are slow, you may raise it up to `50000`.

---

## 7. Step 3 — Authenticate Earth Engine in Python

Before running the script for the first time (and when your token expires), you must authenticate the Earth Engine Python client. Use the `notebook` auth mode as described below.

### 7.1 Run Authentication

```bash
earthengine authenticate --auth_mode notebook
```

### 7.2 Follow the Authentication Steps

1. The command prints a URL. Copy it and open it in a browser.
2. Sign in with the Google account that has access to your GEE Cloud Project.
3. On the permissions screen, grant **all requested permissions**, including read access to Earth Engine assets. Do not uncheck any permission — missing permissions will cause asset access failures later.
4. After granting permissions, Google redirects to a page showing an authorization code. Copy this code.
5. Paste the code back into the terminal when prompted.

### 7.3 Verify Authentication

```bash
python -c "import ee; ee.Initialize(project='your-project-id'); print('EE authenticated OK')"
```

Replace `your-project-id` with your actual GEE Cloud Project ID. If this prints `EE authenticated OK`, you are ready to proceed.

**Note:** Authentication tokens are stored locally and expire periodically. If you receive an authentication error when running the script at a later date, repeat the `earthengine authenticate --auth_mode notebook` command.

---

## 8. Step 4 — Run the Python Application

With `config.yaml` set and authentication complete, run the script from the directory containing both `et_applications.py` and `config.yaml`:

```bash
python et_applications.py
```

This reads all settings from `config.yaml`. The default `application: "all"` generates all five output CSVs in a single GEE download pass.

### Overriding Config Values from the Command Line

Any value in `config.yaml` can be overridden with a CLI flag. Command-line arguments take precedence over the config file.

```bash
# Run only the monthly AET application
python et_applications.py --application monthly_et

# Change the output year
python et_applications.py --year 2021

# Use a different tehsil asset
python et_applications.py --tehsil-asset "projects/myproject/assets/tehsil_x"

# Enable plots even if config.yaml has plot: false
python et_applications.py --plot

# Extract a time-series for a single pixel by coordinates
python et_applications.py --sample-lon 80.34 --sample-lat 25.12

# Use a different config file
python et_applications.py --config /path/to/other_config.yaml
```

Full list of CLI flags:

| Flag | Type | Description |
|---|---|---|
| `--config` | path | Path to a YAML config file (default: `./config.yaml`) |
| `--application` | string | Application mode (see Section 9) |
| `--tehsil-asset` | string | GEE FeatureCollection asset path |
| `--model-aez` | string | GEE RF model asset path |
| `--year` | integer | Year to process |
| `--output` | path | Output directory |
| `--plot` | flag | Enable matplotlib plots |
| `--gee-project` | string | GEE Cloud Project ID |
| `--tehsil-name` | string | Label for output filenames |
| `--chunk-size` | integer | Pixels per download tile |
| `--sample-lon` | float | Longitude for single-pixel time-series |
| `--sample-lat` | float | Latitude for single-pixel time-series |

---

## 9. Application Modes and Output Files

Set `application` in `config.yaml` or use `--application` on the command line.

| Mode | Output File | GEE Calls | Description |
|---|---|---|---|
| `all` | All five CSVs below | Single combined download | Recommended. Builds all stacks once. |
| `monthly_et` | `monthly_et_<TEHSIL>_<YEAR>.csv` | AET only | 12 monthly mean daily AET columns |
| `annual_et` | `annual_et_<TEHSIL>_<YEAR>.csv` | AET only | Annual total AET derived from monthly |
| `pet` | `pet_<TEHSIL>_<YEAR>.csv` | MODIS only | 12 monthly mean daily PET columns |
| `rwdi` | `rwdi_<TEHSIL>_<YEAR>.csv` | AET + MODIS | Monthly RWDI (%) = (1 - AET/PET) x 100 |
| `water_stress` | `water_stress_<TEHSIL>_<YEAR>.csv` | AET + MODIS | Monthly Water Stress = AET/PET ratio |
| `merge` | `merged_<TEHSIL>_<YEAR>.csv` | None (offline) | Joins existing CSVs on pixel_id; no GEE calls |

**Using `merge`:** Run this after any combination of individual applications to produce a single wide CSV containing all available columns. No GEE connection is required.

```bash
python et_applications.py --application merge --tehsil-name TELYANI
```

**Plots:** When `plot: true`, each application also saves a PNG chart into the output directory alongside its CSV.

---

## 10. Output CSV Column Reference

All CSVs share the following identifier columns:

| Column | Description |
|---|---|
| `pixel_id` | Unique integer per 30 m pixel; consistent across all CSVs for the same tehsil and year |
| `longitude` | Pixel centroid longitude (WGS84) |
| `latitude` | Pixel centroid latitude (WGS84) |

Application-specific columns:

| Column Pattern | Unit | Application |
|---|---|---|
| `ET_Jan_daily_01mm` ... `ET_Dec_daily_01mm` | 0.1 mm/day | monthly_et |
| `ET_Jan_daily_mm` ... `ET_Dec_daily_mm` | mm/day | monthly_et |
| `ET_annual_01mm` | 0.1 mm/yr | annual_et |
| `ET_annual_mm` | mm/yr | annual_et |
| `PET_Jan_daily_01mm` ... `PET_Dec_daily_01mm` | 0.1 mm/day | pet |
| `PET_Jan_daily_mm` ... `PET_Dec_daily_mm` | mm/day | pet |
| `RWDI_Jan` ... `RWDI_Dec` | % | rwdi |
| `RWDI_annual` | % (mean of 12 months) | rwdi |
| `WaterStress_Jan` ... `WaterStress_Dec` | 0–1 ratio | water_stress |
| `WaterStress_annual` | 0–1 ratio (mean of 12 months) | water_stress |
| `gap_filled_months` | comma-separated month names | all |

**Gap filling:** Months with zero Landsat 8 scenes are gap-filled using the mean of available scenes within a +/- 60-day window. The `gap_filled_months` column records which months were filled. Pixels where MODIS has no data are written as `NaN` in PET, RWDI, and Water Stress columns.

---

## 11. config.yaml Parameter Reference

Complete reference of every parameter in `config.yaml`:

| Section | Key | Type | Description |
|---|---|---|---|
| (root) | `gee_project` | string | GEE Cloud Project ID |
| `tehsil` | `state` | string | State name — used for output file naming only |
| `tehsil` | `district` | string | District name — used for output file naming only |
| `tehsil` | `name` | string | Tehsil name — used for output file naming only |
| `assets` | `tehsil_asset` | string | Full GEE FeatureCollection path for the tehsil boundary |
| `assets` | `model_aez` | string | Full GEE asset path for the Random Forest AEZ model |
| `modis` | `collection` | string | MODIS ImageCollection ID (default: `MODIS/061/MOD16A2`) |
| `time` | `year` | integer | Calendar year to process |
| `compute` | `chunk_size` | integer | Approx pixels per GEE download tile (default: 25000) |
| (root) | `application` | string | Mode to run: `all`, `monthly_et`, `annual_et`, `pet`, `rwdi`, `water_stress`, `merge` |
| `output` | `directory` | path | Directory where CSV and PNG files are written |
| `output` | `plot` | boolean | Whether to generate matplotlib plots |

---

## 12. Changing to a Different Tehsil

To process a new tehsil, repeat the following steps. There is no need to modify `et_applications.py`.

**Step 1 — Run Script 1 in GEE**

Edit the `CONFIG` block in `1_Check_Tehsil.js`:

```javascript
var CONFIG = {
  state     : 'NEW STATE NAME',
  district  : 'NEW DISTRICT NAME',
  tehsil    : 'NEW TEHSIL NAME',
  geeProject: 'your-gee-project-id'
};
```

Click Run. Confirm the correct tehsil appears on the map and in the Console.

**Step 2 — Export the new asset with Script 2**

Copy the same CONFIG into `2_Generate_Tehsil_Boundary.js` and click Run. In the Tasks tab, click RUN. Wait for COMPLETED status. Copy the asset path from the Console.

**Step 3 — Update config.yaml**

Change these fields:

```yaml
tehsil:
  state    : "NEW STATE NAME"
  district : "NEW DISTRICT NAME"
  name     : "NEW TEHSIL NAME"

assets:
  tehsil_asset : "projects/<your-project>/assets/tehsil_<state>__<district>__<tehsil>"

time:
  year : 2022    # Change year if required
```

**Step 4 — Run**

```bash
python et_applications.py
```

Output files will be named using the tehsil name and year (e.g., `monthly_et_NEW_TEHSIL_2022.csv`), so existing results are not overwritten.

---

## 13. Troubleshooting

**"No features found" in GEE Script 1**

The `state`, `district`, or `tehsil` value in CONFIG does not match the dataset spelling. The script will print a list of valid names for the given district or state. Use those exact strings (UPPER CASE) in your CONFIG.

**Authentication error when running et_applications.py**

Your token has expired or was not created. Re-authenticate:

```bash
earthengine authenticate --auth_mode=notebook
```

Follow the steps in Section 7 and grant all permissions when prompted.

**"User memory limit exceeded" from GEE**

The `chunk_size` value is too large for this tehsil. Lower it in `config.yaml`:

```yaml
compute:
  chunk_size: 1000
```

Progressively halve the value until the error stops. For large tehsils, values as low as 500 may be necessary.

**"Asset not found" or "Permission denied" for an asset**

Either the asset does not exist yet (GEE export task did not complete), or the asset is not shared with your account. Check:

1. In the GEE Code Editor, go to the Assets tab and confirm the asset is present.
2. Click Share on the asset and verify your Google account has at minimum Reader access.
3. For the RF model asset, contact the project team to confirm sharing.

**Slow downloads**

GEE throughput varies by time of day. If downloads are consistently slow, verify that `chunk_size` is not unnecessarily small. The default of `5000` is appropriate for most tehsils.

**Missing months in output / all values NaN for some months**

Months with no Landsat 8 scenes are gap-filled automatically. Check the `gap_filled_months` column in the CSV. If many months are gap-filled, this may indicate cloud cover issues for the selected year. Try a different year in `config.yaml`.

**matplotlib not installed / plots not generating**

Install it explicitly:

```bash
pip install matplotlib>=3.7
```

If you do not need plots, set `plot: false` in `config.yaml` to suppress the warning.