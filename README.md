# ET-Applications — Pan-India ET Downscaling (GeoTIFF Raster Edition)

**Version:** 2.1  
**Platform:** Google Earth Engine (GEE) + Python 3.x  
**Output:** Multi-band GeoTIFF rasters at 30 m resolution for any tehsil in India  
**Models & Training Data:** [Pan_India_Downscaled_Evapotranspiration on GitHub](https://github.com/Shuvam-Chakraborty/Pan_India_Downscaled_Evapotranspiration)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Use Cases](#2-use-cases)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites and Access Requirements](#4-prerequisites-and-access-requirements)
5. [Environment Setup](#5-environment-setup)
6. [Step 1 — Create the Tehsil Boundary Asset in GEE](#6-step-1--create-the-tehsil-boundary-asset-in-gee)
7. [Step 2 — Configure config.yaml](#7-step-2--configure-configyaml)
8. [Step 3 — Authenticate Earth Engine in Python](#8-step-3--authenticate-earth-engine-in-python)
9. [Step 4 — Run the Python Application](#9-step-4--run-the-python-application)
10. [Application Modes and Output Files](#10-application-modes-and-output-files)
11. [Output GeoTIFF Band Reference](#11-output-geotiff-band-reference)
12. [config.yaml Parameter Reference](#12-configyaml-parameter-reference)
13. [AEZ Model Assets](#13-aez-model-assets)
14. [Python Function Reference](#14-python-function-reference)
15. [Working with Output GeoTIFFs](#15-working-with-output-geotiffs)
16. [Changing to a Different Tehsil](#16-changing-to-a-different-tehsil)
17. [Troubleshooting](#17-troubleshooting)

---

## 1. Project Overview

ET-Applications downscales MODIS-resolution evapotranspiration data to the native 30 m Landsat 8 grid using pre-trained Random Forest models — one model per Agro-Ecological Zone (AEZ) covering all of India. For any selected tehsil, the tool generates **multi-band GeoTIFF rasters** containing:

- Monthly mean daily Actual Evapotranspiration (AET) — 12 bands
- Annual total AET — 1 band
- Monthly mean daily Potential Evapotranspiration (PET) from MODIS MOD16A2 — 12 bands
- Relative Water Deficit Index (RWDI) per month + annual mean — 13 bands
- Water Stress ratio (AET/PET) per month + annual mean — 13 bands

All output GeoTIFFs share the same CRS, bounding box, and 30 m pixel grid, making them directly overlay-compatible in any GIS environment without reprojection. Band descriptions and metadata (units, source, year, gap-fill status) are embedded in every file.

The Random Forest models and their training data are fully documented and openly available at:  
**https://github.com/Shuvam-Chakraborty/Pan_India_Downscaled_Evapotranspiration**

---

## 2. Use Cases

ET-Applications is designed for researchers, water resource planners, and agricultural scientists who need high-resolution evapotranspiration data at the tehsil scale. Below are the primary use cases for each output product.

### Actual Evapotranspiration (AET)

AET represents the real water lost from the land surface through evaporation and plant transpiration under actual soil moisture conditions. At 30 m resolution, it can reveal spatial heterogeneity within a tehsil that is invisible in coarser MODIS data.

**Use cases:**
- Estimating crop water consumption and irrigation demand at field scale
- Calibrating and validating hydrological models (e.g., SWAT, VIC) at the tehsil level
- Monitoring seasonal and inter-annual variability in water use across land cover types
- Comparing AET between irrigated and rainfed areas within the same tehsil

### Potential Evapotranspiration (PET)

PET is the amount of water that would evaporate and transpire given unlimited water availability — a measure of atmospheric demand. Sourced from MODIS MOD16A2 and resampled to 30 m, it serves as a baseline reference for stress calculations.

**Use cases:**
- Computing crop coefficients (Kc = AET/PET) for agricultural planning
- Understanding the atmospheric water demand across seasons
- Identifying periods when water demand exceeds available supply

### Relative Water Deficit Index (RWDI)

RWDI = (1 − AET/PET) × 100 (%). A higher value means the land surface is more water-stressed — plants are evapotranspiring much less than they theoretically could. RWDI of 0% indicates no deficit; values above 80% indicate severe to extreme drought stress.

**Use cases:**
- Agricultural drought monitoring at tehsil and village scale
- Identifying which months and which parts of a tehsil experience the worst water stress
- Supporting crop insurance assessment and relief targeting
- Inputs to multi-hazard drought indices for policy reporting

### Water Stress Ratio (AET/PET)

The inverse of RWDI expressed as a ratio from 0 to 1. A value of 1.0 means the land is meeting its full evapotranspiration demand; values near 0 indicate extreme stress.

**Use cases:**
- Integrating into land surface model validation workflows
- Mapping irrigated vs. rainfed agriculture based on seasonal stress patterns
- Input layer for crop yield prediction models
- Long-term trend analysis when run over multiple years

### Sample Point Time Series

The optional single-pixel timeseries plot extracts the 12-month AET, PET, RWDI, and Water Stress values for a user-specified coordinate from the already-downloaded GeoTIFFs without any additional GEE calls.

**Use cases:**
- Field station validation against ground measurements (eddy covariance, lysimeter)
- Quick inspection of a specific farm, forest patch, or monitoring site
- Generating figures for reports and publications from a known coordinate

---

## 3. Repository Structure

```
et-applications/
├── et_applications.py             Main Python CLI application
├── config.yaml                    User configuration file (edit this)
├── requirements.txt               Python package dependencies
├── 1_Check_Tehsil.js              GEE Script 1 — verify tehsil before export
├── 2_Generate_Tehsil_Boundary.js  GEE Script 2 — export tehsil asset
├── README.md
└── results/                       Output directory (created automatically)
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
                                    results/ GeoTIFF + PNG files
```

---

## 4. Prerequisites and Access Requirements

### 4.1 Google Earth Engine Account

- A Google Earth Engine account is required. Register at: https://code.earthengine.google.com
- You must have a **GEE Cloud Project** set up. If you do not have one, create a project at https://console.cloud.google.com and enable the Earth Engine API.
- Note your **Cloud Project ID** (e.g., `shuvamdownscalinget`). This goes into `config.yaml` and the GEE scripts.

### 4.2 GEE Asset Access

Two GEE assets are required before running the Python script:

| Asset | Path in config.yaml | Who Owns It | What You Need to Do |
|---|---|---|---|
| Tehsil boundary asset | `assets.tehsil_asset` | You (created by Script 2) | Created automatically — no action after export |
| RF model asset | `assets.model_aez` | Publicly shared | See Section 13 for all 19 AEZ model paths |
| India block boundaries | `users/mtpictd/india_block_boundaries` | External dataset | Must be public or shared with your account |

**To verify or set an asset as public in GEE:**

1. Open the GEE Code Editor at https://code.earthengine.google.com
2. In the left panel, click the **Assets** tab.
3. Locate the asset, click the three-dot menu, and select **Share**.
4. Under "Add people", type `allUsers` and set permission to **Reader**.
5. Click **Save**.

### 4.3 Python Requirements

- Python 3.9 or higher
- pip

---

## 5. Environment Setup

### 5.1 Install Python Dependencies

```bash
pip install -r requirements.txt
```

The `requirements.txt` installs:

| Package | Minimum Version | Purpose |
|---|---|---|
| earthengine-api | 0.1.355 | GEE Python client — communicates with GEE servers |
| numpy | 1.24 | Array operations, raster maths, NoData masking |
| matplotlib | 3.7 | Optional PNG charts for each output product |
| pyyaml | 6.0 | Parses `config.yaml` into a Python dictionary |
| requests | 2.28 | Downloads GeoTIFF tiles from GEE thumbnail URLs |
| rasterio | 1.3 | Merges downloaded tiles and writes final GeoTIFF files |

> **Note:** `pandas` is not required. This version uses rasterio and numpy to write GeoTIFF rasters directly, replacing the previous CSV-based approach.

### 5.2 Verify Installation

```bash
python -c "import ee, numpy, rasterio, requests, yaml; print('All packages OK')"
```

---

## 6. Step 1 — Create the Tehsil Boundary Asset in GEE

This step is performed entirely in the **GEE Code Editor** (https://code.earthengine.google.com). Run two JavaScript scripts in sequence.

### 6.1 Run Script 1: 1_Check_Tehsil.js

1. Open the GEE Code Editor and create a new script.
2. Paste the full contents of `1_Check_Tehsil.js`.
3. Edit the `CONFIG` block:

```javascript
var CONFIG = {
  state     : 'UTTAR PRADESH',      // Full state name in UPPER CASE
  district  : 'BANDA',              // District name in UPPER CASE
  tehsil    : 'TELYANI',            // Tehsil name in UPPER CASE
  geeProject: 'shuvamdownscalinget' // Your GEE Cloud Project ID
};
```

**Important:** The `state`, `district`, and `tehsil` values must exactly match the spelling in the `users/mtpictd/india_block_boundaries` dataset. Use UPPER CASE throughout.

4. Click **Run**.

### 6.2 Reading the Check Report

After running Script 1, inspect the **Console** panel:

- If the tehsil is found, the console prints the matched names, area in km², centroid coordinates, and bounding box. The map shows the tehsil in green with a red centroid marker and neighbouring blocks in grey for context.
- If nothing is found, the console lists all available tehsil names for that district, or all districts for that state. Use those printed values to correct your CONFIG spelling.

Do not proceed to Script 2 until the Console shows a confirmed match.

### 6.3 Run Script 2: 2_Generate_Tehsil_Boundary.js

1. Create a new script and paste the full contents of `2_Generate_Tehsil_Boundary.js`.
2. Copy the exact same `CONFIG` block from Script 1 — no changes needed.
3. Click **Run**.
4. The Console prints the full asset path:

```
projects/<your-project-id>/assets/tehsil_<state>__<district>__<tehsil>
```

Example:
```
projects/shuvamdownscalinget/assets/tehsil_uttar_pradesh__banda__telyani
```

5. Switch to the **Tasks** tab and click **RUN** next to the export task.
6. Wait for the task to show **COMPLETED** (~1–2 minutes for most tehsils).

### 6.4 Copy the Asset Path

Once the task completes, copy the full asset path from the Console. Paste it into `config.yaml` in the next step.

---

## 7. Step 2 — Configure config.yaml

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
  model_aez    : "projects/shuvamdownscalinget/assets/rf_aez4_final"   # Choose your AEZ (see Section 13)

time:
  year : 2022                         # Year to process (Landsat 8 + MODIS)
```

### Optional Fields

```yaml
application: "all"      # Which outputs to generate (see Section 10)

compute:
  chunk_size: 3000      # Pixels per download tile; lower if memory errors occur

output:
  directory: "./results"
  plot      : true      # Set to false to skip matplotlib charts

# Single-pixel timeseries plot (no extra GEE calls)
sample_point:
  lon: 80.7314
  lat: 25.9735
```

### Parameter Notes

- `gee_project`: Must match the project ID used in the GEE scripts and in your authenticated session.
- `tehsil.name`, `tehsil.state`, `tehsil.district`: Used only for naming output files. The processing geometry always comes from `assets.tehsil_asset`.
- `assets.model_aez`: Select the RF model corresponding to your tehsil's Agro-Ecological Zone. All 19 publicly accessible AEZ model paths are listed in Section 13.
- `time.year`: Landsat 8 (Collection 2, Tier 1 L2) is available from 2013 onward. MODIS MOD16A2 PET is available from 2000 onward.
- `compute.chunk_size`: If you receive `User memory limit exceeded` errors from GEE, reduce this value (e.g., to `1000` or `500`). If downloads are slow, raise it to `50000`.

---

## 8. Step 3 — Authenticate Earth Engine in Python

Before running the script for the first time (and whenever your token expires), authenticate the Earth Engine Python client.

### 8.1 Run Authentication

```bash
earthengine authenticate --auth_mode notebook
```

### 8.2 Follow the Authentication Steps

1. The command prints a URL. Copy it and open it in a browser.
2. Sign in with the Google account linked to your GEE Cloud Project.
3. Grant **all requested permissions**, including Earth Engine asset read access.
4. Copy the authorization code shown and paste it back into the terminal.

### 8.3 Verify Authentication

```bash
python -c "import ee; ee.Initialize(project='your-project-id'); print('EE authenticated OK')"
```

Replace `your-project-id` with your actual GEE Cloud Project ID.

---

## 9. Step 4 — Run the Python Application

With `config.yaml` set and authentication complete, run the script from the directory containing both `et_applications.py` and `config.yaml`:

```bash
python et_applications.py
```

This reads all settings from `config.yaml`. The default `application: "all"` generates all five output GeoTIFFs in a single GEE download pass.

### Overriding Config Values from the Command Line

Any value in `config.yaml` can be overridden with a CLI flag. Command-line arguments always take precedence over the config file.

```bash
# Run only the monthly AET application
python et_applications.py --application monthly_et

# Change the output year
python et_applications.py --year 2021

# Use a different tehsil asset
python et_applications.py --tehsil-asset "projects/myproject/assets/tehsil_x"

# Enable plots even if config.yaml has plot: false
python et_applications.py --plot

# Extract a timeseries plot for a single pixel by coordinates
python et_applications.py --sample-lon 80.34 --sample-lat 25.12

# Use a different config file
python et_applications.py --config /path/to/other_config.yaml
```

Full list of CLI flags:

| Flag | Type | Description |
|---|---|---|
| `--config` | path | Path to a YAML config file (default: `./config.yaml`) |
| `--application` | string | Application mode (see Section 10) |
| `--tehsil-asset` | string | GEE FeatureCollection asset path |
| `--model-aez` | string | GEE RF model asset path |
| `--year` | integer | Year to process |
| `--output` | path | Output directory |
| `--plot` | flag | Enable matplotlib plots |
| `--gee-project` | string | GEE Cloud Project ID |
| `--tehsil-name` | string | Label for output filenames |
| `--chunk-size` | integer | Pixels per download tile |
| `--sample-lon` | float | Longitude for single-pixel timeseries plot |
| `--sample-lat` | float | Latitude for single-pixel timeseries plot |

---

## 10. Application Modes and Output Files

Set `application` in `config.yaml` or use `--application` on the command line.

| Mode | Output File | Bands | Description |
|---|---|---|---|
| `all` | All five GeoTIFFs | — | Recommended. Builds all stacks once, downloads once in a single 49-band pass. Guarantees pixel-perfect spatial alignment across all outputs. |
| `monthly_et` | `monthly_et_<TEHSIL>_<YEAR>.tif` | 12 | Monthly mean daily AET (mm/day) |
| `annual_et` | `annual_et_<TEHSIL>_<YEAR>.tif` | 1 | Annual total AET (mm/yr) |
| `pet` | `pet_<TEHSIL>_<YEAR>.tif` | 12 | Monthly mean daily PET (mm/day) |
| `rwdi` | `rwdi_<TEHSIL>_<YEAR>.tif` | 13 | Monthly RWDI (%) + annual mean |
| `water_stress` | `water_stress_<TEHSIL>_<YEAR>.tif` | 13 | Monthly Water Stress ratio + annual mean |

**Plots:** When `plot: true`, each application saves a PNG chart alongside its GeoTIFF in the output directory.

---

## 11. Output GeoTIFF Band Reference

### monthly_et_\<TEHSIL\>_\<YEAR\>.tif — 12 bands

| Band | Description | Unit |
|---|---|---|
| 1 | `ET_Jan_daily_mm` — January mean daily AET | mm/day |
| 2 | `ET_Feb_daily_mm` — February mean daily AET | mm/day |
| … | … | … |
| 12 | `ET_Dec_daily_mm` — December mean daily AET | mm/day |

TIFF metadata tag `gap_filled_months` lists any months filled by ±60-day temporal interpolation (e.g., `"Jan,Feb"` or `"none"`).

### annual_et_\<TEHSIL\>_\<YEAR\>.tif — 1 band

| Band | Description | Unit |
|---|---|---|
| 1 | `ET_annual_mm` — Annual total AET = Σ_m(daily_ET_m × days_m) | mm/yr |

### pet_\<TEHSIL\>_\<YEAR\>.tif — 12 bands

| Band | Description | Unit |
|---|---|---|
| 1 | `PET_Jan_daily_mm` — January mean daily PET (MODIS MOD16A2) | mm/day |
| … | … | … |
| 12 | `PET_Dec_daily_mm` — December mean daily PET | mm/day |

Pixels where MODIS has no data are written as NoData (–9999).

### rwdi_\<TEHSIL\>_\<YEAR\>.tif — 13 bands

| Band | Description | Unit |
|---|---|---|
| 1–12 | `RWDI_Jan` … `RWDI_Dec` — RWDI = (1 – AET/PET) × 100 | % |
| 13 | `RWDI_annual` — pixel-wise mean RWDI across all 12 months | % |

### water_stress_\<TEHSIL\>_\<YEAR\>.tif — 13 bands

| Band | Description | Unit |
|---|---|---|
| 1–12 | `WaterStress_Jan` … `WaterStress_Dec` — AET/PET ratio | 0–1 |
| 13 | `WaterStress_annual` — pixel-wise mean ratio across 12 months | 0–1 |

### Common metadata (all GeoTIFFs)

- **NoData value:** –9999 — applied to pixels outside the tehsil boundary and pixels where MODIS has no data.
- **CRS:** Native Landsat 8 projection for the scene (typically UTM, WGS84 datum).
- **Pixel size:** 30 m.
- **Compression:** LZW (lossless).
- **Data type:** Float32.
- Band descriptions are embedded and visible in QGIS, ArcGIS, or via `rasterio`.

---

## 12. config.yaml Parameter Reference

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
| (root) | `application` | string | Mode: `all`, `monthly_et`, `annual_et`, `pet`, `rwdi`, `water_stress` |
| `output` | `directory` | path | Directory where GeoTIFF and PNG files are written |
| `output` | `plot` | boolean | Whether to generate matplotlib plots |
| `sample_point` | `lon` | float | Longitude for single-pixel timeseries plot (optional) |
| `sample_point` | `lat` | float | Latitude for single-pixel timeseries plot (optional) |

---

## 13. AEZ Model Assets

The downscaling pipeline uses one Random Forest model per Agro-Ecological Zone (AEZ). All 19 models are publicly accessible as GEE assets under the project `shuvamdownscalinget`. Set the appropriate model in `config.yaml` under `assets.model_aez` based on the AEZ your tehsil falls in.

| AEZ | GEE Asset Path |
|---|---|
| AEZ 1 | `projects/shuvamdownscalinget/assets/rf_aez1_final` |
| AEZ 2 | `projects/shuvamdownscalinget/assets/rf_aez2_final` |
| AEZ 3 | `projects/shuvamdownscalinget/assets/rf_aez3_final` |
| AEZ 4 | `projects/shuvamdownscalinget/assets/rf_aez4_final` |
| AEZ 5 | `projects/shuvamdownscalinget/assets/rf_aez5_final` |
| AEZ 6 | `projects/shuvamdownscalinget/assets/rf_aez6_final` |
| AEZ 7 | `projects/shuvamdownscalinget/assets/rf_aez7_final` |
| AEZ 8 | `projects/shuvamdownscalinget/assets/rf_aez8_final` |
| AEZ 9 | `projects/shuvamdownscalinget/assets/rf_aez9_final` |
| AEZ 10 | `projects/shuvamdownscalinget/assets/rf_aez10_final` |
| AEZ 11 | `projects/shuvamdownscalinget/assets/rf_aez11_final` |
| AEZ 12 | `projects/shuvamdownscalinget/assets/rf_aez12_final` |
| AEZ 13 | `projects/shuvamdownscalinget/assets/rf_aez13_final` |
| AEZ 14 | `projects/shuvamdownscalinget/assets/rf_aez14_final` |
| AEZ 15 | `projects/shuvamdownscalinget/assets/rf_aez15_final` |
| AEZ 16 | `projects/shuvamdownscalinget/assets/rf_aez16_final` |
| AEZ 17 | `projects/shuvamdownscalinget/assets/rf_aez17_final` |
| AEZ 18 | `projects/shuvamdownscalinget/assets/rf_aez18_final` |
| AEZ 19 | `projects/shuvamdownscalinget/assets/rf_aez19_final` |

For full details on how these models were trained, the input features used, validation results, and the AEZ delineation map, see:

**https://github.com/Shuvam-Chakraborty/Pan_India_Downscaled_Evapotranspiration**

---

## 14. Python Function Reference

This section explains what each major function in `et_applications.py` does, why it exists, and where it fits in the processing chain.

### Configuration

**`load_config(path)`**  
Reads `config.yaml` and returns a Python dictionary of all settings. Handles missing keys gracefully by falling back to safe defaults. Called once at startup before anything else.

**`merge_args(cfg, args)`**  
Merges the loaded config dictionary with any CLI arguments the user passed. CLI values always win over config file values, so you can override any setting without editing `config.yaml`.

### Earth Engine Initialisation

**`init_ee(project)`**  
Initialises the GEE Python client. If the token is valid, this succeeds silently. If authentication has expired, it calls `ee.Authenticate()` automatically and re-initialises. Called once per run.

**`load_tehsil(asset)`**  
Loads the tehsil boundary FeatureCollection from GEE and extracts its geometry. This geometry is used as the spatial mask for all subsequent image operations — every band is clipped and downloaded to exactly this shape.

### GEE Image Building

**`build_classifier(model_path)`**  
Loads a pre-trained Random Forest ensemble from a GEE asset. Each feature in the asset contains a serialised decision tree string. The function strips comments, assembles the ensemble, and returns a GEE `Classifier` object ready for `.classify()` calls. This is the AEZ-specific model that converts Landsat + GLDAS features to a daily ET prediction.

**`calc_landsat_indices(img)`**  
Takes a single Landsat 8 image and computes all spectral indices required by the RF model as input features: NDVI, SAVI, MSAVI, NDBI, NDWI, NDMI, NDIIB7, Albedo, and LST. Returns the original image with these new bands appended.

**`predict_daily_et(ls_img, region, classifier)`**  
Combines the Landsat spectral indices with GLDAS climate variables (rainfall, soil moisture, temperature, humidity, wind, radiation, etc.) for the nearest 24-hour window. Selects the 26 feature bands in the exact order the RF model was trained on, runs `.classify()`, and returns a single-band daily ET image (units: 0.1 mm/day internally).

**`build_aet_stack(region, classifier, year)`**  
Builds a 12-band monthly AET image for the full calendar year. For each month it composites all available Landsat 8 scenes, runs the RF prediction on each scene, averages them for the monthly mean, then gap-fills months with zero scenes using the mean of the ±60-day temporal neighbourhood. Returns both the 12-band image and a list of boolean gap flags (one per month).

**`build_pet_stack(region, year, modis_col_id, proj)`**  
Builds a 12-band monthly PET image from MODIS MOD16A2. Each 8-day composite is divided by 8 to get daily rates, monthly composites are averaged, and the 500 m MODIS pixels are bilinearly resampled onto the 30 m Landsat projection so AET and PET share the same pixel grid.

**`build_annual_et_image(aet_stack, year)`**  
Computes annual total ET by multiplying each monthly mean daily ET band by the number of days in that month and summing all 12. The result is a single-band image in units of 0.1 mm/yr (converted to mm/yr by Python after download).

**`build_rwdi_image(aet_stack, pet_stack)`**  
Computes RWDI = (1 − AET/PET) × 100 for each of the 12 months. Returns a 12-band image in percent. NoData (–9999) is applied where PET is missing.

**`build_ws_image(aet_stack, pet_stack)`**  
Computes Water Stress = AET/PET for each of the 12 months. Returns a 12-band image as a dimensionless ratio (0–1). NoData where PET is missing.

**`build_combined_image(aet_stack, pet_stack, year)`**  
Stacks all products into a single 49-band GEE image in this order: AET (12 bands) → PET (12 bands) → Annual ET (1 band) → RWDI (12 bands) → Water Stress (12 bands). This allows a single GEE download pass to retrieve all data at once, guaranteeing that every product is derived from the same underlying pixel grid.

### Download Infrastructure

**`_download_image_as_geotiff(img, region, chunk_size, label)`**  
Tiles the tehsil bounding box into a grid of small rectangles, each approximately `chunk_size` pixels in area. For each tile it checks (using Shapely if available, otherwise server-side) whether the tile actually overlaps the tehsil — empty tiles are skipped without any GEE download attempt to avoid the `empty geometry` error. Each overlapping tile is downloaded as a GeoTIFF via `getDownloadURL`, with up to 4 retries on transient errors (503, timeouts). Returns a list of temporary file paths.

**`_merge_tiles(tile_paths, nodata)`**  
Opens all temporary tile GeoTIFFs, mosaics them using `rasterio.merge`, extracts the profile (CRS, transform, dimensions), and cleans up all temporary files regardless of success or failure. GDAL/libtiff C-level warnings that fire on multi-band tiles are suppressed at the file-descriptor level using the `_quiet_gdal()` context manager.

**`_quiet_gdal()`**  
A context manager that temporarily redirects `stderr` (file descriptor 2) to `/dev/null` during rasterio calls. This is the only reliable way to suppress `Warning 1: TIFFReadDirectory …` messages that come from the libtiff C library and bypass Python's `warnings` module entirely.

**`_save_geotiff(arr, profile, output_path, band_names, metadata)`**  
Writes a numpy array `(n_bands, H, W)` as a LZW-compressed Float32 GeoTIFF. Embeds band descriptions (visible in QGIS and rasterio) and optional metadata tags (units, year, gap-fill status). Uses `PHOTOMETRIC=MINISBLACK` to prevent GDAL from emitting ExtraSamples warnings on files with more than 3 bands. Prints a summary of the saved file (dimensions, valid pixel count, file size).

### Array Utilities

**`_scale_nodata(arr, scale, nodata)`**  
Multiplies an array by a scale factor (e.g., 0.1 to convert from GEE's internal 0.1 mm units to mm) while leaving NoData pixels, NaN values, and infinite values unchanged. Also clamps any values beyond ±1e6 to NoData to catch RF edge artefacts that are physically impossible.

**`_annual_mean_band(monthly_12, nodata)`**  
Computes a pixel-wise mean across the 12 monthly bands, ignoring NoData and NaN pixels. Returns a `(1, H, W)` array. Pixels where all 12 months are invalid remain NoData in the output. RuntimeWarning from `np.nanmean` on all-NaN slices is suppressed with `warnings.catch_warnings`.

**`_band_stats(arr, nodata)`**  
Computes per-band mean and standard deviation across all valid pixels, ignoring NoData, NaN, and ±inf. Used internally by the plot functions.

**`_print_stats(label, arr, nodata)`**  
Prints a concise summary (valid pixel count, mean, min, max, std) for an array to the console. Called after saving each GeoTIFF so the user can immediately sanity-check the outputs.

### Application Runners

**`run_monthly_et(cfg, region, ...)`**  
Builds the AET stack (if not already provided), downloads tiles, converts units, and saves `monthly_et_<TEHSIL>_<YEAR>.tif` with 12 monthly bands.

**`run_annual_et(cfg, region, ...)`**  
Builds the AET stack, computes the annual sum on the GEE server, downloads, converts, and saves `annual_et_<TEHSIL>_<YEAR>.tif` with 1 band.

**`run_pet(cfg, region, ...)`**  
Builds the AET stack (as a pixel-grid carrier) and the PET stack, downloads both together, discards the carrier band, and saves `pet_<TEHSIL>_<YEAR>.tif` with 12 bands.

**`run_rwdi(cfg, region, ...)`**  
Builds both stacks, computes RWDI in GEE, downloads, appends a Python-computed annual mean band (band 13), and saves `rwdi_<TEHSIL>_<YEAR>.tif` with 13 bands.

**`run_water_stress(cfg, region, ...)`**  
Same structure as RWDI but computes AET/PET ratio instead. Saves `water_stress_<TEHSIL>_<YEAR>.tif` with 13 bands.

**`run_all(cfg, region)`**  
The recommended entry point. Builds AET and PET stacks once, assembles the 49-band combined image, performs a single tile download pass, then slices and saves all five GeoTIFFs from the one mosaic. Guarantees spatial identity across all outputs.

### Sample Point

**`run_sample_timeseries(output_paths, cfg)`**  
If a `sample_point` is configured, reads the pixel nearest to the specified lon/lat from each output GeoTIFF using `rasterio.sample` and passes the extracted values to the 4-panel plot function. No additional GEE calls are made — this reuses already-saved local files.

### Plot Functions

| Function | Input shape | Output |
|---|---|---|
| `_plot_monthly_et` | (12, H, W) mm/day | Line chart with ±1 std fill |
| `_plot_annual_et` | (1, H, W) mm/yr | Histogram with mean line |
| `_plot_pet` | (12, H, W) mm/day | Line chart with ±1 std fill |
| `_plot_rwdi` | (12, H, W) % | Bar chart colour-coded by stress class |
| `_plot_water_stress` | (12, H, W) ratio | Line chart with threshold lines at 1.0, 0.5, 0.3 |
| `_plot_sample_timeseries` | 4 × (12,) arrays | 4-panel timeseries for a single pixel |

---

## 15. Working with Output GeoTIFFs

### Opening in QGIS

1. Go to **Layer > Add Layer > Add Raster Layer**.
2. Browse to the `.tif` file and click **Add**.
3. In the **Layer Styling** panel, select individual bands using the **Band** dropdown.
4. Use the **Multiband Color** renderer to display three bands simultaneously (e.g., Jan, Apr, Jul).

### Reading in Python with rasterio

```python
import rasterio
import numpy as np

# Open the monthly AET GeoTIFF
with rasterio.open('results/monthly_et_TELYANI_2022.tif') as ds:
    print("CRS:", ds.crs)
    print("Transform:", ds.transform)
    print("NoData:", ds.nodata)
    print("Band descriptions:", ds.descriptions)

    # Read all 12 monthly bands as a numpy array (12, H, W)
    aet = ds.read()

# Mask NoData
aet_masked = np.where(aet == -9999, np.nan, aet)

# Compute tehsil-wide monthly means
monthly_means = np.nanmean(aet_masked.reshape(12, -1), axis=1)
print("Monthly AET means (mm/day):", monthly_means)
```

### Reading in Python with GDAL

```python
from osgeo import gdal
ds = gdal.Open('results/monthly_et_TELYANI_2022.tif')
band = ds.GetRasterBand(1)        # January (1-indexed)
data = band.ReadAsArray()
nodata = band.GetNoDataValue()    # -9999.0
```

### Stacking all outputs into a single file (GDAL command line)

```bash
gdal_merge.py -o combined_TELYANI_2022.tif -separate \
  results/monthly_et_TELYANI_2022.tif \
  results/annual_et_TELYANI_2022.tif \
  results/pet_TELYANI_2022.tif \
  results/rwdi_TELYANI_2022.tif \
  results/water_stress_TELYANI_2022.tif
```

### Visualising in GEE (after uploading as assets)

When you upload a GeoTIFF to GEE as an image asset, GEE renames bands to `b1`, `b2`, … regardless of the embedded band descriptions. Use `b1` through `b12` (or `b13`) when selecting bands in GEE scripts.

---

## 16. Changing to a Different Tehsil

To process a new tehsil, repeat the following steps. `et_applications.py` does not need to be modified.

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

```yaml
tehsil:
  state    : "NEW STATE NAME"
  district : "NEW DISTRICT NAME"
  name     : "NEW TEHSIL NAME"

assets:
  tehsil_asset : "projects/<your-project>/assets/tehsil_<state>__<district>__<tehsil>"
  model_aez    : "projects/shuvamdownscalinget/assets/rf_aez<N>_final"   # correct AEZ for this tehsil

time:
  year : 2022    # Change if required
```

**Step 4 — Run**

```bash
python et_applications.py
```

Output files are named using the tehsil name and year (e.g., `monthly_et_NEWNAME_2022.tif`), so existing results are not overwritten.

---

## 17. Troubleshooting

**"No features found" in GEE Script 1**

The `state`, `district`, or `tehsil` value in CONFIG does not match the dataset spelling. The script prints a list of valid names for the given district or state. Use those exact strings (UPPER CASE).

**Authentication error when running et_applications.py**

Your token has expired or was never created. Re-authenticate:

```bash
earthengine authenticate --auth_mode=notebook
```

**"User memory limit exceeded" from GEE**

The `chunk_size` value is too large. Lower it:

```yaml
compute:
  chunk_size: 1000
```

Progressively halve the value until the error stops. For large tehsils or the 49-band `all` mode, values as low as `500` may be necessary.

**"Asset not found" or "Permission denied"**

Either the GEE export task did not complete, or the asset is not shared with your account. Check:

1. In the GEE Code Editor, go to the Assets tab and confirm the asset is present.
2. Click Share on the asset and verify your Google account has at minimum Reader access.
3. The 19 RF model assets in Section 13 are publicly accessible — if you receive a permission error on one of them, verify the asset path is typed correctly.

**Slow downloads / partial GeoTIFF**

GEE throughput varies by time of day. If a tile fails after 4 attempts, a warning is printed and that tile is skipped — the output GeoTIFF may have NoData patches. Re-run the same application; the script will re-attempt all tiles. For consistently slow downloads, ensure `chunk_size` is not unnecessarily large.

**Missing months in output / NoData for some months**

Months with no Landsat 8 scenes are gap-filled automatically. Check which months were filled:

```python
import rasterio
with rasterio.open('results/monthly_et_TELYANI_2022.tif') as ds:
    print(ds.tags().get('gap_filled_months'))
```

If many months are gap-filled, try a different year in `config.yaml`.

**`RuntimeWarning: Mean of empty slice`**

This is suppressed in `et_applications.py` v2.1 using `warnings.catch_warnings()` in `_annual_mean_band`. If you still see it, ensure you are running the latest version of the script.

**matplotlib not installed / plots not generating**

```bash
pip install matplotlib>=3.7
```

Set `plot: false` in `config.yaml` if you do not need plots.

**`rasterio` import error**

On some systems rasterio requires GDAL to be installed first. See:  
https://rasterio.readthedocs.io/en/stable/installation.html

**GEE band names show `b1`, `b2`, … instead of descriptive names**

This is expected behaviour when a GeoTIFF is uploaded to GEE as an image asset. GEE ignores embedded band descriptions and assigns sequential names. Use `b1` through `b12`/`b13` when writing GEE scripts against uploaded assets.
