# ET-Applications

**Platform:** Google Earth Engine (GEE) + Python 3.x  
**Output:** Multi-band GeoTIFF rasters at 30 m resolution for any tehsil in India  
**Models & Training Data:** [Pan_India_Downscaled_Evapotranspiration on GitHub](https://github.com/Shuvam-Chakraborty/Pan_India_Downscaled_Evapotranspiration)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Prerequisites and Access Requirements](#3-prerequisites-and-access-requirements)
4. [Environment Setup](#4-environment-setup)
5. [Step 1 - Create the Tehsil Boundary Asset in GEE](#5-step-1---create-the-tehsil-boundary-asset-in-gee)
6. [Step 2 - Configure config.yaml](#6-step-2---configure-configyaml)
7. [Step 3 - Authenticate Earth Engine in Python](#7-step-3---authenticate-earth-engine-in-python)
8. [Step 4 - Run the Python Application](#8-step-4---run-the-python-application)
9. [Output Modes and Output Files](#9-output-modes-and-output-files)
10. [Output GeoTIFF Band Reference](#10-output-geotiff-band-reference)
11. [config.yaml Parameter Reference](#11-configyaml-parameter-reference)
12. [AEZ Model Assets](#12-aez-model-assets)
13. [Python Function Reference](#13-python-function-reference)
14. [Using the GEE Visualizer Scripts](#14-using-the-gee-visualizer-scripts)
15. [Changing to a Different Tehsil](#15-changing-to-a-different-tehsil)

---

## 1. Project Overview

ET-Applications downscales MODIS-resolution evapotranspiration data to the native 30 m Landsat 8 grid using pre-trained Random Forest models, one model per Agro-Ecological Zone (AEZ) covering all of India.

The **feature layers collected/calculated** in this workflow are:

- Actual Evapotranspiration (AET) + annual total AET - 13 bands
- Potential Evapotranspiration (PET) from MODIS MOD16A2 + annual total PET - 13 bands
- Gross Primary Productivity (GPP) + annual mean - 13 bands

The **derived applications created from those layers** are:

- Relative Water Deficit Index (RWDI) per month + annual mean - 13 bands
- Crop coefficient proxy Kc (AET/PET) per month + annual mean - 13 bands
- Water Use Efficiency (WUE = GPP/AET) per month + annual mean - 13 bands

The Random Forest models and their training data are fully documented and openly available at:  
**https://github.com/Shuvam-Chakraborty/Pan_India_Downscaled_Evapotranspiration**

---

## 2. Repository Structure

```text
ET_Applications/
|-- et_applications.py
|-- config.yaml
|-- requirements.txt
|-- 1_Check_Tehsil.js
|-- 2_Generate_Tehsil_Boundary.js
|-- gee_visualizers/
|   |-- aet_viewer.js
|   |-- pet_viewer.js
|   |-- gpp_viewer.js
|   |-- rwdi_viewer.js
|   |-- kc_viewer.js
|   |-- wue_viewer.js
|-- README.md
`-- results/
```

**Workflow at a glance:**

```text
        GEE Code Editor                                    Local Machine
-------------------------------                   -------------------------------
1_Check_Tehsil.js
        |
2_Generate_Tehsil_Boundary.js
        |
GEE tehsil asset created
        |----------------------------------> copy tehsil asset path into config.yaml
                                                                  |
                                                    python3 et_applications.py
                                                                  |
                                                   results/ GeoTIFF + PNG files
upload GeoTIFFs to GEE assets <-----------------------------------|
        |
open gee_visualizers/*.js
        |
visualize different bands in GEE
```

---

## 3. Prerequisites and Access Requirements

### 3.1 Google Earth Engine Account

- A Google Earth Engine account is required. Register at: https://code.earthengine.google.com
- You must have a GEE Cloud Project set up. If you do not have one, create a project at https://console.cloud.google.com and enable the Earth Engine API.
- Note your Cloud Project ID (for example, `shuvamdownscalinget`). This goes into `config.yaml` and the GEE scripts.

### 3.2 GEE Asset Access

Three GEE assets are required before running the Python script:

| Asset | Path in config.yaml | Who Owns It | What You Need to Do |
|---|---|---|---|
| Tehsil boundary asset | `assets.tehsil_asset` | User | Created automatically by Script 2 |
| RF model asset | `assets.model_aez` | Publicly shared | See Section 12 for the AEZ model paths |
| India block boundaries | `users/mtpictd/india_block_boundaries` | Publicly shared | Used by the GEE setup scripts |

### 3.3 Python Requirements

- Python 3.9 or higher
- pip

---

## 4. Environment Setup

### 4.1 Install Python Dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 4.2 Verify Installation

```bash
python3 -c "import ee, numpy, rasterio, requests, yaml; print('All packages OK')"
```

---

## 5. Step 1 - Create the Tehsil Boundary Asset in GEE

This step is performed entirely in the GEE Code Editor at https://code.earthengine.google.com. Run the two JavaScript scripts in sequence.

### 5.1 Run Script 1: 1_Check_Tehsil.js

1. Open the GEE Code Editor and create a new script.
2. Paste the full contents of `1_Check_Tehsil.js`.
3. Edit the `CONFIG` block:

```javascript
var CONFIG = {
  state     : 'UTTAR PRADESH',
  district  : 'BANDA',
  tehsil    : 'TELYANI',
  geeProject: 'shuvamdownscalinget'
};
```

4. Click `Run`.

### 5.2 Reading the Check Report

- If the tehsil is found, the console prints the matched names, area, centroid, and bounding box, and the map displays the tehsil.
- If nothing is found, the console lists valid names for the district or state. Use those exact values in uppercase.

Do not proceed to Script 2 until the console shows a confirmed match.

### 5.3 Run Script 2: 2_Generate_Tehsil_Boundary.js

1. Create a new script and paste the full contents of `2_Generate_Tehsil_Boundary.js`.
2. Copy the exact same `CONFIG` block from Script 1.
3. Click `Run`.
4. The console prints the full asset path:

```text
projects/<your-project-id>/assets/tehsil_<state>__<district>__<tehsil>
```

Example:

```text
projects/shuvamdownscalinget/assets/tehsil_uttar_pradesh__banda__telyani
```

5. Switch to the `Tasks` tab and click `RUN` next to the export task.
6. Wait for the task to show `COMPLETED`.

### 5.4 Copy the Asset Path

Once the task completes, copy the full asset path from the console and paste it into `config.yaml`.

---

## 6. Step 2 - Configure config.yaml

Open `config.yaml` in a text editor and update the following fields:

### Required Fields

```yaml
gee_project: "shuvamdownscalinget"

tehsil:
  state    : "UTTAR PRADESH"
  district : "BANDA"
  name     : "TELYANI"

assets:
  tehsil_asset : "projects/shuvamdownscalinget/assets/tehsil_uttar_pradesh__banda__telyani"
  model_aez    : "projects/shuvamdownscalinget/assets/rf_aez4_final"

time:
  year : 2022
```

### Optional Fields

```yaml
application: "all"

compute:
  chunk_size: 3000

output:
  directory: "./results"
  plot      : true

sample_point:
  lon: 80.7314
  lat: 25.9735
```

---

## 7. Step 3 - Authenticate Earth Engine in Python

Before running the script for the first time, and whenever your token expires, authenticate the Earth Engine Python client.

### 7.1 Run Authentication

```bash
earthengine authenticate --auth_mode=localhost
```

### 7.2 Follow the Authentication Steps

1. The command prints a URL and starts a local callback.
2. Open the URL in a browser and sign in with the Google account linked to your GEE Cloud Project.
3. Grant the requested permissions.
4. Return to the terminal after the browser confirms success.

### 7.3 Set the Active GEE Cloud Project

```bash
earthengine set_project <your-project-id>
```

Replace `<your-project-id>` with your own GEE Cloud Project ID.

### 7.4 Verify Authentication

```bash
python3 -c "import ee; ee.Initialize(project='<your-project-id>'); print('EE authenticated OK')"
```

Replace `<your-project-id>` with your actual GEE Cloud Project ID.

---

## 8. Step 4 - Run the Python Application

With `config.yaml` set and authentication complete, run the script from the directory containing both `et_applications.py` and `config.yaml`:

```bash
python3 et_applications.py
```

This reads all settings from `config.yaml`. The default `application: "all"` generates the three feature layers plus the three derived applications in a single 60-band GEE download pass.

### Overriding Config Values from the Command Line

Any value in `config.yaml` can be overridden with a CLI flag. Command-line arguments always take precedence over the config file.

```bash
# Run only the AET feature layer
python3 et_applications.py --application aet

# Run only the crop coefficient proxy
python3 et_applications.py --application kc

# Run only GPP
python3 et_applications.py --application gpp

# Run only WUE
python3 et_applications.py --application wue

# Change the output year
python3 et_applications.py --year 2021

# Use a different tehsil asset
python3 et_applications.py --tehsil-asset "projects/myproject/assets/tehsil_x"

# Enable plots even if config.yaml has plot: false
python3 et_applications.py --plot

# Extract a timeseries plot for a single pixel by coordinates
python3 et_applications.py --sample-lon 80.34 --sample-lat 25.12

# Use a different config file
python3 et_applications.py --config /path/to/other_config.yaml
```

Full list of CLI flags:

| Flag | Type | Description |
|---|---|---|
| `--config` | path | Path to a YAML config file (default: `./config.yaml`) |
| `--application` | string | Output mode (see Section 9) |
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

## 9. Output Modes and Output Files

Set `application` in `config.yaml` or use `--application` on the command line. The CLI exposes the three feature layers (`aet`, `pet`, `gpp`) and the three derived applications (`rwdi`, `kc`, `wue`).

| Mode | Output File | Bands | Description |
|---|---|---|---|
| `all` | All six primary outputs | - | Recommended. Builds the three feature layers once, downloads once in a single 60-band pass, and writes the three feature layers plus the three derived applications with pixel-perfect spatial alignment. |
| `aet` | `aet_<TEHSIL>_<YEAR>.tif` | 13 | Feature layer. Bands 1-12: monthly mean daily AET (mm/day); band 13: annual total AET (mm/yr) |
| `pet` | `pet_<TEHSIL>_<YEAR>.tif` | 13 | Feature layer. Bands 1-12: monthly mean daily PET (mm/day); band 13: annual total PET (mm/yr) |
| `gpp` | `gpp_<TEHSIL>_<YEAR>.tif` | 13 | Feature layer. Monthly GPP (g C/m2/day) + annual mean |
| `rwdi` | `rwdi_<TEHSIL>_<YEAR>.tif` | 13 | Derived application. Monthly RWDI (%) + annual mean |
| `kc` | `kc_<TEHSIL>_<YEAR>.tif` | 13 | Derived application. Monthly crop coefficient proxy (AET/PET) + annual mean |
| `wue` | `wue_<TEHSIL>_<YEAR>.tif` | 13 | Derived application. Monthly WUE (g C/kg H2O) + annual mean |

When `plot: true`, each output also saves a PNG chart alongside its GeoTIFF.

---

## 10. Output GeoTIFF Band Reference

### `aet_<TEHSIL>_<YEAR>.tif` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1 | `ET_Jan_daily_mm` - January mean daily AET | mm/day |
| 2 | `ET_Feb_daily_mm` - February mean daily AET | mm/day |
| ... | ... | ... |
| 12 | `ET_Dec_daily_mm` - December mean daily AET | mm/day |
| 13 | `ET_annual_mm` - Annual total AET = sum(monthly daily AET x days in month) | mm/yr |

TIFF metadata tag `gap_filled_months` lists any months filled by plus/minus 60-day temporal interpolation.

### `pet_<TEHSIL>_<YEAR>.tif` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1 | `PET_Jan_daily_mm` - January mean daily PET | mm/day |
| ... | ... | ... |
| 12 | `PET_Dec_daily_mm` - December mean daily PET | mm/day |
| 13 | `PET_annual_mm` - Pixel-wise annual total PET = sum(monthly daily PET x days in month) | mm/yr |

### `rwdi_<TEHSIL>_<YEAR>.tif` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `RWDI_Jan` ... `RWDI_Dec` - RWDI = (1 - AET/PET) x 100 | % |
| 13 | `RWDI_annual` - Pixel-wise mean RWDI across 12 months | % |

### `kc_<TEHSIL>_<YEAR>.tif` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `KC_Jan` ... `KC_Dec` - Crop coefficient proxy = AET/PET | ratio |
| 13 | `KC_annual` - Pixel-wise mean Kc across 12 months | ratio |

### `gpp_<TEHSIL>_<YEAR>.tif` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `GPP_Jan_gC_m2_day` ... `GPP_Dec_gC_m2_day` - Monthly mean daily GPP | g C/m2/day |
| 13 | `GPP_annual_mean` - Pixel-wise mean GPP across 12 months | g C/m2/day |

### `wue_<TEHSIL>_<YEAR>.tif` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `WUE_Jan_gC_per_kgH2O` ... `WUE_Dec_gC_per_kgH2O` - WUE = GPP/AET | g C/kg H2O |
| 13 | `WUE_annual_mean` - Pixel-wise mean WUE across 12 months | g C/kg H2O |

---

## 11. config.yaml Parameter Reference

| Section | Key | Type | Description |
|---|---|---|---|
| (root) | `gee_project` | string | GEE Cloud Project ID |
| `tehsil` | `state` | string | State name used for output file naming |
| `tehsil` | `district` | string | District name used for output file naming |
| `tehsil` | `name` | string | Tehsil name used for output file naming |
| `assets` | `tehsil_asset` | string | Full GEE FeatureCollection path for the tehsil boundary |
| `assets` | `model_aez` | string | Full GEE asset path for the Random Forest AEZ model |
| `modis` | `collection` | string | MODIS ImageCollection ID |
| `time` | `year` | integer | Calendar year to process |
| `compute` | `chunk_size` | integer | Approximate pixels per GEE download tile |
| (root) | `application` | string | Mode: `all`, `aet`, `pet`, `rwdi`, `kc`, `gpp`, `wue` |
| `output` | `directory` | path | Directory where GeoTIFF and PNG files are written |
| `output` | `plot` | boolean | Whether to generate matplotlib plots |
| `sample_point` | `lon` | float | Longitude for single-pixel timeseries plot |
| `sample_point` | `lat` | float | Latitude for single-pixel timeseries plot |

---

## 12. AEZ Model Assets

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

For full details on model training, input features, validation results, and AEZ delineation, see:  
**https://github.com/Shuvam-Chakraborty/Pan_India_Downscaled_Evapotranspiration**

---

## 13. Python Function Reference

This section explains the major functions in `et_applications.py` and where they fit in the workflow.

### Configuration

**`load_config(path)`**  
Reads `config.yaml` and returns a Python dictionary of settings.

**`merge_args(cfg, args)`**  
Merges config values with any CLI overrides.

### Earth Engine Initialisation

**`init_ee(project)`**  
Initialises the GEE Python client.

**`load_tehsil(asset)`**  
Loads the tehsil boundary FeatureCollection from GEE and extracts its geometry.

### GEE Image Building

**`build_classifier(model_path)`**  
Loads the pre-trained Random Forest ensemble from a GEE asset.

**`calc_landsat_indices(img)`**  
Computes the Landsat-derived spectral indices required by the RF model.

**`predict_daily_et(ls_img, region, classifier)`**  
Builds the feature stack for one Landsat scene and predicts daily ET.

**`build_aet_stack(region, classifier, year)`**  
Builds the 12-band monthly AET stack and tracks gap-filled months.

**`build_pet_stack(region, year, modis_col_id, proj)`**  
Builds the 12-band monthly PET stack on the AET-aligned grid.

**`build_rwdi_image(aet_stack, pet_stack)`**  
Computes monthly RWDI from AET and PET.

**`build_kc_image(aet_stack, pet_stack)`**  
Computes monthly Kc from AET and PET.

**`build_gpp_stack(region, year, proj)`**  
Builds the 12-band monthly GPP stack using the LUE framework.

**`_build_bplut_image(lc_img)`**  
Maps MODIS land-cover classes to the BPLUT parameter layers used by GPP.

**`build_combined_image(aet_stack, pet_stack, gpp_stack, year)`**  
Stacks the feature and derived layers into one 60-band GEE image for `all` mode.

### Download Infrastructure

**`_download_image_as_geotiff(img, region, chunk_size, label)`**  
Downloads tiled GeoTIFF chunks from GEE.

**`_merge_tiles(tile_paths, nodata)`**  
Merges downloaded tiles into one array and raster profile.

**`_quiet_gdal()`**  
Suppresses low-level GDAL/libtiff warning noise during raster operations.

**`_save_geotiff(arr, profile, output_path, band_names, metadata)`**  
Writes the final multi-band GeoTIFF with band descriptions and metadata.

### Array Utilities

**`_scale_nodata(arr, scale, nodata)`**  
Applies a scale factor while preserving NoData.

**`_annual_mean_band(monthly_12, nodata)`**  
Computes a pixel-wise annual mean from 12 monthly bands.

**`_annual_total_band(monthly_12, year, nodata)`**  
Computes a pixel-wise annual total from 12 monthly mean-daily bands.

**`compute_wue_numpy(gpp_monthly, aet_monthly_raw, nodata)`**  
Computes monthly WUE in Python as `GPP / AET`.

**`_band_stats(arr, nodata)`**  
Computes per-band mean and standard deviation across valid pixels.

**`_print_stats(label, arr, nodata)`**  
Prints a summary of valid pixel count and basic statistics.

### Output Runners

**`run_aet(cfg, region, ...)`**  
Builds or reuses the AET feature layer, appends annual total AET as band 13, and saves `aet_<TEHSIL>_<YEAR>.tif`.

**`run_pet(cfg, region, ...)`**  
Builds or reuses the PET feature layer, appends annual total PET as band 13, and saves `pet_<TEHSIL>_<YEAR>.tif`.

**`run_rwdi(cfg, region, ...)`**  
Builds RWDI, appends annual mean RWDI as band 13, and saves `rwdi_<TEHSIL>_<YEAR>.tif`.

**`run_kc(cfg, region, ...)`**  
Builds Kc, appends annual mean Kc as band 13, and saves `kc_<TEHSIL>_<YEAR>.tif`.

**`run_gpp(cfg, region, ...)`**  
Builds GPP, appends annual mean GPP as band 13, and saves `gpp_<TEHSIL>_<YEAR>.tif`.

**`run_wue(cfg, region, ...)`**  
Builds WUE, appends annual mean WUE as band 13, and saves `wue_<TEHSIL>_<YEAR>.tif`.

**`run_all(cfg, region)`**  
Builds the three feature layers once, downloads the combined image once, and writes all six outputs with aligned pixels.

### Sample Point

**`run_sample_timeseries(output_paths, cfg)`**  
Extracts the nearest pixel from saved GeoTIFFs and generates the optional six-panel sample plot.

### Plot Functions

| Function | Input shape | Output |
|---|---|---|
| `_plot_aet` | `(12, H, W)` mm/day | AET line chart with plus/minus 1 std fill |
| `_plot_pet` | `(12, H, W)` mm/day | PET line chart with plus/minus 1 std fill |
| `_plot_rwdi` | `(12, H, W)` % | RWDI monthly line chart |
| `_plot_kc` | `(12, H, W)` ratio | Kc monthly line chart |
| `_plot_gpp` | `(12, H, W)` g C/m2/day | GPP line chart with plus/minus 1 std fill |
| `_plot_wue` | `(12, H, W)` g C/kg H2O | WUE line chart with plus/minus 1 std fill |
| `_plot_sample_timeseries` | 6 x `(12,)` arrays | Six-panel timeseries for a single pixel |

---

## 14. Using the GEE Visualizer Scripts

The `gee_visualizers/` folder contains one Earth Engine Code Editor script for each output:

- `aet_viewer.js`
- `pet_viewer.js`
- `gpp_viewer.js`
- `rwdi_viewer.js`
- `kc_viewer.js`
- `wue_viewer.js`

Use them as follows:

1. Generate the GeoTIFF locally with `python3 et_applications.py`.
2. Upload the required GeoTIFF from `results/` to your Earth Engine assets.
3. Open the matching script from `gee_visualizers/` in the GEE Code Editor.
4. Replace the `ASSET_ID` value at the top of the script with your uploaded asset path.
5. Run the script.
6. Use the dropdown in the viewer panel to switch between the 12 monthly bands and band 13.

When a GeoTIFF is uploaded to GEE, Earth Engine names the bands `b1` to `b13`. The visualizer scripts are already written to use those band names.

Band 13 meaning:

- `aet` and `pet`: annual total
- `gpp`, `rwdi`, `kc`, and `wue`: annual mean

---

## 15. Changing to a Different Tehsil

To process a new tehsil, repeat the following steps. `et_applications.py` does not need to be modified.

**Step 1 - Run Script 1 in GEE**

Edit the `CONFIG` block in `1_Check_Tehsil.js`:

```javascript
var CONFIG = {
  state     : 'NEW STATE NAME',
  district  : 'NEW DISTRICT NAME',
  tehsil    : 'NEW TEHSIL NAME',
  geeProject: 'your-gee-project-id'
};
```

Click `Run` and confirm the correct tehsil appears in the map and console.

**Step 2 - Export the new asset with Script 2**

Copy the same `CONFIG` into `2_Generate_Tehsil_Boundary.js`, click `Run`, then start the export from the `Tasks` tab. Wait until it reaches `COMPLETED` and copy the asset path from the console.

**Step 3 - Update config.yaml**

```yaml
tehsil:
  state    : "NEW STATE NAME"
  district : "NEW DISTRICT NAME"
  name     : "NEW TEHSIL NAME"

assets:
  tehsil_asset : "projects/<your-project>/assets/tehsil_<state>__<district>__<tehsil>"
  model_aez    : "projects/shuvamdownscalinget/assets/rf_aez<N>_final"

time:
  year : 2022
```

**Step 4 - Run**

```bash
python3 et_applications.py
```

Output files are named using the tehsil name and year, so previous results are not overwritten.
