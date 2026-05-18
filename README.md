# ET-Applications

**Platform:** Google Earth Engine (GEE) + Python 3.x (Preferably 3.11 or later version)  
**Output:** 13-band Earth Engine image assets at 30 m resolution for any tehsil in India using actual ET  
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
9. [Output Modes and Export Assets](#9-output-modes-and-export-assets)
10. [Output Asset Band Reference](#10-output-asset-band-reference)
11. [config.yaml Parameter Reference](#11-configyaml-parameter-reference)
12. [AEZ Model Assets](#12-aez-model-assets)
13. [Python Function Reference](#13-python-function-reference)
14. [Using the GEE Visualizer Scripts](#14-using-the-gee-visualizer-scripts)
15. [Changing to a Different Tehsil](#15-changing-to-a-different-tehsil)

---

## 1. Project Overview

ET-Applications downscales MODIS-resolution evapotranspiration data to the native
30 m Landsat 8 grid using pre-trained Random Forest models, one model per
Agro-Ecological Zone (AEZ) covering all of India.

The **feature layers collected/calculated** in this workflow are:

- Actual Evapotranspiration (AET) + annual total AET - 13 bands
- Potential Evapotranspiration (PET) from MODIS MOD16A2 + annual total PET - 13 bands
- Gross Primary Productivity (GPP) + annual mean - 13 bands

The **derived applications created from those layers** are:

- Relative Water Deficit Index (RWDI) per month + annual mean - 13 bands
- Crop coefficient proxy Kc (AET/PET) per month + annual mean - 13 bands
- Water Use Efficiency (WUE = GPP/AET) per month + annual mean - 13 bands

The Random Forest models and their training data are fully documented and openly
available at:  
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
|   `-- wue_viewer.js
`-- README.md
```

**Workflow at a glance:**

```text
        GEE Code Editor                                             Local Machine
-------------------------------                            -------------------------------
1_Check_Tehsil.js
        |
2_Generate_Tehsil_Boundary.js
        |
GEE tehsil asset created
        |--------------------------------------------> copy tehsil asset path into config.yaml
                                                                           |
                                                                python3 et_applications.py
13-band GEE image assets exported under asset_root <-----------------------|
        |
open gee_visualizers/*.js in GEE
        |
visualize the exported assets directly
```

---

## 3. Prerequisites and Access Requirements

### 3.1 Google Earth Engine Account

- A Google Earth Engine account is required. Register at: https://code.earthengine.google.com
- You must have a GEE Cloud Project set up. If you do not have one, create a project at https://console.cloud.google.com and enable the Earth Engine API.
- Note your Cloud Project ID, for example `shuvamdownscalinget`. This goes into `config.yaml` and the GEE scripts.

### 3.2 GEE Asset Access

Three GEE assets are required before running the Python script:

| Asset | Path in config.yaml | Who Owns It | What You Need to Do |
|---|---|---|---|
| Tehsil boundary asset | `assets.tehsil_asset` | User | Created automatically by Script 2 |
| RF model asset | `assets.model_aez` | Publicly shared | See Section 12 for the AEZ model paths |
| India block boundaries | `users/mtpictd/india_block_boundaries` | Publicly shared | Used by the GEE setup scripts |

### 3.3 Python Requirements

- Python 3.11 or higher
- `pip`

---

## 4. Environment Setup

### 4.1 Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4.2 Verify Installation

```bash
python3 -c "import ee, yaml; print('Core packages OK')"
```

---

## 5. Step 1 - Create the Tehsil Boundary Asset in GEE

This step is performed entirely in the GEE Code Editor at https://code.earthengine.google.com.
Run the two JavaScript scripts in sequence.

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

Open `config.yaml` and update the following fields.

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

modis:
  collection: "MODIS/061/MOD16A2GF"

export:
  asset_root: "projects/shuvamdownscalinget/assets"
  overwrite: true
  wait_for_tasks: true
  poll_interval_seconds: 30
```

---

## 7. Step 3 - Authenticate Earth Engine in Python

Before running the script for the first time, and whenever your token expires,
authenticate the Earth Engine Python client.

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

---

## 8. Step 4 - Run the Python Application

With `config.yaml` set and authentication complete, run the script from the
directory containing both `et_applications.py` and `config.yaml`:

```bash
python3 et_applications.py
```

This reads all settings from `config.yaml`. The default `application: "all"`
builds the three feature layers plus the three derived applications and exports
them directly to Earth Engine assets under `export.asset_root`.

### Overriding Config Values from the Command Line

Any value in `config.yaml` can be overridden with a CLI flag. Command-line
arguments always take precedence over the config file.

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

# Export into a different asset folder
python3 et_applications.py --asset-root "projects/myproject/assets"

# Force replacement of existing output assets
python3 et_applications.py --overwrite-assets

# Start export tasks and return immediately without polling
python3 et_applications.py --no-wait-exports

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
| `--asset-root` | string | Parent GEE asset path where exports will be created |
| `--overwrite-assets` | flag | Delete an existing target asset before exporting |
| `--no-wait-exports` | flag | Start export tasks and exit without polling |
| `--gee-project` | string | GEE Cloud Project ID |
| `--tehsil-name` | string | Label for output asset naming |

---

## 9. Output Modes and Export Assets

Set `application` in `config.yaml` or use `--application` on the command line.
The CLI exposes the three feature layers (`aet`, `pet`, `gpp`) and the three
derived applications (`rwdi`, `kc`, `wue`).

| Mode | Exported Asset | Bands | Description |
|---|---|---|---|
| `all` | All six primary assets under `asset_root` | - | Recommended. Builds the three feature layers once and exports `aet`, `pet`, `rwdi`, `kc`, `gpp`, and `wue` with aligned pixels. |
| `aet` | `<asset_root>/aet_<TEHSIL>_<YEAR>` | 13 | Feature layer. Bands 1-12: monthly mean daily AET (mm/day); band 13: annual total AET (mm/yr) |
| `pet` | `<asset_root>/pet_<TEHSIL>_<YEAR>` | 13 | Feature layer. Bands 1-12: monthly mean daily PET (mm/day); band 13: annual total PET (mm/yr) |
| `gpp` | `<asset_root>/gpp_<TEHSIL>_<YEAR>` | 13 | Feature layer. Monthly GPP (g C/m2/day) + annual mean |
| `rwdi` | `<asset_root>/rwdi_<TEHSIL>_<YEAR>` | 13 | Derived application. Monthly RWDI (%) + annual mean |
| `kc` | `<asset_root>/kc_<TEHSIL>_<YEAR>` | 13 | Derived application. Monthly crop coefficient proxy (AET/PET) + annual mean |
| `wue` | `<asset_root>/wue_<TEHSIL>_<YEAR>` | 13 | Derived application. Monthly WUE (g C/kg H2O) + annual mean |

Each exported image keeps the band order expected by the GEE viewer scripts:
`b1` to `b12` for monthly layers and `b13` for the annual layer.

Each asset also carries image properties such as band descriptions,
`valid_pixel_count`, formula metadata, and source metadata.

---

## 10. Output Asset Band Reference

### `aet_<TEHSIL>_<YEAR>` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1 | `ET_Jan_daily_mm` - January mean daily AET | mm/day |
| 2 | `ET_Feb_daily_mm` - February mean daily AET | mm/day |
| ... | ... | ... |
| 12 | `ET_Dec_daily_mm` - December mean daily AET | mm/day |
| 13 | `ET_annual_mm` - Annual total AET = sum(monthly daily AET x days in month) | mm/yr |

### `pet_<TEHSIL>_<YEAR>` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1 | `PET_Jan_daily_mm` - January mean daily PET | mm/day |
| ... | ... | ... |
| 12 | `PET_Dec_daily_mm` - December mean daily PET | mm/day |
| 13 | `PET_annual_mm` - Pixel-wise annual total PET = sum(monthly daily PET x days in month) | mm/yr |

### `rwdi_<TEHSIL>_<YEAR>` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `RWDI_Jan` ... `RWDI_Dec` - RWDI = (1 - AET/PET) x 100 | % |
| 13 | `RWDI_annual` - Pixel-wise mean RWDI across 12 months | % |

### `kc_<TEHSIL>_<YEAR>` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `KC_Jan` ... `KC_Dec` - Crop coefficient proxy = AET/PET | ratio |
| 13 | `KC_annual` - Pixel-wise mean Kc across 12 months | ratio |

### `gpp_<TEHSIL>_<YEAR>` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `GPP_Jan_gC_m2_day` ... `GPP_Dec_gC_m2_day` - Monthly mean daily GPP | g C/m2/day |
| 13 | `GPP_annual_mean` - Pixel-wise mean GPP across 12 months | g C/m2/day |

### `wue_<TEHSIL>_<YEAR>` - 13 bands

| Band | Description | Unit |
|---|---|---|
| 1-12 | `WUE_Jan_gC_per_kgH2O` ... `WUE_Dec_gC_per_kgH2O` - WUE = GPP/AET | g C/kg H2O |
| 13 | `WUE_annual_mean` - Pixel-wise mean WUE across 12 months | g C/kg H2O |

---

## 11. config.yaml Parameter Reference

| Section | Key | Type | Description |
|---|---|---|---|
| (root) | `gee_project` | string | GEE Cloud Project ID |
| `tehsil` | `state` | string | State name used for labelling |
| `tehsil` | `district` | string | District name used for labelling |
| `tehsil` | `name` | string | Tehsil name used for exported asset naming |
| `assets` | `tehsil_asset` | string | Full GEE FeatureCollection path for the tehsil boundary |
| `assets` | `model_aez` | string | Full GEE asset path for the Random Forest AEZ model |
| `modis` | `collection` | string | MODIS ImageCollection ID |
| `time` | `year` | integer | Calendar year to process |
| (root) | `application` | string | Mode: `all`, `aet`, `pet`, `rwdi`, `kc`, `gpp`, `wue` |
| `export` | `asset_root` | string | Parent GEE asset path where output images will be created |
| `export` | `overwrite` | boolean | Whether to delete and recreate an existing target asset |
| `export` | `wait_for_tasks` | boolean | Whether to poll GEE export tasks until completion |
| `export` | `poll_interval_seconds` | integer | Delay between export task status checks |

---

## 12. AEZ Model Assets

The downscaling pipeline uses one Random Forest model per Agro-Ecological Zone
(AEZ). All 19 models are publicly accessible as GEE assets under the project
`shuvamdownscalinget`. Set the appropriate model in `config.yaml` under
`assets.model_aez` based on the AEZ your tehsil falls in.

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

For full details on model training, input features, validation results, and AEZ
delineation, see:  
**https://github.com/Shuvam-Chakraborty/Pan_India_Downscaled_Evapotranspiration**

---

## 13. Python Function Reference

This section explains the major functions in `et_applications.py` and where they
fit in the workflow.

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
Builds the 12-band monthly AET stack.

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

**`build_wue_image(aet_stack, gpp_stack)`**  
Computes monthly WUE from GPP and AET.

### Annual Aggregation Helpers

**`_ee_annual_total_band(monthly_stack, prefix, year, band_name)`**  
Computes a pixel-wise annual total inside Earth Engine from 12 monthly
mean-daily bands.

**`_ee_annual_mean_band(monthly_stack, prefix, band_name)`**  
Computes a pixel-wise annual mean inside Earth Engine from 12 monthly bands.

### Asset Export Helpers

**`_build_asset_id(cfg, label)`**  
Builds the final asset path under `export.asset_root`.

**`_prepare_asset_target(asset_id, overwrite)`**  
Checks whether the destination asset exists and optionally deletes it before
export.

**`_finalize_export_image(monthly_stack, annual_band, region, metadata, band_descriptions, ...)`**  
Builds the final 13-band image, applies the common tehsil mask, and attaches
metadata.

**`_export_product_asset(label, display_name, image, cfg, export_region, proj_info, stats_label, ...)`**  
Prints annual statistics and launches the Earth Engine asset export task.

**`_wait_for_tasks(task_specs, poll_seconds)`**  
Polls Earth Engine export tasks until completion when `wait_for_tasks: true`.

### Output Runners

**`run_aet(cfg, region, ...)`**  
Builds or reuses the AET feature layer, appends annual total AET as band 13,
and exports `aet_<TEHSIL>_<YEAR>`.

**`run_pet(cfg, region, ...)`**  
Builds or reuses the PET feature layer, appends annual total PET as band 13,
and exports `pet_<TEHSIL>_<YEAR>`.

**`run_rwdi(cfg, region, ...)`**  
Builds RWDI, appends annual mean RWDI as band 13, and exports
`rwdi_<TEHSIL>_<YEAR>`.

**`run_kc(cfg, region, ...)`**  
Builds Kc, appends annual mean Kc as band 13, and exports `kc_<TEHSIL>_<YEAR>`.

**`run_gpp(cfg, region, ...)`**  
Builds GPP, appends annual mean GPP as band 13, and exports
`gpp_<TEHSIL>_<YEAR>`.

**`run_wue(cfg, region, ...)`**  
Builds WUE, appends annual mean WUE as band 13, and exports
`wue_<TEHSIL>_<YEAR>`.

**`run_all(cfg, region)`**  
Builds the three feature layers once and exports all six aligned assets in one
run.

---

## 14. Using the GEE Visualizer Scripts

The `gee_visualizers/` folder contains one Earth Engine Code Editor script for
each output:

- `aet_viewer.js`
- `pet_viewer.js`
- `gpp_viewer.js`
- `rwdi_viewer.js`
- `kc_viewer.js`
- `wue_viewer.js`

Use them as follows:

1. Run `python3 et_applications.py` so the required Earth Engine image asset is exported.
2. Open the matching script from `gee_visualizers/` in the GEE Code Editor.
3. Set the two variables in the `USER CONFIG` block at the top of the script:

```javascript
var ASSET_ID     = 'projects/<your-project>/assets/<exported-asset-name>';
var TEHSIL_ASSET = 'projects/<your-project>/assets/tehsil_<state>__<district>__<tehsil>';
```

`ASSET_ID` is the path to the exported 13-band Earth Engine image asset, for
example `projects/<your-project>/assets/aet_<tehsil>_<year>`. `TEHSIL_ASSET` is
the same tehsil boundary asset created in Step 1 - its path is already
recorded in `config.yaml` under `assets.tehsil_asset`.

4. Click `Run`.

Each of the 13 bands is added as a separate named layer (`AET - Jan`,
`AET - Feb`, ..., `AET - Annual total`). Use GEE's native **Layers** panel
(top-right of the map) to toggle visibility between months. Only the first band
(January) is visible on load. The tehsil boundary is drawn as a black outline
over all layers.

Colour stretch min/max values are computed automatically from the actual pixel
data in the exported asset - no manual adjustment is needed.

The exported assets already use the band names `b1` to `b13`. The visualizer
scripts are written to use those names directly.

Band 13 meaning:

- `aet` and `pet`: annual total
- `gpp`, `rwdi`, `kc`, and `wue`: annual mean

---

## 15. Changing to a Different Tehsil

To process a new tehsil, repeat the following steps. `et_applications.py` does
not need to be modified.

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

Copy the same `CONFIG` into `2_Generate_Tehsil_Boundary.js`, click `Run`, then
start the export from the `Tasks` tab. Wait until it reaches `COMPLETED` and
copy the asset path from the console.

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

export:
  asset_root: "projects/<your-project>/assets"
```

**Step 4 - Run**

```bash
python3 et_applications.py
```

Exported asset names are built from the tehsil name and year. If
`export.overwrite: true` is set, an existing asset with the same name will be
replaced.
