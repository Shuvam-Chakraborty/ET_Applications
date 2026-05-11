#!/usr/bin/env python3
"""
ET-Applications - Pan-India ET Downscaling CLI  (GeoTIFF Raster Edition)
=========================================================================
Each output mode writes its own multi-band GeoTIFF (one band per month /
derived product). All GeoTIFFs are spatially aligned at 30 m resolution
and can be opened directly in QGIS, ArcGIS, or any rasterio/GDAL workflow.

  PIXEL CONSISTENCY GUARANTEE
  ----------------------------
  All GeoTIFFs produced by the same tehsil + year combination share the
  same bounding box, CRS, and 30 m pixel grid so they are immediately
  overlay-compatible. The AET (Landsat 8) pixel grid drives the spatial
  reference; MODIS-derived bands are resampled to this grid before
  download. Pixels outside the tehsil boundary are written as NoData
  (-9999).

  Band descriptions are embedded in every GeoTIFF so band purpose is
  visible without consulting external documentation. Gap-fill status
  and units are stored as TIFF metadata tags.

  Core Layers + Derived Applications
  ----------------------------------
  monthly_et    ->  aet_<TEHSIL>_<YEAR>.tif           13 bands  (12 monthly + annual total)
  pet           ->  pet_<TEHSIL>_<YEAR>.tif            13 bands  (12 monthly + annual total)
  gpp           ->  gpp_<TEHSIL>_<YEAR>.tif            13 bands  (12 monthly + annual mean)
  rwdi          ->  rwdi_<TEHSIL>_<YEAR>.tif           13 bands  (12 monthly + annual mean)
  kc            ->  kc_<TEHSIL>_<YEAR>.tif             13 bands  (12 monthly + annual mean)
  wue           ->  wue_<TEHSIL>_<YEAR>.tif            13 bands  (12 monthly + annual mean)
  all           ->  three feature layers + three derived applications

  GPP Method (Light Use Efficiency)
  ----------------------------------
  GPP = PAR x fAPAR x eps
    PAR    = 0.45 x SWdown_f_tavg (GLDAS W/m2 -> MJ/m2/day)
    fAPAR  = max(0, 1.24 x NDVI - 0.168) from Landsat 8
    eps    = eps_max x TMIN_scalar x VPD_scalar
    eps_max, TMIN/VPD thresholds from MOD17 BPLUT keyed on MCD12Q1 land cover

  WUE Formula
  -----------
  WUE = GPP / AET   (g C m-2 day-1) / (mm day-1) = g C / kg H2O
  where AET is the Landsat + GLDAS RF-downscaled actual ET.

  Quick-start
  -----------
  python3 et_applications.py                          # all from config.yaml
  python3 et_applications.py --application monthly_et
  python3 et_applications.py --application kc
  python3 et_applications.py --application gpp
  python3 et_applications.py --application wue
  python3 et_applications.py --application all --plot
"""

import argparse
import calendar
import contextlib
import io
import itertools
import os
import pathlib
import sys
import tempfile
import time
import warnings
import zipfile

try:
    import yaml

    def _load_yaml(path):
        with open(path) as f:
            return yaml.safe_load(f)
except ImportError:
    print("[ERROR] pyyaml missing. Run: pip install pyyaml")
    sys.exit(1)

import ee
import numpy as np

try:
    import requests
except ImportError:
    print("[ERROR] requests missing. Run: pip install requests")
    sys.exit(1)

try:
    import rasterio
    from rasterio.merge import merge as rio_merge
except ImportError:
    print("[ERROR] rasterio missing. Run: pip install rasterio")
    sys.exit(1)

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches

    HAS_MPL = True
except ImportError:
    HAS_MPL = False


# ---------------------------------------------------------------------------
# CONSTANTS - AET pipeline
# ---------------------------------------------------------------------------
FEATURE_BANDS = [
    "MSAVI", "NDMI", "NDVI", "NDWI", "SAVI", "NDBI", "NDIIB7", "Albedo", "LST",
    "Rainf_tavg", "RootMoist_inst", "SoilMoi0_10cm_inst", "CanopInt_inst",
    "AvgSurfT_inst", "Qair_f_inst", "Wind_f_inst", "Psurf_f_inst",
    "SoilTMP0_10cm_inst", "Qsb_acc", "Swnet_tavg", "Lwnet_tavg",
    "Qg_tavg", "Qh_tavg", "Qle_tavg", "SWdown_f_tavg", "Tair_f_inst",
]
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
CONFIG_PATH = pathlib.Path(__file__).parent / "config.yaml"
MODIS_COL = "MODIS/061/MOD16A2"
MCD12Q1_COL = "MODIS/061/MCD12Q1"

# NoData sentinel used in GEE images and written to masked pixels in GeoTIFF.
NODATA = -9999.0

RWDI_CLASSES = [
    (0, 30, "#228B22", "Normal / Irrigated"),
    (30, 50, "#9ACD32", "Mild Stress"),
    (50, 70, "#FFFF00", "Moderate Stress"),
    (70, 80, "#FFA500", "High Stress"),
    (80, 90, "#F08080", "Severe Stress"),
    (90, 100, "#FF0000", "Extreme Drought"),
]

PLOT_THEME = {
    "aet": {"line": "#2E7D32", "fill": "#81C784"},
    "pet": {"line": "#1565C0", "fill": "#90CAF9"},
    "rwdi": {"line": "#C62828", "fill": "#EF9A9A"},
    "ratio": {"line": "#00897B", "fill": "#80CBC4"},
    "gpp": {"line": "#6D4C41", "fill": "#BCAAA4"},
    "wue": {"line": "#7B1FA2", "fill": "#CE93D8"},
}


# ---------------------------------------------------------------------------
# CONSTANTS - GPP / WUE (Light Use Efficiency, MOD17 framework)
# ---------------------------------------------------------------------------
# BPLUT: IGBP LC_Type1 class -> (eps_max g_C/MJ, TMIN_min C, TMIN_max C,
#                                VPD_min Pa,   VPD_max Pa)
# Source: MOD17 Collection 6 - Running & Zhao (2015) Table 2.2
BPLUT = {
    1: (0.962, -8.0, 8.31, 650, 4600),   # Evergreen Needleleaf Forest
    2: (1.268, -8.0, 9.09, 800, 3100),   # Evergreen Broadleaf Forest
    3: (1.086, -8.0, 10.44, 650, 2300),  # Deciduous Needleleaf Forest
    4: (1.165, -6.0, 9.94, 650, 1650),   # Deciduous Broadleaf Forest
    5: (1.051, -7.0, 9.50, 650, 2400),   # Mixed Forest
    6: (1.281, -8.0, 8.61, 650, 4700),   # Closed Shrublands
    7: (0.841, -8.0, 8.80, 650, 4800),   # Open Shrublands
    8: (1.239, -8.0, 11.39, 650, 3200),  # Woody Savannas
    9: (1.206, -8.0, 11.39, 650, 3100),  # Savannas
    10: (0.860, -8.0, 12.02, 650, 5300), # Grasslands - default fallback
    11: (0.860, -8.0, 12.02, 650, 5300), # Permanent Wetlands -> Grassland
    12: (1.044, -8.0, 12.02, 650, 4300), # Croplands
    13: (0.860, -8.0, 12.02, 650, 5300), # Urban/Built-up -> Grassland
    14: (1.044, -8.0, 12.02, 650, 4300), # Cropland/Natural Veg Mosaic
    15: (0.860, -8.0, 12.02, 650, 5300), # Permanent Snow/Ice -> Grassland
    16: (0.860, -8.0, 12.02, 650, 5300), # Barren/Sparsely Vegetated
    17: (0.860, -8.0, 12.02, 650, 5300), # Water Bodies -> Grassland
}
_BPLUT_DEFAULT_CLASS = 10


# ---------------------------------------------------------------------------
# BAND LAYOUT - combined 60-band download image for run_all()
# ---------------------------------------------------------------------------
# AET(12) PET(12) RWDI(12) KC(12) GPP(12)
_AET_SLICE = slice(0, 12)       # ET_01 ... ET_12        (0.1 mm/day)
_PET_SLICE = slice(12, 24)      # PET_01 ... PET_12      (0.1 mm/day)
_RWDI_SLICE = slice(24, 36)     # RWDI_01 ... RWDI_12    (%)
_KC_SLICE = slice(36, 48)       # KC_01 ... KC_12        (ratio 0-1)
_GPP_SLICE = slice(48, 60)      # GPP_01 ... GPP_12      (g C/m2/day)
# WUE is derived numpy-side from _GPP_SLICE / (_AET_SLICE x 0.1).


@contextlib.contextmanager
def _quiet_gdal():
    """Suppress GDAL/libtiff C-level warnings printed to stderr."""
    old_fd = os.dup(2)
    null_fd = os.open(os.devnull, os.O_WRONLY)
    os.dup2(null_fd, 2)
    os.close(null_fd)
    try:
        yield
    finally:
        os.dup2(old_fd, 2)
        os.close(old_fd)


# =============================================================================
# CONFIG
# =============================================================================

def load_config(path: pathlib.Path) -> dict:
    if not path.exists():
        print(f"[WARN] config.yaml not found at {path}. Using CLI args only.")
        return {}
    raw = _load_yaml(str(path))
    cfg = {}
    cfg["gee_project"] = raw.get("gee_project", "")
    tehsil = raw.get("tehsil", {}) or {}
    cfg["tehsil_name"] = tehsil.get("name", "")
    cfg["tehsil_state"] = tehsil.get("state", "")
    cfg["tehsil_dist"] = tehsil.get("district", "")
    assets = raw.get("assets", {}) or {}
    cfg["tehsil_asset"] = assets.get("tehsil_asset", "")
    cfg["model_aez"] = assets.get("model_aez", "")
    time_cfg = raw.get("time", {}) or {}
    cfg["year"] = int(time_cfg.get("year", 2022))
    compute = raw.get("compute", {}) or {}
    cfg["chunk_size"] = int(compute.get("chunk_size", 25000))
    output = raw.get("output", {}) or {}
    cfg["output"] = output.get("directory", "./results")
    cfg["plot"] = bool(output.get("plot", False))
    cfg["application"] = raw.get("application", "all")
    cfg["modis_collection"] = (raw.get("modis", {}) or {}).get("collection", MODIS_COL)
    sample = raw.get("sample_point", {}) or {}
    cfg["sample_lon"] = sample.get("lon") or sample.get("longitude") or None
    cfg["sample_lat"] = sample.get("lat") or sample.get("latitude") or None
    print(f"[config] Loaded from: {path}")
    return cfg


def merge_args(cfg: dict, args: argparse.Namespace) -> dict:
    merged = dict(cfg)
    for key, value in [
        ("tehsil_asset", args.tehsil_asset),
        ("model_aez", args.model_aez),
        ("year", args.year),
        ("output", args.output),
        ("gee_project", args.gee_project),
        ("tehsil_name", args.tehsil_name),
        ("chunk_size", args.chunk_size),
        ("application", args.application),
        ("sample_lon", args.sample_lon),
        ("sample_lat", args.sample_lat),
    ]:
        if value is not None:
            merged[key] = value
    if args.plot:
        merged["plot"] = True
    return merged


# =============================================================================
# GEE HELPERS
# =============================================================================

def init_ee(project=""):
    try:
        ee.Initialize(project=project) if project else ee.Initialize()
        print("[EE] Initialised.")
    except Exception:
        print("[EE] Running authenticate ...")
        ee.Authenticate()
        ee.Initialize(project=project) if project else ee.Initialize()


def load_tehsil(asset: str):
    fc = ee.FeatureCollection(asset)
    region = fc.geometry()
    return fc, region


def build_classifier(model_path: str) -> ee.Classifier:
    trees = (
        ee.FeatureCollection(model_path)
        .aggregate_array("tree")
        .map(lambda s: ee.String(s).replace("#.*", "", "g").trim())
    )
    return ee.Classifier.decisionTreeEnsemble(trees)


def calc_landsat_indices(img: ee.Image) -> ee.Image:
    ndvi = img.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI")
    savi = img.expression(
        "((NIR-R)/(NIR+R+0.5))*1.5",
        {"NIR": img.select("SR_B5"), "R": img.select("SR_B4")},
    ).rename("SAVI")
    msavi = img.expression(
        "(2*NIR+1-sqrt(pow((2*NIR+1),2)-8*(NIR-R)))/2",
        {"NIR": img.select("SR_B5"), "R": img.select("SR_B4")},
    ).rename("MSAVI")
    ndbi = img.normalizedDifference(["SR_B6", "SR_B5"]).rename("NDBI")
    ndwi = img.normalizedDifference(["SR_B3", "SR_B5"]).rename("NDWI")
    ndmi = img.normalizedDifference(["SR_B5", "SR_B6"]).rename("NDMI")
    ndiib7 = img.normalizedDifference(["SR_B5", "SR_B7"]).rename("NDIIB7")
    albedo = img.expression(
        "((0.356*B1)+(0.130*B2)+(0.373*B3)+(0.085*B4)+(0.072*B5)-0.018)/1.016",
        {
            "B1": img.select("SR_B1"),
            "B2": img.select("SR_B2"),
            "B3": img.select("SR_B3"),
            "B4": img.select("SR_B4"),
            "B5": img.select("SR_B5"),
        },
    ).rename("Albedo")
    lst = img.select("ST_B10").multiply(0.00341802).add(149.0).rename("LST")
    return img.addBands([ndvi, savi, msavi, ndbi, ndwi, ndmi, ndiib7, albedo, lst])


def predict_daily_et(ls_img: ee.Image, region: ee.Geometry,
                     classifier: ee.Classifier) -> ee.Image:
    idx = calc_landsat_indices(ls_img)
    clim = (
        ee.ImageCollection("NASA/GLDAS/V021/NOAH/G025/T3H")
        .filterBounds(region)
        .filterDate(ls_img.date().advance(-12, "hour"),
                    ls_img.date().advance(12, "hour"))
        .mean()
        .resample("bilinear")
        .reproject(crs=ls_img.select("SR_B5").projection(), scale=30)
    )
    return (
        idx.addBands(clim)
        .select(FEATURE_BANDS)
        .classify(classifier)
        .rename("ET_daily")
        .set("system:time_start", ls_img.date().millis())
    )


# =============================================================================
# GPP / WUE - GEE IMAGE BUILDERS
# =============================================================================

def _build_bplut_image(lc_img: ee.Image) -> ee.Image:
    """
    Convert a MCD12Q1 LC_Type1 image to five BPLUT parameter images.

    Returns a 5-band image:
        eps_max   (g C / MJ)
        tmin_min  (C)
        tmin_max  (C)
        vpd_min   (Pa)
        vpd_max   (Pa)
    """
    from_list = list(BPLUT.keys())
    default_values = BPLUT[_BPLUT_DEFAULT_CLASS]

    def _remap_param(idx, name):
        to_list = [BPLUT[k][idx] for k in from_list]
        return (
            lc_img.remap(from_list, to_list, defaultValue=default_values[idx])
            .rename(name)
            .float()
        )

    eps_max = _remap_param(0, "eps_max")
    tmin_min = _remap_param(1, "tmin_min")
    tmin_max = _remap_param(2, "tmin_max")
    vpd_min = _remap_param(3, "vpd_min")
    vpd_max = _remap_param(4, "vpd_max")
    return ee.Image.cat([eps_max, tmin_min, tmin_max, vpd_min, vpd_max])


def build_gpp_stack(region: ee.Geometry, year: int, proj: ee.Projection) -> ee.Image:
    """
    Build a 12-band monthly GPP image at 30 m using the Light Use Efficiency
    method (Monteith 1972; MOD17 framework).

        GPP_m = PAR_m x fAPAR_m x eps_max x TMIN_scalar_m x VPD_scalar_m

    Band names: GPP_01 ... GPP_12
    Units     : g C / m2 / day  (mean daily GPP for the month)
    """
    lc_raw = (
        ee.ImageCollection(MCD12Q1_COL)
        .filterDate(f"{year}-01-01", f"{year + 1}-01-01")
        .first()
        .select("LC_Type1")
    )
    # For categorical land cover, keep Earth Engine's default nearest-neighbour
    # sampling. Image.resample() only accepts bilinear/bicubic, so calling it
    # with "nearest" breaks the whole GPP graph during download.
    lc = lc_raw.reproject(crs=proj, scale=30)
    bplut_img = _build_bplut_image(lc)
    eps_max = bplut_img.select("eps_max")
    tmin_min = bplut_img.select("tmin_min")
    tmin_max = bplut_img.select("tmin_max")
    vpd_min = bplut_img.select("vpd_min")
    vpd_max = bplut_img.select("vpd_max")

    year_start = ee.Date.fromYMD(year, 1, 1)
    year_end = ee.Date.fromYMD(year + 1, 1, 1)
    ls_annual = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(year_start, year_end)
    )
    ndvi_annual_mean = (
        ls_annual
        .map(lambda img: img.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI"))
        .mean()
    )

    bands = []
    for month in range(1, 13):
        start = ee.Date.fromYMD(year, month, 1)
        end = start.advance(1, "month")

        gldas = (
            ee.ImageCollection("NASA/GLDAS/V021/NOAH/G025/T3H")
            .filterBounds(region)
            .filterDate(start, end)
        )

        def _gldas_mean_reproj(band):
            return (
                gldas.select(band).mean()
                .resample("bilinear")
                .reproject(crs=proj, scale=30)
            )

        swdown = _gldas_mean_reproj("SWdown_f_tavg").multiply(0.0864)
        par = swdown.multiply(0.45)

        ls_month = (
            ls_annual
            .filterDate(start, end)
            .map(lambda img: img.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI"))
            .mean()
        )
        ndvi = ls_month.unmask(ndvi_annual_mean).unmask(ee.Image.constant(0.05))
        fapar = (
            ndvi.multiply(1.24)
            .subtract(0.168)
            .max(ee.Image.constant(0.0))
            .min(ee.Image.constant(1.0))
        )

        tmin_c = (
            gldas.select("Tair_f_inst").min()
            .subtract(273.15)
            .resample("bilinear")
            .reproject(crs=proj, scale=30)
        )

        tair_c = _gldas_mean_reproj("Tair_f_inst").subtract(273.15)
        qair = _gldas_mean_reproj("Qair_f_inst")
        psurf = _gldas_mean_reproj("Psurf_f_inst")

        exponent = tair_c.multiply(17.67).divide(tair_c.add(243.5))
        es = exponent.exp().multiply(611.2)
        ea = psurf.multiply(qair).divide(qair.add(0.622))
        vpd = es.subtract(ea).max(ee.Image.constant(0.0))

        tmin_scalar = (
            tmin_c.subtract(tmin_min)
            .divide(tmin_max.subtract(tmin_min))
            .max(ee.Image.constant(0.0))
            .min(ee.Image.constant(1.0))
        )
        vpd_scalar = (
            vpd_max.subtract(vpd)
            .divide(vpd_max.subtract(vpd_min))
            .max(ee.Image.constant(0.0))
            .min(ee.Image.constant(1.0))
        )

        eps = eps_max.multiply(tmin_scalar).multiply(vpd_scalar)
        gpp = par.multiply(fapar).multiply(eps).rename(f"GPP_{month:02d}").float()
        bands.append(gpp)

    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack.clip(region)


# =============================================================================
# IMAGE BUILDERS  (AET / PET / RWDI / WS / KC / combined)
# =============================================================================

def _get_proj_30m(region: ee.Geometry, year: int) -> ee.Projection:
    """Return the 30 m Landsat projection for this region/year."""
    ls_ref = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(ee.Date.fromYMD(year, 1, 1), ee.Date.fromYMD(year, 12, 31))
        .first()
    )
    return ls_ref.select("SR_B5").projection()


def build_aet_stack(region: ee.Geometry, classifier: ee.Classifier, year: int) -> tuple:
    """
    Returns (aet_stack, gap_flags).
    aet_stack : ee.Image, 12 bands ET_01...ET_12  (0.1 mm/day, mean daily)
    gap_flags : list[bool], True = month gap-filled by +/-60-day neighbour mean
    """
    ls_col = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(ee.Date.fromYMD(year, 1, 1), ee.Date.fromYMD(year, 12, 31))
    )

    scene_counts = []
    for month in range(1, 13):
        start = ee.Date.fromYMD(year, month, 1)
        end = start.advance(1, "month")
        count = ls_col.filterDate(start, end).size().getInfo()
        scene_counts.append(count)
        print(f"  {MONTH_ABBR[month-1]:>3}: {count} Landsat scene(s)")

    months = ee.List.sequence(1, 12)
    raw_monthly = ee.ImageCollection.fromImages(
        months.map(lambda m: _make_raw_monthly(ee.Number(m), ls_col, region, classifier, year))
    )

    def interpolate(img):
        time_start = img.get("system:time_start")
        neighbours = (
            raw_monthly.select("ET_daily")
            .filterDate(ee.Date(time_start).advance(-60, "day"),
                        ee.Date(time_start).advance(60, "day"))
        )
        filled = neighbours.mean()
        et_filled = img.select("ET_daily").unmask(filled).unmask(0)
        month_str = ee.String(ee.Number(img.get("month")).format("%02d"))
        return et_filled.rename(ee.String("ET_").cat(month_str)).float()

    interp_col = raw_monthly.map(interpolate)
    stack = interp_col.toBands().clip(region)
    current_names = stack.bandNames()
    new_names = current_names.map(lambda n: ee.String(n).split("_").slice(1).join("_"))
    stack = stack.rename(new_names)

    gap_flags = [count == 0 for count in scene_counts]
    return stack, gap_flags


def _make_raw_monthly(month, ls_col, region, classifier, year):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()
    monthly_collection = ls_col.filterDate(start, end)
    et = (
        monthly_collection.map(lambda img: predict_daily_et(img, region, classifier))
        .mean()
        .rename("ET_daily")
    )
    return et.set("month", month).set("system:time_start", mid)


def build_pet_stack(region: ee.Geometry, year: int,
                    modis_col_id: str, proj: ee.Projection) -> ee.Image:
    """
    12-band PET stack PET_01...PET_12 (0.1 mm/day) at 30 m.
    MOD16A2 is 8-day composite; divide by 8 for daily rate.
    500 m MODIS pixel bilinearly resampled to the 30 m Landsat grid.
    """
    modis_col = ee.ImageCollection(modis_col_id).filterBounds(region)
    bands = []
    for month in range(1, 13):
        start = ee.Date.fromYMD(year, month, 1)
        end = start.advance(1, "month")
        pet = (
            modis_col.filterDate(start, end)
            .select("PET")
            .mean()
            .divide(8)
            .resample("bilinear")
            .reproject(crs=proj, scale=30)
            .rename(f"PET_{month:02d}")
            .float()
        )
        bands.append(pet)
    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack.clip(region)


def build_annual_et_image(aet_stack: ee.Image, year: int) -> ee.Image:
    """annual_ET_01mm = sum_m(daily_ET_m x days_in_month_m)"""
    annual = aet_stack.select("ET_01").multiply(calendar.monthrange(year, 1)[1])
    for month in range(2, 13):
        days = calendar.monthrange(year, month)[1]
        annual = annual.add(aet_stack.select(f"ET_{month:02d}").multiply(days))
    return annual.rename("annual_ET_01mm").float()


def build_rwdi_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """RWDI_01...12 = (1 - AET/PET) x 100 (%)"""
    bands = []
    for month in range(1, 13):
        rwdi = (
            ee.Image(1)
            .subtract(aet_stack.select(f"ET_{month:02d}").divide(pet_stack.select(f"PET_{month:02d}")))
            .multiply(100)
            .rename(f"RWDI_{month:02d}")
            .float()
        )
        bands.append(rwdi)
    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack


def build_kc_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """KC_01...12 = AET / PET (0-1)"""
    bands = []
    for month in range(1, 13):
        kc = (
            aet_stack.select(f"ET_{month:02d}")
            .divide(pet_stack.select(f"PET_{month:02d}"))
            .rename(f"KC_{month:02d}")
            .float()
        )
        bands.append(kc)
    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack


def build_combined_image(aet_stack: ee.Image, pet_stack: ee.Image,
                         gpp_stack: ee.Image, year: int) -> ee.Image:
    """
    Combine all bands into ONE 60-band image for a single-pass download.

    Band layout:
        ET_01 ... ET_12          (0.1 mm/day)
        PET_01 ... PET_12        (0.1 mm/day)
        RWDI_01 ... RWDI_12      (%)
        KC_01 ... KC_12          (0-1)
        GPP_01 ... GPP_12        (g C/m2/day)
        WUE derived from GPP/AET in Python.
    """
    rwdi_img = build_rwdi_image(aet_stack, pet_stack)
    kc_img = build_kc_image(aet_stack, pet_stack)

    return (
        aet_stack
        .addBands(pet_stack.unmask(NODATA))
        .addBands(rwdi_img.unmask(NODATA))
        .addBands(kc_img.unmask(NODATA))
        .addBands(gpp_stack.unmask(NODATA))
    )


# =============================================================================
# NUMPY HELPERS
# =============================================================================

def compute_wue_numpy(gpp_monthly: np.ndarray,
                      aet_monthly_raw: np.ndarray,
                      nodata: float = NODATA) -> np.ndarray:
    """
    Compute WUE pixel-wise from downloaded numpy arrays.

        WUE = GPP / AET_mm     (g C / kg H2O)

    GPP is in g C / m2 / day.
    AET is in raw 0.1 mm/day and is converted to mm/day here.
    """
    aet_mm = aet_monthly_raw.astype(np.float64) * 0.1

    bad_aet = ((aet_monthly_raw == nodata) | np.isnan(aet_monthly_raw) |
               np.isinf(aet_monthly_raw))
    bad_gpp = ((gpp_monthly == nodata) | np.isnan(gpp_monthly) |
               np.isinf(gpp_monthly))
    bad = bad_aet | bad_gpp | (aet_mm <= 0)

    with np.errstate(divide="ignore", invalid="ignore"):
        wue = np.where(
            bad,
            nodata,
            gpp_monthly.astype(np.float64) / np.where(aet_mm > 0, aet_mm, 1.0),
        )

    wue = np.where((wue != nodata) & ((wue < 0) | (wue > 50)), nodata, wue)
    return wue.astype(np.float32)


# =============================================================================
# GEOTIFF DOWNLOAD INFRASTRUCTURE
# =============================================================================

def _download_image_as_geotiff(img: ee.Image, region: ee.Geometry,
                               chunk_size: int = 25000, label: str = "") -> list:
    """
    Tile the tehsil bounding box and download each tile as a GeoTIFF via
    ee.Image.getDownloadURL().
    """
    bbox = region.bounds().getInfo()["coordinates"][0]
    lons = [coord[0] for coord in bbox]
    lats = [coord[1] for coord in bbox]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    deg_per_px = 0.00027
    tile_side = (chunk_size ** 0.5) * deg_per_px
    lon_tiles = max(1, int(np.ceil((max_lon - min_lon) / tile_side)))
    lat_tiles = max(1, int(np.ceil((max_lat - min_lat) / tile_side)))
    lon_step = (max_lon - min_lon) / lon_tiles
    lat_step = (max_lat - min_lat) / lat_tiles
    total_tiles = lon_tiles * lat_tiles

    tag = f"[{label}]" if label else "[download]"
    print(
        f"  {tag} Tiling: {lon_tiles} x {lat_tiles} = {total_tiles} tiles "
        f"(chunk ~= {chunk_size:,} px each)"
    )

    region_geojson = region.getInfo()
    tile_paths = []
    tile_count = 0
    skipped = 0

    empty_geom_msgs = (
        "geometry for image clipping must not be empty",
        "empty geometry",
        "no data",
    )
    fatal_expr_msgs = (
        "invalid interpolation mode",
        "image.resample:",
        "pattern '",
        "band pattern",
        "no band named",
        "dictionary does not contain key",
    )

    for i, j in itertools.product(range(lon_tiles), range(lat_tiles)):
        x0 = min_lon + i * lon_step
        x1 = x0 + lon_step
        y0 = min_lat + j * lat_step
        y1 = y0 + lat_step

        try:
            from shapely.geometry import shape, box as shapely_box

            tehsil_shape = shape(region_geojson)
            tile_shape = shapely_box(x0, y0, x1, y1)
            if not tehsil_shape.intersects(tile_shape):
                skipped += 1
                continue
        except ImportError:
            pass

        tile_rect = ee.Geometry.Rectangle([x0, y0, x1, y1])
        tile_geom = tile_rect.intersection(region, 1)

        try:
            area = tile_geom.area(10).getInfo()
            if area < 100:
                skipped += 1
                continue
        except Exception:
            skipped += 1
            continue

        tile_count += 1
        data = None

        for attempt in range(4):
            try:
                url = img.getDownloadURL({
                    "scale": 30,
                    "region": tile_geom,
                    "format": "GEO_TIFF",
                    "filePerBand": False,
                })
                resp = requests.get(url, timeout=300)
                resp.raise_for_status()
                data = resp.content
                break
            except Exception as exc:
                exc_str = str(exc).lower()
                if any(msg in exc_str for msg in empty_geom_msgs):
                    break
                if any(msg in exc_str for msg in fatal_expr_msgs):
                    raise RuntimeError(
                        "Earth Engine image expression error detected before tile "
                        f"download could proceed: {exc}"
                    ) from exc
                if attempt == 3:
                    print(f"  [WARN] Tile ({i},{j}) failed after 4 attempts: {exc}")
                    break
                wait = 2 ** attempt
                print(f"  [retry {attempt + 1}] tile ({i},{j}): {exc} - waiting {wait}s")
                time.sleep(wait)

        if data is None:
            continue

        if data[:2] == b"PK":
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    tif_names = [name for name in zf.namelist() if name.lower().endswith(".tif")]
                    if not tif_names:
                        print(f"  [WARN] Tile ({i},{j}): ZIP has no .tif files")
                        continue
                    data = zf.read(tif_names[0])
            except Exception as exc:
                print(f"  [WARN] Tile ({i},{j}): could not unzip: {exc}")
                continue

        tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
        try:
            tmp.write(data)
        finally:
            tmp.close()
        tile_paths.append(tmp.name)

        if tile_count % 10 == 0 or (tile_count + skipped) == total_tiles:
            print(
                f"  Progress: {tile_count + skipped}/{total_tiles} checked "
                f"| {tile_count} downloaded | {skipped} skipped (no overlap) "
                f"| {len(tile_paths)} saved",
                flush=True,
            )

    return tile_paths


def _merge_tiles(tile_paths: list, nodata: float = NODATA):
    """
    Mosaic tile GeoTIFFs into a single array using rasterio.merge.
    Returns (mosaic_float32, profile) or (None, None) on failure.
    """
    datasets, bad = [], []
    for path in tile_paths:
        try:
            with _quiet_gdal():
                datasets.append(rasterio.open(path))
        except Exception as exc:
            print(f"  [WARN] Cannot open tile {path}: {exc}")
            bad.append(path)

    result = (None, None)
    if datasets:
        try:
            with _quiet_gdal():
                mosaic, transform = rio_merge(datasets, nodata=nodata)
            profile = datasets[0].profile.copy()
            profile.update({
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": transform,
                "nodata": nodata,
                "count": mosaic.shape[0],
                "compress": "lzw",
                "driver": "GTiff",
                "dtype": "float32",
            })
            result = (mosaic.astype("float32"), profile)
        except Exception as exc:
            print(f"  [ERROR] rasterio merge failed: {exc}")
    else:
        print("  [ERROR] No valid tile datasets to merge.")

    for ds in datasets:
        ds.close()
    for path in tile_paths + bad:
        try:
            os.unlink(path)
        except OSError:
            pass

    return result


def _save_geotiff(arr: np.ndarray, profile: dict, output_path: str,
                  band_names: list = None, metadata: dict = None) -> None:
    """
    Write a numpy array (n_bands, H, W) as a GeoTIFF.
    band_names are embedded as band descriptions.
    metadata dict is stored as TIFF tags.
    """
    profile = profile.copy()
    profile.update({
        "count": arr.shape[0],
        "dtype": "float32",
        "compress": "lzw",
        "photometric": "MINISBLACK",
    })
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(arr.astype("float32"))
        if band_names:
            for idx, name in enumerate(band_names, 1):
                dst.set_band_description(idx, name)
        if metadata:
            dst.update_tags(**metadata)

    n_bands, height, width = arr.shape
    size_mb = os.path.getsize(output_path) / 1e6
    band0 = arr[0]
    valid_mask = ~((band0 == NODATA) | np.isnan(band0) | np.isinf(band0))
    valid_count = int(np.count_nonzero(valid_mask))
    print(f"  GeoTIFF saved -> {output_path}")
    print(
        f"    {n_bands} band(s) | {height}x{width} px grid | "
        f"{valid_count:,} valid pixels (band 1) | {size_mb:.1f} MB"
    )


def _scale_nodata(arr: np.ndarray, scale: float, nodata: float = NODATA) -> np.ndarray:
    """
    Multiply arr by scale while preserving nodata pixels.
    Also clamps +/-inf and extreme RF outliers to nodata.
    """
    out = arr.astype(np.float64).copy()
    mask = (arr == nodata) | np.isnan(arr) | np.isinf(arr)
    out = out * scale
    out[mask] = nodata
    out = np.where((out < -1e6) | (out > 1e6), nodata, out)
    return out.astype(np.float32)


def _annual_mean_band(monthly_12: np.ndarray, nodata: float = NODATA) -> np.ndarray:
    """
    Compute pixel-wise annual mean from 12 monthly bands.
    shape in: (12, H, W) -> shape out: (1, H, W)
    """
    bad = (monthly_12 == nodata) | np.isnan(monthly_12) | np.isinf(monthly_12)
    valid = np.where(bad, np.nan, monthly_12.astype(np.float64))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        annual = np.nanmean(valid, axis=0)
    annual = np.where(np.isnan(annual), nodata, annual).astype(np.float32)
    return annual[np.newaxis, :]


def _annual_total_band(monthly_12: np.ndarray, year: int,
                       nodata: float = NODATA) -> np.ndarray:
    """
    Compute pixel-wise annual total from 12 monthly mean-daily bands.
    shape in: (12, H, W) -> shape out: (1, H, W)
    """
    days = np.array(
        [calendar.monthrange(year, month)[1] for month in range(1, 13)],
        dtype=np.float32,
    )[:, np.newaxis, np.newaxis]
    bad = (monthly_12 == nodata) | np.isnan(monthly_12) | np.isinf(monthly_12)
    valid = np.where(bad, np.nan, monthly_12.astype(np.float64))
    annual = np.nansum(valid * days, axis=0)
    all_bad = np.all(np.isnan(valid), axis=0)
    annual = np.where(all_bad, nodata, annual).astype(np.float32)
    return annual[np.newaxis, :]


# =============================================================================
# UTILITIES
# =============================================================================

def _check_landsat(region, year):
    total = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(ee.Date.fromYMD(year, 1, 1), ee.Date.fromYMD(year, 12, 31))
        .size()
        .getInfo()
    )
    print(f"  Total Landsat scenes for {year}: {total}")
    if total == 0:
        print("[ERROR] No Landsat 8 scenes. Check year and tehsil geometry.")
        sys.exit(1)


def _print_stats(label: str, arr: np.ndarray, nodata: float = NODATA):
    """Print pixel count + min/mean/max/std, excluding NoData and +/-inf."""
    good = ~((arr == nodata) | np.isnan(arr) | np.isinf(arr))
    valid = arr[good].astype(np.float64)
    if valid.size == 0:
        print(f"\n  {label}: no valid data")
        return
    print(f"\n  {label}")
    print(f"    Valid pixels : {valid.size:,}")
    print(f"    Mean : {np.mean(valid):.4f}")
    print(f"    Min  : {np.min(valid):.4f}")
    print(f"    Max  : {np.max(valid):.4f}")
    print(f"    Std  : {np.std(valid):.4f}")


def _valid_pixels(arr: np.ndarray, nodata: float = NODATA) -> np.ndarray:
    """Return a masked numpy array (nodata -> nan)."""
    out = arr.astype(np.float64).copy()
    out[(out == nodata) | np.isnan(out)] = np.nan
    return out


def _make_valid_mask_2d(arr: np.ndarray, nodata: float = NODATA) -> np.ndarray:
    """Return a 2D boolean valid-data mask from a single raster band."""
    return ~((arr == nodata) | np.isnan(arr) | np.isinf(arr))


def _apply_2d_mask(arr: np.ndarray, valid_mask: np.ndarray,
                   nodata: float = NODATA) -> np.ndarray:
    """Apply one common spatial mask to every band of an array."""
    out = arr.astype(np.float32).copy()
    if out.ndim == 2:
        return np.where(valid_mask, out, nodata).astype(np.float32)
    return np.where(valid_mask[np.newaxis, :, :], out, nodata).astype(np.float32)


# =============================================================================
# CORE LAYER 1 - AET
# =============================================================================

def run_monthly_et(cfg: dict, region: ee.Geometry,
                   aet_stack=None, gap_flags=None) -> str:
    """
    Monthly mean daily AET for every 30 m pixel.
    Output  : aet_<TEHSIL>_<YEAR>.tif  (13 bands)
    Bands   : ET_Jan_daily_mm ... ET_Dec_daily_mm, ET_annual_mm
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)

    print(f"\n{'=' * 60}")
    print(f"  [aet]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    print("  Downloading pixel data (AET stack, 12 bands) ...")
    tile_paths = _download_image_as_geotiff(aet_stack, region, chunk_size, "monthly_et")
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    et_data = _scale_nodata(mosaic, scale=0.1)
    annual_et = _annual_total_band(et_data, year)
    aet_data = np.concatenate([et_data, annual_et], axis=0)
    band_names = [f"ET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["ET_annual_mm"]
    gap_months = [MONTH_ABBR[i] for i, gap in enumerate(gap_flags or []) if gap]

    out_path = os.path.join(outdir, f"aet_{tehsil}_{year}.tif")
    _save_geotiff(
        aet_data,
        profile,
        out_path,
        band_names=band_names,
        metadata={
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "year": str(year),
            "tehsil": tehsil,
            "gap_filled_months": ",".join(gap_months) or "none",
            "description": "Bands 1-12: mean daily AET per month at 30 m; band 13: annual total AET",
        },
    )
    _print_stats("Monthly AET (mm/day) - all months / all pixels", et_data)
    _print_stats("Annual ET (mm/yr)", annual_et)

    if cfg.get("plot"):
        _plot_monthly_et(et_data, tehsil, year, outdir)

    return out_path


# =============================================================================
# OPTIONAL STANDALONE EXPORT - ANNUAL ET
# =============================================================================

def run_annual_et(cfg: dict, region: ee.Geometry,
                  aet_stack=None, gap_flags=None) -> str:
    """
    Annual total ET for every 30 m pixel.
    Formula : annual_ET = sum_m(daily_ET_m x days_in_month_m)
    Output  : annual_et_<TEHSIL>_<YEAR>.tif  (1 band, mm/yr)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)

    print(f"\n{'=' * 60}")
    print(f"  [annual_et]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    annual_img = build_annual_et_image(aet_stack, year)

    plot_monthly_et = None
    download_img = annual_img
    if cfg.get("plot"):
        download_img = aet_stack.addBands(annual_img)
        print("  Downloading pixel data (AET stack + annual ET, 13 bands) ...")
    else:
        print("  Downloading pixel data (annual ET, 1 band) ...")

    tile_paths = _download_image_as_geotiff(download_img, region, chunk_size, "annual_et")
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    if cfg.get("plot"):
        plot_monthly_et = _scale_nodata(mosaic[:12], scale=0.1)
        et_annual = _scale_nodata(mosaic[12:13], scale=0.1)
    else:
        et_annual = _scale_nodata(mosaic, scale=0.1)

    out_path = os.path.join(outdir, f"annual_et_{tehsil}_{year}.tif")
    _save_geotiff(
        et_annual,
        profile,
        out_path,
        band_names=["ET_annual_mm"],
        metadata={
            "units": "mm/yr",
            "year": str(year),
            "tehsil": tehsil,
            "description": "Annual total AET at 30 m",
        },
    )
    _print_stats("Annual ET (mm/yr)", et_annual)

    if cfg.get("plot"):
        _plot_annual_et(plot_monthly_et, tehsil, year, outdir)

    return out_path


# =============================================================================
# CORE LAYER 2 - PET
# =============================================================================

def run_pet(cfg: dict, region: ee.Geometry, aet_stack=None,
            gap_flags=None, pet_stack=None) -> str:
    """
    Monthly mean daily PET (MODIS MOD16A2) for every 30 m pixel.
    Output  : pet_<TEHSIL>_<YEAR>.tif  (13 bands)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)
    modis_col = cfg.get("modis_collection", MODIS_COL)

    print(f"\n{'=' * 60}")
    print(f"  [pet]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack (pixel-grid carrier) ...")
        classifier = build_classifier(cfg["model_aez"])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    carrier = aet_stack.select("ET_01").rename("_carrier")
    img_to_dl = carrier.addBands(pet_stack.unmask(NODATA))

    print("  Downloading pixel data (AET carrier + PET, 13 bands) ...")
    tile_paths = _download_image_as_geotiff(img_to_dl, region, chunk_size, "pet")
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    footprint_mask = _make_valid_mask_2d(mosaic[0])
    pet_monthly = _apply_2d_mask(_scale_nodata(mosaic[1:], scale=0.1), footprint_mask)
    pet_annual = _annual_total_band(pet_monthly, year)
    pet_data = np.concatenate([pet_monthly, pet_annual], axis=0)
    band_names = [f"PET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["PET_annual_mm"]

    out_path = os.path.join(outdir, f"pet_{tehsil}_{year}.tif")
    _save_geotiff(
        pet_data,
        profile,
        out_path,
        band_names=band_names,
        metadata={
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "source": "MODIS MOD16A2",
            "year": str(year),
            "tehsil": tehsil,
            "description": "Bands 1-12: mean daily PET per month at 30 m; band 13: annual total PET",
        },
    )
    _print_stats("Monthly PET (mm/day) - all months / all pixels", pet_monthly)
    _print_stats("Annual PET (mm/yr)", pet_annual)

    if cfg.get("plot"):
        _plot_pet(pet_monthly, tehsil, year, outdir)

    return out_path


# =============================================================================
# DERIVED APPLICATION 1 - RWDI
# =============================================================================

def run_rwdi(cfg: dict, region: ee.Geometry, aet_stack=None,
             gap_flags=None, pet_stack=None) -> str:
    """
    RWDI = (1 - AET/PET) x 100 (%)
    Output  : rwdi_<TEHSIL>_<YEAR>.tif  (13 bands)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)
    modis_col = cfg.get("modis_collection", MODIS_COL)

    print(f"\n{'=' * 60}")
    print(f"  [rwdi]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    rwdi_img = build_rwdi_image(aet_stack, pet_stack)
    carrier = aet_stack.select("ET_01").rename("_carrier")
    img_to_dl = carrier.addBands(rwdi_img.unmask(NODATA))

    print("  Downloading pixel data (AET carrier + RWDI, 13 bands) ...")
    tile_paths = _download_image_as_geotiff(img_to_dl, region, chunk_size, "rwdi")
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    footprint_mask = _make_valid_mask_2d(mosaic[0])
    rwdi_monthly = _apply_2d_mask(mosaic[1:], footprint_mask)
    rwdi_annual = _annual_mean_band(rwdi_monthly)
    rwdi_data = np.concatenate([rwdi_monthly, rwdi_annual], axis=0)

    out_path = os.path.join(outdir, f"rwdi_{tehsil}_{year}.tif")
    profile.update({"count": 13})
    _save_geotiff(
        rwdi_data,
        profile,
        out_path,
        band_names=[f"RWDI_{abbr}" for abbr in MONTH_ABBR] + ["RWDI_annual"],
        metadata={
            "units": "percent",
            "formula": "(1 - AET/PET) * 100",
            "year": str(year),
            "tehsil": tehsil,
            "description": "Relative Water Deficit Index per month + annual mean",
        },
    )
    _print_stats("Annual mean RWDI (%)", rwdi_annual)

    if cfg.get("plot"):
        _plot_rwdi(rwdi_monthly, tehsil, year, outdir)

    return out_path


def _run_kc_application(cfg: dict, region: ee.Geometry, aet_stack=None,
                        gap_flags=None, pet_stack=None) -> str:
    """Shared runner for the monthly Kc proxy (AET/PET)."""
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)
    modis_col = cfg.get("modis_collection", MODIS_COL)
    label = "kc"
    title = "Crop Coefficient (Kc)"
    stack_builder = build_kc_image
    band_names = [f"KC_{abbr}" for abbr in MONTH_ABBR] + ["KC_annual"]
    output_name = f"kc_{tehsil}_{year}.tif"
    metadata = {
        "units": "ratio (AET/PET)",
        "formula": "AET / PET",
        "year": str(year),
        "tehsil": tehsil,
        "description": "Monthly Kc proxy from AET/PET + annual mean",
    }
    plotter = _plot_kc

    print(f"\n{'=' * 60}")
    print(f"  [{label}]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    ratio_img = stack_builder(aet_stack, pet_stack)
    carrier = aet_stack.select("ET_01").rename("_carrier")
    img_to_dl = carrier.addBands(ratio_img.unmask(NODATA))

    print(f"  Downloading pixel data (AET carrier + {title}, 13 bands) ...")
    tile_paths = _download_image_as_geotiff(img_to_dl, region, chunk_size, label)
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    footprint_mask = _make_valid_mask_2d(mosaic[0])
    ratio_monthly = _apply_2d_mask(mosaic[1:], footprint_mask)
    ratio_annual = _annual_mean_band(ratio_monthly)
    ratio_data = np.concatenate([ratio_monthly, ratio_annual], axis=0)

    out_path = os.path.join(outdir, output_name)
    profile.update({"count": 13})
    _save_geotiff(ratio_data, profile, out_path, band_names=band_names, metadata=metadata)
    _print_stats(f"Annual mean {title}", ratio_annual)

    if cfg.get("plot"):
        plotter(ratio_monthly, tehsil, year, outdir)

    return out_path


# =============================================================================
# DERIVED APPLICATION 2 - KC
# =============================================================================

def run_kc(cfg: dict, region: ee.Geometry, aet_stack=None,
           gap_flags=None, pet_stack=None) -> str:
    """
    Kc proxy = AET / PET
    Output  : kc_<TEHSIL>_<YEAR>.tif  (13 bands)
    """
    return _run_kc_application(cfg, region, aet_stack=aet_stack,
                               gap_flags=gap_flags, pet_stack=pet_stack)


# =============================================================================
# CORE LAYER 3 - GPP
# =============================================================================

def run_gpp(cfg: dict, region: ee.Geometry, aet_stack=None,
            gap_flags=None, gpp_stack=None) -> str:
    """
    Monthly mean daily GPP via the MOD17 Light Use Efficiency framework.

        GPP = PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)

    print(f"\n{'=' * 60}")
    print(f"  [gpp]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack (pixel-grid carrier) ...")
        classifier = build_classifier(cfg["model_aez"])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if gpp_stack is None:
        print("  Building GPP stack (LUE model: GLDAS + Landsat NDVI + MCD12Q1) ...")
        proj = _get_proj_30m(region, year)
        gpp_stack = build_gpp_stack(region, year, proj)

    carrier = aet_stack.select("ET_01").rename("_carrier")
    img_to_dl = carrier.addBands(gpp_stack.unmask(NODATA))

    print("  Downloading pixel data (carrier + GPP, 13 bands) ...")
    tile_paths = _download_image_as_geotiff(img_to_dl, region, chunk_size, "gpp")
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    footprint_mask = _make_valid_mask_2d(mosaic[0])
    gpp_monthly = _apply_2d_mask(mosaic[1:], footprint_mask)
    gpp_annual = _annual_mean_band(gpp_monthly)
    gpp_data = np.concatenate([gpp_monthly, gpp_annual], axis=0)

    out_path = os.path.join(outdir, f"gpp_{tehsil}_{year}.tif")
    profile.update({"count": 13})
    _save_geotiff(
        gpp_data,
        profile,
        out_path,
        band_names=[f"GPP_{abbr}_gC_m2_day" for abbr in MONTH_ABBR] + ["GPP_annual_mean"],
        metadata={
            "units": "g C / m2 / day",
            "method": "LUE: PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "par_source": "GLDAS SWdown_f_tavg * 0.0864 * 0.45",
            "fapar_source": "Landsat 8 NDVI -> 1.24*NDVI - 0.168",
            "bplut_source": "MOD17 C6 / MCD12Q1 IGBP LC_Type1",
            "tmin_source": "GLDAS Tair_f_inst monthly minimum (K-273.15)",
            "vpd_source": "GLDAS Tair+Qair+Psurf Magnus formula",
            "year": str(year),
            "tehsil": tehsil,
            "description": "Mean daily GPP per month (LUE) + annual mean at 30 m",
        },
    )
    _print_stats("Monthly GPP (g C/m2/day) - all months / all pixels", gpp_monthly)
    _print_stats("Annual mean GPP (g C/m2/day)", gpp_annual)

    if cfg.get("plot"):
        _plot_gpp(gpp_monthly, tehsil, year, outdir)

    return out_path


# =============================================================================
# DERIVED APPLICATION 3 - WUE
# =============================================================================

def run_wue(cfg: dict, region: ee.Geometry, aet_stack=None,
            gap_flags=None, gpp_stack=None) -> str:
    """
    Water Use Efficiency = GPP / AET (g C / kg H2O)
    Output  : wue_<TEHSIL>_<YEAR>.tif  (13 bands)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)

    print(f"\n{'=' * 60}")
    print(f"  [wue]  {tehsil}  |  {year}  |  WUE = GPP / AET")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack (Landsat 8 + GLDAS -> RF model) ...")
        classifier = build_classifier(cfg["model_aez"])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if gpp_stack is None:
        print("  Building GPP stack (LUE: GLDAS + Landsat NDVI + MCD12Q1) ...")
        proj = _get_proj_30m(region, year)
        gpp_stack = build_gpp_stack(region, year, proj)

    combined_wue = aet_stack.addBands(gpp_stack.unmask(NODATA))

    print("  Downloading pixel data (AET 12 bands + GPP 12 bands = 24 bands) ...")
    tile_paths = _download_image_as_geotiff(combined_wue, region, chunk_size, "wue")
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    aet_raw = mosaic[:12]
    gpp_monthly = mosaic[12:]
    footprint_mask = _make_valid_mask_2d(aet_raw[0])
    wue_monthly = _apply_2d_mask(compute_wue_numpy(gpp_monthly, aet_raw), footprint_mask)
    wue_annual = _annual_mean_band(wue_monthly)
    wue_data = np.concatenate([wue_monthly, wue_annual], axis=0)

    gap_months = [MONTH_ABBR[i] for i, gap in enumerate(gap_flags or []) if gap]

    out_path = os.path.join(outdir, f"wue_{tehsil}_{year}.tif")
    profile.update({"count": 13})
    _save_geotiff(
        wue_data,
        profile,
        out_path,
        band_names=[f"WUE_{abbr}_gC_per_kgH2O" for abbr in MONTH_ABBR] + ["WUE_annual_mean"],
        metadata={
            "units": "g C / kg H2O",
            "formula": "GPP (LUE) / AET (RF downscaled)",
            "gpp_method": "PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "aet_method": "Landsat8 + GLDAS features -> Random Forest",
            "bplut_source": "MOD17 C6 BPLUT / MCD12Q1 IGBP LC_Type1",
            "year": str(year),
            "tehsil": tehsil,
            "gap_filled_months": ",".join(gap_months) or "none",
            "description": (
                "WUE = GPP/AET per month + annual mean at 30 m. "
                "Units: g C fixed per kg of water transpired."
            ),
        },
    )
    _print_stats("Monthly WUE (g C / kg H2O) - all months / all pixels", wue_monthly)
    _print_stats("Annual mean WUE (g C / kg H2O)", wue_annual)

    if cfg.get("plot"):
        _plot_wue(wue_monthly, tehsil, year, outdir)

    return out_path


# =============================================================================
# ALL  (single combined download - guaranteed pixel consistency)
# =============================================================================

def run_all(cfg: dict, region: ee.Geometry) -> dict:
    """
    Run the three feature layers and three derived applications
    in ONE combined GEE download.

    Pixel consistency guarantee:
      All outputs are derived from a single 60-band image download.
      They are therefore spatially identical - same CRS, transform, and
      pixel grid. No post-processing alignment is needed.
    """
    year = cfg["year"]
    tehsil = cfg["tehsil_name"]
    outdir = cfg["output"]
    chunk_size = cfg.get("chunk_size", 25000)
    modis_col = cfg.get("modis_collection", MODIS_COL)

    print(f"\n{'=' * 60}")
    print("  [all] Building shared GEE stacks ...")
    print(f"{'=' * 60}")

    print("\n  [1/3] Building AET stack (Landsat 8 + GLDAS -> RF model) ...")
    classifier = build_classifier(cfg["model_aez"])
    _check_landsat(region, year)
    aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    print("\n  [2/3] Building PET stack (MODIS MOD16A2) ...")
    proj = _get_proj_30m(region, year)
    pet_stack = build_pet_stack(region, year, modis_col, proj)

    print("\n  [3/3] Building GPP stack (LUE: GLDAS + Landsat NDVI + MCD12Q1) ...")
    gpp_stack = build_gpp_stack(region, year, proj)

    print("\n  Building combined image (60 bands) ...")
    combined = build_combined_image(aet_stack, pet_stack, gpp_stack, year)

    print("\n  Downloading all bands in ONE pass (60 bands) ...")
    tile_paths = _download_image_as_geotiff(combined, region, chunk_size, "all")
    mosaic, profile = _merge_tiles(tile_paths)

    if mosaic is None:
        print("[ERROR] No pixel data returned.")
        return {}

    gap_months = [MONTH_ABBR[i] for i, gap in enumerate(gap_flags or []) if gap]
    gap_meta = ",".join(gap_months) or "none"
    results = {}

    et_monthly = _scale_nodata(mosaic[_AET_SLICE], scale=0.1)
    footprint_mask = _make_valid_mask_2d(et_monthly[0])
    annual_data = _apply_2d_mask(_annual_total_band(et_monthly, year), footprint_mask)
    et_data = np.concatenate([et_monthly, annual_data], axis=0)
    path = os.path.join(outdir, f"aet_{tehsil}_{year}.tif")
    _save_geotiff(
        et_data,
        profile,
        path,
        band_names=[f"ET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["ET_annual_mm"],
        metadata={
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "year": str(year),
            "tehsil": tehsil,
            "gap_filled_months": gap_meta,
            "description": "Bands 1-12: mean daily AET per month at 30 m; band 13: annual total AET",
        },
    )
    results["aet"] = path
    results["monthly_et"] = path

    pet_monthly = _apply_2d_mask(_scale_nodata(mosaic[_PET_SLICE], scale=0.1), footprint_mask)
    pet_annual = _annual_total_band(pet_monthly, year)
    pet_data = np.concatenate([pet_monthly, pet_annual], axis=0)
    path = os.path.join(outdir, f"pet_{tehsil}_{year}.tif")
    _save_geotiff(
        pet_data,
        profile,
        path,
        band_names=[f"PET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["PET_annual_mm"],
        metadata={
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "source": "MODIS MOD16A2",
            "year": str(year),
            "tehsil": tehsil,
            "description": "Bands 1-12: mean daily PET per month at 30 m; band 13: annual total PET",
        },
    )
    results["pet"] = path

    rwdi_monthly = _apply_2d_mask(mosaic[_RWDI_SLICE], footprint_mask)
    rwdi_annual = _annual_mean_band(rwdi_monthly)
    rwdi_data = np.concatenate([rwdi_monthly, rwdi_annual], axis=0)
    path = os.path.join(outdir, f"rwdi_{tehsil}_{year}.tif")
    _save_geotiff(
        rwdi_data,
        profile,
        path,
        band_names=[f"RWDI_{abbr}" for abbr in MONTH_ABBR] + ["RWDI_annual"],
        metadata={
            "units": "percent",
            "formula": "(1 - AET/PET) * 100",
            "year": str(year),
            "tehsil": tehsil,
            "description": "RWDI per month + annual mean",
        },
    )
    results["rwdi"] = path

    kc_monthly = _apply_2d_mask(mosaic[_KC_SLICE], footprint_mask)
    kc_annual = _annual_mean_band(kc_monthly)
    kc_data = np.concatenate([kc_monthly, kc_annual], axis=0)
    path = os.path.join(outdir, f"kc_{tehsil}_{year}.tif")
    _save_geotiff(
        kc_data,
        profile,
        path,
        band_names=[f"KC_{abbr}" for abbr in MONTH_ABBR] + ["KC_annual"],
        metadata={
            "units": "ratio (AET/PET)",
            "formula": "AET / PET",
            "year": str(year),
            "tehsil": tehsil,
            "description": "Monthly Kc proxy from AET/PET + annual mean",
        },
    )
    results["kc"] = path

    gpp_monthly = _apply_2d_mask(mosaic[_GPP_SLICE], footprint_mask)
    gpp_annual = _annual_mean_band(gpp_monthly)
    gpp_data = np.concatenate([gpp_monthly, gpp_annual], axis=0)
    path = os.path.join(outdir, f"gpp_{tehsil}_{year}.tif")
    _save_geotiff(
        gpp_data,
        profile,
        path,
        band_names=[f"GPP_{abbr}_gC_m2_day" for abbr in MONTH_ABBR] + ["GPP_annual_mean"],
        metadata={
            "units": "g C / m2 / day",
            "method": "LUE: PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "year": str(year),
            "tehsil": tehsil,
            "description": "Mean daily GPP per month (LUE) + annual mean",
        },
    )
    results["gpp"] = path

    wue_monthly = _apply_2d_mask(compute_wue_numpy(gpp_monthly, mosaic[_AET_SLICE]), footprint_mask)
    wue_annual = _annual_mean_band(wue_monthly)
    wue_data = np.concatenate([wue_monthly, wue_annual], axis=0)
    path = os.path.join(outdir, f"wue_{tehsil}_{year}.tif")
    _save_geotiff(
        wue_data,
        profile,
        path,
        band_names=[f"WUE_{abbr}_gC_per_kgH2O" for abbr in MONTH_ABBR] + ["WUE_annual_mean"],
        metadata={
            "units": "g C / kg H2O",
            "formula": "GPP (LUE) / AET (RF downscaled)",
            "year": str(year),
            "tehsil": tehsil,
            "gap_filled_months": gap_meta,
            "description": "WUE per month + annual mean at 30 m",
        },
    )
    results["wue"] = path

    _print_stats("Annual ET (mm/yr)", annual_data)
    _print_stats("Annual PET (mm/yr)", pet_annual)
    _print_stats("Annual mean RWDI (%)", rwdi_annual)
    _print_stats("Annual mean Kc (AET/PET)", kc_annual)
    _print_stats("Annual mean GPP (g C/m2/day)", gpp_annual)
    _print_stats("Annual mean WUE (g C/kg H2O)", wue_annual)

    if cfg.get("plot"):
        _plot_monthly_et(et_monthly, tehsil, year, outdir)
        _plot_pet(pet_monthly, tehsil, year, outdir)
        _plot_rwdi(rwdi_monthly, tehsil, year, outdir)
        _plot_kc(kc_monthly, tehsil, year, outdir)
        _plot_gpp(gpp_monthly, tehsil, year, outdir)
        _plot_wue(wue_monthly, tehsil, year, outdir)

    run_sample_timeseries(results, cfg)
    return results


# =============================================================================
# SAMPLE POINT TIME SERIES
# =============================================================================

def run_sample_timeseries(output_paths: dict, cfg: dict):
    """
    Extract the 12-month time series from the pixel nearest to a requested
    lon/lat and produce a 6-panel plot. Uses rasterio.sample on the output
    GeoTIFFs - no additional GEE calls are made.
    """
    lon = cfg.get("sample_lon")
    lat = cfg.get("sample_lat")
    if lon is None or lat is None:
        return

    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    outdir = cfg["output"]

    print(f"\n[sample] Sampling pixel at lon={lon:.6f}, lat={lat:.6f}")

    def _sample(tif_path):
        if tif_path is None or not os.path.exists(tif_path):
            return None
        with rasterio.open(tif_path) as ds:
            vals = list(ds.sample([(lon, lat)]))[0].astype(np.float32)
            vals[vals == NODATA] = np.nan
        return vals

    aet = _sample(output_paths.get("aet") or output_paths.get("monthly_et"))
    pet = _sample(output_paths.get("pet"))
    rwdi = _sample(output_paths.get("rwdi"))
    kc = _sample(output_paths.get("kc"))
    gpp = _sample(output_paths.get("gpp"))
    wue = _sample(output_paths.get("wue"))

    if aet is None:
        print("  [sample] AET GeoTIFF not available - skipping.")
        return

    aet_m = aet[:12] if aet is not None else np.full(12, np.nan)
    rwdi_m = rwdi[:12] if rwdi is not None else np.full(12, np.nan)
    kc_m = kc[:12] if kc is not None else np.full(12, np.nan)
    pet_m = pet[:12] if pet is not None else np.full(12, np.nan)
    gpp_m = gpp[:12] if gpp is not None else np.full(12, np.nan)
    wue_m = wue[:12] if wue is not None else np.full(12, np.nan)

    if cfg.get("plot"):
        _plot_sample_timeseries(aet_m, pet_m, rwdi_m, kc_m, gpp_m, wue_m,
                                lon, lat, tehsil, year, outdir)


# =============================================================================
# PLOTS
# =============================================================================

def _band_stats(arr: np.ndarray, nodata: float = NODATA):
    """Per-band mean and std, ignoring NoData, NaN, and +/-inf."""
    out_mean, out_std = [], []
    for band_idx in range(arr.shape[0]):
        values = arr[band_idx]
        good = ~((values == nodata) | np.isnan(values) | np.isinf(values))
        values = values[good].astype(np.float64)
        out_mean.append(np.nanmean(values) if values.size else np.nan)
        out_std.append(np.nanstd(values) if values.size else np.nan)
    return np.array(out_mean), np.array(out_std)


def _plot_monthly_series(arr, tehsil, year, outdir, *,
                         filename, title, ylabel, theme_key,
                         marker="o", y_limits=None,
                         ref_lines=None, ref_spans=None):
    """Shared monthly line-chart style for all 12-band products."""
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    means, stds = _band_stats(arr)
    colors = PLOT_THEME[theme_key]

    fig, ax = plt.subplots(figsize=(12, 5))

    for span in ref_spans or []:
        ax.axhspan(span["ymin"], span["ymax"],
                   color=span.get("color", "#cccccc"),
                   alpha=span.get("alpha", 0.06),
                   zorder=0)

    for line in ref_lines or []:
        ax.axhline(line["y"], color=line.get("color", "#666666"),
                   linestyle=line.get("linestyle", "--"),
                   linewidth=line.get("linewidth", 1.2),
                   alpha=line.get("alpha", 0.9),
                   label=line.get("label"))

    ax.plot(MONTH_ABBR, means, marker=marker, color=colors["line"],
            linewidth=2.6, markersize=7, label="Mean across pixels")
    ax.fill_between(range(12), means - stds, means + stds,
                    alpha=0.22, color=colors["fill"], label="+/-1 std")

    ax.set_title(f"{title} - {tehsil} ({year})", fontsize=13, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel(ylabel)
    if y_limits is not None:
        ax.set_ylim(*y_limits)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.legend(fontsize=9, loc="best")
    plt.tight_layout()

    path = os.path.join(outdir, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot -> {path}")


def _plot_monthly_et(arr, tehsil, year, outdir):
    _plot_monthly_series(
        arr, tehsil, year, outdir,
        filename=f"aet_{tehsil}_{year}.png",
        title="Monthly Mean Daily AET",
        ylabel="AET (mm/day)",
        theme_key="aet",
        marker="o",
    )


def _plot_annual_et(arr, tehsil, year, outdir):
    _plot_monthly_series(
        arr, tehsil, year, outdir,
        filename=f"annual_et_{tehsil}_{year}.png",
        title="Monthly Mean Daily AET (basis for Annual ET)",
        ylabel="AET (mm/day)",
        theme_key="aet",
        marker="o",
    )


def _plot_pet(arr, tehsil, year, outdir):
    _plot_monthly_series(
        arr, tehsil, year, outdir,
        filename=f"pet_{tehsil}_{year}.png",
        title="Monthly Mean Daily PET (MODIS MOD16A2)",
        ylabel="PET (mm/day)",
        theme_key="pet",
        marker="s",
    )


def _plot_rwdi(arr, tehsil, year, outdir):
    ref_spans = [
        {"ymin": lo, "ymax": hi, "color": color, "alpha": 0.06}
        for lo, hi, color, _ in RWDI_CLASSES
    ]
    ref_lines = [
        {"y": 30, "color": "#9ACD32", "label": "30%"},
        {"y": 50, "color": "#FFA500", "label": "50%"},
        {"y": 80, "color": "#FF0000", "label": "80%"},
    ]
    _plot_monthly_series(
        arr, tehsil, year, outdir,
        filename=f"rwdi_{tehsil}_{year}.png",
        title="Monthly RWDI (pixel mean)",
        ylabel="RWDI (%)",
        theme_key="rwdi",
        marker="o",
        y_limits=(0, 100),
        ref_lines=ref_lines,
        ref_spans=ref_spans,
    )


def _plot_ratio(arr, tehsil, year, outdir, prefix, title, filename):
    _plot_monthly_series(
        arr, tehsil, year, outdir,
        filename=filename,
        title=title,
        ylabel=prefix,
        theme_key="ratio",
        marker="s",
        y_limits=(0, 1.25),
        ref_lines=[
            {"y": 1.0, "color": "#1A4FA3", "label": "No stress (1.0)"},
            {"y": 0.5, "color": "#FF8C00", "label": "Moderate (0.5)"},
            {"y": 0.3, "color": "#FF0000", "label": "High (0.3)"},
        ],
    )


def _plot_kc(arr, tehsil, year, outdir):
    _plot_ratio(arr, tehsil, year, outdir,
                prefix="Kc (AET / PET)",
                title="Monthly Kc (AET/PET)",
                filename=f"kc_{tehsil}_{year}.png")


def _plot_gpp(arr, tehsil, year, outdir):
    _plot_monthly_series(
        arr, tehsil, year, outdir,
        filename=f"gpp_{tehsil}_{year}.png",
        title="Monthly Mean Daily GPP (LUE)",
        ylabel="GPP (g C / m2 / day)",
        theme_key="gpp",
        marker="^",
    )


def _plot_wue(arr, tehsil, year, outdir):
    _plot_monthly_series(
        arr, tehsil, year, outdir,
        filename=f"wue_{tehsil}_{year}.png",
        title="Monthly Water Use Efficiency (WUE = GPP / AET)",
        ylabel="WUE (g C / kg H2O)",
        theme_key="wue",
        marker="D",
    )


def _plot_sample_timeseries(aet, pet, rwdi, kc, gpp, wue,
                            lon, lat, tehsil, year, outdir):
    """6-panel monthly timeseries for a single pixel."""
    if not HAS_MPL:
        return
    fig, axes = plt.subplots(2, 3, figsize=(18, 9))
    fig.suptitle(
        f"Monthly Time Series - Single Pixel  |  {tehsil} ({year})\n"
        f"lon={lon:.5f}, lat={lat:.5f}",
        fontsize=13, fontweight="bold", y=1.01
    )

    ax = axes[0, 0]
    ax.plot(MONTH_ABBR, aet, marker="o", color=PLOT_THEME["aet"]["line"], linewidth=2.2, markersize=7)
    ax.fill_between(range(12), aet, alpha=0.15, color=PLOT_THEME["aet"]["fill"])
    ax.set_title("Mean Daily AET (mm/day)")
    ax.set_ylabel("AET (mm/day)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    ax = axes[0, 1]
    ax.plot(MONTH_ABBR, pet, marker="s", color=PLOT_THEME["pet"]["line"], linewidth=2.2, markersize=7)
    ax.fill_between(range(12), pet, alpha=0.15, color=PLOT_THEME["pet"]["fill"])
    ax.set_title("Mean Daily PET (mm/day) - MODIS")
    ax.set_ylabel("PET (mm/day)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    ax = axes[0, 2]
    ax.plot(MONTH_ABBR, gpp, marker="^", color=PLOT_THEME["gpp"]["line"], linewidth=2.2, markersize=7)
    ax.fill_between(range(12), gpp, alpha=0.15, color=PLOT_THEME["gpp"]["fill"])
    ax.set_title("Mean Daily GPP (g C/m2/day) - LUE")
    ax.set_ylabel("GPP (g C/m2/day)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    ax = axes[1, 0]
    for lo, hi, color, _ in RWDI_CLASSES:
        ax.axhspan(lo, hi, color=color, alpha=0.05, zorder=0)
    ax.plot(MONTH_ABBR, rwdi, marker="o", color=PLOT_THEME["rwdi"]["line"], linewidth=2.2, markersize=7)
    ax.fill_between(range(12), rwdi, alpha=0.15, color=PLOT_THEME["rwdi"]["fill"])
    ax.set_ylim(0, 100)
    ax.set_title("Monthly RWDI (%)")
    ax.set_ylabel("RWDI (%)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    ax = axes[1, 1]
    ax.plot(MONTH_ABBR, kc, marker="s", color=PLOT_THEME["ratio"]["line"], linewidth=2.2, markersize=7)
    ax.fill_between(range(12), kc, alpha=0.12, color=PLOT_THEME["ratio"]["fill"])
    ax.axhline(1.0, color="#1a4fa3", linestyle="--", linewidth=1.1, label="No stress (1.0)")
    ax.axhline(0.5, color="#FF8C00", linestyle="--", linewidth=1.1, label="Moderate (0.5)")
    ax.axhline(0.3, color="#FF0000", linestyle="--", linewidth=1.1, label="High (0.3)")
    ax.set_ylim(0, 1.25)
    ax.set_title("Monthly Kc (AET/PET)")
    ax.set_ylabel("Kc")
    ax.legend(fontsize=8)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    ax = axes[1, 2]
    ax.plot(MONTH_ABBR, wue, marker="D", color=PLOT_THEME["wue"]["line"], linewidth=2.2, markersize=7)
    ax.fill_between(range(12), wue, alpha=0.15, color=PLOT_THEME["wue"]["fill"])
    ax.set_title("Monthly WUE (g C / kg H2O)")
    ax.set_ylabel("WUE (g C / kg H2O)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    plt.tight_layout()
    path = os.path.join(outdir, f"sample_point_{tehsil}_{year}.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot -> {path}")


# =============================================================================
# CLI
# =============================================================================

def build_parser():
    parser = argparse.ArgumentParser(
        prog="et_applications",
        description="Pan-India ET Downscaling + GPP/WUE - GeoTIFF Raster Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config", default=None, metavar="PATH",
                        help="Path to config YAML (default: ./config.yaml)")
    parser.add_argument("--tehsil-asset", default=None,
                        help="GEE FeatureCollection asset path for the tehsil")
    parser.add_argument("--model-aez", default=None,
                        help="GEE asset path for the RF ensemble model")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--output", default=None, metavar="DIR")
    parser.add_argument("--plot", action="store_true", default=False)
    parser.add_argument("--gee-project", default=None)
    parser.add_argument("--tehsil-name", default=None)
    parser.add_argument("--chunk-size", type=int, default=None,
                        help="Approx pixels per download tile (default: 25000). "
                             "Lower if you get GEE memory errors.")
    parser.add_argument("--application", default=None,
                        choices=["all", "monthly_et", "annual_et", "pet",
                                 "rwdi", "kc", "gpp", "wue"],
                        help="Which output mode to run (default: all).")
    parser.add_argument("--sample-lon", type=float, default=None,
                        help="Longitude for single-pixel timeseries plot.")
    parser.add_argument("--sample-lat", type=float, default=None,
                        help="Latitude for single-pixel timeseries plot.")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    config_path = pathlib.Path(args.config) if args.config else CONFIG_PATH
    cfg = load_config(config_path)
    cfg = merge_args(cfg, args)

    app = cfg.get("application", "all")

    if not cfg.get("tehsil_asset"):
        parser.error("tehsil_asset is required (config.yaml or --tehsil-asset)")
    if not cfg.get("model_aez"):
        parser.error("model_aez is required (config.yaml or --model-aez)")
    if not cfg.get("tehsil_name"):
        cfg["tehsil_name"] = cfg["tehsil_asset"].rstrip("/").split("/")[-1].upper()

    print("\n" + "=" * 68)
    print("  ET-Applications  (GeoTIFF Raster Edition - v3.0 with GPP/WUE/Kc)")
    print("=" * 68)
    for label, key in [
        ("Tehsil", "tehsil_name"),
        ("Year", "year"),
        ("Application", "application"),
        ("Output dir", "output"),
        ("GEE project", "gee_project"),
        ("Model (AEZ)", "model_aez"),
        ("Tehsil asset", "tehsil_asset"),
        ("Chunk size", "chunk_size"),
        ("Plots", "plot"),
        ("MODIS collection", "modis_collection"),
    ]:
        print(f"  {label:<22}: {cfg.get(key, 'N/A')}")
    if cfg.get("sample_lon") is not None:
        print(f"  {'Sample point':<22}: lon={cfg['sample_lon']}, lat={cfg['sample_lat']}")
    print("=" * 68 + "\n")

    init_ee(cfg.get("gee_project", ""))
    _, region = load_tehsil(cfg["tehsil_asset"])

    dispatch = {
        "monthly_et": lambda: run_monthly_et(cfg, region),
        "annual_et": lambda: run_annual_et(cfg, region),
        "pet": lambda: run_pet(cfg, region),
        "rwdi": lambda: run_rwdi(cfg, region),
        "kc": lambda: run_kc(cfg, region),
        "gpp": lambda: run_gpp(cfg, region),
        "wue": lambda: run_wue(cfg, region),
        "all": lambda: run_all(cfg, region),
    }

    result = dispatch[app]()

    if app != "all" and isinstance(result, str):
        output_paths = {app: result}
        run_sample_timeseries(output_paths, cfg)

    print(f"\nDone. All outputs in: {os.path.abspath(cfg.get('output', './results'))}")


if __name__ == "__main__":
    main()
