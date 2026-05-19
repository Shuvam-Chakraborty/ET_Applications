#!/usr/bin/env python3
"""
ET-Applications - Pan-India ET Downscaling CLI  (GEE Asset Export Edition)
===========================================================================
Each output mode now builds its 13-band image fully inside Earth Engine and
exports it directly to a GEE asset. The monthly calculations are performed
entirely server-side in Earth Engine.

  PIXEL CONSISTENCY GUARANTEE
  ----------------------------
  All exported assets produced by the same tehsil + year combination share
  the same bounding box, CRS, and 30 m pixel grid. The AET (Landsat 8)
  pixel grid drives the spatial reference; MODIS-derived bands are
  resampled to this grid before export. Pixels outside the tehsil boundary
  are written as NoData (-9999).

  Band descriptions and valid-pixel counts are stored as Earth Engine image
  properties on the exported assets.

  Core Layers + Derived Applications
  ----------------------------------
  aet           ->  aet_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual total)
  pet           ->  pet_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual total)
  gpp           ->  gpp_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual mean)
  rwdi          ->  rwdi_<TEHSIL>_<YEAR>               13 bands  (12 monthly + annual mean)
  kc            ->  kc_<TEHSIL>_<YEAR>                 13 bands  (12 monthly + annual mean)
  wue           ->  wue_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual mean)
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
  python3 et_applications.py --application aet
  python3 et_applications.py --application kc
  python3 et_applications.py --application gpp
  python3 et_applications.py --application wue
  python3 et_applications.py --application all
"""

import argparse
import calendar
import pathlib
import sys
import time

try:
    import yaml

    def _load_yaml(path):
        with open(path) as f:
            return yaml.safe_load(f)
except ImportError:
    print("[ERROR] pyyaml missing. Run: pip install pyyaml")
    sys.exit(1)

import ee


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
EXPORT_BAND_NAMES = [f"b{i}" for i in range(1, 14)]
CONFIG_PATH = pathlib.Path(__file__).parent / "config.yaml"
MODIS_COL = "MODIS/061/MOD16A2GF"
MCD12Q1_COL = "MODIS/061/MCD12Q1"

# NoData sentinel used in GEE images and written to masked pixels in assets.
NODATA = -9999.0


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
    assets = raw.get("assets", {}) or {}
    cfg["tehsil_asset"] = assets.get("tehsil_asset", "")
    cfg["model_aez"] = assets.get("model_aez", "")
    time_cfg = raw.get("time", {}) or {}
    cfg["year"] = int(time_cfg.get("year", 2022))
    export = raw.get("export", {}) or {}
    cfg["asset_root"] = export.get("asset_root", "")
    cfg["overwrite_assets"] = bool(export.get("overwrite", False))
    cfg["wait_exports"] = bool(export.get("wait_for_tasks", True))
    cfg["poll_seconds"] = int(export.get("poll_interval_seconds", 30))
    cfg["application"] = raw.get("application", "all")
    cfg["modis_collection"] = (raw.get("modis", {}) or {}).get("collection", MODIS_COL)
    print(f"[config] Loaded from: {path}")
    return cfg


def merge_args(cfg: dict, args: argparse.Namespace) -> dict:
    merged = dict(cfg)
    for key, value in [
        ("tehsil_asset", args.tehsil_asset),
        ("model_aez", args.model_aez),
        ("year", args.year),
        ("asset_root", args.asset_root),
        ("gee_project", args.gee_project),
        ("tehsil_name", args.tehsil_name),
        ("application", args.application),
    ]:
        if value is not None:
            merged[key] = value
    if args.no_wait_exports:
        merged["wait_exports"] = False
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


def _fill_monthly_collection(raw_monthly: ee.ImageCollection, value_band: str,
                             fallback_value=None) -> ee.ImageCollection:
    """Fill monthly gaps from neighbouring months within a +/-60 day window."""
    def interpolate(img):
        time_start = img.get("system:time_start")
        neighbours = (
            raw_monthly.select(value_band)
            .filterDate(ee.Date(time_start).advance(-60, "day"),
                        ee.Date(time_start).advance(60, "day"))
        )
        filled = neighbours.mean()
        out = img.select(value_band).unmask(filled)
        if fallback_value is not None:
            out = out.unmask(fallback_value)
        return (
            out.rename(value_band).float()
            .set("month", img.get("month"))
            .set("system:time_start", time_start)
        )

    return raw_monthly.map(interpolate)


def _monthly_collection_to_stack(monthly_col: ee.ImageCollection, value_band: str,
                                 output_prefix: str, region: ee.Geometry) -> ee.Image:
    """Convert a monthly image collection into a named 12-band stack."""
    def rename_month(img):
        month_str = ee.String(ee.Number(img.get("month")).format("%02d"))
        return img.select(value_band).rename(ee.String(output_prefix).cat(month_str)).float()

    named = monthly_col.map(rename_month)
    stack = named.toBands().clip(region)
    current_names = stack.bandNames()
    new_names = current_names.map(lambda n: ee.String(n).split("_").slice(1).join("_"))
    return stack.rename(new_names)


def _make_raw_monthly_ndvi(month, ls_col, year):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()
    monthly_collection = ls_col.filterDate(start, end)
    ndvi = (
        monthly_collection
        .map(lambda img: img.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI"))
        .mean()
        .rename("NDVI")
    )
    return ndvi.set("month", month).set("system:time_start", mid)


def build_gpp_stack(region: ee.Geometry, year: int,
                    proj: ee.Projection) -> ee.Image:
    """
    Build a 12-band monthly GPP image at 30 m using the documented
    Light Use Efficiency method (Monteith 1972; MOD17 framework).

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
    ls_col = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(year_start, year_end)
    )
    months = ee.List.sequence(1, 12)
    raw_ndvi_monthly = ee.ImageCollection.fromImages(
        months.map(lambda m: _make_raw_monthly_ndvi(ee.Number(m), ls_col, year))
    )
    ndvi_monthly = _fill_monthly_collection(raw_ndvi_monthly, "NDVI")
    ndvi_by_month = {
        month: ee.Image(ndvi_monthly.filter(ee.Filter.eq("month", month)).first()).select("NDVI")
        for month in range(1, 13)
    }

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

        ndvi = ndvi_by_month[month]
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


def build_aet_stack(region: ee.Geometry, classifier: ee.Classifier, year: int) -> ee.Image:
    """12-band ET_01...ET_12 stack (0.1 mm/day, mean daily)."""
    ls_col = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(ee.Date.fromYMD(year, 1, 1), ee.Date.fromYMD(year, 12, 31))
    )

    months = ee.List.sequence(1, 12)
    raw_monthly = ee.ImageCollection.fromImages(
        months.map(lambda m: _make_raw_monthly(ee.Number(m), ls_col, region, classifier, year))
    )
    interp_col = _fill_monthly_collection(raw_monthly, "ET_daily", fallback_value=0)
    stack = _monthly_collection_to_stack(interp_col, "ET_daily", "ET_", region)
    return stack


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


def _make_raw_monthly_pet(month, modis_col, year, proj):
    start = ee.Date.fromYMD(year, month, 1)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()
    pet = (
        modis_col.filterDate(start, end)
        .select("PET")
        .mean()
        .divide(8)
        .resample("bilinear")
        .reproject(crs=proj, scale=30)
        .rename("PET_daily")
        .float()
    )
    return pet.set("month", month).set("system:time_start", mid)


def build_pet_stack(region: ee.Geometry, year: int,
                    modis_col_id: str, proj: ee.Projection) -> ee.Image:
    """
    12-band PET stack PET_01...PET_12 (0.1 mm/day) at 30 m.
    MOD16A2 is 8-day composite; divide by 8 for daily rate.
    500 m MODIS pixel bilinearly resampled to the 30 m Landsat grid.
    Months with no MODIS composites are filled using a +/-60 day window.
    """
    modis_col = ee.ImageCollection(modis_col_id).filterBounds(region)
    months = ee.List.sequence(1, 12)
    raw_monthly = ee.ImageCollection.fromImages(
        months.map(lambda m: _make_raw_monthly_pet(ee.Number(m), modis_col, year, proj))
    )
    interp_col = _fill_monthly_collection(raw_monthly, "PET_daily", fallback_value=0)
    stack = _monthly_collection_to_stack(interp_col, "PET_daily", "PET_", region)
    return stack


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


# =============================================================================
# GEE EXPORT HELPERS
# =============================================================================

def _asset_token(value: str) -> str:
    cleaned = []
    for char in str(value).strip().lower():
        cleaned.append(char if char.isalnum() else "_")
    token = "".join(cleaned).strip("_")
    while "__" in token:
        token = token.replace("__", "_")
    return token or "unknown"


def _build_asset_id(cfg: dict, label: str) -> str:
    root = str(cfg.get("asset_root", "")).rstrip("/")
    if not root:
        raise ValueError("asset_root is required (config.yaml export.asset_root or --asset-root)")
    tehsil = _asset_token(cfg.get("tehsil_name", "tehsil"))
    year = int(cfg["year"])
    return f"{root}/{label}_{tehsil}_{year}"


def _asset_exists(asset_id: str) -> bool:
    try:
        ee.data.getAsset(asset_id)
        return True
    except Exception:
        return False


def _prepare_asset_target(asset_id: str, overwrite: bool) -> None:
    if not _asset_exists(asset_id):
        return
    if not overwrite:
        raise RuntimeError(
            f"GEE asset already exists: {asset_id}\n"
            "Set export.overwrite: true in config.yaml to replace it."
        )
    print(f"  Overwriting existing asset -> {asset_id}")
    ee.data.deleteAsset(asset_id)


def _build_common_pixel_mask(region: ee.Geometry,
                             default_proj: ee.Projection) -> ee.Image:
    """Rasterize the tehsil once on the chosen 30 m grid for all outputs."""
    return (
        ee.Image.constant(1)
        .rename("common_mask")
        .setDefaultProjection(default_proj)
        .clip(region)
        .unmask(0)
        .gt(0)
        .selfMask()
    )


def _ee_annual_total_band(monthly_stack: ee.Image, prefix: str, year: int,
                          band_name: str = "annual") -> ee.Image:
    annual = ee.Image.constant(0).float()
    valid_count = ee.Image.constant(0).float()
    for month in range(1, 13):
        month_band = monthly_stack.select(f"{prefix}_{month:02d}")
        days = calendar.monthrange(year, month)[1]
        annual = annual.add(month_band.unmask(0).multiply(days))
        valid_count = valid_count.add(month_band.mask().gt(0).unmask(0))
    return annual.updateMask(valid_count.gt(0)).rename(band_name).float()


def _ee_annual_mean_band(monthly_stack: ee.Image, prefix: str,
                         band_name: str = "annual") -> ee.Image:
    images = [
        monthly_stack.select(f"{prefix}_{month:02d}").rename("annual_src").float()
        for month in range(1, 13)
    ]
    return ee.ImageCollection.fromImages(images).mean().rename(band_name).float()


def build_wue_image(aet_stack: ee.Image, gpp_stack: ee.Image) -> ee.Image:
    """WUE_01...12 = GPP / AET_mm (g C / kg H2O)."""
    bands = []
    for month in range(1, 13):
        aet_mm = aet_stack.select(f"ET_{month:02d}").multiply(0.1)
        wue = (
            gpp_stack.select(f"GPP_{month:02d}")
            .divide(aet_mm)
            .updateMask(aet_mm.gt(0))
        )
        wue = wue.updateMask(wue.gte(0)).updateMask(wue.lte(50))
        bands.append(wue.rename(f"WUE_{month:02d}").float())
    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack


def _apply_image_properties(img: ee.Image, props: dict) -> ee.Image:
    out = img
    for key, value in props.items():
        out = out.set(key, value)
    return out


def _finalize_export_image(monthly_stack: ee.Image, annual_band: ee.Image,
                           region: ee.Geometry, metadata: dict,
                           band_descriptions: list,
                           default_proj: ee.Projection = None,
                           common_mask: ee.Image = None) -> ee.Image:
    image = monthly_stack.addBands(annual_band).rename(EXPORT_BAND_NAMES)
    if default_proj is not None:
        image = image.setDefaultProjection(default_proj)
    if common_mask is not None:
        image = image.updateMask(common_mask)
    image = image.clip(region)
    image = image.unmask(NODATA).float()
    props = {"nodata": NODATA}
    props.update(metadata)
    for idx, desc in enumerate(band_descriptions, start=1):
        props[f"band_{idx}_description"] = desc
    return _apply_image_properties(image, props)


def _start_asset_export(image: ee.Image, asset_id: str, description: str):
    export_kwargs = {
        "image": image,
        "description": description,
        "assetId": asset_id,
        "scale": 30,
        "maxPixels": 1e13,
    }
    task = ee.batch.Export.image.toAsset(**export_kwargs)
    task.start()
    print(f"  Export task started -> {asset_id}")
    return task


def _wait_for_tasks(task_specs: list, poll_seconds: int = 30,
                    fail_on_error: bool = False) -> dict:
    if not task_specs:
        return {}
    poll_seconds = max(5, int(poll_seconds))
    pending = {spec["asset_id"]: spec for spec in task_specs}
    final_statuses = {}
    print(f"\n[exports] Waiting for {len(task_specs)} Earth Engine task(s) ...")
    while pending:
        finished_now = []
        for asset_id, spec in pending.items():
            status = spec["task"].status()
            state = status.get("state", "UNKNOWN")
            if state in {"COMPLETED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"}:
                finished_now.append(asset_id)
                final_statuses[asset_id] = status
                print(f"  [{state}] {spec['label']} -> {asset_id}")
                if status.get("error_message"):
                    print(f"    Error: {status['error_message']}")
        for asset_id in finished_now:
            pending.pop(asset_id, None)
        if pending:
            print(f"  Still running: {len(pending)} task(s). Checking again in {poll_seconds}s ...")
            time.sleep(poll_seconds)
    if fail_on_error:
        failed = []
        for spec in task_specs:
            asset_id = spec["asset_id"]
            status = final_statuses.get(asset_id, {})
            state = status.get("state", "UNKNOWN")
            if state != "COMPLETED":
                message = status.get("error_message", "No error message from Earth Engine.")
                failed.append(f"{spec['label']} ({asset_id}) -> {state}: {message}")
        if failed:
            raise RuntimeError(
                "One or more Earth Engine export tasks did not complete successfully:\n"
                + "\n".join(failed)
            )
    return final_statuses


def _export_product_asset(label: str, display_name: str, image: ee.Image,
                          cfg: dict) -> dict:
    asset_id = _build_asset_id(cfg, label)
    _prepare_asset_target(asset_id, bool(cfg.get("overwrite_assets", False)))
    print(f"  {display_name} asset -> {asset_id}")
    task = _start_asset_export(
        image,
        asset_id,
        description=f"export_{label}_{_asset_token(cfg['tehsil_name'])}_{cfg['year']}",
    )
    return {"asset_id": asset_id, "task": task, "label": label}


# =============================================================================
# UTILITIES
# =============================================================================

# =============================================================================
# CORE LAYER 1 - AET
# =============================================================================

def run_aet(cfg: dict, region: ee.Geometry,
            aet_stack=None) -> str:
    """
    Monthly mean daily AET for every 30 m pixel.
    Output  : aet_<tehsil>_<year> GEE asset (13 bands)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]

    print(f"\n{'=' * 60}")
    print(f"  [aet]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = _build_common_pixel_mask(region, grid_proj)
    aet_monthly = aet_stack.multiply(0.1)
    footprint = aet_monthly.select("ET_01").mask()
    aet_annual_total = (
        _ee_annual_total_band(aet_monthly, "ET", year, band_name="ET_annual")
        .updateMask(footprint)
    )
    image = _finalize_export_image(
        aet_monthly,
        aet_annual_total,
        region,
        metadata={
            "application": "aet",
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "model_aez": cfg["model_aez"],
            "description": "Bands 1-12: mean daily AET per month at 30 m; band 13: annual total AET",
        },
        band_descriptions=[f"ET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["ET_annual_mm"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    task_spec = _export_product_asset("aet", "AET", image, cfg)
    if cfg.get("wait_exports", True):
        _wait_for_tasks([task_spec], cfg.get("poll_seconds", 30))
    return task_spec["asset_id"]


# =============================================================================
# CORE LAYER 2 - PET
# =============================================================================

def run_pet(cfg: dict, region: ee.Geometry, aet_stack=None,
            pet_stack=None) -> str:
    """
    Monthly mean daily PET (MODIS MOD16A2) for every 30 m pixel.
    Output  : pet_<tehsil>_<year> GEE asset (13 bands)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    modis_col = cfg.get("modis_collection", MODIS_COL)

    print(f"\n{'=' * 60}")
    print(f"  [pet]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack (pixel-grid carrier) ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = _build_common_pixel_mask(region, grid_proj)
    footprint = aet_stack.select("ET_01").mask()
    pet_monthly = pet_stack.multiply(0.1).updateMask(footprint)
    pet_annual = _ee_annual_total_band(pet_monthly, "PET", year, band_name="PET_annual").updateMask(footprint)
    image = _finalize_export_image(
        pet_monthly,
        pet_annual,
        region,
        metadata={
            "application": "pet",
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "source": "MODIS MOD16A2",
            "modis_collection": modis_col,
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "Bands 1-12: mean daily PET per month at 30 m; band 13: annual total PET",
        },
        band_descriptions=[f"PET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["PET_annual_mm"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    task_spec = _export_product_asset("pet", "PET", image, cfg)
    if cfg.get("wait_exports", True):
        _wait_for_tasks([task_spec], cfg.get("poll_seconds", 30))
    return task_spec["asset_id"]


# =============================================================================
# DERIVED APPLICATION 1 - RWDI
# =============================================================================

def run_rwdi(cfg: dict, region: ee.Geometry, aet_stack=None,
             pet_stack=None) -> str:
    """
    RWDI = (1 - AET/PET) x 100 (%)
    Output  : rwdi_<tehsil>_<year> GEE asset (13 bands)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    modis_col = cfg.get("modis_collection", MODIS_COL)

    print(f"\n{'=' * 60}")
    print(f"  [rwdi]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    rwdi_img = build_rwdi_image(aet_stack, pet_stack)
    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = _build_common_pixel_mask(region, grid_proj)
    footprint = aet_stack.select("ET_01").mask()
    rwdi_monthly = rwdi_img.updateMask(footprint)
    rwdi_annual = _ee_annual_mean_band(rwdi_monthly, "RWDI", band_name="RWDI_annual").updateMask(footprint)
    image = _finalize_export_image(
        rwdi_monthly,
        rwdi_annual,
        region,
        metadata={
            "application": "rwdi",
            "units": "percent",
            "formula": "(1 - AET/PET) * 100",
            "modis_collection": modis_col,
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "Relative Water Deficit Index per month + annual mean",
        },
        band_descriptions=[f"RWDI_{abbr}" for abbr in MONTH_ABBR] + ["RWDI_annual"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    task_spec = _export_product_asset("rwdi", "RWDI", image, cfg)
    if cfg.get("wait_exports", True):
        _wait_for_tasks([task_spec], cfg.get("poll_seconds", 30))
    return task_spec["asset_id"]


def _run_kc_application(cfg: dict, region: ee.Geometry, aet_stack=None,
                        pet_stack=None) -> str:
    """Shared runner for the monthly Kc proxy (AET/PET)."""
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]
    modis_col = cfg.get("modis_collection", MODIS_COL)
    label = "kc"
    title = "Crop Coefficient (Kc)"
    stack_builder = build_kc_image
    band_names = [f"KC_{abbr}" for abbr in MONTH_ABBR] + ["KC_annual"]
    metadata = {
        "application": "kc",
        "units": "ratio (AET/PET)",
        "formula": "AET / PET",
        "modis_collection": modis_col,
        "year": str(year),
        "tehsil": tehsil,
        "tehsil_asset": cfg["tehsil_asset"],
        "description": "Monthly Kc proxy from AET/PET + annual mean",
    }

    print(f"\n{'=' * 60}")
    print(f"  [{label}]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    ratio_img = stack_builder(aet_stack, pet_stack)
    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = _build_common_pixel_mask(region, grid_proj)
    footprint = aet_stack.select("ET_01").mask()
    ratio_monthly = ratio_img.updateMask(footprint)
    ratio_annual = _ee_annual_mean_band(ratio_monthly, "KC", band_name="KC_annual").updateMask(footprint)
    image = _finalize_export_image(
        ratio_monthly,
        ratio_annual,
        region,
        metadata=metadata,
        band_descriptions=band_names,
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    task_spec = _export_product_asset(label, title, image, cfg)
    if cfg.get("wait_exports", True):
        _wait_for_tasks([task_spec], cfg.get("poll_seconds", 30))
    return task_spec["asset_id"]


# =============================================================================
# DERIVED APPLICATION 2 - KC
# =============================================================================

def run_kc(cfg: dict, region: ee.Geometry, aet_stack=None,
           pet_stack=None) -> str:
    """
    Kc proxy = AET / PET
    Output  : kc_<tehsil>_<year> GEE asset (13 bands)
    """
    return _run_kc_application(cfg, region, aet_stack=aet_stack,
                               pet_stack=pet_stack)


# =============================================================================
# CORE LAYER 3 - GPP
# =============================================================================

def run_gpp(cfg: dict, region: ee.Geometry, aet_stack=None,
            gpp_stack=None) -> str:
    """
    Monthly mean daily GPP via the MOD17 Light Use Efficiency framework.

        GPP = PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]

    print(f"\n{'=' * 60}")
    print(f"  [gpp]  {tehsil}  |  {year}")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack (pixel-grid carrier) ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

    if gpp_stack is None:
        print("  Building GPP stack (LUE model: GLDAS + Landsat NDVI + MCD12Q1) ...")
        proj = _get_proj_30m(region, year)
        gpp_stack = build_gpp_stack(region, year, proj)

    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = _build_common_pixel_mask(region, grid_proj)
    footprint = aet_stack.select("ET_01").mask()
    gpp_monthly = gpp_stack.updateMask(footprint)
    gpp_annual = _ee_annual_mean_band(gpp_monthly, "GPP", band_name="GPP_annual").updateMask(footprint)
    image = _finalize_export_image(
        gpp_monthly,
        gpp_annual,
        region,
        metadata={
            "application": "gpp",
            "units": "g C / m2 / day",
            "method": "LUE: PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "par_source": "GLDAS SWdown_f_tavg * 0.0864 * 0.45",
            "fapar_source": "Landsat 8 NDVI -> 1.24*NDVI - 0.168",
            "bplut_source": "MOD17 C6 / MCD12Q1 IGBP LC_Type1",
            "tmin_source": "GLDAS Tair_f_inst monthly minimum (K-273.15)",
            "vpd_source": "GLDAS Tair+Qair+Psurf Magnus formula",
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "Mean daily GPP per month (LUE) + annual mean at 30 m",
        },
        band_descriptions=[f"GPP_{abbr}_gC_m2_day" for abbr in MONTH_ABBR] + ["GPP_annual_mean"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    task_spec = _export_product_asset("gpp", "GPP", image, cfg)
    if cfg.get("wait_exports", True):
        _wait_for_tasks([task_spec], cfg.get("poll_seconds", 30))
    return task_spec["asset_id"]


# =============================================================================
# DERIVED APPLICATION 3 - WUE
# =============================================================================

def run_wue(cfg: dict, region: ee.Geometry, aet_stack=None,
            gpp_stack=None) -> str:
    """
    Water Use Efficiency = GPP / AET (g C / kg H2O)
    Output  : wue_<tehsil>_<year> GEE asset (13 bands)
    """
    tehsil = cfg["tehsil_name"]
    year = cfg["year"]

    print(f"\n{'=' * 60}")
    print(f"  [wue]  {tehsil}  |  {year}  |  WUE = GPP / AET")
    print(f"{'=' * 60}")

    if aet_stack is None:
        print("  Building AET stack (Landsat 8 + GLDAS -> RF model) ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

    if gpp_stack is None:
        print("  Building GPP stack (LUE: GLDAS + Landsat NDVI + MCD12Q1) ...")
        proj = _get_proj_30m(region, year)
        gpp_stack = build_gpp_stack(region, year, proj)

    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = _build_common_pixel_mask(region, grid_proj)
    footprint = aet_stack.select("ET_01").mask()
    wue_monthly = build_wue_image(aet_stack, gpp_stack).updateMask(footprint)
    wue_annual = _ee_annual_mean_band(wue_monthly, "WUE", band_name="WUE_annual").updateMask(footprint)
    image = _finalize_export_image(
        wue_monthly,
        wue_annual,
        region,
        metadata={
            "application": "wue",
            "units": "g C / kg H2O",
            "formula": "GPP (LUE) / AET (RF downscaled)",
            "gpp_method": "PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "aet_method": "Landsat8 + GLDAS features -> Random Forest",
            "bplut_source": "MOD17 C6 BPLUT / MCD12Q1 IGBP LC_Type1",
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": (
                "WUE = GPP/AET per month + annual mean at 30 m. "
                "Units: g C fixed per kg of water transpired."
            ),
        },
        band_descriptions=[f"WUE_{abbr}_gC_per_kgH2O" for abbr in MONTH_ABBR] + ["WUE_annual_mean"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    task_spec = _export_product_asset("wue", "WUE", image, cfg)
    if cfg.get("wait_exports", True):
        _wait_for_tasks([task_spec], cfg.get("poll_seconds", 30))
    return task_spec["asset_id"]


# =============================================================================
# ALL  (single combined download - guaranteed pixel consistency)
# =============================================================================

def run_all(cfg: dict, region: ee.Geometry) -> dict:
    """
    Run the three core layers first, wait for them to finish exporting,
    then export the three derived applications.

    Export sequencing guarantee:
      1. Start AET, PET, and GPP export tasks.
      2. Wait until all three core exports reach a terminal state.
      3. Only then start RWDI, KC, and WUE export tasks.

    Pixel consistency guarantee:
      All outputs are derived from the same AET/PET/GPP stacks.
      They are therefore spatially identical - same CRS, transform, and
      pixel grid. No post-processing alignment is needed.
    """
    year = cfg["year"]
    tehsil = cfg["tehsil_name"]
    modis_col = cfg.get("modis_collection", MODIS_COL)

    print(f"\n{'=' * 60}")
    print("  [all] Building shared GEE stacks ...")
    print(f"{'=' * 60}")

    print("\n  [1/3] Building AET stack (Landsat 8 + GLDAS -> RF model) ...")
    classifier = build_classifier(cfg["model_aez"])
    aet_stack = build_aet_stack(region, classifier, year)

    print("\n  [2/3] Building PET stack (MODIS MOD16A2) ...")
    proj = _get_proj_30m(region, year)
    pet_stack = build_pet_stack(region, year, modis_col, proj)

    print("\n  [3/3] Building GPP stack (LUE: GLDAS + Landsat NDVI + MCD12Q1) ...")
    gpp_stack = build_gpp_stack(region, year, proj)
    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = _build_common_pixel_mask(region, grid_proj)
    footprint = aet_stack.select("ET_01").mask()
    results = {}
    core_task_specs = []
    derived_task_specs = []

    aet_monthly = aet_stack.multiply(0.1)
    aet_annual = (
        _ee_annual_total_band(aet_monthly, "ET", year, band_name="ET_annual")
        .updateMask(footprint)
    )
    aet_image = _finalize_export_image(
        aet_monthly,
        aet_annual,
        region,
        metadata={
            "application": "aet",
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "model_aez": cfg["model_aez"],
            "description": "Bands 1-12: mean daily AET per month at 30 m; band 13: annual total AET",
        },
        band_descriptions=[f"ET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["ET_annual_mm"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = _export_product_asset("aet", "AET", aet_image, cfg)
    core_task_specs.append(spec)
    results["aet"] = spec["asset_id"]

    pet_monthly = pet_stack.multiply(0.1).updateMask(footprint)
    pet_annual = _ee_annual_total_band(pet_monthly, "PET", year, band_name="PET_annual").updateMask(footprint)
    pet_image = _finalize_export_image(
        pet_monthly,
        pet_annual,
        region,
        metadata={
            "application": "pet",
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "source": "MODIS MOD16A2",
            "modis_collection": modis_col,
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "Bands 1-12: mean daily PET per month at 30 m; band 13: annual total PET",
        },
        band_descriptions=[f"PET_{abbr}_daily_mm" for abbr in MONTH_ABBR] + ["PET_annual_mm"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = _export_product_asset("pet", "PET", pet_image, cfg)
    core_task_specs.append(spec)
    results["pet"] = spec["asset_id"]

    gpp_monthly = gpp_stack.updateMask(footprint)
    gpp_annual = _ee_annual_mean_band(gpp_monthly, "GPP", band_name="GPP_annual").updateMask(footprint)
    gpp_image = _finalize_export_image(
        gpp_monthly,
        gpp_annual,
        region,
        metadata={
            "application": "gpp",
            "units": "g C / m2 / day",
            "method": "LUE: PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "par_source": "GLDAS SWdown_f_tavg * 0.0864 * 0.45",
            "fapar_source": "Landsat 8 NDVI -> 1.24*NDVI - 0.168",
            "bplut_source": "MOD17 C6 / MCD12Q1 IGBP LC_Type1",
            "tmin_source": "GLDAS Tair_f_inst monthly minimum (K-273.15)",
            "vpd_source": "GLDAS Tair+Qair+Psurf Magnus formula",
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "Mean daily GPP per month (LUE) + annual mean",
        },
        band_descriptions=[f"GPP_{abbr}_gC_m2_day" for abbr in MONTH_ABBR] + ["GPP_annual_mean"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = _export_product_asset("gpp", "GPP", gpp_image, cfg)
    core_task_specs.append(spec)
    results["gpp"] = spec["asset_id"]

    print("\n  [phase 1/2] Waiting for core exports (AET, PET, GPP) before derived exports ...")
    _wait_for_tasks(core_task_specs, cfg.get("poll_seconds", 30), fail_on_error=True)

    print("\n  [phase 2/2] Starting derived exports (RWDI, KC, WUE) from the shared core stacks ...")

    rwdi_monthly = build_rwdi_image(aet_stack, pet_stack).updateMask(footprint)
    rwdi_annual = _ee_annual_mean_band(rwdi_monthly, "RWDI", band_name="RWDI_annual").updateMask(footprint)
    rwdi_image = _finalize_export_image(
        rwdi_monthly,
        rwdi_annual,
        region,
        metadata={
            "application": "rwdi",
            "units": "percent",
            "formula": "(1 - AET/PET) * 100",
            "modis_collection": modis_col,
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "RWDI per month + annual mean",
        },
        band_descriptions=[f"RWDI_{abbr}" for abbr in MONTH_ABBR] + ["RWDI_annual"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = _export_product_asset("rwdi", "RWDI", rwdi_image, cfg)
    derived_task_specs.append(spec)
    results["rwdi"] = spec["asset_id"]

    kc_monthly = build_kc_image(aet_stack, pet_stack).updateMask(footprint)
    kc_annual = _ee_annual_mean_band(kc_monthly, "KC", band_name="KC_annual").updateMask(footprint)
    kc_image = _finalize_export_image(
        kc_monthly,
        kc_annual,
        region,
        metadata={
            "application": "kc",
            "units": "ratio (AET/PET)",
            "formula": "AET / PET",
            "modis_collection": modis_col,
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "Monthly Kc proxy from AET/PET + annual mean",
        },
        band_descriptions=[f"KC_{abbr}" for abbr in MONTH_ABBR] + ["KC_annual"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = _export_product_asset("kc", "Crop Coefficient (Kc)", kc_image, cfg)
    derived_task_specs.append(spec)
    results["kc"] = spec["asset_id"]

    wue_monthly = build_wue_image(aet_stack, gpp_stack).updateMask(footprint)
    wue_annual = _ee_annual_mean_band(wue_monthly, "WUE", band_name="WUE_annual").updateMask(footprint)
    wue_image = _finalize_export_image(
        wue_monthly,
        wue_annual,
        region,
        metadata={
            "application": "wue",
            "units": "g C / kg H2O",
            "formula": "GPP (LUE) / AET (RF downscaled)",
            "gpp_method": "PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "aet_method": "Landsat8 + GLDAS features -> Random Forest",
            "bplut_source": "MOD17 C6 BPLUT / MCD12Q1 IGBP LC_Type1",
            "year": str(year),
            "tehsil": tehsil,
            "tehsil_asset": cfg["tehsil_asset"],
            "description": "WUE per month + annual mean at 30 m",
        },
        band_descriptions=[f"WUE_{abbr}_gC_per_kgH2O" for abbr in MONTH_ABBR] + ["WUE_annual_mean"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = _export_product_asset("wue", "WUE", wue_image, cfg)
    derived_task_specs.append(spec)
    results["wue"] = spec["asset_id"]

    if cfg.get("wait_exports", True):
        _wait_for_tasks(derived_task_specs, cfg.get("poll_seconds", 30), fail_on_error=True)
    else:
        print("\n[exports] Derived export tasks started. Final completion polling skipped (wait_exports=false).")
    return results


# =============================================================================
# CLI
# =============================================================================

def build_parser():
    parser = argparse.ArgumentParser(
        prog="et_applications",
        description="Pan-India ET Downscaling + GPP/WUE - direct GEE asset export",
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
    parser.add_argument("--asset-root", default=None,
                        help="Parent GEE asset path where exports will be created")
    parser.add_argument("--overwrite-assets", action="store_true", default=False,
                        help="Delete an existing target asset before exporting.")
    parser.add_argument("--no-wait-exports", action="store_true", default=False,
                        help="Start GEE export tasks and exit without polling for completion.")
    parser.add_argument("--gee-project", default=None)
    parser.add_argument("--tehsil-name", default=None)
    parser.add_argument("--application", default=None,
                        choices=["all", "aet", "pet",
                                 "rwdi", "kc", "gpp", "wue"],
                        help="Which output mode to run (default: all).")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    config_path = pathlib.Path(args.config) if args.config else CONFIG_PATH
    cfg = load_config(config_path)
    cfg = merge_args(cfg, args)
    if args.overwrite_assets:
        cfg["overwrite_assets"] = True

    app = cfg.get("application", "all")

    if not cfg.get("tehsil_asset"):
        parser.error("tehsil_asset is required (config.yaml or --tehsil-asset)")
    if not cfg.get("model_aez"):
        parser.error("model_aez is required (config.yaml or --model-aez)")
    if not cfg.get("asset_root"):
        parser.error("export.asset_root is required (config.yaml or --asset-root)")
    if not cfg.get("tehsil_name"):
        cfg["tehsil_name"] = cfg["tehsil_asset"].rstrip("/").split("/")[-1].upper()

    print("\n" + "=" * 68)
    print("  ET-Applications  (GEE Asset Export Edition - v3.0 with GPP/WUE/Kc)")
    print("=" * 68)
    for label, key in [
        ("Tehsil", "tehsil_name"),
        ("Year", "year"),
        ("Output mode", "application"),
        ("Asset root", "asset_root"),
        ("Overwrite assets", "overwrite_assets"),
        ("GEE project", "gee_project"),
        ("Model (AEZ)", "model_aez"),
        ("Tehsil asset", "tehsil_asset"),
        ("Wait for exports", "wait_exports"),
        ("Poll interval (s)", "poll_seconds"),
        ("MODIS collection", "modis_collection"),
    ]:
        print(f"  {label:<22}: {cfg.get(key, 'N/A')}")
    print("=" * 68 + "\n")

    init_ee(cfg.get("gee_project", ""))
    _, region = load_tehsil(cfg["tehsil_asset"])

    dispatch = {
        "aet": lambda: run_aet(cfg, region),
        "pet": lambda: run_pet(cfg, region),
        "rwdi": lambda: run_rwdi(cfg, region),
        "kc": lambda: run_kc(cfg, region),
        "gpp": lambda: run_gpp(cfg, region),
        "wue": lambda: run_wue(cfg, region),
        "all": lambda: run_all(cfg, region),
    }

    result = dispatch[app]()
    if app == "all":
        print("\nDone. Export assets:")
        for label, asset_id in result.items():
            print(f"  {label:<5} -> {asset_id}")
    else:
        print(f"\nDone. Export asset: {result}")


if __name__ == "__main__":
    main()
