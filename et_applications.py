#!/usr/bin/env python3
"""
ET-Applications — Pan-India ET Downscaling CLI  (Pixel-Wise CSV Edition)
=========================================================================
Each application writes its OWN CSV (one row per 30 m pixel).
All CSVs share pixel_id / longitude / latitude as the join key.

  PIXEL CONSISTENCY GUARANTEE
  ----------------------------
  All CSVs produced by the same tehsil + year combination always contain
  exactly the same set of pixels in the same order, regardless of whether
  you run the applications separately or all at once.

  How it works:
  * The canonical 30 m pixel grid is always defined by the AET (Landsat 8)
    stack — the image with the densest native coverage.
  * MODIS-derived bands (PET, RWDI, Water Stress) are combined with an AET
    carrier band before sampling so that the download is driven by AET pixels.
    Pixels where MODIS has no data are written as NaN in the CSV.
  * pixel_id is assigned after sorting by (longitude, latitude), making it
    deterministic and reproducible across separate runs.

  Applications
  ------------
  monthly_et    ->  monthly_et_<TEHSIL>_<YEAR>.csv
  annual_et     ->  annual_et_<TEHSIL>_<YEAR>.csv
  pet           ->  pet_<TEHSIL>_<YEAR>.csv
  rwdi          ->  rwdi_<TEHSIL>_<YEAR>.csv
  water_stress  ->  water_stress_<TEHSIL>_<YEAR>.csv
  all           ->  all five CSVs (ONE combined download, stacks built once)
  merge         ->  merged_<TEHSIL>_<YEAR>.csv  (no GEE calls, offline join)

  Quick-start
  -----------
  python et_applications.py                          # all from config.yaml
  python et_applications.py --application monthly_et
  python et_applications.py --application pet
  python et_applications.py --application rwdi
  python et_applications.py --application water_stress
  python et_applications.py --application merge      # join existing CSVs
  python et_applications.py --application all --plot
"""

import argparse
import calendar
import itertools
import os
import pathlib
import sys
import time

try:
    import yaml
    def _load_yaml(p):
        with open(p) as f:
            return yaml.safe_load(f)
except ImportError:
    print("[ERROR] pyyaml missing.  Run: pip install pyyaml")
    sys.exit(1)

import ee
import pandas as pd
import numpy as np

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
FEATURE_BANDS = [
    'MSAVI', 'NDMI', 'NDVI', 'NDWI', 'SAVI', 'NDBI', 'NDIIB7', 'Albedo', 'LST',
    'Rainf_tavg', 'RootMoist_inst', 'SoilMoi0_10cm_inst', 'CanopInt_inst',
    'AvgSurfT_inst', 'Qair_f_inst', 'Wind_f_inst', 'Psurf_f_inst',
    'SoilTMP0_10cm_inst', 'Qsb_acc', 'Swnet_tavg', 'Lwnet_tavg',
    'Qg_tavg', 'Qh_tavg', 'Qle_tavg', 'SWdown_f_tavg', 'Tair_f_inst',
]
MONTH_ABBR  = ['Jan','Feb','Mar','Apr','May','Jun',
               'Jul','Aug','Sep','Oct','Nov','Dec']
CONFIG_PATH = pathlib.Path(__file__).parent / 'config.yaml'
MODIS_COL   = 'MODIS/061/MOD16A2'

# Sentinel written to pixels where MODIS has no data.
# Converted to NaN when the CSV is assembled.
NODATA = -9999.0

RWDI_CLASSES = [
    (0,  30, '#228B22', 'Normal / Irrigated'),
    (30, 50, '#9ACD32', 'Mild Stress'),
    (50, 70, '#FFFF00', 'Moderate Stress'),
    (70, 80, '#FFA500', 'High Stress'),
    (80, 90, '#F08080', 'Severe Stress'),
    (90,100, '#FF0000', 'Extreme Drought'),
]


# =============================================================================
# CONFIG
# =============================================================================

def load_config(path: pathlib.Path) -> dict:
    if not path.exists():
        print(f"[WARN] config.yaml not found at {path}. Using CLI args only.")
        return {}
    raw = _load_yaml(str(path))
    cfg = {}
    cfg['gee_project']  = raw.get('gee_project', '')
    t = raw.get('tehsil', {}) or {}
    cfg['tehsil_name']  = t.get('name', '')
    cfg['tehsil_state'] = t.get('state', '')
    cfg['tehsil_dist']  = t.get('district', '')
    a = raw.get('assets', {}) or {}
    cfg['tehsil_asset'] = a.get('tehsil_asset', '')
    cfg['model_aez']    = a.get('model_aez', '')
    ti = raw.get('time', {}) or {}
    cfg['year']         = int(ti.get('year', 2022))
    cmp = raw.get('compute', {}) or {}
    cfg['chunk_size']   = int(cmp.get('chunk_size', 25000))
    out = raw.get('output', {}) or {}
    cfg['output']       = out.get('directory', './results')
    cfg['plot']         = bool(out.get('plot', False))
    cfg['application']  = raw.get('application', 'all')
    cfg['modis_collection'] = (raw.get('modis', {}) or {}).get('collection', MODIS_COL)
    sp = raw.get('sample_point', {}) or {}
    cfg['sample_lon'] = sp.get('lon') or sp.get('longitude') or None
    cfg['sample_lat'] = sp.get('lat') or sp.get('latitude') or None
    print(f"[config] Loaded from: {path}")
    return cfg


def merge_args(cfg: dict, args: argparse.Namespace) -> dict:
    m = dict(cfg)
    for k, v in [('tehsil_asset', args.tehsil_asset),
                 ('model_aez',    args.model_aez),
                 ('year',         args.year),
                 ('output',       args.output),
                 ('gee_project',  args.gee_project),
                 ('tehsil_name',  args.tehsil_name),
                 ('chunk_size',   args.chunk_size),
                 ('application',  args.application),
                 ('sample_lon',   args.sample_lon),
                 ('sample_lat',   args.sample_lat)]:
        if v is not None:
            m[k] = v
    if args.plot:
        m['plot'] = True
    return m


# =============================================================================
# GEE HELPERS
# =============================================================================

def init_ee(project=''):
    try:
        ee.Initialize(project=project) if project else ee.Initialize()
        print("[EE] Initialised.")
    except Exception:
        print("[EE] Running authenticate ...")
        ee.Authenticate()
        ee.Initialize(project=project) if project else ee.Initialize()


def load_tehsil(asset: str):
    fc     = ee.FeatureCollection(asset)
    region = fc.geometry()
    return fc, region


def build_classifier(model_path: str) -> ee.Classifier:
    trees = (ee.FeatureCollection(model_path)
             .aggregate_array('tree')
             .map(lambda s: ee.String(s).replace('#.*', '', 'g').trim()))
    return ee.Classifier.decisionTreeEnsemble(trees)


def calc_landsat_indices(img: ee.Image) -> ee.Image:
    ndvi   = img.normalizedDifference(['SR_B5', 'SR_B4']).rename('NDVI')
    savi   = img.expression('((NIR-R)/(NIR+R+0.5))*1.5',
                            {'NIR': img.select('SR_B5'), 'R': img.select('SR_B4')}).rename('SAVI')
    msavi  = img.expression('(2*NIR+1-sqrt(pow((2*NIR+1),2)-8*(NIR-R)))/2',
                            {'NIR': img.select('SR_B5'), 'R': img.select('SR_B4')}).rename('MSAVI')
    ndbi   = img.normalizedDifference(['SR_B6', 'SR_B5']).rename('NDBI')
    ndwi   = img.normalizedDifference(['SR_B3', 'SR_B5']).rename('NDWI')
    ndmi   = img.normalizedDifference(['SR_B5', 'SR_B6']).rename('NDMI')
    ndiib7 = img.normalizedDifference(['SR_B5', 'SR_B7']).rename('NDIIB7')
    albedo = img.expression(
        '((0.356*B1)+(0.130*B2)+(0.373*B3)+(0.085*B4)+(0.072*B5)-0.018)/1.016',
        {'B1': img.select('SR_B1'), 'B2': img.select('SR_B2'),
         'B3': img.select('SR_B3'), 'B4': img.select('SR_B4'),
         'B5': img.select('SR_B5')}).rename('Albedo')
    lst = img.select('ST_B10').multiply(0.00341802).add(149.0).rename('LST')
    return img.addBands([ndvi, savi, msavi, ndbi, ndwi, ndmi, ndiib7, albedo, lst])


def predict_daily_et(ls_img: ee.Image, region: ee.Geometry,
                     classifier: ee.Classifier) -> ee.Image:
    idx = calc_landsat_indices(ls_img)
    clim = (ee.ImageCollection('NASA/GLDAS/V021/NOAH/G025/T3H')
            .filterBounds(region)
            .filterDate(ls_img.date().advance(-12, 'hour'),
                        ls_img.date().advance(12,  'hour'))
            .mean()
            .resample('bilinear')
            .reproject(crs=ls_img.select('SR_B5').projection(), scale=30))
    return (idx.addBands(clim).select(FEATURE_BANDS)
              .classify(classifier)
              .rename('ET_daily')
              .set('system:time_start', ls_img.date().millis()))


# =============================================================================
# IMAGE BUILDERS
# =============================================================================

def _get_proj_30m(region: ee.Geometry, year: int) -> ee.Projection:
    """Return the 30 m Landsat projection for this region/year."""
    ls_ref = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
              .filterBounds(region)
              .filterDate(ee.Date.fromYMD(year, 1, 1),
                          ee.Date.fromYMD(year, 12, 31))
              .first())
    return ls_ref.select('SR_B5').projection()


def build_aet_stack(region: ee.Geometry,
                    classifier: ee.Classifier,
                    year: int) -> tuple:
    """
    Returns (aet_stack, gap_flags).
    aet_stack : ee.Image, 12 bands  ET_01...ET_12  (0.1 mm/day, mean daily)
    gap_flags : list[bool], True = month was gap-filled by +/-60-day neighbour mean
    """
    ls_col = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
              .filterBounds(region)
              .filterDate(ee.Date.fromYMD(year, 1, 1),
                          ee.Date.fromYMD(year, 12, 31)))

    scene_counts = []
    for m in range(1, 13):
        start = ee.Date.fromYMD(year, m, 1)
        end   = start.advance(1, 'month')
        n     = ls_col.filterDate(start, end).size().getInfo()
        scene_counts.append(n)
        print(f"  {MONTH_ABBR[m-1]:>3}: {n} Landsat scene(s)")

    months      = ee.List.sequence(1, 12)
    raw_monthly = ee.ImageCollection.fromImages(months.map(
        lambda m: _make_raw_monthly(ee.Number(m), ls_col, region, classifier, year)))

    def interpolate(img):
        t          = img.get('system:time_start')
        neighbours = (raw_monthly.select('ET_daily')
                      .filterDate(ee.Date(t).advance(-60, 'day'),
                                  ee.Date(t).advance( 60, 'day')))
        filled     = neighbours.mean()
        et_filled  = img.select('ET_daily').unmask(filled).unmask(0)
        m_str      = ee.String(ee.Number(img.get('month')).format('%02d'))
        return et_filled.rename(ee.String('ET_').cat(m_str)).float()

    interp_col = raw_monthly.map(interpolate)
    stack      = interp_col.toBands().clip(region)
    cur  = stack.bandNames()
    new  = cur.map(lambda n: ee.String(n).split('_').slice(1).join('_'))
    stack = stack.rename(new)

    gap_flags = [n == 0 for n in scene_counts]
    return stack, gap_flags


def _make_raw_monthly(m, ls_col, region, classifier, year):
    start = ee.Date.fromYMD(year, m, 1)
    end   = start.advance(1, 'month')
    mid   = start.advance(15, 'day').millis()
    mc    = ls_col.filterDate(start, end)
    et    = (mc.map(lambda img: predict_daily_et(img, region, classifier))
               .mean()
               .rename('ET_daily'))
    return et.set('month', m).set('system:time_start', mid)


def build_pet_stack(region: ee.Geometry,
                    year: int,
                    modis_col_id: str,
                    proj: ee.Projection) -> ee.Image:
    """
    12-band PET stack  PET_01...PET_12  (0.1 mm/day) at 30 m.
    MOD16A2 is 8-day composite; divide by 8 for daily rate.
    500 m MODIS pixel value bilinearly resampled to the 30 m Landsat grid.
    Pixels are left masked where MODIS has no data — caller applies unmask.
    """
    modis_col = ee.ImageCollection(modis_col_id).filterBounds(region)
    bands = []
    for m in range(1, 13):
        start = ee.Date.fromYMD(year, m, 1)
        end   = start.advance(1, 'month')
        pet   = (modis_col.filterDate(start, end)
                 .select('PET')
                 .mean()
                 .divide(8)
                 .resample('bilinear')
                 .reproject(crs=proj, scale=30)
                 .rename(f'PET_{m:02d}')
                 .float())
        bands.append(pet)
    stack = bands[0]
    for b in bands[1:]:
        stack = stack.addBands(b)
    return stack.clip(region)


def build_annual_et_image(aet_stack: ee.Image, year: int) -> ee.Image:
    """annual_ET_01mm = sum_m(daily_ET_m x days_in_month_m)"""
    annual = aet_stack.select('ET_01').multiply(calendar.monthrange(year, 1)[1])
    for m in range(2, 13):
        days   = calendar.monthrange(year, m)[1]
        annual = annual.add(aet_stack.select(f'ET_{m:02d}').multiply(days))
    return annual.rename('annual_ET_01mm').float()


def build_rwdi_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """RWDI_01...12 = (1 - AET/PET) x 100  (%)"""
    bands = []
    for m in range(1, 13):
        rwdi = (ee.Image(1)
                .subtract(aet_stack.select(f'ET_{m:02d}')
                          .divide(pet_stack.select(f'PET_{m:02d}')))
                .multiply(100)
                .rename(f'RWDI_{m:02d}')
                .float())
        bands.append(rwdi)
    stack = bands[0]
    for b in bands[1:]:
        stack = stack.addBands(b)
    return stack


def build_ws_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """WS_01...12 = AET / PET  (0-1)"""
    bands = []
    for m in range(1, 13):
        ws = (aet_stack.select(f'ET_{m:02d}')
              .divide(pet_stack.select(f'PET_{m:02d}'))
              .rename(f'WS_{m:02d}')
              .float())
        bands.append(ws)
    stack = bands[0]
    for b in bands[1:]:
        stack = stack.addBands(b)
    return stack


def build_combined_image(aet_stack: ee.Image,
                         pet_stack: ee.Image,
                         year: int) -> ee.Image:
    """
    Combine all bands into ONE image for a single-pass download.

    Pixel grid rule
    ---------------
    AET (Landsat 30 m native) defines the canonical pixel set.
    PET, RWDI, and Water Stress bands are filled with NODATA (-9999)
    where MODIS has no data, so every AET pixel appears in the output.
    Callers must replace NODATA with NaN after downloading.

    Band layout (49 bands total):
        ET_01...ET_12          (0.1 mm/day)    -- AET, drives pixel grid
        PET_01...PET_12        (0.1 mm/day)    -- MODIS, NODATA where missing
        annual_ET_01mm         (0.1 mm/yr)     -- derived from AET
        RWDI_01...RWDI_12      (%)             -- NODATA where PET missing
        WS_01...WS_12          (0-1)           -- NODATA where PET missing
    """
    annual_img = build_annual_et_image(aet_stack, year)
    rwdi_img   = build_rwdi_image(aet_stack, pet_stack)
    ws_img     = build_ws_image(aet_stack, pet_stack)

    return (aet_stack
            .addBands(pet_stack .unmask(NODATA))
            .addBands(annual_img)
            .addBands(rwdi_img  .unmask(NODATA))
            .addBands(ws_img    .unmask(NODATA)))


# =============================================================================
# TILED PIXEL DOWNLOAD
# =============================================================================

def _download_image(img: ee.Image,
                    region: ee.Geometry,
                    chunk_size: int = 25000,
                    label: str = '') -> pd.DataFrame:
    """
    Sample every 30 m pixel centroid in region from img.
    Tiles the bounding box to stay within GEE memory limits.
    Returns a raw DataFrame (unsorted, no pixel_id).
    """
    bbox   = region.bounds().getInfo()['coordinates'][0]
    lons   = [c[0] for c in bbox]
    lats   = [c[1] for c in bbox]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    deg_per_px  = 0.00027
    tile_side   = (chunk_size ** 0.5) * deg_per_px
    lon_tiles   = max(1, int(np.ceil((max_lon - min_lon) / tile_side)))
    lat_tiles   = max(1, int(np.ceil((max_lat - min_lat) / tile_side)))
    lon_step    = (max_lon - min_lon) / lon_tiles
    lat_step    = (max_lat - min_lat) / lat_tiles
    total_tiles = lon_tiles * lat_tiles

    tag = f"[{label}]" if label else "[download]"
    print(f"  {tag} Tiling: {lon_tiles} x {lat_tiles} = {total_tiles} tiles "
          f"(chunk approx {chunk_size:,} px each)")

    all_rows   = []
    tile_count = 0

    for i, j in itertools.product(range(lon_tiles), range(lat_tiles)):
        x0 = min_lon + i * lon_step
        x1 = x0 + lon_step
        y0 = min_lat + j * lat_step
        y1 = y0 + lat_step

        tile_geom  = (ee.Geometry.Rectangle([x0, y0, x1, y1])
                      .intersection(region, 1))
        tile_count += 1

        for attempt in range(4):
            try:
                fc    = img.sample(region=tile_geom, scale=30,
                                   geometries=True, dropNulls=False)
                feats = fc.getInfo().get('features', [])
                break
            except Exception as exc:
                if attempt == 3:
                    print(f"  [WARN] Tile ({i},{j}) failed after 4 attempts: {exc}")
                    feats = []
                    break
                wait = 2 ** attempt
                print(f"  [retry {attempt+1}] tile ({i},{j}): {exc} -- waiting {wait}s")
                time.sleep(wait)

        for feat in feats:
            props = feat.get('properties', {})
            geom  = feat.get('geometry', {})
            if geom and geom.get('type') == 'Point':
                props['longitude'] = geom['coordinates'][0]
                props['latitude']  = geom['coordinates'][1]
            all_rows.append(props)

        if tile_count % 10 == 0 or tile_count == total_tiles:
            print(f"  Progress: {tile_count}/{total_tiles} tiles "
                  f"| rows so far: {len(all_rows):,}", flush=True)

    if not all_rows:
        print(f"  [ERROR] No pixel data returned -- check tehsil geometry and GEE auth.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    if 'longitude' in df.columns and 'latitude' in df.columns:
        df = df.drop_duplicates(subset=['longitude', 'latitude']).reset_index(drop=True)

    # Sort by (longitude, latitude) -> stable, reproducible pixel_id across runs
    df = df.sort_values(['longitude', 'latitude']).reset_index(drop=True)

    print(f"  Pixel count: {len(df):,}")
    return df


def _add_pixel_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepend pixel_id / longitude / latitude.
    df must already be sorted by (longitude, latitude) -- _download_image does this.
    pixel_id is therefore deterministic and reproducible across runs.
    """
    out = pd.DataFrame()
    out['pixel_id']  = df.index + 1
    out['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    out['latitude']  = pd.to_numeric(df['latitude'],  errors='coerce')
    return out


def _nodata_to_nan(df: pd.DataFrame, sentinel: float = NODATA) -> pd.DataFrame:
    """Replace the NODATA sentinel value with NaN throughout the DataFrame."""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].replace(sentinel, np.nan)
    return df


def _save_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    df.to_csv(path, index=False, float_format='%.4f')
    print(f"  CSV saved -> {path}  ({len(df):,} rows x {len(df.columns)} columns)")


# =============================================================================
# APPLICATION 1 — MONTHLY AET
# =============================================================================

def run_monthly_et(cfg: dict,
                   region: ee.Geometry,
                   aet_stack=None,
                   gap_flags=None,
                   raw_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Monthly mean daily AET for every 30 m pixel.
    Output  : monthly_et_<TEHSIL>_<YEAR>.csv
    Columns : pixel_id, longitude, latitude,
              ET_Jan_daily_01mm ... ET_Dec_daily_01mm,
              ET_Jan_daily_mm   ... ET_Dec_daily_mm,
              gap_filled_months
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)

    print(f"\n{'='*60}")
    print(f"  [monthly_et]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if raw_df is None:
        # standalone run: download just the AET stack
        if aet_stack is None:
            print("  Building AET stack ...")
            classifier = build_classifier(cfg['model_aez'])
            _check_landsat(region, year)
            aet_stack, gap_flags = build_aet_stack(region, classifier, year)
        print("  Downloading pixel data ...")
        raw_df = _download_image(aet_stack, region, chunk_size, label='monthly_et')

    if raw_df.empty:
        return pd.DataFrame()

    base = _add_pixel_id(raw_df)
    for m, abbr in enumerate(MONTH_ABBR, 1):
        v = pd.to_numeric(raw_df.get(f'ET_{m:02d}', np.nan), errors='coerce')
        base[f'ET_{abbr}_daily_01mm'] = v
        base[f'ET_{abbr}_daily_mm']   = v / 10.0

    gf_list = [MONTH_ABBR[i] for i, gf in enumerate(gap_flags or []) if gf]
    base['gap_filled_months'] = ','.join(gf_list) if gf_list else ''

    path = os.path.join(outdir, f'monthly_et_{tehsil}_{year}.csv')
    _save_csv(base, path)

    if cfg.get('plot'):
        _plot_monthly_et(base, tehsil, year, outdir)

    return base


# =============================================================================
# APPLICATION 2 — ANNUAL ET
# =============================================================================

def run_annual_et(cfg: dict,
                  region: ee.Geometry,
                  aet_stack=None,
                  gap_flags=None,
                  raw_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Annual total ET for every 30 m pixel.
    Formula : annual_ET = sum_m(daily_ET_m x days_in_month_m)
    Output  : annual_et_<TEHSIL>_<YEAR>.csv
    Columns : pixel_id, longitude, latitude, ET_annual_01mm, ET_annual_mm
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)

    print(f"\n{'='*60}")
    print(f"  [annual_et]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if raw_df is None:
        if aet_stack is None:
            print("  Building AET stack ...")
            classifier = build_classifier(cfg['model_aez'])
            _check_landsat(region, year)
            aet_stack, gap_flags = build_aet_stack(region, classifier, year)
        annual_img = build_annual_et_image(aet_stack, year)
        print("  Downloading pixel data ...")
        raw_df = _download_image(annual_img, region, chunk_size, label='annual_et')

    if raw_df.empty:
        return pd.DataFrame()

    base = _add_pixel_id(raw_df)
    base['ET_annual_01mm'] = pd.to_numeric(
        raw_df.get('annual_ET_01mm', np.nan), errors='coerce')
    base['ET_annual_mm'] = base['ET_annual_01mm'] / 10.0

    path = os.path.join(outdir, f'annual_et_{tehsil}_{year}.csv')
    _save_csv(base, path)
    _print_stats('Annual ET (mm/yr)', base['ET_annual_mm'])

    if cfg.get('plot'):
        _plot_annual_et(base, tehsil, year, outdir)

    return base


# =============================================================================
# APPLICATION 3 — PET  (MODIS MOD16A2)
# =============================================================================

def run_pet(cfg: dict,
            region: ee.Geometry,
            aet_stack=None,
            gap_flags=None,
            pet_stack=None,
            raw_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Monthly mean daily PET (MODIS MOD16A2) for every 30 m pixel.

    Pixel consistency:
      PET is sampled on the AET pixel grid (not the MODIS native grid).
      An AET carrier band is used to drive sampling so the pixel count matches
      monthly_et and annual_et exactly.  Pixels where MODIS has no data appear
      as NaN in the output CSV.

    Output  : pet_<TEHSIL>_<YEAR>.csv
    Columns : pixel_id, longitude, latitude,
              PET_Jan_daily_01mm ... PET_Dec_daily_01mm,
              PET_Jan_daily_mm   ... PET_Dec_daily_mm
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)
    modis_col  = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [pet]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if raw_df is None:
        # Build AET stack to serve as the pixel-grid carrier
        if aet_stack is None:
            print("  Building AET stack (used as pixel-grid carrier) ...")
            classifier = build_classifier(cfg['model_aez'])
            _check_landsat(region, year)
            aet_stack, gap_flags = build_aet_stack(region, classifier, year)

        if pet_stack is None:
            print("  Building PET stack (MODIS MOD16A2) ...")
            proj = _get_proj_30m(region, year)
            pet_stack = build_pet_stack(region, year, modis_col, proj)

        # Combine: AET carrier + PET (unmasked with NODATA sentinel)
        # Sampling is driven by AET pixels -> same count as monthly_et / annual_et
        print("  Downloading pixel data (AET pixel grid + MODIS PET) ...")
        carrier = aet_stack.select('ET_01').rename('_carrier')
        img_to_sample = carrier.addBands(pet_stack.unmask(NODATA))
        raw_df = _download_image(img_to_sample, region, chunk_size, label='pet')

    if raw_df.empty:
        return pd.DataFrame()

    base = _add_pixel_id(raw_df)
    for m, abbr in enumerate(MONTH_ABBR, 1):
        v = pd.to_numeric(raw_df.get(f'PET_{m:02d}', np.nan), errors='coerce')
        # replace NODATA sentinel with NaN
        v = v.replace(NODATA, np.nan)
        base[f'PET_{abbr}_daily_01mm'] = v
        base[f'PET_{abbr}_daily_mm']   = v / 10.0

    path = os.path.join(outdir, f'pet_{tehsil}_{year}.csv')
    _save_csv(base, path)

    if cfg.get('plot'):
        _plot_pet(base, tehsil, year, outdir)

    return base


# =============================================================================
# APPLICATION 4 — RWDI
# =============================================================================

def run_rwdi(cfg: dict,
             region: ee.Geometry,
             aet_stack=None,
             gap_flags=None,
             pet_stack=None,
             raw_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    RWDI = (1 - AET/PET) x 100  (%)

    Pixel consistency:
      Sampled on the AET pixel grid.  RWDI is NaN for pixels where MODIS
      has no PET data.

    Output  : rwdi_<TEHSIL>_<YEAR>.csv
    Columns : pixel_id, longitude, latitude,
              RWDI_Jan ... RWDI_Dec, RWDI_annual
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)
    modis_col  = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [rwdi]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if raw_df is None:
        if aet_stack is None:
            print("  Building AET stack ...")
            classifier = build_classifier(cfg['model_aez'])
            _check_landsat(region, year)
            aet_stack, gap_flags = build_aet_stack(region, classifier, year)
        if pet_stack is None:
            print("  Building PET stack (MODIS MOD16A2) ...")
            proj = _get_proj_30m(region, year)
            pet_stack = build_pet_stack(region, year, modis_col, proj)

        rwdi_img = build_rwdi_image(aet_stack, pet_stack)
        # AET carrier + RWDI (unmasked) -> AET pixel grid
        carrier = aet_stack.select('ET_01').rename('_carrier')
        img_to_sample = carrier.addBands(rwdi_img.unmask(NODATA))
        print("  Downloading pixel data (AET pixel grid + RWDI) ...")
        raw_df = _download_image(img_to_sample, region, chunk_size, label='rwdi')

    if raw_df.empty:
        return pd.DataFrame()

    base = _add_pixel_id(raw_df)
    rwdi_cols = []
    for m, abbr in enumerate(MONTH_ABBR, 1):
        col = f'RWDI_{abbr}'
        v   = pd.to_numeric(raw_df.get(f'RWDI_{m:02d}', np.nan), errors='coerce')
        v   = v.replace(NODATA, np.nan)
        base[col] = v
        rwdi_cols.append(col)
    base['RWDI_annual'] = base[rwdi_cols].mean(axis=1)

    path = os.path.join(outdir, f'rwdi_{tehsil}_{year}.csv')
    _save_csv(base, path)
    _print_stats('Annual mean RWDI (%)', base['RWDI_annual'])

    if cfg.get('plot'):
        _plot_rwdi(base, tehsil, year, outdir)

    return base


# =============================================================================
# APPLICATION 5 — WATER STRESS
# =============================================================================

def run_water_stress(cfg: dict,
                     region: ee.Geometry,
                     aet_stack=None,
                     gap_flags=None,
                     pet_stack=None,
                     raw_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Water Stress = AET / PET  (0-1)

    Pixel consistency:
      Sampled on the AET pixel grid.  Water Stress is NaN for pixels where
      MODIS has no PET data.

    Output  : water_stress_<TEHSIL>_<YEAR>.csv
    Columns : pixel_id, longitude, latitude,
              WaterStress_Jan ... WaterStress_Dec, WaterStress_annual
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)
    modis_col  = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [water_stress]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if raw_df is None:
        if aet_stack is None:
            print("  Building AET stack ...")
            classifier = build_classifier(cfg['model_aez'])
            _check_landsat(region, year)
            aet_stack, gap_flags = build_aet_stack(region, classifier, year)
        if pet_stack is None:
            print("  Building PET stack (MODIS MOD16A2) ...")
            proj = _get_proj_30m(region, year)
            pet_stack = build_pet_stack(region, year, modis_col, proj)

        ws_img = build_ws_image(aet_stack, pet_stack)
        carrier = aet_stack.select('ET_01').rename('_carrier')
        img_to_sample = carrier.addBands(ws_img.unmask(NODATA))
        print("  Downloading pixel data (AET pixel grid + Water Stress) ...")
        raw_df = _download_image(img_to_sample, region, chunk_size, label='water_stress')

    if raw_df.empty:
        return pd.DataFrame()

    base = _add_pixel_id(raw_df)
    ws_cols = []
    for m, abbr in enumerate(MONTH_ABBR, 1):
        col = f'WaterStress_{abbr}'
        v   = pd.to_numeric(raw_df.get(f'WS_{m:02d}', np.nan), errors='coerce')
        v   = v.replace(NODATA, np.nan)
        base[col] = v
        ws_cols.append(col)
    base['WaterStress_annual'] = base[ws_cols].mean(axis=1)

    path = os.path.join(outdir, f'water_stress_{tehsil}_{year}.csv')
    _save_csv(base, path)
    _print_stats('Annual mean Water Stress (AET/PET)', base['WaterStress_annual'])

    if cfg.get('plot'):
        _plot_water_stress(base, tehsil, year, outdir)

    return base


# =============================================================================
# MERGE  (offline, no GEE)
# =============================================================================

def run_merge(cfg: dict) -> pd.DataFrame:
    """
    Joins any combination of the five per-application CSVs on pixel_id.
    Works with whatever CSVs are present in the output directory.
    longitude and latitude are taken from the first CSV found.

    Output : merged_<TEHSIL>_<YEAR>.csv

    Example:
        python et_applications.py --application monthly_et
        python et_applications.py --application annual_et
        python et_applications.py --application merge
    """
    tehsil = cfg['tehsil_name']
    year   = cfg['year']
    outdir = cfg['output']

    print(f"\n{'='*60}")
    print(f"  [merge]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    csv_map = {
        'monthly_et'   : f'monthly_et_{tehsil}_{year}.csv',
        'annual_et'    : f'annual_et_{tehsil}_{year}.csv',
        'pet'          : f'pet_{tehsil}_{year}.csv',
        'rwdi'         : f'rwdi_{tehsil}_{year}.csv',
        'water_stress' : f'water_stress_{tehsil}_{year}.csv',
    }

    frames = {}
    for app, fname in csv_map.items():
        fpath = os.path.join(outdir, fname)
        if os.path.exists(fpath):
            frames[app] = pd.read_csv(fpath)
            print(f"  Found  {fname}  "
                  f"({len(frames[app]):,} rows x {len(frames[app].columns)} cols)")
        else:
            print(f"  Missing {fname}  (skipped)")

    if not frames:
        print(f"\n[ERROR] No application CSVs found in {outdir}")
        print("  Run at least one application first, e.g.:")
        print("    python et_applications.py --application monthly_et")
        return pd.DataFrame()

    # Verify pixel counts match
    counts = {app: len(df) for app, df in frames.items()}
    if len(set(counts.values())) > 1:
        print(f"\n  [WARN] Pixel counts differ across CSVs:")
        for app, n in counts.items():
            print(f"    {app:<15} : {n:,}")
        print("  This can happen if some CSVs were generated with an older version.")
        print("  Re-run the affected applications to regenerate them.")
    else:
        n = list(counts.values())[0]
        print(f"\n  All CSVs have consistent pixel count: {n:,}")

    merged = None
    for df in frames.values():
        if merged is None:
            merged = df.copy()
        else:
            right = df.drop(columns=[c for c in ['longitude', 'latitude']
                                     if c in df.columns])
            merged = merged.merge(right, on='pixel_id', how='outer')

    out_path = os.path.join(outdir, f'merged_{tehsil}_{year}.csv')
    _save_csv(merged, out_path)
    print(f"\n  Merged : {merged.shape[0]:,} rows x {merged.shape[1]} columns")
    print(f"  Sources: {', '.join(frames.keys())}")
    return merged


# =============================================================================
# ALL  (single combined download — guaranteed pixel consistency)
# =============================================================================

def run_all(cfg: dict, region: ee.Geometry) -> dict:
    """
    Run all five applications in ONE combined GEE download.

    Pixel consistency guarantee:
      All five CSVs are derived from a single image.sample() call on the
      combined 49-band image.  Every CSV contains exactly the same
      pixel_id / longitude / latitude rows.

    Efficiency:
      AET and PET stacks are built once and shared.
      Only ONE set of tile downloads is performed (280 tiles for TELYANI).
    """
    year      = cfg['year']
    modis_col = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [all] Building shared GEE stacks ...")
    print(f"{'='*60}")

    print("\n  [1/2] Building AET stack (Landsat 8 + GLDAS -> RF model) ...")
    classifier = build_classifier(cfg['model_aez'])
    _check_landsat(region, year)
    aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    print("\n  [2/2] Building PET stack (MODIS MOD16A2) ...")
    proj      = _get_proj_30m(region, year)
    pet_stack = build_pet_stack(region, year, modis_col, proj)

    # Build combined image (49 bands)
    print("\n  Building combined image (all 49 bands) ...")
    combined  = build_combined_image(aet_stack, pet_stack, year)

    # ONE download for everything
    print(f"\n  Downloading all pixels in ONE pass ...")
    raw_df = _download_image(combined, region, cfg.get('chunk_size', 25000),
                             label='all')

    if raw_df.empty:
        print("[ERROR] No pixel data returned.")
        return {}

    pixel_count = len(raw_df)
    print(f"\n  All five applications share exactly {pixel_count:,} pixels.")

    # --- Split raw_df into per-application DataFrames and CSVs ---
    results = {}
    results['monthly_et']   = run_monthly_et(
        cfg, region, gap_flags=gap_flags, raw_df=raw_df)
    results['annual_et']    = run_annual_et(
        cfg, region, raw_df=raw_df)
    results['pet']          = run_pet(
        cfg, region, raw_df=raw_df)
    results['rwdi']         = run_rwdi(
        cfg, region, raw_df=raw_df)
    results['water_stress'] = run_water_stress(
        cfg, region, raw_df=raw_df)

    results['merged'] = run_merge(cfg)

    if not results['merged'].empty:
        run_sample_timeseries(results['merged'], cfg)

    return results


# =============================================================================
# SAMPLE POINT TIME SERIES
# =============================================================================

def run_sample_timeseries(master: pd.DataFrame, cfg: dict):
    """
    Extract the 12-month time series from the pixel nearest to a requested
    lon/lat point.  Works on the merged CSV or any individual CSV that has
    lon/lat columns.  No GEE calls are made.

    Outputs:
      sample_point_<TEHSIL>_<YEAR>.csv   (12 rows, one per month)
      sample_point_<TEHSIL>_<YEAR>.png   (4-panel plot, if --plot)

    Config YAML:
        sample_point:
          lon: 80.7314
          lat: 25.9735

    CLI:  --sample-lon 80.7314 --sample-lat 25.9735
    """
    lon = cfg.get('sample_lon')
    lat = cfg.get('sample_lat')
    if lon is None or lat is None:
        return
    if master.empty or 'longitude' not in master.columns:
        print("[sample] Skipped: DataFrame is empty or has no coordinates.")
        return

    tehsil = cfg['tehsil_name']
    year   = cfg['year']
    outdir = cfg['output']
    os.makedirs(outdir, exist_ok=True)

    dists_sq   = (master['longitude'] - lon) ** 2 + (master['latitude'] - lat) ** 2
    idx        = dists_sq.idxmin()
    px         = master.loc[idx]
    actual_lon = float(px['longitude'])
    actual_lat = float(px['latitude'])
    dist_m     = float(dists_sq[idx] ** 0.5) * 111_320

    print(f"\n[sample] Requested  : lon={lon:.6f}, lat={lat:.6f}")
    print(f"         Nearest px : lon={actual_lon:.6f}, lat={actual_lat:.6f} "
          f"(approx {dist_m:.0f} m away)")

    rows = []
    for m, abbr in enumerate(MONTH_ABBR, 1):
        days   = calendar.monthrange(year, m)[1]
        aet_mm = px.get(f'ET_{abbr}_daily_mm')
        rows.append({
            'month'          : m,
            'month_label'    : abbr,
            'days_in_month'  : days,
            'AET_daily_01mm' : px.get(f'ET_{abbr}_daily_01mm'),
            'AET_daily_mm'   : aet_mm,
            'PET_daily_01mm' : px.get(f'PET_{abbr}_daily_01mm'),
            'PET_daily_mm'   : px.get(f'PET_{abbr}_daily_mm'),
            'AET_monthly_mm' : (float(aet_mm) * days) if pd.notna(aet_mm) else None,
            'RWDI_pct'       : px.get(f'RWDI_{abbr}'),
            'WaterStress'    : px.get(f'WaterStress_{abbr}'),
        })

    df = pd.DataFrame(rows)
    df.insert(0, 'lon', actual_lon)
    df.insert(1, 'lat', actual_lat)
    df.insert(2, 'annual_ET_mm', px.get('ET_annual_mm'))
    df.insert(3, 'RWDI_annual',  px.get('RWDI_annual'))
    df.insert(4, 'WS_annual',    px.get('WaterStress_annual'))

    csv_path = os.path.join(outdir, f'sample_point_{tehsil}_{year}.csv')
    df.to_csv(csv_path, index=False, float_format='%.4f')
    print(f"  Sample CSV -> {csv_path}")

    if cfg.get('plot'):
        _plot_sample_timeseries(df, actual_lon, actual_lat, tehsil, year, outdir)


# =============================================================================
# UTILITIES
# =============================================================================

def _check_landsat(region, year):
    total = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
             .filterBounds(region)
             .filterDate(ee.Date.fromYMD(year, 1, 1),
                         ee.Date.fromYMD(year, 12, 31))
             .size().getInfo())
    print(f"  Total Landsat scenes for {year}: {total}")
    if total == 0:
        print("[ERROR] No Landsat 8 scenes. Check year and tehsil geometry.")
        sys.exit(1)


def _print_stats(label: str, series: pd.Series):
    s = series.dropna().describe()
    print(f"\n  {label}")
    print(f"    Mean : {s['mean']:.4f}")
    print(f"    Min  : {s['min']:.4f}")
    print(f"    Max  : {s['max']:.4f}")
    print(f"    Std  : {s['std']:.4f}")


# =============================================================================
# PLOTS
# =============================================================================

def _plot_monthly_et(df, tehsil, year, outdir):
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    means = [df[f'ET_{a}_daily_mm'].mean() for a in MONTH_ABBR]
    stds  = [df[f'ET_{a}_daily_mm'].std()  for a in MONTH_ABBR]
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(MONTH_ABBR, means, marker='o', color='#004400', linewidth=2.5,
            markersize=7, label='Mean across pixels')
    ax.fill_between(range(12),
                    [m - s for m, s in zip(means, stds)],
                    [m + s for m, s in zip(means, stds)],
                    alpha=0.2, color='#228B22', label='+-1 std')
    ax.set_title(f'Monthly Mean Daily AET -- {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('AET (mm/day)')
    ax.legend(fontsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    p = os.path.join(outdir, f'monthly_et_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot -> {p}")


def _plot_annual_et(df, tehsil, year, outdir):
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(df['ET_annual_mm'].dropna(), bins=60, color='#228B22',
            edgecolor='white', linewidth=0.4)
    mean_val = df['ET_annual_mm'].mean()
    ax.axvline(mean_val, color='red', linestyle='--', linewidth=1.5,
               label=f'Mean = {mean_val:.1f} mm/yr')
    ax.set_title(f'Annual ET Distribution -- {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_xlabel('Annual ET (mm/yr)')
    ax.set_ylabel('Pixel count')
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    p = os.path.join(outdir, f'annual_et_hist_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot -> {p}")


def _plot_pet(df, tehsil, year, outdir):
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    means = [df[f'PET_{a}_daily_mm'].mean() for a in MONTH_ABBR]
    stds  = [df[f'PET_{a}_daily_mm'].std()  for a in MONTH_ABBR]
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(MONTH_ABBR, means, marker='s', color='#1a6fa3', linewidth=2.5,
            markersize=7, label='Mean across pixels')
    ax.fill_between(range(12),
                    [m - s for m, s in zip(means, stds)],
                    [m + s for m, s in zip(means, stds)],
                    alpha=0.2, color='#1a6fa3', label='+-1 std')
    ax.set_title(f'Monthly Mean Daily PET (MODIS MOD16A2) -- {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('PET (mm/day)')
    ax.legend(fontsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    p = os.path.join(outdir, f'pet_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot -> {p}")


def _plot_rwdi(df, tehsil, year, outdir):
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    rwdi_means = [df[f'RWDI_{a}'].mean() for a in MONTH_ABBR]

    def _clr(v):
        if np.isnan(v):
            return '#cccccc'
        for lo, hi, c, _ in RWDI_CLASSES:
            if lo <= v < hi:
                return c
        return '#FF0000'

    fig, ax = plt.subplots(figsize=(12, 5))
    for i, (abbr, val) in enumerate(zip(MONTH_ABBR, rwdi_means)):
        safe = 0 if np.isnan(val) else val
        ax.bar(abbr, safe, color=_clr(val), edgecolor='black', linewidth=0.6)
        if not np.isnan(val):
            ax.text(i, val + 1, f'{val:.0f}%', ha='center', va='bottom', fontsize=8.5)
    for thresh, lc in [(30, '#9ACD32'), (50, '#FFA500'), (80, '#FF0000')]:
        ax.axhline(thresh, color=lc, linestyle='--', linewidth=1.2)
    ax.set_ylim(0, 110)
    ax.set_title(f'Monthly RWDI (pixel mean) -- {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('RWDI (%)')
    patches = [mpatches.Patch(color=c, label=lbl) for _, _, c, lbl in RWDI_CLASSES]
    ax.legend(handles=patches, fontsize=8, loc='upper right')
    plt.tight_layout()
    p = os.path.join(outdir, f'rwdi_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot -> {p}")


def _plot_water_stress(df, tehsil, year, outdir):
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    ws_means = [df[f'WaterStress_{a}'].mean() for a in MONTH_ABBR]
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(MONTH_ABBR, ws_means, marker='s', color='#1a9641',
            linewidth=2.5, markersize=7)
    ax.fill_between(range(12), ws_means, alpha=0.15, color='#66bd63')
    ax.axhline(1.0, color='#1a4fa3', linestyle='--', linewidth=1.3, label='No stress (1.0)')
    ax.axhline(0.5, color='#FF8C00', linestyle='--', linewidth=1.3, label='Moderate (0.5)')
    ax.axhline(0.3, color='#FF0000', linestyle='--', linewidth=1.3, label='High (0.3)')
    ax.set_ylim(0, 1.25)
    ax.set_title(f'Monthly Water Stress (AET/PET) -- {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('AET / PET')
    ax.legend(fontsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    p = os.path.join(outdir, f'water_stress_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot -> {p}")


def _plot_sample_timeseries(df, lon, lat, tehsil, year, outdir):
    if not HAS_MPL:
        return
    months = df['month_label'].tolist()

    def _safe(col):
        return pd.to_numeric(df[col], errors='coerce').tolist()

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        f'Monthly Time Series -- Single Pixel  |  {tehsil} ({year})\n'
        f'lon={lon:.5f}, lat={lat:.5f}',
        fontsize=13, fontweight='bold', y=1.01
    )

    ax = axes[0, 0]
    aet = _safe('AET_daily_mm')
    ax.plot(months, aet, marker='o', color='#228B22', linewidth=2.2, markersize=7)
    ax.fill_between(range(12), aet, alpha=0.15, color='#228B22')
    ax.set_title('Mean Daily AET (mm/day)')
    ax.set_ylabel('AET (mm/day)')
    ax.grid(axis='y', linestyle='--', alpha=0.4)

    ax = axes[0, 1]
    pet = _safe('PET_daily_mm')
    ax.plot(months, pet, marker='s', color='#1a6fa3', linewidth=2.2, markersize=7)
    ax.fill_between(range(12), pet, alpha=0.15, color='#1a6fa3')
    ax.set_title('Mean Daily PET (mm/day) -- MODIS')
    ax.set_ylabel('PET (mm/day)')
    ax.grid(axis='y', linestyle='--', alpha=0.4)

    ax = axes[1, 0]
    rwdi_vals = _safe('RWDI_pct')

    def _rclr(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return '#cccccc'
        for lo, hi, c, _ in RWDI_CLASSES:
            if lo <= float(v) < hi:
                return c
        return '#FF0000'

    safe_vals = [v if not (isinstance(v, float) and np.isnan(v)) else 0
                 for v in rwdi_vals]
    bars = ax.bar(months, safe_vals,
                  color=[_rclr(v) for v in rwdi_vals],
                  edgecolor='black', linewidth=0.5)
    for bar, val in zip(bars, rwdi_vals):
        if not (isinstance(val, float) and np.isnan(val)):
            ax.text(bar.get_x() + bar.get_width() / 2, val + 1,
                    f'{val:.0f}%', ha='center', va='bottom', fontsize=7.5)
    ax.set_ylim(0, 115)
    ax.set_title('Monthly RWDI (%)')
    ax.set_ylabel('RWDI (%)')
    patches = [mpatches.Patch(color=c, label=lbl) for _, _, c, lbl in RWDI_CLASSES]
    ax.legend(handles=patches, fontsize=7, loc='upper right')

    ax = axes[1, 1]
    ws = _safe('WaterStress')
    ax.plot(months, ws, marker='^', color='#d62728', linewidth=2.2, markersize=7)
    ax.fill_between(range(12), ws, alpha=0.12, color='#d62728')
    ax.axhline(1.0, color='#1a4fa3', linestyle='--', linewidth=1.1, label='No stress (1.0)')
    ax.axhline(0.5, color='#FF8C00', linestyle='--', linewidth=1.1, label='Moderate (0.5)')
    ax.axhline(0.3, color='#FF0000', linestyle='--', linewidth=1.1, label='High (0.3)')
    ax.set_ylim(0, 1.25)
    ax.set_title('Monthly Water Stress (AET/PET)')
    ax.set_ylabel('AET / PET')
    ax.legend(fontsize=8)
    ax.grid(axis='y', linestyle='--', alpha=0.4)

    plt.tight_layout()
    p = os.path.join(outdir, f'sample_point_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot -> {p}")


# =============================================================================
# CLI
# =============================================================================

def build_parser():
    p = argparse.ArgumentParser(
        prog='et_applications',
        description='Pan-India ET Downscaling -- Pixel-Wise CSV Edition',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    p.add_argument('--config',       default=None, metavar='PATH',
                   help='Path to config YAML (default: ./config.yaml)')
    p.add_argument('--tehsil-asset', default=None,
                   help='GEE FeatureCollection asset path for the tehsil')
    p.add_argument('--model-aez',    default=None,
                   help='GEE asset path for the RF ensemble model')
    p.add_argument('--year',         type=int, default=None)
    p.add_argument('--output',       default=None, metavar='DIR')
    p.add_argument('--plot',         action='store_true', default=False)
    p.add_argument('--gee-project',  default=None)
    p.add_argument('--tehsil-name',  default=None)
    p.add_argument('--chunk-size',   type=int, default=None,
                   help='Approx pixels per download tile (default: 25000). '
                        'Lower if you get GEE memory errors.')
    p.add_argument('--application',  default=None,
                   choices=['all', 'monthly_et', 'annual_et', 'pet',
                             'rwdi', 'water_stress', 'merge'],
                   help='Which application to run (default: all).')
    p.add_argument('--sample-lon',   type=float, default=None,
                   help='Longitude for single-pixel time-series output.')
    p.add_argument('--sample-lat',   type=float, default=None,
                   help='Latitude  for single-pixel time-series output.')
    return p


def main():
    parser = build_parser()
    args   = parser.parse_args()

    config_path = pathlib.Path(args.config) if args.config else CONFIG_PATH
    cfg = load_config(config_path)
    cfg = merge_args(cfg, args)

    app = cfg.get('application', 'all')

    if app == 'merge':
        if not cfg.get('tehsil_name'):
            parser.error('tehsil_name is required for merge '
                         '(set in config.yaml or --tehsil-name)')
        run_merge(cfg)
        print(f"\nDone.  Output in: {os.path.abspath(cfg.get('output', './results'))}")
        return

    if not cfg.get('tehsil_asset'):
        parser.error('tehsil_asset is required (config.yaml or --tehsil-asset)')
    if not cfg.get('model_aez'):
        parser.error('model_aez is required (config.yaml or --model-aez)')
    if not cfg.get('tehsil_name'):
        cfg['tehsil_name'] = cfg['tehsil_asset'].rstrip('/').split('/')[-1].upper()

    print("\n" + "="*68)
    print("  ET-Applications  (Pixel-Wise CSV Edition)")
    print("="*68)
    for lbl, key in [
        ("Tehsil",           'tehsil_name'),
        ("Year",             'year'),
        ("Application",      'application'),
        ("Output dir",       'output'),
        ("GEE project",      'gee_project'),
        ("Model (AEZ)",      'model_aez'),
        ("Tehsil asset",     'tehsil_asset'),
        ("Chunk size",       'chunk_size'),
        ("Plots",            'plot'),
        ("MODIS collection", 'modis_collection'),
    ]:
        print(f"  {lbl:<22}: {cfg.get(key, 'N/A')}")
    if cfg.get('sample_lon') is not None:
        print(f"  {'Sample point':<22}: "
              f"lon={cfg['sample_lon']}, lat={cfg['sample_lat']}")
    print("="*68 + "\n")

    init_ee(cfg.get('gee_project', ''))
    _, region = load_tehsil(cfg['tehsil_asset'])

    dispatch = {
        'monthly_et'   : lambda: run_monthly_et(cfg, region),
        'annual_et'    : lambda: run_annual_et(cfg, region),
        'pet'          : lambda: run_pet(cfg, region),
        'rwdi'         : lambda: run_rwdi(cfg, region),
        'water_stress' : lambda: run_water_stress(cfg, region),
        'all'          : lambda: run_all(cfg, region),
    }

    result = dispatch[app]()

    if app != 'all' and isinstance(result, pd.DataFrame) and not result.empty:
        run_sample_timeseries(result, cfg)

    print(f"\nDone.  All outputs in: {os.path.abspath(cfg.get('output', './results'))}")


if __name__ == '__main__':
    main()