#!/usr/bin/env python3
"""
ET-Applications — Pan-India ET Downscaling CLI  (GeoTIFF Raster Edition)
=========================================================================
Each application writes its OWN multi-band GeoTIFF (one band per month /
derived product).  All GeoTIFFs are spatially aligned at 30 m resolution
and can be opened directly in QGIS, ArcGIS, or any rasterio/GDAL workflow.

  PIXEL CONSISTENCY GUARANTEE
  ----------------------------
  All GeoTIFFs produced by the same tehsil + year combination share the
  same bounding box, CRS, and 30 m pixel grid so they are immediately
  overlay-compatible.  The AET (Landsat 8) pixel grid drives the spatial
  reference; MODIS-derived bands are resampled to this grid before
  download.  Pixels outside the tehsil boundary are written as NoData
  (–9999).

  Band descriptions are embedded in every GeoTIFF so band purpose is
  visible without consulting external documentation.  Gap-fill status
  and units are stored as TIFF metadata tags.

  Applications
  ------------
  monthly_et    ->  monthly_et_<TEHSIL>_<YEAR>.tif   12 bands  (mm/day)
  annual_et     ->  annual_et_<TEHSIL>_<YEAR>.tif     1 band   (mm/yr)
  pet           ->  pet_<TEHSIL>_<YEAR>.tif           12 bands  (mm/day)
  rwdi          ->  rwdi_<TEHSIL>_<YEAR>.tif          13 bands  (%)
  water_stress  ->  water_stress_<TEHSIL>_<YEAR>.tif  13 bands  (ratio)
  all           ->  all five GeoTIFFs (ONE combined download, stacks built once)

  Quick-start
  -----------
  python et_applications.py                          # all from config.yaml
  python et_applications.py --application monthly_et
  python et_applications.py --application pet
  python et_applications.py --application rwdi
  python et_applications.py --application water_stress
  python et_applications.py --application all --plot
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
    def _load_yaml(p):
        with open(p) as f:
            return yaml.safe_load(f)
except ImportError:
    print("[ERROR] pyyaml missing.  Run: pip install pyyaml")
    sys.exit(1)

import ee
import numpy as np

try:
    import requests
except ImportError:
    print("[ERROR] requests missing.  Run: pip install requests")
    sys.exit(1)

try:
    import rasterio
    from rasterio.merge import merge as rio_merge
except ImportError:
    print("[ERROR] rasterio missing.  Run: pip install rasterio")
    sys.exit(1)

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

# NoData sentinel used in GEE images and written to masked pixels in GeoTIFF.
NODATA = -9999.0

RWDI_CLASSES = [
    (0,  30, '#228B22', 'Normal / Irrigated'),
    (30, 50, '#9ACD32', 'Mild Stress'),
    (50, 70, '#FFFF00', 'Moderate Stress'),
    (70, 80, '#FFA500', 'High Stress'),
    (80, 90, '#F08080', 'Severe Stress'),
    (90,100, '#FF0000', 'Extreme Drought'),
]

# Band layout in the combined 49-band image (0-indexed)
_AET_SLICE    = slice(0,  12)   # ET_01 … ET_12        (0.1 mm/day)
_PET_SLICE    = slice(12, 24)   # PET_01 … PET_12      (0.1 mm/day)
_ANNUAL_SLICE = slice(24, 25)   # annual_ET_01mm        (0.1 mm/yr)
_RWDI_SLICE   = slice(25, 37)   # RWDI_01 … RWDI_12    (%)
_WS_SLICE     = slice(37, 49)   # WS_01 … WS_12        (ratio 0-1)


@contextlib.contextmanager
def _quiet_gdal():
    """Suppress GDAL/libtiff C-level warnings printed to stderr.

    These "Warning 1: TIFFReadDirectory …" lines come from the libtiff C
    library and bypass Python's warnings module entirely.  Redirecting fd 2
    to /dev/null for the duration of the rasterio call is the only reliable
    way to silence them without discarding real Python-level error output.
    """
    old_fd  = os.dup(2)
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
    gap_flags : list[bool], True = month gap-filled by ±60-day neighbour mean
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
    500 m MODIS pixel bilinearly resampled to the 30 m Landsat grid.
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
    """annual_ET_01mm = sum_m(daily_ET_m × days_in_month_m)"""
    annual = aet_stack.select('ET_01').multiply(calendar.monthrange(year, 1)[1])
    for m in range(2, 13):
        days   = calendar.monthrange(year, m)[1]
        annual = annual.add(aet_stack.select(f'ET_{m:02d}').multiply(days))
    return annual.rename('annual_ET_01mm').float()


def build_rwdi_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """RWDI_01...12 = (1 – AET/PET) × 100  (%)"""
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
    """WS_01...12 = AET / PET  (0–1)"""
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

    Band layout (49 bands total):
        ET_01 … ET_12          (0.1 mm/day)  ← AET, drives pixel grid
        PET_01 … PET_12        (0.1 mm/day)  ← MODIS, NoData where missing
        annual_ET_01mm         (0.1 mm/yr)   ← derived from AET
        RWDI_01 … RWDI_12      (%)           ← NoData where PET missing
        WS_01 … WS_12          (0–1)         ← NoData where PET missing
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
# GEOTIFF DOWNLOAD INFRASTRUCTURE
# =============================================================================

def _download_image_as_geotiff(img: ee.Image,
                                region: ee.Geometry,
                                chunk_size: int = 25000,
                                label: str = '') -> list:
    """
    Tile the tehsil bounding box and download each tile as a GeoTIFF via
    ee.Image.getDownloadURL().

    Returns a list of temporary file paths (one per downloaded tile).
    The caller is responsible for cleanup (handled by _merge_tiles).

    GEE may return either a raw GeoTIFF or a ZIP containing a GeoTIFF;
    both are handled transparently.

    Tiles whose intersection with the tehsil is empty are detected via a
    client-side area check and skipped without any GEE download attempt,
    so the common "The geometry for image clipping must not be empty" error
    is never triggered.
    """
    bbox   = region.bounds().getInfo()['coordinates'][0]
    lons   = [c[0] for c in bbox]
    lats   = [c[1] for c in bbox]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    deg_per_px  = 0.00027          # ≈ 30 m at the equator in decimal degrees
    tile_side   = (chunk_size ** 0.5) * deg_per_px
    lon_tiles   = max(1, int(np.ceil((max_lon - min_lon) / tile_side)))
    lat_tiles   = max(1, int(np.ceil((max_lat - min_lat) / tile_side)))
    lon_step    = (max_lon - min_lon) / lon_tiles
    lat_step    = (max_lat - min_lat) / lat_tiles
    total_tiles = lon_tiles * lat_tiles

    tag = f"[{label}]" if label else "[download]"
    print(f"  {tag} Tiling: {lon_tiles} × {lat_tiles} = {total_tiles} tiles "
          f"(chunk ≈ {chunk_size:,} px each)")

    # Pre-fetch the region geometry once as a GeoJSON dict for fast
    # client-side intersection checks (avoids a GEE round-trip per tile).
    region_geojson = region.getInfo()

    tile_paths   = []
    tile_count   = 0
    skipped      = 0

    # Error substrings that mean the tile geometry was empty or outside the
    # image footprint — retrying will never help, so skip immediately.
    _EMPTY_GEOM_MSGS = (
        'geometry for image clipping must not be empty',
        'empty geometry',
        'no data',
    )

    for i, j in itertools.product(range(lon_tiles), range(lat_tiles)):
        x0 = min_lon + i * lon_step
        x1 = x0 + lon_step
        y0 = min_lat + j * lat_step
        y1 = y0 + lat_step

        # Fast client-side empty-geometry guard:
        # Use shapely if available, otherwise fall back to a lightweight
        # bounding-box overlap check against the tehsil bounds.
        try:
            from shapely.geometry import shape, box as shapely_box
            tehsil_shape = shape(region_geojson)
            tile_shape   = shapely_box(x0, y0, x1, y1)
            if not tehsil_shape.intersects(tile_shape):
                skipped += 1
                continue
        except ImportError:
            # Shapely not available: rely on the GEE area check below.
            pass

        tile_rect = ee.Geometry.Rectangle([x0, y0, x1, y1])
        tile_geom = tile_rect.intersection(region, 1)

        # Server-side area check — skip tiles that don't overlap the tehsil.
        try:
            area = tile_geom.area(10).getInfo()
            if area < 100:          # less than ~100 m² — effectively empty
                skipped += 1
                continue
        except Exception:
            skipped += 1
            continue

        tile_count += 1
        data = None

        for attempt in range(4):
            try:
                url  = img.getDownloadURL({
                    'scale'      : 30,
                    'region'     : tile_geom,
                    'format'     : 'GEO_TIFF',
                    'filePerBand': False,
                })
                resp = requests.get(url, timeout=300)
                resp.raise_for_status()
                data = resp.content
                break
            except Exception as exc:
                exc_str = str(exc).lower()
                # Empty geometry errors will never succeed on retry — skip.
                if any(msg in exc_str for msg in _EMPTY_GEOM_MSGS):
                    break
                if attempt == 3:
                    print(f"  [WARN] Tile ({i},{j}) failed after 4 attempts: {exc}")
                    break
                wait = 2 ** attempt
                print(f"  [retry {attempt+1}] tile ({i},{j}): {exc} — "
                      f"waiting {wait}s")
                time.sleep(wait)

        if data is None:
            continue

        # GEE may wrap the GeoTIFF in a ZIP (magic bytes 'PK')
        if data[:2] == b'PK':
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    tif_names = [n for n in zf.namelist()
                                 if n.lower().endswith('.tif')]
                    if not tif_names:
                        print(f"  [WARN] Tile ({i},{j}): ZIP has no .tif files")
                        continue
                    data = zf.read(tif_names[0])
            except Exception as exc:
                print(f"  [WARN] Tile ({i},{j}): could not unzip: {exc}")
                continue

        tmp = tempfile.NamedTemporaryFile(suffix='.tif', delete=False)
        try:
            tmp.write(data)
        finally:
            tmp.close()
        tile_paths.append(tmp.name)

        if tile_count % 10 == 0 or (tile_count + skipped) == total_tiles:
            print(f"  Progress: {tile_count + skipped}/{total_tiles} checked "
                  f"| {tile_count} downloaded | {skipped} skipped (no overlap) "
                  f"| {len(tile_paths)} saved", flush=True)

    return tile_paths


def _merge_tiles(tile_paths: list, nodata: float = NODATA):
    """
    Mosaic tile GeoTIFFs into a single array using rasterio.merge.
    Cleans up all temp tile files regardless of success.

    Returns (mosaic_float32, profile) on success, or (None, None) on failure.
    mosaic shape: (n_bands, height, width).
    """
    datasets, bad = [], []
    for p in tile_paths:
        try:
            with _quiet_gdal():
                datasets.append(rasterio.open(p))
        except Exception as exc:
            print(f"  [WARN] Cannot open tile {p}: {exc}")
            bad.append(p)

    result = (None, None)
    if datasets:
        try:
            with _quiet_gdal():
                mosaic, transform = rio_merge(datasets, nodata=nodata)
            profile = datasets[0].profile.copy()
            profile.update({
                'height'   : mosaic.shape[1],
                'width'    : mosaic.shape[2],
                'transform': transform,
                'nodata'   : nodata,
                'count'    : mosaic.shape[0],
                'compress' : 'lzw',
                'driver'   : 'GTiff',
                'dtype'    : 'float32',
            })
            result = (mosaic.astype('float32'), profile)
        except Exception as exc:
            print(f"  [ERROR] rasterio merge failed: {exc}")
    else:
        print("  [ERROR] No valid tile datasets to merge.")

    for ds in datasets:
        ds.close()
    for p in tile_paths + bad:
        try:
            os.unlink(p)
        except OSError:
            pass

    return result


def _save_geotiff(arr: np.ndarray,
                  profile: dict,
                  output_path: str,
                  band_names: list = None,
                  metadata: dict = None) -> None:
    """
    Write a numpy array (n_bands, H, W) as a GeoTIFF.
    band_names are embedded as band descriptions.
    metadata dict is stored as TIFF tags.
    PHOTOMETRIC=MINISBLACK suppresses the GDAL ExtraSamples warning that
    fires when writing more than 3 bands.
    """
    profile = profile.copy()
    profile.update({
        'count'        : arr.shape[0],
        'dtype'        : 'float32',
        'compress'     : 'lzw',
        'photometric'  : 'MINISBLACK',   # treat all bands as grayscale data
    })
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(arr.astype('float32'))
        if band_names:
            for idx, name in enumerate(band_names, 1):
                dst.set_band_description(idx, name)
        if metadata:
            dst.update_tags(**metadata)

    n_bands, height, width = arr.shape
    size_mb    = os.path.getsize(output_path) / 1e6
    # Count valid (non-NoData, non-inf) pixels in band 1 as representative
    b0         = arr[0]
    valid_mask = ~((b0 == NODATA) | np.isnan(b0) | np.isinf(b0))
    n_valid    = int(np.count_nonzero(valid_mask))
    print(f"  GeoTIFF saved → {output_path}")
    print(f"    {n_bands} band(s) | {height}×{width} px grid | "
          f"{n_valid:,} valid pixels (band 1) | {size_mb:.1f} MB")


def _scale_nodata(arr: np.ndarray,
                  scale: float,
                  nodata: float = NODATA) -> np.ndarray:
    """
    Multiply arr by scale while preserving nodata pixels.
    Also clamps ±inf and extreme RF outliers to nodata so they never
    propagate into stats or plots.
    """
    out  = arr.astype(np.float64).copy()
    mask = (arr == nodata) | np.isnan(arr) | np.isinf(arr)
    out  = out * scale
    out[mask] = nodata
    # Clamp any remaining extreme values that slipped through
    # (e.g. RF model edge artefacts that aren't exactly ±inf but are
    # physically impossible).  ±1e6 catches only genuine corruption.
    out = np.where((out < -1e6) | (out > 1e6), nodata, out)
    return out.astype(np.float32)


def _annual_mean_band(monthly_12: np.ndarray,
                      nodata: float = NODATA) -> np.ndarray:
    """
    Compute pixel-wise annual mean from 12 monthly bands.
    shape in: (12, H, W)  →  shape out: (1, H, W)
    NoData, NaN, and ±inf pixels are excluded; result is NoData where all
    12 months are invalid.
    """
    bad    = (monthly_12 == nodata) | np.isnan(monthly_12) | np.isinf(monthly_12)
    valid  = np.where(bad, np.nan, monthly_12.astype(np.float64))
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', RuntimeWarning)
        annual = np.nanmean(valid, axis=0)
    annual = np.where(np.isnan(annual), nodata, annual).astype(np.float32)
    return annual[np.newaxis, :]


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


def _print_stats(label: str, arr: np.ndarray, nodata: float = NODATA):
    """Print pixel count + min/mean/max/std, excluding NoData and ±inf."""
    good  = ~((arr == nodata) | np.isnan(arr) | np.isinf(arr))
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
    """Return a masked numpy array (nodata → nan)."""
    out = arr.astype(np.float64).copy()
    out[(out == nodata) | np.isnan(out)] = np.nan
    return out


# =============================================================================
# APPLICATION 1 — MONTHLY AET
# =============================================================================

def run_monthly_et(cfg: dict,
                   region: ee.Geometry,
                   aet_stack=None,
                   gap_flags=None) -> str:
    """
    Monthly mean daily AET for every 30 m pixel.
    Output  : monthly_et_<TEHSIL>_<YEAR>.tif  (12 bands, mm/day)
    Bands   : ET_Jan_daily_mm … ET_Dec_daily_mm
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)

    print(f"\n{'='*60}")
    print(f"  [monthly_et]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg['model_aez'])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    print("  Downloading pixel data (AET stack, 12 bands) ...")
    tile_paths = _download_image_as_geotiff(aet_stack, region, chunk_size, 'monthly_et')
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    # Convert from 0.1 mm/day (GEE internal) → mm/day
    et_data    = _scale_nodata(mosaic, scale=0.1)
    band_names = [f'ET_{abbr}_daily_mm' for abbr in MONTH_ABBR]
    gf_list    = [MONTH_ABBR[i] for i, gf in enumerate(gap_flags or []) if gf]

    out_path = os.path.join(outdir, f'monthly_et_{tehsil}_{year}.tif')
    _save_geotiff(et_data, profile, out_path,
                  band_names=band_names,
                  metadata={
                      'units'             : 'mm/day',
                      'year'              : str(year),
                      'tehsil'            : tehsil,
                      'gap_filled_months' : ','.join(gf_list) or 'none',
                      'description'       : 'Mean daily AET per month at 30 m',
                  })
    _print_stats('Monthly AET (mm/day) — all months / all pixels', et_data)

    if cfg.get('plot'):
        _plot_monthly_et(et_data, tehsil, year, outdir)

    return out_path


# =============================================================================
# APPLICATION 2 — ANNUAL ET
# =============================================================================

def run_annual_et(cfg: dict,
                  region: ee.Geometry,
                  aet_stack=None,
                  gap_flags=None) -> str:
    """
    Annual total ET for every 30 m pixel.
    Formula : annual_ET = Σ_m(daily_ET_m × days_in_month_m)
    Output  : annual_et_<TEHSIL>_<YEAR>.tif  (1 band, mm/yr)
    Bands   : ET_annual_mm
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)

    print(f"\n{'='*60}")
    print(f"  [annual_et]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg['model_aez'])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    annual_img = build_annual_et_image(aet_stack, year)

    print("  Downloading pixel data (annual ET, 1 band) ...")
    tile_paths = _download_image_as_geotiff(annual_img, region, chunk_size, 'annual_et')
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    # Convert from 0.1 mm/yr → mm/yr
    et_annual = _scale_nodata(mosaic, scale=0.1)

    out_path = os.path.join(outdir, f'annual_et_{tehsil}_{year}.tif')
    _save_geotiff(et_annual, profile, out_path,
                  band_names=['ET_annual_mm'],
                  metadata={
                      'units'      : 'mm/yr',
                      'year'       : str(year),
                      'tehsil'     : tehsil,
                      'description': 'Annual total AET at 30 m',
                  })
    _print_stats('Annual ET (mm/yr)', et_annual)

    if cfg.get('plot'):
        _plot_annual_et(et_annual, tehsil, year, outdir)

    return out_path


# =============================================================================
# APPLICATION 3 — PET  (MODIS MOD16A2)
# =============================================================================

def run_pet(cfg: dict,
            region: ee.Geometry,
            aet_stack=None,
            gap_flags=None,
            pet_stack=None) -> str:
    """
    Monthly mean daily PET (MODIS MOD16A2) for every 30 m pixel.

    Pixel consistency:
      PET is sampled on the AET pixel grid.  An AET carrier band drives
      sampling so the spatial extent matches monthly_et exactly.  Pixels
      where MODIS has no data are written as NoData.

    Output  : pet_<TEHSIL>_<YEAR>.tif  (12 bands, mm/day)
    Bands   : PET_Jan_daily_mm … PET_Dec_daily_mm
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)
    modis_col  = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [pet]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if aet_stack is None:
        print("  Building AET stack (pixel-grid carrier) ...")
        classifier = build_classifier(cfg['model_aez'])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj      = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    # 1 carrier band + 12 PET bands → total 13 bands
    carrier       = aet_stack.select('ET_01').rename('_carrier')
    img_to_dl     = carrier.addBands(pet_stack.unmask(NODATA))

    print("  Downloading pixel data (AET carrier + PET, 13 bands) ...")
    tile_paths = _download_image_as_geotiff(img_to_dl, region, chunk_size, 'pet')
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    # Drop carrier (band 0), keep PET bands (1-12), convert 0.1 mm/day → mm/day
    pet_data   = _scale_nodata(mosaic[1:], scale=0.1)
    band_names = [f'PET_{abbr}_daily_mm' for abbr in MONTH_ABBR]

    out_path = os.path.join(outdir, f'pet_{tehsil}_{year}.tif')
    profile.update({'count': 12})
    _save_geotiff(pet_data, profile, out_path,
                  band_names=band_names,
                  metadata={
                      'units'      : 'mm/day',
                      'source'     : 'MODIS MOD16A2',
                      'year'       : str(year),
                      'tehsil'     : tehsil,
                      'description': 'Mean daily PET per month at 30 m (resampled from MODIS)',
                  })

    if cfg.get('plot'):
        _plot_pet(pet_data, tehsil, year, outdir)

    return out_path


# =============================================================================
# APPLICATION 4 — RWDI
# =============================================================================

def run_rwdi(cfg: dict,
             region: ee.Geometry,
             aet_stack=None,
             gap_flags=None,
             pet_stack=None) -> str:
    """
    RWDI = (1 – AET/PET) × 100  (%)

    Output  : rwdi_<TEHSIL>_<YEAR>.tif  (13 bands)
    Bands   : RWDI_Jan … RWDI_Dec, RWDI_annual
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)
    modis_col  = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [rwdi]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg['model_aez'])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj      = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    rwdi_img  = build_rwdi_image(aet_stack, pet_stack)
    carrier   = aet_stack.select('ET_01').rename('_carrier')
    img_to_dl = carrier.addBands(rwdi_img.unmask(NODATA))

    print("  Downloading pixel data (AET carrier + RWDI, 13 bands) ...")
    tile_paths = _download_image_as_geotiff(img_to_dl, region, chunk_size, 'rwdi')
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    # Drop carrier (band 0); bands 1-12 are monthly RWDI (already in %)
    rwdi_monthly = mosaic[1:]                         # shape (12, H, W)
    rwdi_annual  = _annual_mean_band(rwdi_monthly)    # shape (1, H, W)
    rwdi_data    = np.concatenate([rwdi_monthly, rwdi_annual], axis=0)  # (13, H, W)

    band_names = ([f'RWDI_{abbr}' for abbr in MONTH_ABBR]
                  + ['RWDI_annual'])

    out_path = os.path.join(outdir, f'rwdi_{tehsil}_{year}.tif')
    profile.update({'count': 13})
    _save_geotiff(rwdi_data, profile, out_path,
                  band_names=band_names,
                  metadata={
                      'units'      : 'percent',
                      'formula'    : '(1 - AET/PET) * 100',
                      'year'       : str(year),
                      'tehsil'     : tehsil,
                      'description': 'Relative Water Deficit Index per month + annual mean',
                  })
    _print_stats('Annual mean RWDI (%)', rwdi_annual)

    if cfg.get('plot'):
        _plot_rwdi(rwdi_monthly, tehsil, year, outdir)

    return out_path


# =============================================================================
# APPLICATION 5 — WATER STRESS
# =============================================================================

def run_water_stress(cfg: dict,
                     region: ee.Geometry,
                     aet_stack=None,
                     gap_flags=None,
                     pet_stack=None) -> str:
    """
    Water Stress = AET / PET  (0–1)

    Output  : water_stress_<TEHSIL>_<YEAR>.tif  (13 bands)
    Bands   : WaterStress_Jan … WaterStress_Dec, WaterStress_annual
    """
    tehsil     = cfg['tehsil_name']
    year       = cfg['year']
    outdir     = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)
    modis_col  = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [water_stress]  {tehsil}  |  {year}")
    print(f"{'='*60}")

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg['model_aez'])
        _check_landsat(region, year)
        aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj      = _get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, modis_col, proj)

    ws_img    = build_ws_image(aet_stack, pet_stack)
    carrier   = aet_stack.select('ET_01').rename('_carrier')
    img_to_dl = carrier.addBands(ws_img.unmask(NODATA))

    print("  Downloading pixel data (AET carrier + Water Stress, 13 bands) ...")
    tile_paths = _download_image_as_geotiff(img_to_dl, region, chunk_size, 'water_stress')
    mosaic, profile = _merge_tiles(tile_paths)
    if mosaic is None:
        return None

    ws_monthly = mosaic[1:]
    ws_annual  = _annual_mean_band(ws_monthly)
    ws_data    = np.concatenate([ws_monthly, ws_annual], axis=0)

    band_names = ([f'WaterStress_{abbr}' for abbr in MONTH_ABBR]
                  + ['WaterStress_annual'])

    out_path = os.path.join(outdir, f'water_stress_{tehsil}_{year}.tif')
    profile.update({'count': 13})
    _save_geotiff(ws_data, profile, out_path,
                  band_names=band_names,
                  metadata={
                      'units'      : 'ratio (0-1)',
                      'formula'    : 'AET / PET',
                      'year'       : str(year),
                      'tehsil'     : tehsil,
                      'description': 'Water Stress ratio per month + annual mean',
                  })
    _print_stats('Annual mean Water Stress (AET/PET)', ws_annual)

    if cfg.get('plot'):
        _plot_water_stress(ws_monthly, tehsil, year, outdir)

    return out_path


# =============================================================================
# ALL  (single combined download — guaranteed pixel consistency)
# =============================================================================

def run_all(cfg: dict, region: ee.Geometry) -> dict:
    """
    Run all five applications in ONE combined GEE download.

    Pixel consistency guarantee:
      All five GeoTIFFs are derived from a single 49-band image download.
      They are therefore spatially identical — same CRS, transform, and
      pixel grid.  No post-processing alignment is needed.

    Efficiency:
      AET and PET stacks are built once and shared.
      Only ONE set of tile downloads is performed.
    """
    year      = cfg['year']
    tehsil    = cfg['tehsil_name']
    outdir    = cfg['output']
    chunk_size = cfg.get('chunk_size', 25000)
    modis_col = cfg.get('modis_collection', MODIS_COL)

    print(f"\n{'='*60}")
    print(f"  [all] Building shared GEE stacks ...")
    print(f"{'='*60}")

    print("\n  [1/2] Building AET stack (Landsat 8 + GLDAS → RF model) ...")
    classifier = build_classifier(cfg['model_aez'])
    _check_landsat(region, year)
    aet_stack, gap_flags = build_aet_stack(region, classifier, year)

    print("\n  [2/2] Building PET stack (MODIS MOD16A2) ...")
    proj      = _get_proj_30m(region, year)
    pet_stack = build_pet_stack(region, year, modis_col, proj)

    print("\n  Building combined image (49 bands) ...")
    combined = build_combined_image(aet_stack, pet_stack, year)

    print(f"\n  Downloading all bands in ONE pass ...")
    tile_paths = _download_image_as_geotiff(combined, region, chunk_size, 'all')
    mosaic, profile = _merge_tiles(tile_paths)

    if mosaic is None:
        print("[ERROR] No pixel data returned.")
        return {}

    # --- Slice and convert each application from the combined mosaic ---
    gf_list  = [MONTH_ABBR[i] for i, gf in enumerate(gap_flags or []) if gf]
    gap_meta = ','.join(gf_list) or 'none'
    results  = {}

    # 1. Monthly AET  (bands 0-11, ×0.1 → mm/day)
    et_data = _scale_nodata(mosaic[_AET_SLICE], scale=0.1)
    p = os.path.join(outdir, f'monthly_et_{tehsil}_{year}.tif')
    _save_geotiff(et_data, profile, p,
                  band_names=[f'ET_{a}_daily_mm' for a in MONTH_ABBR],
                  metadata={'units': 'mm/day', 'year': str(year),
                            'tehsil': tehsil,
                            'gap_filled_months': gap_meta,
                            'description': 'Mean daily AET per month at 30 m'})
    results['monthly_et'] = p

    # 2. Annual ET  (band 24, ×0.1 → mm/yr)
    annual_data = _scale_nodata(mosaic[_ANNUAL_SLICE], scale=0.1)
    p = os.path.join(outdir, f'annual_et_{tehsil}_{year}.tif')
    _save_geotiff(annual_data, profile, p,
                  band_names=['ET_annual_mm'],
                  metadata={'units': 'mm/yr', 'year': str(year),
                            'tehsil': tehsil,
                            'description': 'Annual total AET at 30 m'})
    results['annual_et'] = p

    # 3. PET  (bands 12-23, ×0.1 → mm/day)
    pet_data = _scale_nodata(mosaic[_PET_SLICE], scale=0.1)
    p = os.path.join(outdir, f'pet_{tehsil}_{year}.tif')
    _save_geotiff(pet_data, profile, p,
                  band_names=[f'PET_{a}_daily_mm' for a in MONTH_ABBR],
                  metadata={'units': 'mm/day', 'source': 'MODIS MOD16A2',
                            'year': str(year), 'tehsil': tehsil,
                            'description': 'Mean daily PET per month at 30 m'})
    results['pet'] = p

    # 4. RWDI  (bands 25-36, already in %; add annual mean as band 13)
    rwdi_monthly = mosaic[_RWDI_SLICE]
    rwdi_annual  = _annual_mean_band(rwdi_monthly)
    rwdi_data    = np.concatenate([rwdi_monthly, rwdi_annual], axis=0)
    p = os.path.join(outdir, f'rwdi_{tehsil}_{year}.tif')
    _save_geotiff(rwdi_data, profile, p,
                  band_names=[f'RWDI_{a}' for a in MONTH_ABBR] + ['RWDI_annual'],
                  metadata={'units': 'percent', 'formula': '(1 - AET/PET) * 100',
                            'year': str(year), 'tehsil': tehsil,
                            'description': 'RWDI per month + annual mean'})
    results['rwdi'] = p

    # 5. Water Stress  (bands 37-48, ratio 0-1; add annual mean as band 13)
    ws_monthly = mosaic[_WS_SLICE]
    ws_annual  = _annual_mean_band(ws_monthly)
    ws_data    = np.concatenate([ws_monthly, ws_annual], axis=0)
    p = os.path.join(outdir, f'water_stress_{tehsil}_{year}.tif')
    _save_geotiff(ws_data, profile, p,
                  band_names=[f'WaterStress_{a}' for a in MONTH_ABBR] + ['WaterStress_annual'],
                  metadata={'units': 'ratio (0-1)', 'formula': 'AET / PET',
                            'year': str(year), 'tehsil': tehsil,
                            'description': 'Water Stress per month + annual mean'})
    results['water_stress'] = p

    # Print summary stats
    _print_stats('Annual ET (mm/yr)',               annual_data)
    _print_stats('Annual mean RWDI (%)',             rwdi_annual)
    _print_stats('Annual mean Water Stress (ratio)', ws_annual)

    # Optional plots
    if cfg.get('plot'):
        _plot_monthly_et(et_data,     tehsil, year, outdir)
        _plot_annual_et(annual_data,  tehsil, year, outdir)
        _plot_pet(pet_data,           tehsil, year, outdir)
        _plot_rwdi(rwdi_monthly,      tehsil, year, outdir)
        _plot_water_stress(ws_monthly, tehsil, year, outdir)

    # Sample point timeseries
    run_sample_timeseries(results, cfg)

    return results


# =============================================================================
# SAMPLE POINT TIME SERIES
# =============================================================================

def run_sample_timeseries(output_paths: dict, cfg: dict):
    """
    Extract the 12-month time series from the pixel nearest to a requested
    lon/lat and produce a 4-panel plot.  Uses rasterio.sample on the output
    GeoTIFFs — no additional GEE calls are made.

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

    tehsil = cfg['tehsil_name']
    year   = cfg['year']
    outdir = cfg['output']

    print(f"\n[sample] Sampling pixel at lon={lon:.6f}, lat={lat:.6f}")

    def _sample(tif_path):
        if tif_path is None or not os.path.exists(tif_path):
            return None
        with rasterio.open(tif_path) as ds:
            vals = list(ds.sample([(lon, lat)]))[0].astype(np.float32)
            vals[vals == NODATA] = np.nan
        return vals

    aet = _sample(output_paths.get('monthly_et'))    # shape (12,) mm/day
    pet = _sample(output_paths.get('pet'))           # shape (12,) mm/day
    rwdi = _sample(output_paths.get('rwdi'))         # shape (13,) %
    ws  = _sample(output_paths.get('water_stress'))  # shape (13,) ratio

    if aet is None:
        print("  [sample] monthly_et GeoTIFF not available — skipping.")
        return

    rwdi_m = rwdi[:12] if rwdi is not None else np.full(12, np.nan)
    ws_m   = ws[:12]   if ws   is not None else np.full(12, np.nan)
    pet_m  = pet       if pet  is not None else np.full(12, np.nan)

    if cfg.get('plot'):
        _plot_sample_timeseries(aet, pet_m, rwdi_m, ws_m,
                                lon, lat, tehsil, year, outdir)


# =============================================================================
# PLOTS  (accept numpy arrays; no pandas dependency)
# =============================================================================

def _band_stats(arr: np.ndarray, nodata: float = NODATA):
    """Per-band mean and std, ignoring NoData, NaN, and ±inf. arr shape: (n_bands, H, W)."""
    out_mean, out_std = [], []
    for b in range(arr.shape[0]):
        v = arr[b]
        good = ~((v == nodata) | np.isnan(v) | np.isinf(v))
        v = v[good].astype(np.float64)
        out_mean.append(np.nanmean(v) if v.size else np.nan)
        out_std.append(np.nanstd(v)  if v.size else np.nan)
    return np.array(out_mean), np.array(out_std)


def _plot_monthly_et(arr, tehsil, year, outdir):
    """arr shape: (12, H, W), values in mm/day."""
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    means, stds = _band_stats(arr)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(MONTH_ABBR, means, marker='o', color='#004400', linewidth=2.5,
            markersize=7, label='Mean across pixels')
    ax.fill_between(range(12), means - stds, means + stds,
                    alpha=0.2, color='#228B22', label='±1 std')
    ax.set_title(f'Monthly Mean Daily AET — {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('AET (mm/day)')
    ax.legend(fontsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    p = os.path.join(outdir, f'monthly_et_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot → {p}")


def _plot_annual_et(arr, tehsil, year, outdir):
    """arr shape: (1, H, W), values in mm/yr."""
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    b    = arr[0]
    good = ~((b == NODATA) | np.isnan(b) | np.isinf(b))
    valid = b[good]
    if valid.size == 0:
        print("  [plot] No valid data for annual ET histogram — skipping.")
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(valid, bins=60, color='#228B22', edgecolor='white', linewidth=0.4)
    mean_val = np.mean(valid)
    ax.axvline(mean_val, color='red', linestyle='--', linewidth=1.5,
               label=f'Mean = {mean_val:.1f} mm/yr')
    ax.set_title(f'Annual ET Distribution — {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_xlabel('Annual ET (mm/yr)')
    ax.set_ylabel('Pixel count')
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    p = os.path.join(outdir, f'annual_et_hist_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot → {p}")


def _plot_pet(arr, tehsil, year, outdir):
    """arr shape: (12, H, W), values in mm/day."""
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    means, stds = _band_stats(arr)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(MONTH_ABBR, means, marker='s', color='#1a6fa3', linewidth=2.5,
            markersize=7, label='Mean across pixels')
    ax.fill_between(range(12), means - stds, means + stds,
                    alpha=0.2, color='#1a6fa3', label='±1 std')
    ax.set_title(f'Monthly Mean Daily PET (MODIS MOD16A2) — {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('PET (mm/day)')
    ax.legend(fontsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    p = os.path.join(outdir, f'pet_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot → {p}")


def _plot_rwdi(arr, tehsil, year, outdir):
    """arr shape: (12, H, W), values in %."""
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    means, _ = _band_stats(arr)

    def _clr(v):
        if np.isnan(v):
            return '#cccccc'
        for lo, hi, c, _ in RWDI_CLASSES:
            if lo <= v < hi:
                return c
        return '#FF0000'

    fig, ax = plt.subplots(figsize=(12, 5))
    for i, (abbr, val) in enumerate(zip(MONTH_ABBR, means)):
        safe = 0 if np.isnan(val) else val
        ax.bar(abbr, safe, color=_clr(val), edgecolor='black', linewidth=0.6)
        if not np.isnan(val):
            ax.text(i, val + 1, f'{val:.0f}%', ha='center', va='bottom', fontsize=8.5)
    for thresh, lc in [(30, '#9ACD32'), (50, '#FFA500'), (80, '#FF0000')]:
        ax.axhline(thresh, color=lc, linestyle='--', linewidth=1.2)
    ax.set_ylim(0, 110)
    ax.set_title(f'Monthly RWDI (pixel mean) — {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('RWDI (%)')
    patches = [mpatches.Patch(color=c, label=lbl) for _, _, c, lbl in RWDI_CLASSES]
    ax.legend(handles=patches, fontsize=8, loc='upper right')
    plt.tight_layout()
    p = os.path.join(outdir, f'rwdi_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot → {p}")


def _plot_water_stress(arr, tehsil, year, outdir):
    """arr shape: (12, H, W), values as ratio 0–1."""
    if not HAS_MPL:
        return
    os.makedirs(outdir, exist_ok=True)
    means, _ = _band_stats(arr)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(MONTH_ABBR, means, marker='s', color='#1a9641',
            linewidth=2.5, markersize=7)
    ax.fill_between(range(12), means, alpha=0.15, color='#66bd63')
    ax.axhline(1.0, color='#1a4fa3', linestyle='--', linewidth=1.3,
               label='No stress (1.0)')
    ax.axhline(0.5, color='#FF8C00', linestyle='--', linewidth=1.3,
               label='Moderate (0.5)')
    ax.axhline(0.3, color='#FF0000', linestyle='--', linewidth=1.3,
               label='High (0.3)')
    ax.set_ylim(0, 1.25)
    ax.set_title(f'Monthly Water Stress (AET/PET) — {tehsil} ({year})',
                 fontsize=13, fontweight='bold')
    ax.set_ylabel('AET / PET')
    ax.legend(fontsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    p = os.path.join(outdir, f'water_stress_{tehsil}_{year}.png')
    fig.savefig(p, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  Plot → {p}")


def _plot_sample_timeseries(aet, pet, rwdi, ws, lon, lat, tehsil, year, outdir):
    """4-panel monthly timeseries for a single pixel."""
    if not HAS_MPL:
        return
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        f'Monthly Time Series — Single Pixel  |  {tehsil} ({year})\n'
        f'lon={lon:.5f}, lat={lat:.5f}',
        fontsize=13, fontweight='bold', y=1.01
    )

    ax = axes[0, 0]
    ax.plot(MONTH_ABBR, aet, marker='o', color='#228B22', linewidth=2.2, markersize=7)
    ax.fill_between(range(12), aet, alpha=0.15, color='#228B22')
    ax.set_title('Mean Daily AET (mm/day)')
    ax.set_ylabel('AET (mm/day)')
    ax.grid(axis='y', linestyle='--', alpha=0.4)

    ax = axes[0, 1]
    ax.plot(MONTH_ABBR, pet, marker='s', color='#1a6fa3', linewidth=2.2, markersize=7)
    ax.fill_between(range(12), pet, alpha=0.15, color='#1a6fa3')
    ax.set_title('Mean Daily PET (mm/day) — MODIS')
    ax.set_ylabel('PET (mm/day)')
    ax.grid(axis='y', linestyle='--', alpha=0.4)

    ax = axes[1, 0]
    def _rclr(v):
        if np.isnan(v):
            return '#cccccc'
        for lo, hi, c, _ in RWDI_CLASSES:
            if lo <= float(v) < hi:
                return c
        return '#FF0000'
    safe_rwdi = np.where(np.isnan(rwdi), 0, rwdi)
    bars = ax.bar(MONTH_ABBR, safe_rwdi, color=[_rclr(v) for v in rwdi],
                  edgecolor='black', linewidth=0.5)
    for bar, val in zip(bars, rwdi):
        if not np.isnan(val):
            ax.text(bar.get_x() + bar.get_width() / 2, val + 1,
                    f'{val:.0f}%', ha='center', va='bottom', fontsize=7.5)
    ax.set_ylim(0, 115)
    ax.set_title('Monthly RWDI (%)')
    ax.set_ylabel('RWDI (%)')
    patches = [mpatches.Patch(color=c, label=lbl) for _, _, c, lbl in RWDI_CLASSES]
    ax.legend(handles=patches, fontsize=7, loc='upper right')

    ax = axes[1, 1]
    ax.plot(MONTH_ABBR, ws, marker='^', color='#d62728', linewidth=2.2, markersize=7)
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
    print(f"  Plot → {p}")


# =============================================================================
# CLI
# =============================================================================

def build_parser():
    p = argparse.ArgumentParser(
        prog='et_applications',
        description='Pan-India ET Downscaling — GeoTIFF Raster Edition',
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
                             'rwdi', 'water_stress'],
                   help='Which application to run (default: all).')
    p.add_argument('--sample-lon',   type=float, default=None,
                   help='Longitude for single-pixel timeseries plot.')
    p.add_argument('--sample-lat',   type=float, default=None,
                   help='Latitude  for single-pixel timeseries plot.')
    return p


def main():
    parser = build_parser()
    args   = parser.parse_args()

    config_path = pathlib.Path(args.config) if args.config else CONFIG_PATH
    cfg = load_config(config_path)
    cfg = merge_args(cfg, args)

    app = cfg.get('application', 'all')

    if not cfg.get('tehsil_asset'):
        parser.error('tehsil_asset is required (config.yaml or --tehsil-asset)')
    if not cfg.get('model_aez'):
        parser.error('model_aez is required (config.yaml or --model-aez)')
    if not cfg.get('tehsil_name'):
        cfg['tehsil_name'] = cfg['tehsil_asset'].rstrip('/').split('/')[-1].upper()

    print("\n" + "="*68)
    print("  ET-Applications  (GeoTIFF Raster Edition)")
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

    # For individual application runs, attempt sample timeseries if requested
    if app != 'all' and isinstance(result, str):
        output_paths = {app: result}
        run_sample_timeseries(output_paths, cfg)

    print(f"\nDone.  All outputs in: {os.path.abspath(cfg.get('output', './results'))}")


if __name__ == '__main__':
    main()
