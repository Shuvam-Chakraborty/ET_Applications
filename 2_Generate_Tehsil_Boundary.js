/**
 * ET-Applications — GEE Script 2 of 2
 * FILE   : 2_Generate_Tehsil_Boundary.js
 * PURPOSE: Export the verified tehsil boundary as a GEE FeatureCollection asset.
 *          Run this ONLY after 1_Check_Tehsil.js confirmed the correct tehsil.
 *
 * HOW TO USE:
 *   1. Copy the same CONFIG block you used in 1 (no changes needed).
 *   2. Click Run — the console will print the asset path and a summary.
 *   3. Switch to the Tasks tab → click RUN next to the export task.
 *   4. Wait for the task to complete (~1 min for most tehsils).
 *   5. Paste the printed asset path into config.yaml → assets.tehsil_asset.
 */

// ============================================================================
// ★ USER CONFIG — paste the same block from 01a, no changes needed
// ============================================================================
var CONFIG = {
  state     : 'UTTAR PRADESH',
  district  : 'BANDA',
  tehsil    : 'TELYANI',
  geeProject: 'shuvamdownscalinget'
};
// ============================================================================

var blocks = ee.FeatureCollection('users/mtpictd/india_block_boundaries');

// ── 1. FILTER (same logic as 01a) ────────────────────────────────────────────
var primaryMatch = blocks.filter(
  ee.Filter.and(
    ee.Filter.eq('tehsil',   CONFIG.tehsil),
    ee.Filter.eq('district', CONFIG.district)
  )
);
var fallbackMatch = blocks.filter(ee.Filter.eq('tehsil', CONFIG.tehsil));
var selectedTehsil = ee.FeatureCollection(
  ee.Algorithms.If(primaryMatch.size().gt(0), primaryMatch, fallbackMatch)
);

var count = selectedTehsil.size().getInfo();

// ── 2. GUARD — abort if nothing found ────────────────────────────────────────
if (count === 0) {
  print('');
  print('✖ ERROR: No features found for this CONFIG.');
  print('  Please run 01a_check_tehsil.js first to find the correct spelling.');
  print('  Export has NOT been submitted.');

} else {

  // ── 3. BUILD ASSET NAME ────────────────────────────────────────────────────
  // Format: tehsil_<state>__<district>__<tehsil>  (lower-case, spaces → _)
  function toSlug(str) {
    return str.toLowerCase().replace(/ /g, '_');
  }
  var assetName = 'tehsil_' +
    toSlug(CONFIG.state) + '__' +
    toSlug(CONFIG.district) + '__' +
    toSlug(CONFIG.tehsil);

  var assetId = 'projects/' + CONFIG.geeProject + '/assets/' + assetName;

  // ── 4. ADD EXPORT METADATA PROPERTY ───────────────────────────────────────
  // Stamp each feature with export info so the asset is self-documenting
  var exportDate = ee.Date(Date.now()).format('YYYY-MM-dd').getInfo();
  selectedTehsil = selectedTehsil.map(function(f) {
    return f.set({
      'exported_by'  : 'ET-Applications/01b_generate_tehsil_boundary.js',
      'export_date'  : exportDate,
      'gee_project'  : CONFIG.geeProject,
      'config_state' : CONFIG.state,
      'config_district': CONFIG.district,
      'config_tehsil': CONFIG.tehsil
    });
  });

  // ── 5. CONSOLE SUMMARY ────────────────────────────────────────────────────
  var region   = selectedTehsil.geometry();
  var areaSqKm = region.area(100).divide(1e6).getInfo();
  var centroid = region.centroid(100).coordinates().getInfo();

  print('════════════════════════════════════════════════');
  print('  TEHSIL BOUNDARY EXPORT — SUMMARY');
  print('════════════════════════════════════════════════');
  print('State          :', CONFIG.state);
  print('District       :', CONFIG.district);
  print('Tehsil         :', CONFIG.tehsil);
  print('Features found :', count);
  print('Area (km²)     :', areaSqKm.toFixed(2));
  print('Centroid lon   :', centroid[0].toFixed(5));
  print('Centroid lat   :', centroid[1].toFixed(5));
  print('────────────────────────────────────────────────');
  print('Asset name     :', assetName);
  print('Asset path     :', assetId);
  print('────────────────────────────────────────────────');
  print('→ Go to the Tasks tab and click RUN to start the export.');
  print('→ After it completes, add this to config.yaml:');
  print('');
  print('     assets:');
  print('       tehsil_asset: "' + assetId + '"');
  print('');
  print('════════════════════════════════════════════════');

  // ── 6. PREVIEW ON MAP ─────────────────────────────────────────────────────
  Map.centerObject(selectedTehsil, 11);

  Map.addLayer(
    selectedTehsil.style({
      color    : '1a6b1a',
      width    : 3,
      fillColor: '228B2244'
    }),
    {},
    'Exporting: ' + CONFIG.tehsil
  );

  Map.addLayer(
    selectedTehsil.style({
      color    : '000000',
      width    : 2,
      fillColor: '00000000'
    }),
    {},
    'Boundary outline (black)'
  );

  // ── 7. UI PANEL ───────────────────────────────────────────────────────────
  var panel = ui.Panel({
    style: {
      position: 'top-right',
      padding : '10px 14px',
      width   : '280px',
      backgroundColor: 'rgba(255,255,255,0.93)'
    }
  });

  panel.add(ui.Label('📦 Export Ready', {
    fontWeight: 'bold', fontSize: '15px', margin: '0 0 6px 0'
  }));
  panel.add(ui.Label('Tehsil : ' + CONFIG.tehsil, {
    fontWeight: 'bold', fontSize: '13px', color: '#1a6b1a'
  }));
  panel.add(ui.Label('District : ' + CONFIG.district, {fontSize: '12px'}));
  panel.add(ui.Label('State    : ' + CONFIG.state,    {fontSize: '12px'}));
  panel.add(ui.Label('─────────────────────────────', {fontSize: '10px', color: '#bbb'}));
  panel.add(ui.Label('Area : ' + areaSqKm.toFixed(1) + ' km²', {fontSize: '12px'}));
  panel.add(ui.Label('─────────────────────────────', {fontSize: '10px', color: '#bbb'}));

  panel.add(ui.Label('Asset path:', {fontSize: '11px', color: '#555', margin: '4px 0 2px 0'}));
  panel.add(ui.Label(assetId, {
    fontSize: '10px', color: '#1a4fa3', whiteSpace: 'pre'
  }));

  panel.add(ui.Label('─────────────────────────────', {fontSize: '10px', color: '#bbb'}));
  panel.add(ui.Label('Next step → Tasks tab → RUN', {
    fontSize: '12px', fontWeight: 'bold', color: '#b03000'
  }));

  Map.add(panel);

  // ── 8. SUBMIT EXPORT ──────────────────────────────────────────────────────
  Export.table.toAsset({
    collection : selectedTehsil,
    description: assetName,           // appears as the task name in Tasks tab
    assetId    : assetId
  });
}