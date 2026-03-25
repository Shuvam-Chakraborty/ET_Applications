/**
 * ET-Applications — GEE Script 1 of 2
 * FILE   : 1_Check_Tehsil.js
 * PURPOSE: Visualize and verify your tehsil BEFORE generating the asset.
 *          Run this first. When you are happy with what you see on the map,
 *          run 2_Generate_Tehsil_Boundary.js to export the asset.
 *
 * HOW TO USE:
 *   1. Edit the CONFIG block below (same block used in 01b).
 *   2. Click Run.
 *   3. Inspect the map, console stats, and Inspector pixel values.
 *   4. If everything looks right → go to 01b and run that script.
 */

// ============================================================================
// ★ USER CONFIG — edit here, then copy the same block into 01b
// ============================================================================
var CONFIG = {
  state     : 'UTTAR PRADESH',     // UPPER CASE, must match the dataset
  district  : 'BANDA',             // UPPER CASE
  tehsil    : 'TELYANI',           // UPPER CASE
  geeProject: 'shuvamdownscalinget' // Your GEE cloud project ID
};
// ============================================================================

var blocks = ee.FeatureCollection('users/mtpictd/india_block_boundaries');

// ── 1. SEARCH ────────────────────────────────────────────────────────────────
// Primary: match by tehsil + district to guard against same-name tehsils
var primaryMatch = blocks.filter(
  ee.Filter.and(
    ee.Filter.eq('tehsil',   CONFIG.tehsil),
    ee.Filter.eq('district', CONFIG.district)
  )
);

// Fallback: tehsil-only match (catches datasets where district field differs)
var fallbackMatch = blocks.filter(ee.Filter.eq('tehsil', CONFIG.tehsil));

var selectedTehsil = ee.Algorithms.If(
  primaryMatch.size().gt(0),
  primaryMatch,
  fallbackMatch
);
selectedTehsil = ee.FeatureCollection(selectedTehsil);

// ── 2. CONSOLE REPORT ────────────────────────────────────────────────────────
var count = selectedTehsil.size().getInfo();

print('════════════════════════════════════');
print('  TEHSIL CHECK REPORT');
print('════════════════════════════════════');
print('Query → State    :', CONFIG.state);
print('Query → District :', CONFIG.district);
print('Query → Tehsil   :', CONFIG.tehsil);
print('────────────────────────────────────');

if (count === 0) {
  print('⚠ NO FEATURES FOUND for this combination.');
  print('');
  print('Try one of the suggestions below:');

  // List all tehsils in the given district to help the user correct spelling
  var districtTehsils = blocks
    .filter(ee.Filter.eq('district', CONFIG.district))
    .aggregate_array('tehsil')
    .distinct()
    .sort();

  districtTehsils.evaluate(function(list) {
    if (list && list.length > 0) {
      print('Tehsils found in district "' + CONFIG.district + '":');
      list.forEach(function(t) { print('  •', t); });
    } else {
      print('No tehsils found in district "' + CONFIG.district +
            '" either. Check your district spelling.');

      // Go one level up — list all districts in the state
      var stateDistricts = blocks
        .filter(ee.Filter.eq('state', CONFIG.state))
        .aggregate_array('district')
        .distinct()
        .sort();
      stateDistricts.evaluate(function(dlist) {
        if (dlist && dlist.length > 0) {
          print('Districts found in state "' + CONFIG.state + '":');
          dlist.forEach(function(d) { print('  •', d); });
        } else {
          print('No districts found in state "' + CONFIG.state +
                '" either. Check your state spelling.');
        }
      });
    }
  });

} else {

  // ── 3. METADATA SUMMARY ────────────────────────────────────────────────────
  var region   = selectedTehsil.geometry();
  var areaSqKm = region.area(100).divide(1e6);
  var centroid = region.centroid(100);
  var bounds   = region.bounds(100);
  var coords   = bounds.coordinates().get(0);

  selectedTehsil.first().toDictionary().evaluate(function(props) {
    print('✔ Match found (' + count + ' feature' + (count > 1 ? 's' : '') + ')');
    print('────────────────────────────────────');
    print('Stored State    :', props.state    || 'N/A');
    print('Stored District :', props.district || 'N/A');
    print('Stored Tehsil   :', props.tehsil   || 'N/A');
    if (props.block)  print('Block           :', props.block);
    if (props.taluka) print('Taluka          :', props.taluka);
  });

  areaSqKm.evaluate(function(a) {
    print('Area            :', a.toFixed(2) + ' km²');
  });

  centroid.coordinates().evaluate(function(c) {
    print('Centroid (lon)  :', c[0].toFixed(5));
    print('Centroid (lat)  :', c[1].toFixed(5));
  });

  ee.List(coords).evaluate(function(pts) {
    var lons = pts.map(function(p){ return p[0]; });
    var lats = pts.map(function(p){ return p[1]; });
    print('Bounding Box    :');
    print('  West :', Math.min.apply(null, lons).toFixed(5));
    print('  East :', Math.max.apply(null, lons).toFixed(5));
    print('  South:', Math.min.apply(null, lats).toFixed(5));
    print('  North:', Math.max.apply(null, lats).toFixed(5));
  });

  print('────────────────────────────────────');
  print('✔ If all looks correct, run 2_Generate_Tehsil_Boundary.js');
  print('════════════════════════════════════');

  // ── 4. MAP VISUALIZATION ──────────────────────────────────────────────────
  Map.centerObject(selectedTehsil, 11);

  // Filled polygon — semi-transparent green
  Map.addLayer(
    selectedTehsil.style({
      color      : '1a6b1a',   // dark green outline
      width      : 3,
      fillColor  : '228B2233'  // light green fill, ~20% opacity
    }),
    {},
    'Tehsil — ' + CONFIG.tehsil + ' (filled)'
  );

  // Outline only — sits on top for crisp boundary
  Map.addLayer(
    selectedTehsil.style({
      color    : '000000',
      width    : 2,
      fillColor: '00000000'
    }),
    {},
    'Tehsil — boundary outline'
  );

  // Centroid marker
  Map.addLayer(
    centroid,
    {color: 'FF0000'},
    'Centroid'
  );

  // ── 5. NEIGHBOUR CONTEXT ─────────────────────────────────────────────────
  // Show all neighbouring blocks in the same district (grey) for spatial context
  var neighbours = blocks
    .filter(ee.Filter.eq('district', CONFIG.district))
    .filter(ee.Filter.neq('tehsil',  CONFIG.tehsil));

  Map.addLayer(
    neighbours.style({color: '999999', width: 1, fillColor: 'CCCCCC33'}),
    {},
    'Neighbouring blocks (' + CONFIG.district + ')',
    false    // hidden by default — toggle in Layers panel
  );

  // ── 6. INSPECTOR HINT ─────────────────────────────────────────────────────
  Map.style().set('cursor', 'crosshair');
  Map.onClick(function(coords) {
    var pt = ee.Geometry.Point([coords.lon, coords.lat]);
    selectedTehsil.filterBounds(pt).first().toDictionary().evaluate(function(props) {
      if (props) {
        print('Inspector click →', coords.lon.toFixed(5), coords.lat.toFixed(5));
        print('  Inside tehsil:', JSON.stringify(props));
      } else {
        print('Inspector click →', coords.lon.toFixed(5), coords.lat.toFixed(5),
              '(outside selected tehsil)');
      }
    });
  });

  // ── 7. UI PANEL (on-map info card) ────────────────────────────────────────
  var panel = ui.Panel({
    style: {
      position : 'top-right',
      padding  : '10px 14px',
      width    : '260px',
      backgroundColor: 'rgba(255,255,255,0.92)'
    }
  });

  panel.add(ui.Label('🗺 Tehsil Check', {
    fontWeight: 'bold', fontSize: '15px', margin: '0 0 6px 0'
  }));
  panel.add(ui.Label('State    : ' + CONFIG.state,    {fontSize: '12px', margin: '2px 0'}));
  panel.add(ui.Label('District : ' + CONFIG.district, {fontSize: '12px', margin: '2px 0'}));
  panel.add(ui.Label('Tehsil   : ' + CONFIG.tehsil,   {
    fontSize: '13px', fontWeight: 'bold', color: '#1a6b1a', margin: '4px 0'
  }));
  panel.add(ui.Label('─────────────────────',         {fontSize: '10px', color: '#aaa'}));
  panel.add(ui.Label('✔ Features found: ' + count,    {fontSize: '12px', color: '#1a6b1a'}));
  panel.add(ui.Label('─────────────────────',         {fontSize: '10px', color: '#aaa'}));
  panel.add(ui.Label('If this looks correct →\nrun 2 to export the asset.',
                      {fontSize: '11px', color: '#333', fontStyle: 'italic'}));

  Map.add(panel);
}