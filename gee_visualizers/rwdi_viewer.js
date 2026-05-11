// ============================================================
// USER CONFIG
// ============================================================
var ASSET_ID     = 'projects/your-project/assets/rwdi_TEHSIL_YEAR';
var TEHSIL_ASSET = 'projects/<your-project>/assets/tehsil_<state>__<district>__<tehsil>';
// ============================================================

var MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Annual mean'];
var BAND_NAMES   = ['b1','b2','b3','b4','b5','b6','b7','b8','b9','b10','b11','b12','b13'];

// Expanded palette so dark green covers 0-50%, then 10-unit steps per colour
var PALETTE = [
  '#228B22','#228B22','#228B22','#228B22','#228B22',  // 0–50%
  '#9ACD32',                                           // 50–60%
  '#FFFF00',                                           // 60–70%
  '#FFA500',                                           // 70–80%
  '#F08080',                                           // 80–90%
  '#FF0000','#FF0000'                                  // 90–100%
];

var rawImage = ee.Image(ASSET_ID);
var image    = rawImage.updateMask(rawImage.neq(-9999));
var tehsil   = ee.FeatureCollection(TEHSIL_ASSET);

var stats = image.reduceRegion({ reducer: ee.Reducer.minMax(), geometry: image.geometry(), scale: 30, maxPixels: 1e13, bestEffort: true }).getInfo();
var minVal = Math.min.apply(null, Object.keys(stats).filter(function(k){ return k.indexOf('_min') > -1; }).map(function(k){ return stats[k]; }));
var maxVal = Math.max.apply(null, Object.keys(stats).filter(function(k){ return k.indexOf('_max') > -1; }).map(function(k){ return stats[k]; }));

var VIS = { min: minVal, max: maxVal, palette: PALETTE };

Map.setOptions('HYBRID');
Map.centerObject(image);

for (var i = 0; i < MONTH_LABELS.length; i++) {
  Map.addLayer(image.select(BAND_NAMES[i]), VIS, 'RWDI - ' + MONTH_LABELS[i], i === 0);
}

Map.addLayer(
  tehsil.style({ color: '#000000', width: 2, fillColor: '00000000' }),
  {}, 'Tehsil Boundary', true
);
