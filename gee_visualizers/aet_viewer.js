// ============================================================
// USER CONFIG
// ============================================================
var ASSET_ID     = 'projects/your-project/assets/aet_TEHSIL_YEAR';
var TEHSIL_ASSET = 'projects/<your-project>/assets/tehsil_<state>__<district>__<tehsil>';
// ============================================================

var MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Annual total'];
var BAND_NAMES   = ['b1','b2','b3','b4','b5','b6','b7','b8','b9','b10','b11','b12','b13'];
var PALETTE      = ['#8B4513','#F5F5DC','#C1D7AE','#228B22','#004400'];

var rawImage = ee.Image(ASSET_ID);
var image    = rawImage.updateMask(rawImage.neq(-9999));
var tehsil   = ee.FeatureCollection(TEHSIL_ASSET);

var mStats = image.select(['b1','b2','b3','b4','b5','b6','b7','b8','b9','b10','b11','b12'])
  .reduceRegion({ reducer: ee.Reducer.minMax(), geometry: image.geometry(), scale: 30, maxPixels: 1e13, bestEffort: true })
  .getInfo();
var aStats = image.select('b13')
  .reduceRegion({ reducer: ee.Reducer.minMax(), geometry: image.geometry(), scale: 30, maxPixels: 1e13, bestEffort: true })
  .getInfo();

var mMin = Math.min.apply(null, Object.keys(mStats).filter(function(k){ return k.indexOf('_min') > -1; }).map(function(k){ return mStats[k]; }));
var mMax = Math.max.apply(null, Object.keys(mStats).filter(function(k){ return k.indexOf('_max') > -1; }).map(function(k){ return mStats[k]; }));

var MONTHLY_VIS = { min: mMin,            max: mMax,            palette: PALETTE };
var ANNUAL_VIS  = { min: aStats['b13_min'], max: aStats['b13_max'], palette: PALETTE };

Map.setOptions('HYBRID');
Map.centerObject(image);

for (var i = 0; i < MONTH_LABELS.length; i++) {
  var vis = (MONTH_LABELS[i] === 'Annual total') ? ANNUAL_VIS : MONTHLY_VIS;
  Map.addLayer(image.select(BAND_NAMES[i]), vis, 'AET - ' + MONTH_LABELS[i], i === 0);
}

Map.addLayer(
  tehsil.style({ color: '#000000', width: 2, fillColor: '00000000' }),
  {}, 'Tehsil Boundary', true
);
