// ============================================================
// USER CONFIG
// ============================================================
var ASSET_ID     = 'projects/your-project/assets/kc_TEHSIL_YEAR';
var TEHSIL_ASSET = 'projects/<your-project>/assets/tehsil_<state>__<district>__<tehsil>';
// ============================================================

var MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Annual mean'];
var BAND_NAMES   = ['b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'b8', 'b9', 'b10', 'b11', 'b12', 'b13'];
var PALETTE      = ['#d7191c', '#e75b35', '#f59d4e', '#fdcf7c', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63', '#1a9641', '#00441b'];

var rawImage = ee.Image(ASSET_ID);
var image    = rawImage.updateMask(rawImage.neq(-9999));
var tehsil   = ee.FeatureCollection(TEHSIL_ASSET);

function formatInteger(value) {
  var rounded = Math.round(Number(value)).toString();
  return rounded.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function formatFixed(value) {
  return Number(value).toFixed(4);
}

function printAnnualStats(title, annualBand) {
  var reducer = ee.Reducer.count()
    .combine({reducer2: ee.Reducer.mean(), sharedInputs: true})
    .combine({reducer2: ee.Reducer.minMax(), sharedInputs: true})
    .combine({reducer2: ee.Reducer.stdDev(), sharedInputs: true});

  var statsDict = image.select(annualBand).reduceRegion({
    reducer: reducer,
    geometry: tehsil.geometry(),
    scale: 30,
    maxPixels: 1e13
  });

  statsDict.evaluate(function(stats) {
    var prefix = annualBand;
    if (!stats || !stats[prefix + '_count']) {
      print(title + ' stats', 'No valid data');
      return;
    }

    print(title + ' stats', {
      validPixels: formatInteger(stats[prefix + '_count']),
      mean: formatFixed(stats[prefix + '_mean']),
      min: formatFixed(stats[prefix + '_min']),
      max: formatFixed(stats[prefix + '_max']),
      stdDev: formatFixed(stats[prefix + '_stdDev'])
    });
  });
}

var stats = image.reduceRegion({
  reducer: ee.Reducer.minMax(),
  geometry: image.geometry(),
  scale: 30,
  maxPixels: 1e13,
  bestEffort: true
}).getInfo();
var minVal = Math.min.apply(null, Object.keys(stats).filter(function(k) { return k.indexOf('_min') > -1; }).map(function(k) { return stats[k]; }));
var maxVal = Math.max.apply(null, Object.keys(stats).filter(function(k) { return k.indexOf('_max') > -1; }).map(function(k) { return stats[k]; }));

var VIS = {min: minVal, max: maxVal, palette: PALETTE};

Map.setOptions('HYBRID');
Map.centerObject(image);

for (var i = 0; i < MONTH_LABELS.length; i++) {
  Map.addLayer(image.select(BAND_NAMES[i]), VIS, 'Kc - ' + MONTH_LABELS[i], i === 0);
}

Map.addLayer(
  tehsil.style({color: '#000000', width: 2, fillColor: '00000000'}),
  {},
  'Tehsil Boundary',
  true
);

printAnnualStats('Annual mean Crop Coefficient (Kc)', 'b13');
