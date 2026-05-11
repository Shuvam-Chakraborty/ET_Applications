var ASSET_ID = 'projects/your-project/assets/kc_TEHSIL_YEAR';

var MONTH_LABELS = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
  'Annual mean'
];

var BAND_NAMES = [
  'b1', 'b2', 'b3', 'b4', 'b5', 'b6',
  'b7', 'b8', 'b9', 'b10', 'b11', 'b12',
  'b13'
];

var VIS = {
  min: 0,
  max: 1,
  palette: ['fff5eb', 'fdd0a2', 'f16913', '8c2d04']
};

var rawImage = ee.Image(ASSET_ID);
var image = rawImage.updateMask(rawImage.neq(-9999));

Map.setOptions('HYBRID');
Map.centerObject(image, 9);

var panel = ui.Panel({style: {position: 'top-left', width: '320px', padding: '8px'}});
panel.add(ui.Label('Kc GeoTIFF Viewer', {fontWeight: 'bold', fontSize: '16px'}));
panel.add(ui.Label('Set ASSET_ID, then switch between monthly bands and the annual mean band.'));
panel.add(ui.Label('Band selection'));

var select = ui.Select({
  items: MONTH_LABELS,
  value: MONTH_LABELS[0],
  onChange: drawLayer
});
panel.add(select);
Map.add(panel);

function drawLayer(label) {
  var index = MONTH_LABELS.indexOf(label);
  var layer = ui.Map.Layer(image.select(BAND_NAMES[index]), VIS, 'Kc - ' + label, true, 1.0);
  if (Map.layers().length() > 0) {
    Map.layers().set(0, layer);
  } else {
    Map.layers().add(layer);
  }
}

drawLayer(MONTH_LABELS[0]);
