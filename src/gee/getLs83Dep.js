var getLandsat = true;
var get3Dep10m = false;
var get3Dep1m = false;

// Exporting data to Google Drive
var place_key = "FB2"; // Change as necessary

// Define the bounding box area
var geom = geometry; // Replace 'geometry' with your specific geometry if not defined
var bb_area_meters = geom.area({maxError: 1});
var bb_area_miles = bb_area_meters.divide(2589988.11);
var bb_pts = geom.coordinates();
print('ROI Square Miles:', bb_area_miles);

// Get bounding box coordinates
var bb_pts_coords = bb_pts.getInfo();
var roi = ee.Geometry.BBox(
  bb_pts_coords[0][3][0], bb_pts_coords[0][3][1],
  bb_pts_coords[0][1][0], bb_pts_coords[0][1][1]
);

var point1 = bb_pts_coords[0][3];
var point2 = bb_pts_coords[0][1];

// Function to calculate the center point of the bounding box
function calculateCenter(lat1, lon1, lat2, lon2) {
  var centerLat = (lat1 + lat2) / 2;
  var centerLon = (lon1 + lon2) / 2;
  return [centerLat, centerLon];
}

var mapCenter = calculateCenter(point1[0], point1[1], point2[0], point2[1]);
Map.setCenter(mapCenter[0], mapCenter[1], 10);
print(mapCenter);
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
if (get3Dep1m === true){
  // USGS/3DEP/1m dataset (as ImageCollection)
  var dataset = ee.ImageCollection('USGS/3DEP/1m');
  
  // Filter the ImageCollection by region (bounding box)
  // We use `mosaic()` to combine tiles in the region and get a single Image
  var elevation = dataset.filterBounds(roi).mosaic();
  var elevation_clip = elevation.clip(roi);
  
  // Derive slope from the elevation data
  //var slope = ee.Terrain.slope(elevation);
  //var slope_clip = slope.clip(roi);
  
  // Visualization parameters for elevation
  var elevationVis = {
    min: 0,
    max: 3000,
    palette: [
      '3ae237', 'b5e22e', 'd6e21f', 'fff705', 'ffd611', 'ffb613', 'ff8b13',
      'ff6e08', 'ff500d', 'ff0000', 'de0101', 'c21301', '0602ff', '235cb1',
      '307ef3', '269db1', '30c8e2', '32d3ef', '3be285', '3ff38f', '86e26f'
    ]
  };
  
  // Visualization parameters for slope
  var slopeVis = {
    min: 0,
    max: 60
  };
  
  // Add elevation and slope layers to the map
  Map.addLayer(elevation_clip, elevationVis, 'elevation_clip');
  //Map.addLayer(slope_clip, slopeVis, 'slope');
  
  // Export elevation data to GeoTIFF
  Export.image.toDrive({
    image: elevation_clip,
    description: '3DEPe1m_' + place_key,
    folder: '3DEP',
    region: roi,
    scale: 1, // Adjusting scale to 1m resolution
    fileFormat: 'GeoTIFF'
  });
};
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
if (get3Dep10m === true){
  // Load the USGS/3DEP/10m dataset (which is an Image, not an ImageCollection)
  var elevation = ee.Image('USGS/3DEP/10m');
  
  // Clip the elevation image to the region of interest (ROI)
  var elevation_clip = elevation.clip(roi);
  
  // Derive slope from the elevation data
  var slope = ee.Terrain.slope(elevation_clip);
  
  // Visualization parameters for elevation
  var elevationVis = {
    min:3,
    max:1200,
    palette: [
      '3ae237', 'b5e22e', 'd6e21f', 'fff705', 'ffd611', 'ffb613', 'ff8b13',
      'ff6e08', 'ff500d', 'ff0000', 'de0101', 'c21301', '0602ff', '235cb1',
      '307ef3', '269db1', '30c8e2', '32d3ef', '3be285', '3ff38f', '86e26f'
    ]
  };
  
  // Visualization parameters for slope
  var slopeVis = {
    min: 0,
    max: 60
  };
  
  // Add elevation and slope layers to the map
  Map.addLayer(elevation_clip, elevationVis, 'elevation_clip');
  Map.addLayer(slope, slopeVis, 'slope');
  
  // Export elevation data to GeoTIFF
  Export.image.toDrive({
    image: elevation_clip,
    description: '3DEPe10m_' + place_key,
    folder: '3DEP',
    region: roi,
    scale: 10, // Adjusting scale to 10m resolution
    fileFormat: 'GeoTIFF'
  });
  
  // Export slope data to GeoTIFF
  Export.image.toDrive({
    image: slope,
    description: '3DEPs10m_' + place_key,
    folder: '3DEP',
    region: roi,
    scale: 10, // Adjusting scale to 10m resolution
    fileFormat: 'GeoTIFF'
  });
};

if (getLandsat === true){
  var year = '2024';
  var startDateTime = year + '-05-01T10:00:00'; // Start date and time in UTC
  var endDateTime = year + '-10-01T15:00:00';   // End date and time in UTC
  
  
  function prepSrL8(image) {
    var qaMask = image.select('QA_PIXEL').bitwiseAnd(parseInt('11111', 2)).eq(0);
    var saturationMask = image.select('QA_RADSAT').eq(0);
  
    var getFactorImg = function(factorNames) {
      var factorList = image.toDictionary().select(factorNames).values();
      return ee.Image.constant(factorList);
    };
    var scaleImg = getFactorImg([
      'REFLECTANCE_MULT_BAND_.',
      'TEMPERATURE_MULT_BAND_ST_B10']);
    var offsetImg = getFactorImg([
      'REFLECTANCE_ADD_BAND_.',
      'TEMPERATURE_ADD_BAND_ST_B10']);
      
    var scaled = image.select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'ST_B10'])
                      .multiply(scaleImg).add(offsetImg);
                      
    return image.addBands(scaled, null, true)
      .updateMask(qaMask).updateMask(saturationMask);
  }
  
  var l8_filtered = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
    .filterBounds(roi)
    .filterDate(startDateTime, endDateTime)
    .map(prepSrL8)
    .select('SR.*', 'ST.*')
    .median();
  
  var l8_clip = l8_filtered.clip(roi);
  
  var l8_raw_filtered = ee.ImageCollection('LANDSAT/LC08/C02/T1')
    .filterDate(startDateTime, endDateTime);
  
  var composite = ee.Algorithms.Landsat.simpleComposite({
    collection: l8_raw_filtered,
    asFloat: true
  });
  
  var l8_composite_clip = composite.clip(roi);
  
  var thermalBands = l8_clip.select('ST_B10').toFloat(); // Convert to Float32
  var opticalBands = l8_clip.select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).toFloat(); // Convert to Float32
  var l8_composite_oli = l8_composite_clip.select(['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7']).toFloat(); // Convert to Float32
  
  var thermalBands_converted = thermalBands.expression(
    '(dn - 273.15) * 1.8 + 32.0', {
      'dn': thermalBands
    }
  ).rename('thermalBands_converted').toFloat(); // Ensure converted band is Float32
  
  // Combine the images into one image
  var combinedImage = ee.Image.cat([
    l8_composite_oli,
    thermalBands_converted
  ]);
  
  // Print band names for verification
  print('Bands in combined image:', combinedImage.bandNames());
  
  // Display the combined image
  Map.addLayer(combinedImage, {
    bands: ['B4', 'B3', 'B2'], // Example visualization for the RGB bands
    min: 0,
    max: 0.3,
    gamma: 1.4
  }, 'Combined Image');
  
  // Add the converted thermal bands to the map with a different visualization
  Map.addLayer(thermalBands_converted, {palette:['purple', 'blue', 'red', 'orange', 'yellow'], min: 32, max: 110}, 'thermalBands_converted');
  
  // Export the combined image
  Export.image.toDrive({
    image: combinedImage,
    description: 'LS8_'+place_key + '_' + year,
    folder: 'L8_ARD',
    region: roi,
    scale: 30,
    fileFormat: 'GeoTIFF'
  });
};