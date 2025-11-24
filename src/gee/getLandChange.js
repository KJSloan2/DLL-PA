// Urban Growth Change Detection using Dynamic World Probability Bands
var exportLayer = true;  // Flag for export
var locationId = "PECOS";

var aoi = ee.FeatureCollection('projects/ee-landsat-24/assets/aoi_rectangle');
var geometry = aoi.geometry(); // if you just want the Geometry
var geom = geometry;

//var geom = geometry;
//Map.centerObject(geometry);

// Define the before and after time periods.
var beforeYear = 2020;
var afterYear = beforeYear+1;

// Create start and end dates for the before and after periods.
var beforeStart = ee.Date.fromYMD(beforeYear, 1 , 1);
var beforeEnd = beforeStart.advance(1, 'year');

var afterStart = ee.Date.fromYMD(afterYear, 1 , 1);
var afterEnd = afterStart.advance(1, 'year');

// Load the Dynamic World collection
var dw = ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')

// Filter the collection and select the 'built' band.
var dwFiltered = dw
  .filter(ee.Filter.bounds(geom))
  .select('built');

// Create mean composites
var beforeDw = dwFiltered.filter(
  ee.Filter.date(beforeStart, beforeEnd)).mean();
  
var afterDw = dwFiltered.filter(
  ee.Filter.date(afterStart, afterEnd)).mean();


// Add Sentinel-2 Composites to verify the results.
var s2 = ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
     .filterBounds(geom)
     .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 35));

// Create a median composite from sentinel-2 images.
var beforeS2 = s2.filterDate(beforeStart, beforeEnd).median();
var afterS2 = s2.filterDate(afterStart, afterEnd).median();
  
// Visualize images
var s2VisParams = {bands: ['B4', 'B3', 'B2'], min: 0, max: 3000};
Map.centerObject(geom, 10);
Map.addLayer(beforeS2.clip(geom), s2VisParams, 'Before S2');
Map.addLayer(afterS2.clip(geom), s2VisParams, 'After S2');

// Select all pixels that have experienced large change
// in 'built' probability
var builtChangeThreshold = 0.05; 
var newUrban = afterDw.subtract(beforeDw).gt(builtChangeThreshold);

var changeVisParams = {min: 0, max: 1, palette: ['white', '#ff23f8']};
Map.addLayer(newUrban.clip(geom), changeVisParams, 'New Urban');

// Mask all pixels with 0 value using selfMask()
var newUrbanMasked = newUrban.selfMask();

Map.addLayer(
  newUrbanMasked.clip(geom), changeVisParams, 'New Urban (Masked)');

// To ensure the masked values are set to NoData, 
// we cast the image to float and clip to geometry
var newUrbanMaskedExport = newUrbanMasked.toFloat().clip(geometry);

// Export the newUrbanMasked layer if the exportLayer flag is set to true
if (exportLayer) {
  Export.image.toDrive({
    image: newUrbanMaskedExport,
    //description: String('UCA_' + locationId + '_' + String(beforeStart + '-' + String(beforeEnd)),
    description: "LCA_"+locationId+"_"+beforeYear,
    folder: 'UrbanGrowth',  // Optional: Name of the Google Drive folder
    region: geom,           // Export region
    scale: 10,              // Set the scale to 10m or other desired resolution
    maxPixels: 1e13,        // To avoid pixel limit issues
    fileFormat: 'GeoTIFF'   // Export format
  });
}