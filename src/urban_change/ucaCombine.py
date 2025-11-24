import fiona
import pandas as pd
import json
import os
import shutil
################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import json_serialize
######################################################################################
######################################################################################
# Define the location ID and start and end years of the analysis period
locationId = 'SGF'
yearStart = 2018
yearEnd = 2024
######################################################################################
# Make new data structure for geojson features
fc = {
	"type": "FeatureCollection",
	"features": []
}
################################################################################
for year in range(yearStart, yearEnd, 1):
	analysisPeriodKey = str(year)+'-'+str(year+1)
	fileName = 'UCA_'+locationId+'_'+analysisPeriodKey+'.geojson'
	
	with fiona.open(os.path.join(r'eo_v3', 'output', 'urban_change', fileName), mode="r") as src:
		for feature in src:
			try:
				properties = feature['properties']
				geometry = feature['geometry']
				# Make new  feature to store data
				newFeature = {
					'type': 'Feature',
					'properties': {
							'group_id': properties['group_id'],
							'analysis_period': analysisPeriodKey,
							'area': properties['area']
						},
					'geometry': {
						'type': 'Polygon',
						'coordinates': geometry['coordinates']
					}
				}
				
				fc['features'].append(newFeature)
			except Exception as e:
				print(e)
				continue
################################################################################
outputFileName = 'UCA_'+locationId+'_comp_'+str(yearStart)+'-'+str(yearEnd)+'.geojson'
outputPath = os.path.join(r'eo_v3', 'output', 'urban_change', outputFileName)
with open(outputPath, "w", encoding='utf-8') as output_json:
	output_json.write(json.dumps(fc, indent=1, default=json_serialize, ensure_ascii=True))
######################################################################################
# Copy and shuttle the output files to the target directory for public access by the interface app
target_dir = r'/Users/kevinsloan/Documents/GitHub/placeChangeInterface/placechangeinterface/public/siteAnalysis'
os.makedirs(target_dir, exist_ok=True)
shutil.copy(outputPath, target_dir)
######################################################################################
print("DONE")