import json
import os
import csv
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
from utils import  get_files
from utils import  get_directories
######################################################################################
# Get the full path of the script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))

lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))
public_dir = os.path.join(root_dir, "frontend", "public", "acs")
######################################################################################
selectMetricKeys = {
    "dp03": {
        "DP03_0001E" : "Estimate!!EMPLOYMENT STATUS!!Population 16 years and over",
        "DP03_0009E": "Unemployment Rate",
        "DP03_0018E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over",
        "DP03_0019E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Car, truck, or van -- drove alone",
        "DP03_0020E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Car, truck, or van -- carpooled",
        "DP03_0021E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Public transportation (excluding taxicab)",
        "DP03_0022E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Walked",
        "DP03_0023E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Other means",
        "DP03_0024E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Worked from home",
        "DP03_0025E": "Estimate!!COMMUTING TO WORK!!Workers 16 years and over!!Mean travel time to work (minutes)",
        "DP03_0032E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over",
        "DP03_0033E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Agriculture, forestry, fishing and hunting, and mining",
        "DP03_0034E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Construction",
        "DP03_0035E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Manufacturing",
        "DP03_0036E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Wholesale trade",
        "DP03_0037E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Retail trade",
        "DP03_0038E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Transportation and warehousing, and utilities",
        "DP03_0039E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Information",
        "DP03_0040E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Finance and insurance, and real estate and rental and leasing",
        "DP03_0041E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Professional, scientific, and management, and administrative and waste management services",
        "DP03_0042E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Educational services, and health care and social assistance",
        "DP03_0043E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Arts, entertainment, and recreation, and accommodation and food services",
        "DP03_0044E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Other services, except public administration",
        "DP03_0045E": "Estimate!!INDUSTRY!!Civilian employed population 16 years and over!!Public administration"
        },
    "s0101": {
        "S0101_C01_001E" : "Estimate!!Total!!Total population",
    }
}

fc = {
    "type": "FeatureCollection",
    "features": []
}
 
geoidRef = []
directories = get_directories(os.path.join(parent_dir, "data", "acs"))
for directory in directories:
    files = get_files(os.path.join(parent_dir, "data", "acs", directory))
    for file in files:
        split_file = file.split("_")
        acsKey = split_file[0]
        file_path = os.path.join(parent_dir, "data", "acs", directory, file)
        dataJson = json.load(open(file_path))
        
        calculationKeys = [None, "apc", "arc", "sepc", "serc"]
        for feature in dataJson["features"]:
            geoid = feature["properties"]["GEOID"]
            name = feature["properties"]["NAME"]
            namelsad = feature["properties"]["NAMELSAD"]
            centroid = feature["properties"]["centroid"]

            if geoid not in geoidRef:
                fc["features"].append({
                    "type": "Feature",
                    "properties": {
                        "geoid": geoid,
                        "name": name,
                        "namelsad": namelsad,
                        "centroid": centroid
                    },
                    "geometry": feature["geometry"]
                })
                geoidRef.append(geoid)

            featureIdx = geoidRef.index(geoid)
            if acsKey in selectMetricKeys.keys():
                for propKeyBase in selectMetricKeys[acsKey].keys():
                    for calcKey in calculationKeys:
                        propKeyBase = propKeyBase.lower()
                        if calcKey is None:
                            propKey = propKeyBase
                        else:
                            propKey = f"{propKeyBase}_{calcKey}"
                        if propKey in feature["properties"]:
                            propValue = feature["properties"][propKey]
                            if isinstance(propValue, list):
                                propValue = [float(x) for x in propValue]
                            elif isinstance(propValue, str):
                                try:
                                    propValue = float(propValue)
                                except ValueError:
                                    pass
                            fc["features"][featureIdx]["properties"][propKey] = propValue

csvData = {}
for feature in fc["features"]:
    for key in feature["properties"].keys():
        if key not in csvData:
            csvData[key] = []

for feature in fc["features"]:
    properties = feature["properties"]
    for key in csvData.keys():
        if key in properties:
            csvData[key].append(properties[key])
        else:
            csvData[key].append("") 

def write_to_csv():
    fieldnames = list(csvData.keys())
    num_rows = len(csvData["geoid"]) if "geoid" in csvData and csvData["geoid"] else 0
    csv_path = os.path.join(root_dir, "frontend", "public", "summary", "locationSummaryStats.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write rows
        for i in range(num_rows):
            row_dict = {field: csvData[field][i] if i < len(csvData[field]) else '' 
                       for field in fieldnames}
            writer.writerow(row_dict)
    
    print(f"CSV data written to {csv_path}")

write_to_csv()

with open(os.path.join("backend", "data", "acs", "acsCompositeStats_uscbPlace.geojson"), "w", encoding='utf-8') as output_json:
    output_json.write(json.dumps(fc, indent=1, default=json_serialize, ensure_ascii=True))

print("DONE")