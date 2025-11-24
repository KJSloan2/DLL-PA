import json
import os
######################################################################################
# Get the full path of the script
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))

lib_dir = os.path.join(root_dir, "frontend", "src", "lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

public_dir = os.path.join(root_dir, "frontend", "public", "acs")
#######################################################################################
# Walk up the directory tree until we find "src"
parts = script_dir.split(os.sep)
try:
    idx_src = parts.index("src")
except ValueError:
    raise Exception("'src' directory not found in path.")

# Reconstruct full path to backend/src and backend/resources
parent_dir = os.sep.join(parts[:idx_src + 1])  # includes "src"
resources_path = os.path.join(os.sep.join(parts[:idx_src]), "resources")

# Make sure the folder exists
os.makedirs(resources_path, exist_ok=True)
######################################################################################
print("Script location:", script_dir)
print("Parent dir (backend/src):", parent_dir)
print("Resources path:", resources_path)
######################################################################################
# Collect inputs
location_key = input("Location Key: ")
has_landsat = input("Conduct Landsat LST and NDVI analysis? (y/n): ").strip().lower()
has_3dep = input("Conduct 3DEP terrain analysis? (y/n): ").strip().lower()

if has_3dep == "y":
    while True:
        resolution3Dep = input("Enter desired 3DEP resolution (1m or 10m): ").strip().lower()
        if resolution3Dep in ["1m", "10m"]:
            break
        else:
            print("Invalid input. Please enter '1m' or '10m'.")
else:
    resolution3Dep = None
######################################################################################
has_uca = input("Conduct urban change analysis? (y/n): ").strip().lower()
has_acs = input("Conduct census analysis? (y/n): ").strip().lower()
has_osm = input("Get structures data? (y/n): ").strip().lower()
######################################################################################
# Convert to boolean
has_landsat = has_landsat == "y"
has_3dep = has_3dep == "y"
has_uca = has_uca == "y"
has_acs = has_acs == "y"
has_osm = has_osm == "y"
######################################################################################
location_key = location_key.strip()
'''data_dir = os.path.join(parent_dir, 'backend', 'data')
if location_key:
    landsat_loc_dir = os.path.join(data_dir, 'landsat', location_key)
    os.makedirs(landsat_loc_dir, exist_ok=True)'''

# Construct log dictionary
logJson = {
    "location_key": location_key,
    "year_start": None,
    "year_end": None,
    "has_landsat": has_landsat,
    "has_3dep": has_3dep,
    "3dep_resolution": resolution3Dep,
    "has_uca": has_uca,
    "has_acs": has_acs,
    "has_osm": has_osm,
    "run_stats": {
        "start_time": None,
        "end_time": None,
        "duration": None
    },
    "final_output_files": {
        "landsat_temporal": None,
        "3dep_terain": None
    },
    "processed_tifs": {},
    "preprocessing_output": [],
    "tiles": {}
}
######################################################################################
if location_key not in siteFileMap:
    siteFileMap[location_key] = {
        "terrain": None,
        "landsat": None,
        "fieldPhotos": None,
        "structures": None,
        "constructionSites": None,
        "proposedConstructionSites": None,
        "landuse": None,
        "tornados": None,
        "wind": None,
        "hail": None,
        "acs_s0101": None,
        "acs_dp03": None,
        "acs_s2801": None,
        "coordinates": [],
    }
######################################################################################
with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))
print("terrainComposite.py DONE")

# Write the log to backend/resources/<location_key>_log.json
output_path = os.path.join(resources_path, "log.json")
with open(output_path, "w", encoding='utf-8') as output_json:
    json.dump(logJson, output_json, indent=2, ensure_ascii=False)

print(f"Log written to: {output_path}")