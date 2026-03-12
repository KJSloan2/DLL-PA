import os
import sys
import subprocess
import json
import sqlite3
######################################################################################
# Get the full path of the script
script_dir = os.path.dirname(os.path.abspath(__file__))

parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

databaseSriptsDir = os.path.join(parent_dir, "src", "databases")

logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))

lib_dir = os.path.join(r"C:\Users\Kjslo\Documents\local_dev\dynamic_lands_lab\frontend\src\lib")
siteFileMap = json.load(open(os.path.join(lib_dir, "siteFileMap.json")))

public_dir = os.path.join(root_dir, "frontend", "public", "acs")
#######################################################################################
databasesToCreate = {
    "runtime.db": {"script_subdir": "runtime", "script_name": "dbNew_runtime.py"},
    "tempGeo.duckdb": {"script_subdir": "terrain_composite", "script_name": "duckDb_create_terrainComposite.py"},
    "'usda_nass_cdl.duckdb'": {"script_subdir": "cropland", "script_name": "dbCreate_cropland.py"}
}
for dbName, dbInfo in databasesToCreate.items():
    try:
        print(f"Creating {dbName}...")
        script_path = os.path.join(databaseSriptsDir, dbInfo["script_subdir"], dbInfo["script_name"])
        subprocess.run([sys.executable, script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {dbInfo['script_name']}: {e}")
    except Exception as e:
        print(f"Unexpected error running {dbInfo['script_name']}: {e}")
#######################################################################################

# Walk up the directory tree until it finds "src"
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
TABLE_NAME = "site_info"
RESET_TABLE = True

conn = sqlite3.connect('runtime.db')
cursor = conn.cursor()

if RESET_TABLE:
    cursor.execute(f"DELETE FROM {TABLE_NAME}")
    conn.commit()
    cursor = conn.cursor()

    cursor.execute(
        '''INSERT INTO site_info (
        NAME) 
        VALUES (?)''',
        ( location_key, )
    )

#print upated site_info
cursor.execute(f"SELECT * FROM {TABLE_NAME}")
rows = cursor.fetchall()
headers = [description[0] for description in cursor.description]
for row in rows:
    row_dict = dict(zip(headers, row))
    print(row_dict)
    
conn.commit()
conn.close()
######################################################################################
with open(os.path.join(lib_dir, "siteFileMap.json"), "w", encoding='utf-8') as json_siteFileMap:
	json_siteFileMap.write(json.dumps(siteFileMap, indent=2, ensure_ascii=False))

# Write the log to backend/resources/<location_key>_log.json
output_path = os.path.join(resources_path, "log.json")
with open(output_path, "w", encoding='utf-8') as output_json:
    json.dump(logJson, output_json, indent=2, ensure_ascii=False)

print(f"Log written to: {output_path}")
