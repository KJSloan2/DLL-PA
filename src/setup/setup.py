import os
import sys
import subprocess
import sqlite3
#############################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import get_directories, ensure_folder
from global_functions.sqlite_utils import get_table_info
#############################################################################
DB_NAME = "runtime.db"
DB_TABLE = "site_info"

siteName = None

siteInfo = get_table_info(DB_NAME, DB_TABLE, ["NAME"])
siteName = siteInfo['NAME']

interimDirPath = r"C:\Users\Kjslo\Documents\local_dev\dll_interim"
interimDirs = get_directories(interimDirPath)
print(siteName)

dataSubDirs = ["3dep", "landsat", "hydro", "cdl", "osm", "spatial_filters", "composite"]

ensure_folder(interimDirPath, siteName)

for subDir in dataSubDirs:
    ensure_folder(os.path.join(interimDirPath, siteName), subDir)
    
'''data_dir = os.path.join(project_root, 'backend', 'data')
output_dir = os.path.join(project_root, 'backend', 'output')
# List of subdirectories to create
subdirs = ['landsat', '3dep', 'uca']
for dir in [data_dir, output_dir]:
    for subdir in subdirs:
        dir_path = os.path.join(dir, subdir)
        os.makedirs(dir_path, exist_ok=True)
        print(f"Directory created or already exists: {dir_path}")'''