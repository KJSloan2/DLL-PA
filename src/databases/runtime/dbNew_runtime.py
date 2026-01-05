import os
import sys
import sqlite3
###################################################################
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlite_utils import create_table
###################################################################
DROP = True
###################################################################
args = {
    "make_dir_lib_table": True,
    "make_site_info_table": True
}

conn = sqlite3.connect('runtime.db')
###################################################################
if args["make_dir_lib_table"]:
    create_table(conn, 'dir_lib', {
        'DIR_NAME': 'TEXT PRIMARY KEY',
        'DIR_PATH': 'TEXT',
        'USEAGE': 'TEXT',
        }, DROP)
    
if args["make_site_info_table"]:
    create_table(conn, 'site_info', {
        'NAME': 'TEXT PRIMARY KEY',
        'STATE_NAME': 'TEXT',
        'STATE_FIPS': 'TEXT',
        'COUNTY_NAME': 'TEXT',
        'COUNTY_FIPS': 'TEXT',
        'PLACE_NAME': 'TEXT',
        'PLACE_GEOID': 'TEXT',
        'HAS_HYDRO_FEATURES': 'TEXT',
        'AOI_BB_PT_SW': 'TEXT',
        'AOI_BB_PT_SE': 'TEXT',
        'AOI_BB_PT_NW': 'TEXT',
        'AOI_BB_PT_NE': 'TEXT',
        'AOI_CENTROID': 'TEXT'
        }, DROP)
conn.close()