import os
import sys
import sqlite3
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlite_utils import create_table

DROP = True
args = {
    "make_osm_srf_table": False,
    "make_composite_table": True,
    "make_spectral_table": False,
    "make_threeDep_table": False
}

conn = sqlite3.connect('tempGeo.db')

if args["make_osm_srf_table"] == True:
    #"railway", "waterway", "aeroway", "powerline", "pipeline"
    osmFeatureTags = ["highway_srf", "building_srf", "construction_srf"]
    for tag in osmFeatureTags:
        create_table(conn, tag, {
            'geoid': 'INTEGER PRIMARY KEY',
            'lat': 'FLOAT',
            'lon': 'FLOAT', 
            'srf': 'TEXT', 
            }, DROP)

if args["make_composite_table"] == True:
    # Create table for 3Dep + LS8 composite data
    create_table(conn, 'terrain_composite', {
        'geoid': 'INTEGER PRIMARY KEY',
        'lat': 'FLOAT',
        'lon': 'FLOAT', 
        'lstf': 'FLOAT',
        'lstf_serc': 'FLOAT', 
        'lstf_arc': 'FLOAT',
        'lstf_flag': 'TEXT', 
        'ndvi': 'FLOAT', 
        'ndvi_serc': 'FLOAT', 
        'ndvi_arc': 'FLOAT', 
        'ndvi_flag': 'TEXT', 
        'ndmi': 'FLOAT',
        'ndmi_serc': 'FLOAT',
        'ndmi_arc': 'FLOAT',
        'ndmi_flag': 'TEXT',
        'elv_rel': 'FLOAT',
        'elv': 'FLOAT',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        "mdrdasp": 'FLOAT',
        "mdrdconc": 'FLOAT',
        "mdrdgrv": 'FLOAT',
        "mdrdunp": 'FLOAT',
        "mdconst": 'FLOAT',
        "mdbldg": 'FLOAT',
        'lstf_ndvi_corr': 'FLOAT',
        'lstf_ndmi_corr': 'FLOAT', 
        'ndvi_ndmi_corr': 'FLOAT',
        'lstf_ndvi_pval': 'FLOAT', 
        'lstf_ndmi_pval': 'FLOAT', 
        'ndvi_ndmi_pval': 'FLOAT',
        'lstf_temporal': 'TEXT',
        'ndvi_temporal': 'TEXT',
        'ndmi_temporal': 'TEXT',
        'ls_land_cover': 'TEXT',
        'cl_land_cover': 'INTEGER',
        }, DROP)

if args["make_spectral_table"] == True:
    # Create table for 3Dep + LS8 composite data
    create_table(conn, 'spectral_temporal', {
        'geoid': 'INTEGER PRIMARY KEY',
        'lat': 'FLOAT',
        'lon': 'FLOAT', 
        'lstf': 'FLOAT',
        'lstf_serc': 'FLOAT', 
        'lstf_arc': 'FLOAT',
        'ndvi': 'FLOAT', 
        'ndvi_serc': 'FLOAT', 
        'ndvi_arc': 'FLOAT', 
        'ndmi': 'FLOAT',
        'ndmi_serc': 'FLOAT',
        'ndmi_arc': 'FLOAT',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        'lstf_ndvi_corr': 'FLOAT',
        'lstf_ndmi_corr': 'FLOAT', 
        'ndvi_ndmi_corr': 'FLOAT',
        'lstf_ndvi_pval': 'FLOAT', 
        'lstf_ndmi_pval': 'FLOAT', 
        'ndvi_ndmi_pval': 'FLOAT',
        'lstf_temporal': 'TEXT',
        'ndvi_temporal': 'TEXT',
        'ndmi_temporal': 'TEXT',
        }, DROP)

if args["make_threeDep_table"] == True:
    # Create table for 3Dep + LS8 composite data
    create_table(conn, 'three_dep', {
        'rowId': 'INTEGER PRIMARY KEY',
        'idx_row': 'INTEGER',
        'idx_col': 'INTEGER',
        'lat': 'FLOAT',
        'lon': 'FLOAT',
        'elv_rel': 'FLOAT',
        'elv': 'FLOAT',
        'slope': 'FLOAT',
        }, DROP)
    
conn.commit()
conn.close()