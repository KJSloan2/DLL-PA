import os
import sys
import sqlite3

from duckdb import query
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from duckDbUtils import create_duckDb_table
######################################################################################
DROP = True

conn = sqlite3.connect('tempGeo.db')
        
create_duckDb_table(conn, 'ct_aoi_filtered', {
    'geoid': 'INTEGER PRIMARY KEY',
    'lat': 'DOUBLE',
    'lon': 'DOUBLE', 
    'lstf': 'DOUBLE',
    'lstf_serc': 'DOUBLE', 
    'lstf_arc': 'DOUBLE',
    'lstf_flag': 'VARCHAR', 
    'ndvi': 'DOUBLE', 
    'ndvi_serc': 'DOUBLE', 
    'ndvi_arc': 'DOUBLE', 
    'ndvi_flag': 'VARCHAR', 
    'ndmi': 'DOUBLE',
    'ndmi_serc': 'DOUBLE',
    'ndmi_arc': 'DOUBLE',
    'ndmi_flag': 'VARCHAR',
    'elv_rel': 'DOUBLE',
    'elv': 'DOUBLE',
    'idx_row': 'INTEGER',
    'idx_col': 'INTEGER',
    'mdrdasp': 'DOUBLE',
    'mdrdconc': 'DOUBLE',
    'mdrdgrv': 'DOUBLE',
    'mdrdunp': 'DOUBLE',
    'mdconst': 'DOUBLE',
    'mdbldg': 'DOUBLE',
    'lstf_ndvi_corr': 'DOUBLE',
    'lstf_ndmi_corr': 'DOUBLE', 
    'ndvi_ndmi_corr': 'DOUBLE',
    'lstf_ndvi_pval': 'DOUBLE', 
    'lstf_ndmi_pval': 'DOUBLE', 
    'ndvi_ndmi_pval': 'DOUBLE',
    'lstf_temporal': 'VARCHAR',
    'ndvi_temporal': 'VARCHAR',
    'ndmi_temporal': 'VARCHAR',
    'ls_land_cover': 'VARCHAR',
    'cl_land_cover': 'INTEGER',
    # Terrain attributes
    'terrain_classification': 'VARCHAR',
    'slope': 'DOUBLE',
    'aspect': 'DOUBLE',
    'tri': 'DOUBLE',
    'tpi': 'DOUBLE',
    'anisotropy': 'DOUBLE',
    'cgap': 'DOUBLE',
    'downhill_fraction': 'DOUBLE',
    'cgap_uphill': 'DOUBLE',
    'uphill_fraction': 'DOUBLE',
    'dom_angle_deg': 'DOUBLE',
    'dom_dir': 'DOUBLE',
    'dom_ptb_lat': 'DOUBLE',
    'dom_ptb_lon': 'DOUBLE',
    'dom_dir_elv': 'DOUBLE',
    'dd_geoid': 'INTEGER',
    }, DROP)

    
conn.commit()
conn.close()

print("DuckDB ct_aoi_filtered table created")