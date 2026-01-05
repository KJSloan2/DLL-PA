import os
import sqlite3
DROP = True
def create_table(table_name, columns):
    cursor = conn.cursor()
    # Create a dynamic SQL query to create a table
    column_defs = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
    # Execute the query
    cursor.execute(query)
    # Commit and close
    conn.commit()
    print(table_name)

conn = sqlite3.connect('usda_nass_cdl.db')

'''create_table('land_cover_classification_ref', {
    'VAL': 'INTEGER PRIMARY KEY',
    'HEX_COLOR': 'TEXT',
    'R': 'INTEGER',
    'G': 'INTEGER',
    'B': 'INTEGER',
    'LAND_COVER': 'TEXT'
     })'''

create_table('cdl_data', {
    'raster_id': 'TEXT PRIMARY KEY',
    'lc_val': 'INTEGER',
    "lat": 'REAL',
    "lon": 'REAL',
    'lc_label': 'TEXT'
     })

conn.close()