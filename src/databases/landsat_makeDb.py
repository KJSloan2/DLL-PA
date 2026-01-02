import os
import sqlite3
import pandas as pd
import sqlite3

conn = sqlite3.connect('landsat.db')

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

conn = sqlite3.connect('landsat.db')

#Initialize a data table to store temporal representations of the raster data for the tile
create_table('landsat', {
    'geoid': 'TEXT PRIMARY KEY',
    'tile_id': 'STRING',
    'year': 'INTERGER',
    'lstf': 'FLOAT',
    'ndvi': 'FLOAT',
    'lat': 'FLOAT',
    'lon': 'FLOAT', 
    'elv': 'FLOAT', 
    'slope':'FLOAT',
    'r': 'INTERGER',
    'g': 'INTERGER',
    'b': 'INTERGER'
    })

conn.close()