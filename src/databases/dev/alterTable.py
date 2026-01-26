import sqlite3

DB_NAME = "tempGeo.db"
TABLE_NAME = "terrain_composite"
PRINT = False

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

columnsToAdd = {
        'terrain_classification': 'TEXT',
        'slope': 'FLOAT',
        'aspect': 'FLOAT',
        'tri': 'FLOAT',
        'tpi': 'FLOAT',
        'anisotropy': 'FLOAT',
        'cgap': 'FLOAT',
        'downhill_fraction': 'FLOAT',
        'cgap_uphill': 'FLOAT',
        'uphill_fraction': 'FLOAT',
        'dom_angle_deg': 'FLOAT',
        'dom_dir': 'FLOAT',
        'dom_ptb_lat': 'FLOAT',
        'dom_ptb_lon': 'FLOAT',
        'dom_dir_elv': 'FLOAT'
}
for column_name, column_type in columnsToAdd.items():
    cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {column_name} {column_type};")
    conn.commit()

print(f"Added columns to {TABLE_NAME} table.")

if PRINT:
    cursor.execute(f"SELECT * FROM {TABLE_NAME};")
    rows = cursor.fetchall()
    for row in rows[0:10]:
        print(row)

conn.close()