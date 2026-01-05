import sqlite3

conn = sqlite3.connect('tempGeo.db')
cursor = conn.cursor()

for osmFturCategory in ["building", "construction"]:
    # cursor = conn.cursor()
    query = f"SELECT * FROM {osmFturCategory}_srf"
    cursor.execute(query)

    for row in cursor.fetchall():
        # row structure: geoid, lat, lon, surface
        geoid, lat, lon, surface = row[0], row[1], row[2], row[3]
        print(osmFturCategory, geoid, lat, lon, surface)