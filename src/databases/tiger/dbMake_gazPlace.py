import os
import sqlite3
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
idx_src = split_dir.index("src")
parent_dir = split_dir[idx_src-1]
######################################################################################
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

conn = sqlite3.connect('tabulation_ref.db')

#USPS	GEOID	ANSICODE	NAME	LSAD	FUNCSTAT	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG 
#'WI', '5530450', '01583305', 'Grantsburg village', '47', 'A', '7871515', '57865', '3.039', '0.022', '45.78379', '-92.677329'

create_table('gaz_place', {
    'geoid': 'TEXT PRIMARY KEY','usps': 'TEXT', 'ansicode': 'TEXT', 'name': 'TEXT', 'designation': 'TEXT',
    'lsad': 'TEXT', 'funcstat': 'TEXT', 'aland': 'INTEGER', 'awater': 'INTEGER',
    'aland_sqmi': 'FLOAT', 'aqater_sqmi': 'FLOAT', 'intptlat': 'FLOAT','intptlong': 'FLOAT', 'msa_geoid': 'TEXT', 'msa_name': 'TEXT',
    'county_geoid': 'TEXT', 'county_fips': 'TEXT', 'county_name': 'TEXT'
     })
cursor = conn.cursor()
## Add data to the table
dataPath = os.path.join(parent_dir, 'data', 'gazetteer', '2024_Gaz_place_national.txt' )

#USPS	GEOID	ANSICODE	NAME	LSAD	FUNCSTAT	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG 
#'WI', '5530450', '01583305', 'Grantsburg village', '47', 'A', '7871515', '57865', '3.039', '0.022', '45.78379', '-92.677329'


placeDesignations = [
    'CDP', 'municipality', 'city', 'town', 'village', 'borough',
    'metropolitan government (balance)',
    'unified government (balance)',
    'consolidated government (balance)',
    'unified government',
    'consolidated government',
    'metro township',
    'metro government (balance)']

store_designation = []
with open(dataPath, "r", encoding="utf-8") as file:
    next(file)
    for line in file:
        items = line.strip().split("\t")
        clean_items = [item.strip() for item in items]
            
        usps = str(clean_items[0])
        geoid = str(clean_items[1])
        ansicode = str(clean_items[2])
        name = str(clean_items[3])
        lsad = str(clean_items[4])
        funcstat = str(clean_items[5])
        aland = int(clean_items[6])
        awater = int(clean_items[7])
        aland_sqmi = float(clean_items[8])
        aqater_sqmi = float(clean_items[9])
        intptlat = float(clean_items[10])
        intptlong = float(clean_items[11])

        msa_geoid, msa_name = "None", "None"
        county_geoid, county_fips, county_name = "None", "None", "None"

        placeDesignation = "None"
        placeName = name
        for designation in placeDesignations:
            if designation == name[len(designation)*-1:]:
                store_designation.append(designation)
                placeDesignation = designation
                placeName = name[0:len(designation)*-1]
                break

        cursor.execute(
            '''INSERT INTO gaz_place (
                geoid, usps, ansicode, name, designation, lsad, 
                funcstat, aland, awater, aland_sqmi, aqater_sqmi, 
                intptlat, intptlong, msa_geoid, msa_name,
                county_geoid, county_fips, county_name) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                       (geoid, usps, ansicode, placeName.strip(), placeDesignation, 
                        lsad, funcstat, aland, awater, aland_sqmi, aqater_sqmi, 
                        intptlat, intptlong, msa_geoid, msa_name, county_geoid, county_fips, county_name))

conn.commit()
print("Done")
