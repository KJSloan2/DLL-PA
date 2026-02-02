import sqlite3
import duckdb

# Connect to databases
sqlite_conn = sqlite3.connect('tempGeo.db')
duckdb_conn = duckdb.connect('tempGeo.duckdb')

# Install and load SQLite extension for DuckDB
duckdb_conn.execute("INSTALL sqlite")
duckdb_conn.execute("LOAD sqlite")

# tables to migrate
tables_to_migrate = [
    'highway_srf',
    'building_srf', 
    'construction_srf',
    'terrain_composite',
    'spectral_temporal',
    'three_dep'
]

print("Start migration")

# Get list of existing tables in SQLite database
cursor = sqlite_conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
existing_tables = [row[0] for row in cursor.fetchall()]

# Migrate each table
migrated_count = 0
for table_name in tables_to_migrate:
    if table_name in existing_tables:
        print(f"Migrating table: {table_name}...")
        
        # Drop table if it exists in DuckDB
        duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table from SQLite data
        duckdb_conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM sqlite_scan('tempGeo.db', '{table_name}')
        """)
        
        # row count
        result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0]
        
        print(f"  ✓ {table_name}: {row_count} rows migrated")
        migrated_count += 1
    else:
        print(f"{table_name}: Table not found")

print(f"\nMigration complete! {migrated_count} tables migrated.")
print(f"DuckDB file created: tempGeo.duckdb")

# Verify
print("\nVerifying DuckDB tables:")
duckdb_tables = duckdb_conn.execute("SHOW TABLES").fetchall()
for table in duckdb_tables:
    table_name = table[0]
    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  {table_name}: {count} rows")

duckdb_conn.close()
sqlite_conn.close()

print("Migration complete")