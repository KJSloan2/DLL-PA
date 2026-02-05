from duckdb import query
######################################################################################
######################################################################################
def create_duckDb_table(conn, table_name, columns, drop=False):
    """
    Works with both SQLite and DuckDB connections
    """
    # Check if DuckDB connection
    is_duckdb = hasattr(conn, 'execute') and 'duckdb' in str(type(conn))
    
    # Handle DROP
    if drop:
        if is_duckdb:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        else:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
    
    # Build column definitions with type conversion
    col_defs = []
    for col_name, col_type in columns.items():
        # Convert SQLite types to DuckDB types if needed
        if is_duckdb:
            if col_type == 'FLOAT':
                col_type = 'DOUBLE'
            elif col_type == 'TEXT':
                col_type = 'VARCHAR'
        col_defs.append(f"{col_name} {col_type}")
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
    
    # Execute CREATE TABLE
    if is_duckdb:
        conn.execute(create_sql)
        conn.commit()
    else:
        cursor = conn.cursor()
        cursor.execute(create_sql)
        conn.commit()
######################################################################################
######################################################################################