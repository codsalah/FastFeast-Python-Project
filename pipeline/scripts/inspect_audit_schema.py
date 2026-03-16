import sys
import os
import psycopg2
from psycopg2 import pool as pg_pool

# Get schema name from command-line argument (default: 'audit')
schema_name = sys.argv[1] if len(sys.argv) > 1 else 'audit'

# Get database connection details from environment variables
db_host = os.getenv('POSTGRES_HOST', 'localhost')
db_port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'fastfeast_db')
db_user = os.getenv('POSTGRES_USER', 'fastfeast')
db_pass = os.getenv('POSTGRES_PASSWORD', 'fastfeast_pass')

CONNECTION_STRING = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}"

_pool = pg_pool.SimpleConnectionPool(minconn=1, maxconn=2, dsn=CONNECTION_STRING, connect_timeout=10)
conn  = _pool.getconn()

print("\n" + "=" * 80)
wow = "="* 24
print(f"{wow} {schema_name.upper()} SCHEMA TABLES AND COLUMNS {wow}")
print("=" * 80 + "\n")

with conn.cursor() as cur:

    cur.execute("""
        SELECT table_name FROM information_schema.tables
         WHERE table_schema = %s ORDER BY table_name
    """, (schema_name,))
    tables = [row[0] for row in cur.fetchall()]

    for i, table in enumerate(tables, 1):
        print(f"TABLE {i}: {schema_name}.{table}")
        print("-" * 80)

        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
              FROM information_schema.columns
             WHERE table_schema = %s
               AND table_name   = %s
             ORDER BY ordinal_position
        """, (schema_name, table))

        print("COLUMNS:")
        for j, (col, dtype, nullable, default) in enumerate(cur.fetchall(), 1):
            null_str    = "✓ NULLABLE" if nullable == "YES" else "NOT NULL  "
            default_str = f"| Default: {default}" if default else ""
            print(f"  {j:2}. {col:<30} {dtype:<28} {null_str}  {default_str}")
        print()

_pool.putconn(conn)
_pool.closeall()

print("=" * 80)
print(f"                    SUMMARY: {len(tables)} tables found in {schema_name} schema")
print("=" * 80 + "\n")