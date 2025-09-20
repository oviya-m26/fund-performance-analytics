import snowflake.connector
import pandas as pd
import os

# === CONFIGURATION ===
SNOWFLAKE_ACCOUNT = "mi40664.me-central2.gcp"
USER = "OVIYA26"
PASSWORD = "FwvqSFv3BUeDLwg"
WAREHOUSE = "COMPUTE_WH"
DATABASE = "MUTUAL_FUNDS_DATABASE"
SCHEMA = "CORE"

TABLES = [
    "mutual_fund_scheme",
    "mutual_fund_company",
    "nav_history",
    "current_nav",
    "benchmark_history",
    "scheme_benchmark_mapping",
    "benchmark_index",
    "etl_audit"
]

BACKUP_DIR = "snowflake_backup"
DATA_DIR = os.path.join(BACKUP_DIR, "data")
DDL_DIR = os.path.join(BACKUP_DIR, "schema")

# === SETUP OUTPUT FOLDERS ===
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DDL_DIR, exist_ok=True)

# === CONNECT TO SNOWFLAKE ===
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

cur = conn.cursor()

for table in TABLES:
    print(f"Backing up table: {table}")

    # === EXPORT DATA ===
    query = f"SELECT * FROM {table}"
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
    csv_path = os.path.join(DATA_DIR, f"{table}.csv")
    df.to_csv(csv_path, index=False)
    print(f"  Data saved to {csv_path}")

    # === EXPORT SCHEMA ===
    print(f"Table name being queried: {table}") # Added print statement for debugging table name
    ddl_query = f"SELECT GET_DDL('TABLE', '{table}')"
    cur.execute(ddl_query)
    ddl_result = cur.fetchone()
    ddl_text = ddl_result[0]  # GET_DDL returns the DDL in the first column
    ddl_path = os.path.join(DDL_DIR, f"{table}.sql")
    with open(ddl_path, "w") as f:
        f.write(ddl_text)
    print(f"  Schema saved to {ddl_path}")

cur.close()
conn.close()
print("✅ Backup complete! All tables and schemas saved.")
