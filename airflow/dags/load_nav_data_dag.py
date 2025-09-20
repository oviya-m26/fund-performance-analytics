from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import mysql.connector

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def parse_float(val):
    return float(val) if val and val != 'N.A.' else None

def load_nav_to_mysql():
    print("START: load_nav_to_mysql")
    folder = "/opt/airflow/data/nav"
    print("Checking folder:", folder)
    files = os.listdir(folder)
    print("Files in folder:", files)
    total_inserted = 0
    total_skipped = 0

    print("Connecting to MySQL...")
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="Smart12#",
        database="db"
    )
    print("Connected to MySQL")
    cursor = conn.cursor()

    for filename in files:
        print(f"Processing file: {filename}")
        if filename.startswith("mf_") and filename.endswith(".txt"):
            amc_code = int(filename.split("_")[1].split(".")[0])
            with open(os.path.join(folder, filename), "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if i % 100 == 0:
                        print(f"Processing line {i} in {filename}")
                    parts = [p.strip() for p in line.split(";")]
                    if len(parts) < 5 or parts[0].lower() == 'scheme code':
                        total_skipped += 1
                        continue
                    try:
                        scheme_code = int(parts[0])
                        scheme_name = parts[3]
                        nav_date = datetime.strptime(parts[-1], "%d-%b-%Y").date()
                        nav = parse_float(parts[4])
                        repurchase = parse_float(parts[5]) if len(parts) > 5 else None
                        sale = parse_float(parts[6]) if len(parts) > 6 else None

                        cursor.execute("""
                            INSERT INTO nav_history (
                                scheme_code, scheme_name, nav_date, nav,
                                repurchase_price, sale_price, amc_code
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (scheme_code, scheme_name, nav_date, nav, repurchase, sale, amc_code))
                        total_inserted += 1
                    except Exception as e:
                        print(f"Skipped line due to error: {e}")
                        total_skipped += 1
                        continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Total rows inserted: {total_inserted}")
    print(f"Total lines skipped: {total_skipped}")
    print("END: load_nav_to_mysql")

with DAG(
    dag_id='load_nav_data_dag',
    default_args=default_args,
    description='Load NAV data into MySQL weekly',
    schedule='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nav']
) as dag:

    load_nav = PythonOperator(
        task_id='load_nav_data_task',
        python_callable=load_nav_to_mysql
    )

    load_nav
