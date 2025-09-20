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

def parse_float_safe(value):
    """Safely parse float values, handling 'N.A.' and empty strings"""
    if not value or value == 'N.A.' or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def load_nav_to_mysql():
    folder = "/opt/airflow/data/nav"
    batch_size = 5000  # Increased batch size for better performance

    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",  
        password="Smart12#", 
        database="db"
    )
    cursor = conn.cursor()

    total_files_processed = 0
    total_rows_inserted = 0

    for filename in os.listdir(folder):
        if filename.startswith("mf_") and filename.endswith(".txt"):
            print(f"Processing {filename}...")
            amc_code = int(filename.split("_")[1].split(".")[0])
            file_path = os.path.join(folder, filename)

            batch_data = []
            file_inserted = 0

            with open(file_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    parts = [p.strip() for p in line.split(";")]
                    
                    # Skip header lines
                    if line_num == 1 or (len(parts) > 0 and parts[0].lower() == 'scheme code'):
                        continue
                        
                    if len(parts) < 5:
                        continue
                        
                    try:
                        scheme_code = int(parts[0])
                        scheme_name = parts[3]
                        nav_date = datetime.strptime(parts[-1], "%d-%b-%Y").date()
                        
                        # Use safe float parsing
                        nav = parse_float_safe(parts[4])
                        repurchase = parse_float_safe(parts[5]) if len(parts) > 5 else None
                        sale = parse_float_safe(parts[6]) if len(parts) > 6 else None

                        batch_data.append((
                            scheme_code, scheme_name, nav_date, nav,
                            repurchase, sale, amc_code
                        ))
                        
                        # Insert in batches
                        if len(batch_data) >= batch_size:
                            cursor.executemany("""
                                INSERT INTO nav_history (
                                    scheme_code, scheme_name, nav_date, nav,
                                    repurchase_price, sale_price, amc_code
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, batch_data)
                            conn.commit()
                            file_inserted += len(batch_data)
                            batch_data = []
                            
                    except Exception as e:
                        # Skip problematic lines silently for speed
                        continue

            # Insert remaining data
            if batch_data:
                cursor.executemany("""
                    INSERT INTO nav_history (
                        scheme_code, scheme_name, nav_date, nav,
                        repurchase_price, sale_price, amc_code
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, batch_data)
                conn.commit()
                file_inserted += len(batch_data)

            total_rows_inserted += file_inserted
            total_files_processed += 1
            print(f"✓ {filename}: {file_inserted:,} rows inserted")

    cursor.close()
    conn.close()
    
    print(f"\n=== SUMMARY ===")
    print(f"Files processed: {total_files_processed}")
    print(f"Total rows inserted: {total_rows_inserted:,}")
    print("ETL completed successfully!")

with DAG(
    dag_id='final_etl_dag',
    default_args=default_args,
    description='Load NAV data into MySQL (optimized)',
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
