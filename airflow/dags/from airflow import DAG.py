from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import pandas as pd
import snowflake.connector

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONN = {
    "account": "aftjkdh-wn01017",
    "user": "OVIYA26",
    "password": "FwvqSFv3BUeDLwg",
    "warehouse": "COMPUTE_WH",
    "database": "MUTUAL_FUNDS_DATABASE",
    "schema": "CORE"
}

with DAG(
    dag_id='mutual_fund_nav_etl',
    default_args=default_args,
    description='ETL pipeline for mutual fund NAV + benchmarks',
    schedule='0 6 * * *',  
    start_date=datetime(2025, 8, 19),
    catchup=True,
    tags=['mutualfunds', 'etl']
) as dag:

    @task()
    def extract_nav():
        url = "https://www.amfiindia.com/spages/NAVAll.txt"
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        data = []
        for line in response.text.splitlines():
            parts = [p.strip() for p in line.split(";")]
            if len(parts) < 6 or parts[0] == "Scheme Code":
                continue
            try:
                scheme_code = int(parts[0])
                scheme_name = parts[3]
                nav_date = pd.to_datetime(parts[-1], format="%d-%b-%Y", errors="coerce").date()
                nav = float(parts[4]) if parts[4] not in ("", "N.A.") else None
                repurchase = float(parts[5]) if len(parts) > 5 and parts[5] not in ("", "N.A.") else None
                sale = float(parts[6]) if len(parts) > 6 and parts[6] not in ("", "N.A.") else None
                data.append({
                    "scheme_code": scheme_code,
                    "scheme_name": scheme_name,
                    "nav_date": nav_date,
                    "nav": nav,
                    "repurchase_price": repurchase,
                    "sale_price": sale,
                    "amc_code": 0
                })
            except Exception:
                continue
        return data

    @task()
    def load_to_snowflake(nav_records):
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()
        inserted_rows = 0
        for row in nav_records:
            try:
                cur.execute("""
                    MERGE INTO current_nav t
                    USING (SELECT %s AS scheme_code, %s AS nav_date, %s AS nav,
                                  %s AS repurchase_price, %s AS sale_price, %s AS amc_code) s
                    ON t.scheme_code = s.scheme_code AND t.nav_date = s.nav_date
                    WHEN MATCHED THEN UPDATE SET
                        t.nav = s.nav,
                        t.repurchase_price = s.repurchase_price,
                        t.sale_price = s.sale_price
                    WHEN NOT MATCHED THEN
                        INSERT (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
                        VALUES (s.scheme_code, s.nav_date, s.nav, s.repurchase_price, s.sale_price, s.amc_code)
                """, (
                    row['scheme_code'], row['nav_date'], row['nav'],
                    row['repurchase_price'], row['sale_price'], row['amc_code']
                ))
                inserted_rows += 1
            except Exception:
                continue
        conn.commit()
        cur.close()
        conn.close()
        return inserted_rows

    @task()
    def update_nav_history():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO nav_history (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
            SELECT scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code
            FROM current_nav
            WHERE nav_date = CURRENT_DATE()
        """)
        inserted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return inserted

    @task()
    def update_benchmarks():
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://www.nseindia.com/api/market-data-pre-open?key=NIFTY"
        response = requests.get(url, headers=headers, timeout=30)

        try:
            latest = response.json()["data"][0]
            index_value = latest["lastPrice"]
        except Exception:
            index_value = None

        index_date = datetime.today().date()

        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()
        cur.execute("""
            MERGE INTO benchmark_history bh
            USING (
                SELECT (SELECT benchmark_id FROM benchmark_index WHERE benchmark_name='NIFTY 50'),
                       %s, %s
            ) s(benchmark_id, index_date, index_value)
            ON bh.benchmark_id = s.benchmark_id AND bh.index_date = s.index_date
            WHEN MATCHED THEN UPDATE SET bh.index_value = s.index_value
            WHEN NOT MATCHED THEN
                INSERT (benchmark_id, index_date, index_value)
                VALUES (s.benchmark_id, s.index_date, s.index_value)
        """, (index_date, index_value))
        conn.commit()
        cur.close()
        conn.close()
        return True

    @task()
    def compare_to_benchmark():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()
        cur.execute("""
            SELECT s.scheme_name, n.nav_date, n.nav, b.index_value
            FROM nav_history n
            JOIN mutual_fund_scheme s ON n.scheme_code = s.scheme_code
            JOIN scheme_benchmark_mapping m ON s.scheme_code = m.scheme_code
            JOIN benchmark_history b ON m.benchmark_id = b.benchmark_id
                AND n.nav_date = b.index_date
            WHERE n.nav_date >= DATEADD(month, -1, CURRENT_DATE())
            LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    @task()
    def audit_logging(inserted_nav, inserted_history):
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO etl_audit (table_name, file_name, rows_inserted)
            VALUES (%s, %s, %s)
        """, ("current_nav + nav_history", "daily_run", inserted_nav + inserted_history))
        conn.commit()
        cur.close()
        conn.close()
        return True


    nav_data = extract_nav()
    inserted_nav = load_to_snowflake(nav_data)
    inserted_history = update_nav_history()
    benchmark_update = update_benchmarks()
    comparison = compare_to_benchmark()
    audit = audit_logging(inserted_nav, inserted_history)

    nav_data >> inserted_nav >> inserted_history >> benchmark_update >> comparison >> audit
