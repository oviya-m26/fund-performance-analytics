# dags/mf_moneycontrol_style_etl.py
from __future__ import annotations

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta, timezone
import csv, io, requests
import snowflake.connector

# ===================== CONFIG =====================
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Run daily ~06:00 IST (00:30 UTC)
CRON_6AM_IST = "30 0 * * *"

# Use your working Snowflake creds (or wire Airflow Conn if you have one)
SNOWFLAKE_CONN = {
    "account":   "aftjkdh-wn01017",
    "user":      "OVIYA26",
    "password":  "FwvqSFv3BUeDLwg",
    "warehouse": "COMPUTE_WH",
    "database":  "MUTUAL_FUNDS_DATABASE",
    "schema":    "CORE",
}

AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
NSE_URL  = "https://www.nseindia.com/api/market-data-pre-open?key=NIFTY"
NSE_BENCHMARK_NAME = "NIFTY 50"   # must exist in benchmark_index
# ==================================================


with DAG(
    dag_id="mf_moneycontrol_style_etl",
    description="AMFI NAV + NSE benchmark -> Snowflake (history + stored growth%)",
    default_args=DEFAULT_ARGS,
    schedule=CRON_6AM_IST,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["mutualfunds", "etl", "moneycontrol", "snowflake"],
) as dag:

    # ----------------------------------------------------------
    # 1) Extract NAVs (AMFI NAVAll.txt)
    # ----------------------------------------------------------
    @task()
    def extract_nav():
        resp = requests.get(AMFI_URL, timeout=60)
        resp.raise_for_status()
        text = resp.text

        out = []
        reader = csv.reader(io.StringIO(text), delimiter=';')
        header = True
        for row in reader:
            if not row:
                continue
            if header:
                header = False
                continue
            try:
                scheme_code = int(row[0].strip())
                scheme_name = row[3].strip()
                nav_raw     = row[4].strip()
                rep_raw     = row[5].strip() if len(row) > 5 else ""
                sale_raw    = row[6].strip() if len(row) > 6 else ""
                date_raw    = row[-1].strip()  # e.g. 24-Feb-2025

                nav_date = datetime.strptime(date_raw, "%d-%b-%Y").date().isoformat()

                def num(x):
                    if x in ("", "N.A.", "NA", "NaN", None):
                        return None
                    return float(x)

                out.append({
                    "scheme_code": scheme_code,
                    "scheme_name": scheme_name,
                    "nav_date": nav_date,
                    "nav": num(nav_raw),
                    "repurchase_price": num(rep_raw),
                    "sale_price": num(sale_raw),
                    "amc_code": 0,
                })
            except Exception:
                continue
        return out

    # ----------------------------------------------------------
    # 2) Load → Snowflake (MERGE current_nav, MERGE nav_history)
    # ----------------------------------------------------------
    @task()
    def load_to_snowflake(nav_records: list[dict]) -> dict:
        if not nav_records:
            return {"staged": 0, "merged": 0, "history": 0}

        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()

        # Ensure % columns exist
        cur.execute("ALTER TABLE IF EXISTS nav_history  ADD COLUMN IF NOT EXISTS growth_from_start_pct FLOAT")
        cur.execute("ALTER TABLE IF EXISTS nav_history  ADD COLUMN IF NOT EXISTS day_return_pct FLOAT")
        cur.execute("ALTER TABLE IF EXISTS current_nav  ADD COLUMN IF NOT EXISTS growth_from_start_pct FLOAT")
        cur.execute("ALTER TABLE IF EXISTS current_nav  ADD COLUMN IF NOT EXISTS day_return_pct FLOAT")

        # Temp staging
        cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS stg_current_nav (
              scheme_code INT, nav_date DATE, nav FLOAT,
              repurchase_price FLOAT, sale_price FLOAT, amc_code INT
            );
        """)
        cur.execute("TRUNCATE TABLE stg_current_nav")

        rows = [(r["scheme_code"], r["nav_date"], r["nav"], r["repurchase_price"], r["sale_price"], r["amc_code"])
                for r in nav_records]
        cur.executemany("""
            INSERT INTO stg_current_nav (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, rows)
        staged = cur.rowcount or 0

        # Upsert latest into current_nav
        cur.execute("""
            MERGE INTO current_nav t
            USING stg_current_nav s
            ON t.scheme_code = s.scheme_code AND t.nav_date = s.nav_date
            WHEN MATCHED THEN UPDATE SET
              nav = s.nav, repurchase_price = s.repurchase_price,
              sale_price = s.sale_price, amc_code = s.amc_code
            WHEN NOT MATCHED THEN
              INSERT (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
              VALUES (s.scheme_code, s.nav_date, s.nav, s.repurchase_price, s.sale_price, s.amc_code)
        """)
        merged = cur.rowcount or 0

        # Append/refresh same rows into history
        cur.execute("""
            MERGE INTO nav_history h
            USING stg_current_nav s
            ON h.scheme_code = s.scheme_code AND h.nav_date = s.nav_date
            WHEN MATCHED THEN UPDATE SET
              nav = s.nav, repurchase_price = s.repurchase_price, sale_price = s.sale_price, amc_code = s.amc_code
            WHEN NOT MATCHED THEN
              INSERT (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
              VALUES (s.scheme_code, s.nav_date, s.nav, s.repurchase_price, s.sale_price, s.amc_code)
        """)
        history = cur.rowcount or 0

        conn.commit()
        cur.close()
        conn.close()
        return {"staged": staged, "merged": merged, "history": history}

    # ----------------------------------------------------------
    # 3) ALWAYS fill % columns for funds (latest date; only if NULL)
    # ----------------------------------------------------------
    @task()
    def compute_nav_percentages_incremental():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()

        cur.execute("SELECT COALESCE(MAX(nav_date), CURRENT_DATE()) FROM current_nav")
        target_date = cur.fetchone()[0]

        # Compute cumulative & daily only for rows that still have NULLs (idempotent)
        cur.execute("""
        UPDATE nav_history t
        SET
          growth_from_start_pct = c.gfs,
          day_return_pct        = c.dr
        FROM (
          SELECT
            scheme_code,
            nav_date,
            ROUND( (nav / NULLIF(FIRST_VALUE(nav) OVER (
                                   PARTITION BY scheme_code ORDER BY nav_date
                                 ), 0) - 1) * 100, 4) AS gfs,
            COALESCE(ROUND( (nav / NULLIF(LAG(nav) OVER (
                                       PARTITION BY scheme_code ORDER BY nav_date
                                     ), 0) - 1) * 100, 4), 0) AS dr
          FROM nav_history
          WHERE nav_date <= %s
        ) c
        WHERE t.scheme_code = c.scheme_code
          AND t.nav_date    = c.nav_date
          AND t.nav_date    = %s
          AND (t.growth_from_start_pct IS NULL OR t.day_return_pct IS NULL)
        """, (target_date, target_date))

        # Mirror into current_nav if NULL
        cur.execute("""
        UPDATE current_nav t
        SET   growth_from_start_pct = h.growth_from_start_pct,
              day_return_pct        = h.day_return_pct
        FROM  nav_history h
        WHERE t.scheme_code = h.scheme_code
          AND t.nav_date    = h.nav_date
          AND t.nav_date    = %s
          AND (t.growth_from_start_pct IS NULL OR t.day_return_pct IS NULL)
        """, (target_date,))

        conn.commit()
        cur.close()
        conn.close()
        return True

    # ----------------------------------------------------------
    # 4) Update benchmark (NIFTY 50) and ALWAYS fill growth% (latest; if NULL)
    # ----------------------------------------------------------
    @task()
    def update_benchmarks_and_growth():
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()

        # align to same date the funds have
        cur.execute("SELECT COALESCE(MAX(nav_date), CURRENT_DATE()) FROM current_nav")
        target_date = cur.fetchone()[0]

        # ensure growth column exists
        cur.execute("ALTER TABLE IF NOT EXISTS benchmark_history ADD COLUMN IF NOT EXISTS growth_from_start_pct FLOAT")

        # find benchmark_id
        cur.execute("SELECT benchmark_id FROM benchmark_index WHERE benchmark_name = %s LIMIT 1", (NSE_BENCHMARK_NAME,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            raise ValueError(f"Benchmark '{NSE_BENCHMARK_NAME}' not found in benchmark_index")
        benchmark_id = row[0]

        # NSE session (headers + cookies)
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
            "Connection": "keep-alive",
        })
        try:
            sess.get("https://www.nseindia.com", timeout=30)
        except Exception:
            pass

        try:
            r = sess.get(NSE_URL, timeout=30)
            data = r.json()
            index_value = float(data["data"][0]["lastPrice"])
        except Exception:
            index_value = None  # keep null if fetch fails

        # upsert today's benchmark
        cur.execute("""
            MERGE INTO benchmark_history t
            USING (SELECT %s AS benchmark_id, %s AS index_date, %s AS index_value) s
            ON t.benchmark_id = s.benchmark_id AND t.index_date = s.index_date
            WHEN MATCHED THEN UPDATE SET t.index_value = s.index_value
            WHEN NOT MATCHED THEN INSERT (benchmark_id, index_date, index_value)
            VALUES (s.benchmark_id, s.index_date, s.index_value)
        """, (benchmark_id, target_date, index_value))

        # compute growth% for that date only if NULL (idempotent)
        cur.execute("""
        UPDATE benchmark_history t
        SET growth_from_start_pct = c.gfs
        FROM (
          SELECT
            benchmark_id,
            index_date,
            ROUND( (index_value / NULLIF(FIRST_VALUE(index_value) OVER (
                                           PARTITION BY benchmark_id ORDER BY index_date
                                         ), 0) - 1) * 100, 4) AS gfs
          FROM benchmark_history
          WHERE benchmark_id = %s AND index_date <= %s
        ) c
        WHERE t.benchmark_id = c.benchmark_id
          AND t.index_date   = c.index_date
          AND t.index_date   = %s
          AND t.growth_from_start_pct IS NULL
        """, (benchmark_id, target_date, target_date))

        conn.commit()
        cur.close()
        conn.close()
        return {"benchmark_id": benchmark_id, "date": str(target_date), "value": index_value}

    # ----------------------------------------------------------
    # 5) Audit
    # ----------------------------------------------------------
    @task()
    def audit_logging(load_counts: dict):
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
        cur = conn.cursor()
        rows_inserted = (load_counts or {}).get("merged", 0) + (load_counts or {}).get("history", 0)
        cur.execute("""
            INSERT INTO etl_audit (table_name, file_name, rows_inserted, loadTimestamp)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP())
        """, ("current_nav + nav_history + benchmark_history", "AMFI/NSE daily", rows_inserted))
        conn.commit()
        cur.close()
        conn.close()
        return True

    # ----------------- dependencies -----------------
    nav_data   = extract_nav()
    load_counts = load_to_snowflake(nav_data)
    nav_pcts   = compute_nav_percentages_incremental()
    bench_step = update_benchmarks_and_growth()
    audit      = audit_logging(load_counts)

    nav_data >> load_counts >> nav_pcts >> bench_step >> audit
