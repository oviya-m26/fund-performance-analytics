import os
import logging
from datetime import datetime
import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ---- settings ----
SF_ACCOUNT   = "aftjkdh-wn01017"
SF_USER      = "OVIYA26"
SF_PASSWORD  = "FwvqSFv3BUeDLwg"
SF_WAREHOUSE = "COMPUTE_WH"
SF_DATABASE  = "MUTUAL_FUNDS_DATABASE"
SF_SCHEMA    = "CORE"

BENCHMARKS = {
    "NIFTY 50": "^NSEI",
    "BSE Sensex": "^BSESN",
}
START_DATE = "2015-01-01"

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
log = logging.getLogger(__name__)

def ensure_objects(cur):
    cur.execute(f"USE WAREHOUSE {SF_WAREHOUSE}")
    cur.execute(f"USE DATABASE {SF_DATABASE}")
    cur.execute(f"USE SCHEMA {SF_SCHEMA}")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_index (
            benchmark_id NUMBER AUTOINCREMENT PRIMARY KEY,
            benchmark_name VARCHAR(200) UNIQUE NOT NULL,
            ticker VARCHAR(100),
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_history (
            history_id NUMBER AUTOINCREMENT PRIMARY KEY,
            benchmark_id NUMBER NOT NULL,
            index_date DATE NOT NULL,
            index_value FLOAT,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            CONSTRAINT uq_benchmark_date UNIQUE (benchmark_id, index_date),
            FOREIGN KEY (benchmark_id) REFERENCES benchmark_index(benchmark_id)
        )
    """)

def upsert_benchmarks(cur, mapping):
    for name, ticker in mapping.items():
        cur.execute(
            """
            MERGE INTO benchmark_index tgt
            USING (SELECT %s AS benchmark_name, %s AS ticker) src
            ON tgt.benchmark_name = src.benchmark_name
            WHEN MATCHED THEN UPDATE SET tgt.ticker = src.ticker
            WHEN NOT MATCHED THEN INSERT (benchmark_name, ticker)
            VALUES (src.benchmark_name, src.ticker)
            """,
            (name, ticker),
        )
    # re-fetch ids
    q_marks = ",".join(["%s"] * len(mapping))
    cur.execute(
        f"SELECT benchmark_id, benchmark_name FROM benchmark_index WHERE benchmark_name IN ({q_marks})",
        tuple(mapping.keys())
    )
    rows = cur.fetchall()
    return {name: bid for (bid, name) in rows}

def fetch_yf_close_series(ticker: str, start: str) -> pd.Series:
    # Force auto_adjust=False so Close is the official close
    df = yf.download(
        ticker,
        start=start,
        end=datetime.today().strftime("%Y-%m-%d"),
        progress=False,
        auto_adjust=False,
        threads=False,
        interval="1d",
    )
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    if "Close" not in df.columns:
        return pd.Series(dtype="float64")

    # Make sure index is DatetimeIndex and drop NA closes
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    s = df["Close"].dropna()
    s = s[~s.index.isna()]
    # De-duplicate days if any (take last)
    s = s[~s.index.duplicated(keep="last")]
    return s

def upsert_history_with_dataframe(conn, cur, benchmark_id: int, series: pd.Series):
    if series.empty:
        return 0

    # Build a DataFrame for load
    df = pd.DataFrame({
        "BENCHMARK_ID": benchmark_id,
        "INDEX_DATE": [d.date() if hasattr(d, "date") else pd.to_datetime(d).date() for d in series.index],
        "INDEX_VALUE": series.values.astype(float),
    })

    # Load to a temp table
    temp_table = "BENCHMARK_HISTORY_LOAD"
    cur.execute(f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS {temp_table} (
            BENCHMARK_ID NUMBER,
            INDEX_DATE DATE,
            INDEX_VALUE FLOAT
        )
    """)
    cur.execute(f"TRUNCATE TABLE {temp_table}")
    ok, nchunks, nrows, _ = write_pandas(conn, df, temp_table, auto_create_table=False)
    if not ok or nrows == 0:
        return 0

    # MERGE into target
    cur.execute(f"""
        MERGE INTO benchmark_history tgt
        USING (
            SELECT BENCHMARK_ID, INDEX_DATE, INDEX_VALUE FROM {temp_table}
        ) src
        ON tgt.benchmark_id = src.bENCHMARK_ID AND tgt.index_date = src.index_date
        WHEN MATCHED THEN UPDATE SET tgt.index_value = src.index_value
        WHEN NOT MATCHED THEN INSERT (benchmark_id, index_date, index_value)
        VALUES (src.benchmark_id, src.index_date, src.index_value)
    """)
    # Count rows for this benchmark just loaded (approx by source size)
    return int(nrows)

def main():
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        autocommit=False,
    )
    cur = conn.cursor()
    try:
        ensure_objects(cur)
        id_map = upsert_benchmarks(cur, BENCHMARKS)
        conn.commit()

        total = 0
        for name, ticker in BENCHMARKS.items():
            log.info(f"Fetching {name} ({ticker})")
            s = fetch_yf_close_series(ticker, START_DATE)
            if s.empty:
                log.warning(f"No rows for {name}. If you are on a corporate network or VPN, try another network.")
                continue
            inserted = upsert_history_with_dataframe(conn, cur, id_map[name], s)
            conn.commit()
            log.info(f"Upserted {inserted} rows for {name}")
            total += inserted

        cur.execute("SELECT COUNT(*) FROM benchmark_history")
        final_count = cur.fetchone()[0]
        log.info(f"Done. Total rows processed this run: {total}. Final benchmark_history count: {final_count}")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
