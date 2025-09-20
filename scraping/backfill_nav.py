import requests
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta

SNOWFLAKE_CONN = {
    "account": "aftjkdh-wn01017",
    "user": "OVIYA26",
    "password": "FwvqSFv3BUeDLwg",
    "warehouse": "COMPUTE_WH",
    "database": "MUTUAL_FUNDS_DATABASE",
    "schema": "CORE"
}

def fetch_nav_for_date(date):
    """
    Fetch NAV data from AMFI India and filter for a specific date.
    """
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    response = requests.get(url)
    response.raise_for_status()

    data = []
    for line in response.text.splitlines():
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 6 or parts[0] == "Scheme Code":
            continue
        try:
            scheme_code = int(parts[0])
            scheme_name = parts[3]
            nav_date = pd.to_datetime(parts[-1], format="%d-%b-%Y", errors="coerce")
            nav = float(parts[4]) if parts[4] not in ("", "N.A.") else None
            repurchase = float(parts[5]) if len(parts) > 5 and parts[5] not in ("", "N.A.") else None
            sale = float(parts[6]) if len(parts) > 6 and parts[6] not in ("", "N.A.") else None

            if nav_date and nav_date.date() == date:
                data.append((scheme_code, scheme_name, nav_date.date(), nav, repurchase, sale))
        except Exception:
            continue

    df = pd.DataFrame(data, columns=["scheme_code", "scheme_name", "nav_date", "nav", "repurchase_price", "sale_price"])
    return df

def backfill_last_10_days():
    all_data = []
    today = datetime.today().date()
    for i in range(10):
        date = today - timedelta(days=i)
        df = fetch_nav_for_date(date)
        if df is not None and not df.empty:
            all_data.append(df)

    if not all_data:
        print("⚠️ No data fetched.")
        return

    final_df = pd.concat(all_data)

    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()
    inserted_rows = 0
    current_rows = 0

    for _, row in final_df.iterrows():
        # Insert into NAV History
        cur.execute("""
            MERGE INTO nav_history t
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
            row['repurchase_price'], row['sale_price'], 0
        ))
        inserted_rows += 1

        # If it's the latest date, also update current_nav
        if row['nav_date'] == today:
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
                row['repurchase_price'], row['sale_price'], 0
            ))
            current_rows += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Backfill complete. Rows inserted/updated into NAV_HISTORY: {inserted_rows}, into CURRENT_NAV: {current_rows}")

if __name__ == "__main__":
    backfill_last_10_days()
