import requests
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SNOWFLAKE_CONN = {
    "account": "aftjkdh-wn01017",
    "user": "OVIYA26",
    "password": "FwvqSFv3BUeDLwg",
    "warehouse": "COMPUTE_WH",
    "database": "MUTUAL_FUNDS_DATABASE",
    "schema": "CORE"
}

def fetch_nav_for_date(date_str):
    """
    Fetch NAV data from AMFI India for a specific date string.
    """
    url = f"https://www.amfiindia.com/spages/NAVAll_{date_str}.txt"
    
    try:
        logger.info(f"Fetching NAV data from {url}")
        response = requests.get(url, timeout=30)
        
        if response.status_code == 404:
            logger.warning(f"No NAV data available for {date_str}")
            return None
            
        response.raise_for_status()
        logger.info(f"Successfully fetched NAV data for {date_str}")
        return response.text
        
    except Exception as e:
        logger.error(f"Error fetching NAV data for {date_str}: {e}")
        return None

def parse_nav_data(raw_text, target_date):
    """
    Parse NAV data and extract scheme information with AMC codes.
    """
    data = []
    current_amc = None
    
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
            
        parts = [p.strip() for p in line.split(";")]
        
        # Check if this is a company/AMC line
        if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) <= 3:
            # This is an AMC line (AMC code, AMC name)
            current_amc = int(parts[0])
            continue
            
        # Check if this is a scheme line (scheme code is longer than 3 digits)
        if len(parts) >= 6 and parts[0].isdigit() and len(parts[0]) > 3:
            try:
                scheme_code = int(parts[0])
                scheme_name = parts[3]
                
                # Find the NAV value - it should be a numeric value
                nav = None
                repurchase = None
                sale = None
                
                # Look for NAV value in the middle columns (usually around column 4-6)
                for i in range(4, min(7, len(parts))):
                    if parts[i] and parts[i] not in ("", "N.A.", "-"):
                        try:
                            nav = float(parts[i])
                            # If we found NAV, the next column might be repurchase, and the last column is date
                            
                            # Try to get repurchase and sale prices
                            if i + 1 < len(parts) and parts[i + 1] not in ("", "N.A.", "-"):
                                try:
                                    repurchase = float(parts[i + 1])
                                except:
                                    pass
                            
                            if i + 2 < len(parts) and parts[i + 2] not in ("", "N.A.", "-"):
                                try:
                                    sale = float(parts[i + 2])
                                except:
                                    pass
                            
                            break
                        except ValueError:
                            continue
                
                if nav is not None and current_amc is not None:
                    data.append({
                        'scheme_code': scheme_code,
                        'scheme_name': scheme_name,
                        'nav_date': target_date,
                        'nav': nav,
                        'repurchase_price': repurchase,
                        'sale_price': sale,
                        'amc_code': current_amc
                    })
                    
            except Exception as e:
                logger.warning(f"Error parsing line: {line[:100]}... Error: {e}")
                continue
    
    logger.info(f"Parsed {len(data)} NAV records for {target_date}")
    return data

def load_nav_data_to_snowflake(nav_data):
    """
    Load NAV data into Snowflake using batch operations.
    """
    if not nav_data:
        logger.warning("No NAV data to load")
        return 0, 0
    
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur = conn.cursor()
    
    try:
        # Set context
        cur.execute("USE DATABASE MUTUAL_FUNDS_DATABASE")
        cur.execute("USE SCHEMA CORE")
        
        # Prepare batch data
        nav_history_batch = []
        current_nav_batch = []
        
        today = datetime.today().date()
        
        for record in nav_data:
            nav_history_batch.append((
                record['scheme_code'],
                record['nav_date'],
                record['nav'],
                record['repurchase_price'],
                record['sale_price'],
                record['amc_code']
            ))
            
            # If it's today's data, also add to current_nav
            if record['nav_date'] == today:
                current_nav_batch.append((
                    record['scheme_code'],
                    record['nav_date'],
                    record['nav'],
                    record['repurchase_price'],
                    record['sale_price'],
                    record['amc_code']
                ))
        
        # Batch insert into nav_history
        if nav_history_batch:
            # Delete existing data for the dates we're inserting
            dates_to_insert = list(set([record['nav_date'] for record in nav_data]))
            for date in dates_to_insert:
                cur.execute("DELETE FROM nav_history WHERE nav_date = %s", (date,))
            
            cur.executemany("""
                INSERT INTO nav_history (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, nav_history_batch)
            logger.info(f"Inserted {len(nav_history_batch)} records into nav_history")
        
        # Batch insert into current_nav
        if current_nav_batch:
            cur.execute("DELETE FROM current_nav WHERE nav_date = %s", (today,))
            cur.executemany("""
                INSERT INTO current_nav (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, current_nav_batch)
            logger.info(f"Inserted {len(current_nav_batch)} records into current_nav")
        
        conn.commit()
        logger.info("Data loaded successfully")
        
        return len(nav_history_batch), len(current_nav_batch)
        
    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {e}")
        conn.rollback()
        return 0, 0
    finally:
        cur.close()
        conn.close()

def fetch_last_10_days():
    """
    Fetch NAV data for the last 10 days using hardcoded date strings.
    """
    logger.info("Starting NAV data fetch for last 10 days...")
    
    # Hardcoded date strings for the last 10 days
    date_strings = [
        "20-Aug-2025",
        "19-Aug-2025", 
        "18-Aug-2025",
        "17-Aug-2025",
        "16-Aug-2025",
        "15-Aug-2025",
        "14-Aug-2025",
        "13-Aug-2025",
        "12-Aug-2025",
        "11-Aug-2025"
    ]
    
    all_data = []
    total_records = 0
    
    # Loop through the date strings
    for date_str in date_strings:
        logger.info(f"Processing date: {date_str}")
        
        # Fetch NAV data for this date
        raw_data = fetch_nav_for_date(date_str)
        if raw_data:
            # Parse the date string to get the actual date object
            try:
                target_date = datetime.strptime(date_str, "%d-%b-%Y").date()
            except:
                logger.error(f"Could not parse date: {date_str}")
                continue
                
            # Parse the data
            nav_data = parse_nav_data(raw_data, target_date)
            if nav_data:
                all_data.extend(nav_data)
                total_records += len(nav_data)
                logger.info(f"Successfully processed {len(nav_data)} records for {date_str}")
            else:
                logger.warning(f"No NAV data parsed for {date_str}")
        else:
            logger.warning(f"No NAV data fetched for {date_str}")
    
    if not all_data:
        logger.error("No NAV data collected for any of the last 10 days")
        return
    
    logger.info(f"Total NAV records collected: {total_records}")
    
    # Load to Snowflake
    nav_history_count, current_nav_count = load_nav_data_to_snowflake(all_data)
    
    logger.info(f"✅ NAV data fetch complete!")
    logger.info(f"   Records loaded to nav_history: {nav_history_count}")
    logger.info(f"   Records loaded to current_nav: {current_nav_count}")
    
    # Show summary by date
    if all_data:
        date_counts = {}
        for record in all_data:
            date_str = str(record['nav_date'])
            date_counts[date_str] = date_counts.get(date_str, 0) + 1
        
        logger.info("Records loaded by date:")
        for date_str in sorted(date_counts.keys(), reverse=True):
            logger.info(f"  {date_str}: {date_counts[date_str]} records")

if __name__ == "__main__":
    fetch_last_10_days()


