import snowflake.connector
from datetime import datetime, timedelta
import logging
import random

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

def generate_sample_nav_data():
    """
    Generate realistic sample NAV data for the last 10 days.
    """
    # Sample mutual fund schemes with realistic NAV ranges
    sample_schemes = [
        {"scheme_code": 100001, "scheme_name": "HDFC Mid-Cap Opportunities Fund - Direct Plan - Growth", "amc_code": 1, "base_nav": 45.50},
        {"scheme_code": 100002, "scheme_name": "ICICI Prudential Bluechip Fund - Direct Plan - Growth", "amc_code": 2, "base_nav": 67.80},
        {"scheme_code": 100003, "scheme_name": "SBI Bluechip Fund - Direct Plan - Growth", "amc_code": 3, "base_nav": 52.30},
        {"scheme_code": 100004, "scheme_name": "Axis Bluechip Fund - Direct Plan - Growth", "amc_code": 4, "base_nav": 38.90},
        {"scheme_code": 100005, "scheme_name": "Kotak Emerging Equity Fund - Direct Plan - Growth", "amc_code": 5, "base_nav": 28.45},
        {"scheme_code": 100006, "scheme_name": "Mirae Asset Large Cap Fund - Direct Plan - Growth", "amc_code": 6, "base_nav": 42.15},
        {"scheme_code": 100007, "scheme_name": "Nippon India Large Cap Fund - Direct Plan - Growth", "amc_code": 7, "base_nav": 35.70},
        {"scheme_code": 100008, "scheme_name": "UTI Mid Cap Fund - Direct Plan - Growth", "amc_code": 8, "base_nav": 31.25},
        {"scheme_code": 100009, "scheme_name": "Aditya Birla Sun Life Frontline Equity Fund - Direct Plan - Growth", "amc_code": 9, "base_nav": 48.90},
        {"scheme_code": 100010, "scheme_name": "Franklin India Prima Fund - Direct Plan - Growth", "amc_code": 10, "base_nav": 26.80},
        {"scheme_code": 100011, "scheme_name": "DSP Midcap Fund - Direct Plan - Growth", "amc_code": 11, "base_nav": 39.45},
        {"scheme_code": 100012, "scheme_name": "Tata Mid Cap Growth Fund - Direct Plan - Growth", "amc_code": 12, "base_nav": 33.20},
        {"scheme_code": 100013, "scheme_name": "Reliance Large Cap Fund - Direct Plan - Growth", "amc_code": 13, "base_nav": 55.60},
        {"scheme_code": 100014, "scheme_name": "L&T Midcap Fund - Direct Plan - Growth", "amc_code": 14, "base_nav": 29.75},
        {"scheme_code": 100015, "scheme_name": "Canara Robeco Emerging Equities Fund - Direct Plan - Growth", "amc_code": 15, "base_nav": 41.30}
    ]
    
    # Generate data for last 10 days
    today = datetime.today().date()
    all_data = []
    
    for i in range(10):
        target_date = today - timedelta(days=i)
        
        for scheme in sample_schemes:
            # Generate realistic NAV with small daily variations
            daily_change = random.uniform(-0.02, 0.03)  # -2% to +3% daily change
            nav = round(scheme["base_nav"] * (1 + daily_change), 4)
            
            # Generate repurchase and sale prices (usually close to NAV)
            repurchase = round(nav * random.uniform(0.995, 0.999), 4)
            sale = round(nav * random.uniform(1.001, 1.005), 4)
            
            all_data.append({
                'scheme_code': scheme["scheme_code"],
                'scheme_name': scheme["scheme_name"],
                'nav_date': target_date,
                'nav': nav,
                'repurchase_price': repurchase,
                'sale_price': sale,
                'amc_code': scheme["amc_code"]
            })
    
    logger.info(f"Generated {len(all_data)} sample NAV records for {len(sample_schemes)} schemes over 10 days")
    return all_data

def load_sample_nav_data_to_snowflake(nav_data):
    """
    Load sample NAV data into Snowflake using batch operations.
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
        logger.info("Sample data loaded successfully")
        
        return len(nav_history_batch), len(current_nav_batch)
        
    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {e}")
        conn.rollback()
        return 0, 0
    finally:
        cur.close()
        conn.close()

def insert_sample_data():
    """
    Main function to generate and insert sample NAV data.
    """
    logger.info("Starting sample NAV data insertion...")
    
    # Generate sample data
    sample_data = generate_sample_nav_data()
    
    # Load to Snowflake
    nav_history_count, current_nav_count = load_sample_nav_data_to_snowflake(sample_data)
    
    logger.info(f"✅ Sample NAV data insertion complete!")
    logger.info(f"   Records loaded to nav_history: {nav_history_count}")
    logger.info(f"   Records loaded to current_nav: {current_nav_count}")
    
    # Show summary by date
    if sample_data:
        date_counts = {}
        for record in sample_data:
            date_str = str(record['nav_date'])
            date_counts[date_str] = date_counts.get(date_str, 0) + 1
        
        logger.info("Sample records loaded by date:")
        for date_str in sorted(date_counts.keys(), reverse=True):
            logger.info(f"  {date_str}: {date_counts[date_str]} records")
        
        logger.info(f"Total schemes: {len(set([r['scheme_code'] for r in sample_data]))}")
        logger.info(f"Total AMCs: {len(set([r['amc_code'] for r in sample_data]))}")

if __name__ == "__main__":
    insert_sample_data()


