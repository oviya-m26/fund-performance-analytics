import requests
import snowflake.connector
from datetime import datetime, timedelta
import logging
import sys

# Set up logging for Airflow
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_current_nav():
    """Load current NAV data from AMFI - designed for Airflow scheduling"""
    
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            account="aftjkdh-wn01017",
            user="OVIYA26",
            password="FwvqSFv3BUeDLwg",
            warehouse="COMPUTE_WH",
            database="MUTUAL_FUNDS_DATABASE",
            schema="CORE",
            autocommit=False
        )
        cursor = conn.cursor()
        
        # Set context
        cursor.execute("USE DATABASE MUTUAL_FUNDS_DATABASE")
        cursor.execute("USE SCHEMA CORE")
        
        # Create current_nav table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS current_nav (
                nav_id NUMBER AUTOINCREMENT PRIMARY KEY,
                scheme_code INTEGER NOT NULL,
                nav_date DATE NOT NULL,
                nav DECIMAL(10,4),
                repurchase_price DECIMAL(10,4),
                sale_price DECIMAL(10,4),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Clear existing current NAV data for today
        today = datetime.now().date()
        logger.info(f"Clearing existing current NAV data for {today}...")
        cursor.execute("DELETE FROM current_nav WHERE nav_date = %s", (today,))
        conn.commit()
        
        # Download current NAV data
        url = "https://www.amfiindia.com/spages/NAVAll.txt"
        logger.info(f"Downloading current NAV data from: {url}")
        
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        lines = response.text.splitlines()
        logger.info(f"Downloaded {len(lines)} lines of current NAV data")
        
        batch_data = []
        current_company = None
        
        for line_num, line in enumerate(lines, 1):
            try:
                parts = [p.strip() for p in line.split(";")]
                
                # Skip empty lines and headers
                if not line.strip() or parts[0] == "Scheme Code":
                    continue
                
                # Company line (single part, no semicolon)
                if len(parts) == 1 and parts[0]:
                    current_company = parts[0].strip()
                    continue
                
                # Scheme line (multiple parts, first part is numeric)
                if len(parts) >= 6 and parts[0].isdigit():
                    scheme_code = int(parts[0])
                    
                    # Based on the data format:
                    # Column 4: Net Asset Value (NAV)
                    # Column 5: Date
                    nav = None
                    nav_date = None
                    
                    # Get NAV from column 4
                    if len(parts) > 4 and parts[4] and parts[4] != "N.A.":
                        try:
                            nav = float(parts[4])
                        except:
                            pass
                    
                    # Get date from column 5
                    if len(parts) > 5 and parts[5]:
                        try:
                            nav_date = datetime.strptime(parts[5], "%d-%b-%Y").date()
                        except:
                            pass
                    
                    # Only insert if we have valid NAV and date
                    if nav is not None and nav_date is not None:
                        batch_data.append((scheme_code, nav_date, nav, None, None))
                    
            except Exception as e:
                if line_num % 5000 == 0:  # Log every 5000 lines
                    logger.warning(f"Error processing line {line_num}: {str(e)}")
                continue
        
        logger.info(f"Parsed {len(batch_data)} current NAV records")
        
        # Batch insert current NAV data
        if batch_data:
            logger.info("Inserting current NAV data in batches...")
            
            # Insert in batches of 1000 for better performance
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(batch_data), batch_size):
                batch = batch_data[i:i + batch_size]
                cursor.executemany("""
                    INSERT INTO current_nav (scheme_code, nav_date, nav, repurchase_price, sale_price)
                    VALUES (%s, %s, %s, %s, %s)
                """, batch)
                total_inserted += len(batch)
                logger.info(f"Inserted batch {i//batch_size + 1} - {total_inserted}/{len(batch_data)} records")
            
            conn.commit()
            logger.info(f"Successfully inserted {total_inserted} current NAV records")
            
            # Return success for Airflow
            return True
        else:
            logger.error("No valid NAV data found to insert")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading data: {str(e)}")
        return False
    except snowflake.connector.Error as e:
        logger.error(f"Snowflake database error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False
    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")

if __name__ == "__main__":
    # For direct execution (testing)
    success = load_current_nav()
    if success:
        print(" Current NAVs loaded successfully.")
        sys.exit(0)
    else:
        print(" Failed to load current NAVs.")
        sys.exit(1)
