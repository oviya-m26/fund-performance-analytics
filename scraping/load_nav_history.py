import os
import snowflake.connector
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    
    # Create nav_history table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nav_history (
            nav_id NUMBER AUTOINCREMENT PRIMARY KEY,
            scheme_code INTEGER NOT NULL,
            nav_date DATE NOT NULL,
            nav DECIMAL(10,4),
            repurchase_price DECIMAL(10,4),
            sale_price DECIMAL(10,4),
            amc_code INTEGER,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    # Clear existing NAV data
    logger.info("Clearing existing NAV history data...")
    cursor.execute("DELETE FROM nav_history")
    conn.commit()
    
    folder = r"C:/Users/ADMIN/Desktop/NAV data/nav"
    
    if not os.path.exists(folder):
        logger.error(f"Folder not found: {folder}")
        exit(1)
    
    total_files_processed = 0
    total_rows_inserted = 0
    
    for filename in os.listdir(folder):
        if filename.startswith("mf_") and filename.endswith(".txt"):
            try:
                amc_code = int(filename.split("_")[1].split(".")[0])
                file_path = os.path.join(folder, filename)
                batch_data = []
                
                logger.info(f"Processing file: {filename}")
                
                with open(file_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            parts = [p.strip() for p in line.split(";")]
                            if len(parts) < 5 or parts[0] == "Scheme Code":
                                continue
                            
                            scheme_code = int(parts[0])
                            nav_date = datetime.strptime(parts[-1], "%d-%b-%Y").date()
                            nav = float(parts[4]) if parts[4] and parts[4] != "N.A." else None
                            repurchase = float(parts[5]) if len(parts) > 5 and parts[5] and parts[5] != "N.A." else None
                            sale = float(parts[6]) if len(parts) > 6 and parts[6] and parts[6] != "N.A." else None
                            
                            batch_data.append((scheme_code, nav_date, nav, repurchase, sale, amc_code))
                            
                        except Exception as e:
                            if line_num % 10000 == 0:  # Log every 10k lines
                                logger.warning(f"Error processing line {line_num} in {filename}: {str(e)}")
                            continue
                
                if batch_data:
                    cursor.executemany("""
                        INSERT INTO nav_history (scheme_code, nav_date, nav, repurchase_price, sale_price, amc_code)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, batch_data)
                    conn.commit()
                    total_rows_inserted += len(batch_data)
                    total_files_processed += 1
                    logger.info(f"Inserted {len(batch_data)} rows from {filename}")
                else:
                    logger.warning(f"No valid data found in {filename}")
                    
            except Exception as e:
                logger.error(f"Error processing file {filename}: {str(e)}")
                continue
    
    # Final validation
    cursor.execute("SELECT COUNT(*) FROM nav_history")
    final_count = cursor.fetchone()[0]
    
    logger.info(f"Processing completed:")
    logger.info(f"- Files processed: {total_files_processed}")
    logger.info(f"- Total rows inserted: {total_rows_inserted}")
    logger.info(f"- Final count in database: {final_count}")
    
    # Show sample data
    cursor.execute("SELECT scheme_code, nav_date, nav FROM nav_history LIMIT 5")
    sample_data = cursor.fetchall()
    logger.info(f"Sample NAV data: {sample_data}")
    
    if final_count == 0:
        logger.error("ERROR: No NAV data was inserted!")
    else:
        logger.info("✅ Historical NAVs loaded successfully.")
    
except snowflake.connector.Error as e:
    logger.error(f"Snowflake database error: {str(e)}")
except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
finally:
    try:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Error closing database connection: {str(e)}")

print("NAV history loading process completed. Check logs for details.")
