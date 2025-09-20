import requests
import snowflake.connector
from datetime import datetime
import logging
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Connect to Snowflake with explicit autocommit
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
    
    # Explicitly set context to ensure we're in the right database/schema
    cursor.execute("USE DATABASE MUTUAL_FUNDS_DATABASE")
    cursor.execute("USE SCHEMA CORE")
    
    # Create tables if they don't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mutual_fund_company (
            company_id NUMBER AUTOINCREMENT PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mutual_fund_scheme (
            scheme_id NUMBER AUTOINCREMENT PRIMARY KEY,
            scheme_code INTEGER NOT NULL,
            scheme_name VARCHAR(500) NOT NULL,
            scheme_type VARCHAR(100),
            company_id INTEGER,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (company_id) REFERENCES mutual_fund_company(company_id)
        )
    """)
    
    # Download scheme master file
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    logger.info(f"Downloading data from: {url}")
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    lines = response.text.splitlines()
    logger.info(f"Downloaded {len(lines)} lines of data")
    
    # Parse data into memory first
    companies_data = []
    schemes_data = []
    current_company = None
    
    logger.info("Parsing data...")
    for line_num, line in enumerate(lines, 1):
        try:
            if not line.strip():
                continue
                
            parts = [p.strip() for p in line.split(";")]
            
            # Company line (single part, no semicolon)
            if len(parts) == 1 and parts[0]:
                current_company = parts[0].strip()
                if current_company:
                    companies_data.append((current_company,))
            
            # Scheme line (multiple parts, first part is numeric)
            elif len(parts) >= 4 and parts[0].isdigit() and current_company:
                scheme_code = int(parts[0])
                scheme_type = parts[1] if len(parts) > 1 else ""
                scheme_name = parts[3] if len(parts) > 3 else ""
                
                if scheme_name:
                    schemes_data.append((scheme_code, scheme_name, scheme_type, current_company))
                    
        except Exception as e:
            logger.error(f"Error processing line {line_num}: {line[:100]}... Error: {str(e)}")
            continue
    
    logger.info(f"Parsed {len(companies_data)} companies and {len(schemes_data)} schemes")
    
    # Clear existing data to avoid duplicates
    logger.info("Clearing existing data...")
    cursor.execute("DELETE FROM mutual_fund_scheme")
    cursor.execute("DELETE FROM mutual_fund_company")
    conn.commit()
    
    # Batch insert companies
    if companies_data:
        logger.info("Inserting companies in batch...")
        cursor.executemany("""
            INSERT INTO mutual_fund_company (company_name)
            VALUES (%s)
        """, companies_data)
        conn.commit()
        logger.info(f"Inserted {len(companies_data)} companies")
        
        # Immediately verify companies were inserted
        cursor.execute("SELECT COUNT(*) FROM mutual_fund_company")
        immediate_count = cursor.fetchone()[0]
        logger.info(f"Immediate verification - Companies count: {immediate_count}")
    
    # Get company IDs mapping
    logger.info("Creating company ID mapping...")
    cursor.execute("SELECT company_id, company_name FROM mutual_fund_company")
    company_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Prepare schemes data with company IDs
    schemes_with_ids = []
    for scheme_code, scheme_name, scheme_type, company_name in schemes_data:
        if company_name in company_mapping:
            schemes_with_ids.append((scheme_code, scheme_name, scheme_type, company_mapping[company_name]))
    
    # Batch insert schemes
    if schemes_with_ids:
        logger.info("Inserting schemes in batch...")
        cursor.executemany("""
            INSERT INTO mutual_fund_scheme (scheme_code, scheme_name, scheme_type, company_id)
            VALUES (%s, %s, %s, %s)
        """, schemes_with_ids)
        conn.commit()
        logger.info(f"Inserted {len(schemes_with_ids)} schemes")
    
    logger.info(f"Successfully loaded {len(companies_data)} companies and {len(schemes_with_ids)} schemes")
    
    # Final validation and commit
    logger.info("Final validation...")
    cursor.execute("SELECT COUNT(*) FROM mutual_fund_company")
    company_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM mutual_fund_scheme") 
    scheme_count = cursor.fetchone()[0]
    
    logger.info(f"Final validation - Companies in database: {company_count}")
    logger.info(f"Final validation - Schemes in database: {scheme_count}")
    
    # Show sample data to prove it's there
    cursor.execute("SELECT company_id, company_name FROM mutual_fund_company LIMIT 5")
    sample_companies = cursor.fetchall()
    logger.info(f"Sample companies: {sample_companies}")
    
    cursor.execute("SELECT scheme_id, scheme_name, company_id FROM mutual_fund_scheme LIMIT 5")
    sample_schemes = cursor.fetchall()
    logger.info(f"Sample schemes: {sample_schemes}")
    
    # One final commit to ensure everything is persisted
    conn.commit()
    logger.info("Final commit completed")
    
except requests.exceptions.RequestException as e:
    logger.error(f"Error downloading data: {str(e)}")
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

print("Data loading process completed. Check logs for details.")
