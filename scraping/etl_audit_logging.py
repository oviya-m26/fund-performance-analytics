import snowflake.connector
import logging
from datetime import datetime
from typing import List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def classify_benchmark(scheme_name: str, scheme_type: str):
	text = f"{scheme_name or ''} {scheme_type or ''}".lower()
	# Direct index clues
	if "sensex" in text:
		return "BSE Sensex"
	if "nifty next 50" in text or "next 50" in text:
		return "NIFTY Next 50"
	if "nifty 50" in text:
		return "NIFTY 50"
	if "nifty 500" in text:
		return "NIFTY 500"
	if "midcap 100" in text or "mid cap 100" in text:
		return "NIFTY Midcap 100"
	if "smallcap 100" in text or "small cap 100" in text:
		return "NIFTY Smallcap 100"
	# Category rules
	if "large & mid" in text or "large and mid" in text:
		return "NIFTY 500"
	if "large cap" in text:
		return "NIFTY 50"
	if "mid cap" in text or "midcap" in text:
		return "NIFTY Midcap 100"
	if "small cap" in text or "smallcap" in text:
		return "NIFTY Smallcap 100"
	if "flexi cap" in text or "multi cap" in text or "multicap" in text:
		return "NIFTY 500"
	if "elss" in text or "tax saver" in text or "taxsaver" in text:
		return "NIFTY 500"
	if "value" in text or "focused" in text or "contra" in text or "dividend yield" in text:
		return "NIFTY 500"
	# Debt / hybrid -> skip
	if any(k in text for k in ["gilt", "government", "psu", "liquid", "money market", "overnight", "debt", "bond", "hybrid", "arbitrage"]):
		return None
	return None


try:
	# Connect to Snowflake in the correct context
	conn = snowflake.connector.connect(
		account="aftjkdh-wn01017",
		user="OVIYA26",
		password="FwvqSFv3BUeDLwg",
		warehouse="COMPUTE_WH",
		database="MUTUAL_FUNDS_DATABASE",
		schema="CORE",
		autocommit=False,
	)
	cursor = conn.cursor()
	cursor.execute("USE DATABASE MUTUAL_FUNDS_DATABASE")
	cursor.execute("USE SCHEMA CORE")

	# Load schemes
	cursor.execute("SELECT scheme_code, scheme_name, scheme_type FROM mutual_fund_scheme ORDER BY scheme_code")
	schemes: List[Tuple[int, str, str]] = cursor.fetchall()
	logger.info(f"Loaded {len(schemes)} schemes")

	# Load existing benchmarks
	cursor.execute("SELECT benchmark_id, benchmark_name FROM benchmark_index")
	rows = cursor.fetchall()
	if not rows:
		raise RuntimeError("benchmark_index is empty. Please populate it before running audit mapping.")
	name_to_id = {r[1]: r[0] for r in rows}

	# Build mapping
	to_map: List[Tuple[int, int]] = []
	skipped = 0
	for scheme_code, scheme_name, scheme_type in schemes:
		bname = classify_benchmark(scheme_name, scheme_type)
		if not bname:
			skipped += 1
			continue
		bid = name_to_id.get(bname)
		if not bid:
			skipped += 1
			continue
		to_map.append((scheme_code, bid))

	logger.info(f"Prepared {len(to_map)} mappings; skipped {skipped}")

	# Apply mappings in batches using UPDATE + INSERT (no temp tables)
	applied = 0
	if to_map:
		batch_size = 2000
		for i in range(0, len(to_map), batch_size):
			batch = to_map[i:i+batch_size]
			placeholders = ", ".join(["(%s, %s)"] * len(batch))
			params: List[int] = []
			for sc, bid in batch:
				params.extend([sc, bid])
			# update
			sql_update = f"""
			UPDATE scheme_benchmark_mapping AS m
			SET benchmark_id = s.benchmark_id
			FROM (
			  SELECT column1::NUMBER AS scheme_code, column2::NUMBER AS benchmark_id
			  FROM VALUES {placeholders}
			) AS s
			WHERE m.scheme_code = s.scheme_code
			"""
			cursor.execute(sql_update, params)
			# insert
			sql_insert = f"""
			INSERT INTO scheme_benchmark_mapping (scheme_code, benchmark_id)
			SELECT s.scheme_code, s.benchmark_id
			FROM (
			  SELECT column1::NUMBER AS scheme_code, column2::NUMBER AS benchmark_id
			  FROM VALUES {placeholders}
			) AS s
			LEFT JOIN scheme_benchmark_mapping AS m ON m.scheme_code = s.scheme_code
			WHERE m.scheme_code IS NULL
			"""
			cursor.execute(sql_insert, params)
			applied += len(batch)
		logger.info(f"Applied {applied} mappings in total")

	conn.commit()

	# Audit: single summary row (no table creation)
	cursor.execute(
		"""
		INSERT INTO etl_audit (table_name, file_name, rows_inserted, load_timestamp)
		VALUES (%s, %s, %s, %s)
		""",
		("scheme_benchmark_mapping", "etl_audit_logging.py", applied, datetime.now()),
	)
	conn.commit()
	logger.info("Audit row inserted")

	# Final count
	cursor.execute("SELECT COUNT(*) FROM scheme_benchmark_mapping")
	final_cnt = cursor.fetchone()[0]
	logger.info(f"Final mapping count: {final_cnt}")

except Exception as e:
	logger.error(f"Error: {e}")
	try:
		conn.rollback()
	except Exception:
		pass
	raise
finally:
	try:
		if 'cursor' in locals():
			cursor.close()
		if 'conn' in locals():
			conn.close()
		logger.info("Connection closed")
	except Exception:
		pass
