import os
import logging
import psycopg2
import psycopg2.extras
import pandas as pd
import gridstatus
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load Credentials
load_dotenv()
DB_HOST = os.getenv("RDS_POSTGRES_HOST")
DB_USER = os.getenv("RDS_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("RDS_POSTGRES_PASSWORD")
DB_NAME = "postgres"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, port=5432
    )

def bootstrap_caiso_data():
    logger.info("Initializing CAISO Historical Bootstrap (Last 7 Days)...")
    
    # Initialize CAISO client
    caiso = gridstatus.CAISO()
    
    # Define start and end time range (UTC)
    end_time = pd.Timestamp.now(tz="UTC")
    start_time = end_time - pd.Timedelta(days=7)
    
    logger.info(f"Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # ========================================================
    # 1. FETCH & BULK-LOAD CAISO SYSTEM LOAD
    # ========================================================
    logger.info("Fetching CAISO System Load (Demand)...")
    try:
        load_df = caiso.get_load(start=start_time, end=end_time)
        if not load_df.empty:
            load_df = load_df.rename(columns={
                "Time": "event_timestamp",
                "Actual Load": "actual_load_mw",
                "Load": "actual_load_mw",
                "Forecast Load": "forecast_load_mw",
                "Forecast": "forecast_load_mw"
            })
            
            load_df["date"] = pd.to_datetime(load_df["event_timestamp"]).dt.date
            
            load_records = []
            for _, row in load_df.iterrows():
                ts = row["event_timestamp"].isoformat()
                actual = int(row["actual_load_mw"]) if not pd.isna(row["actual_load_mw"]) else 0
                forecast = int(row["forecast_load_mw"]) if not pd.isna(row["forecast_load_mw"]) else 0
                date_str = str(row["date"])
                load_records.append((ts, actual, forecast, date_str))
                
            logger.info(f"Bulk-inserting {len(load_records)} Load intervals into fact_caiso_load...")
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO fact_caiso_load (event_timestamp, actual_load_mw, forecast_load_mw, date)
                VALUES %s
                ON CONFLICT (event_timestamp) DO UPDATE SET 
                    actual_load_mw = EXCLUDED.actual_load_mw,
                    forecast_load_mw = EXCLUDED.forecast_load_mw;
                """,
                load_records
            )
            conn.commit()
            logger.info("CAISO Load bootstrap completed successfully!")
        else:
            logger.warning("No CAISO Load data returned from API.")
            
    except Exception as e:
        logger.error(f"Failed to bootstrap CAISO Load: {e}")
        conn.rollback()

    # ========================================================
    # 2. FETCH & BULK-LOAD CAISO LMPs (PRICING)
    # ========================================================
    logger.info("Fetching CAISO Locational Marginal Prices (LMPs)...")
    locations = ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]
    
    try:
        lmp_df = caiso.get_lmp(
            start=start_time,
            end=end_time,
            market="REAL_TIME_5_MIN",
            locations=locations
        )
        
        if not lmp_df.empty:
            node_mapping = {
                "TH_NP15_GEN-APND": "TH_NP15",
                "TH_SP15_GEN-APND": "TH_SP15",
                "TH_ZP26_GEN-APND": "TH_ZP26"
            }
            
            lmp_df["node_clean"] = lmp_df["Location"].map(node_mapping)
            
            lmp_records = []
            for _, row in lmp_df.iterrows():
                node = row["node_clean"]
                if pd.isna(node):
                    continue
                    
                ts = row["Time"].isoformat()
                date_str = str(row["Time"].date())
                
                lmp_records.append((node, ts, "LMP", float(row["LMP"]), date_str))
                lmp_records.append((node, ts, "MCC", float(row["Congestion"]), date_str))
                lmp_records.append((node, ts, "MCL", float(row["Loss"]), date_str))
                
            logger.info(f"Bulk-inserting {len(lmp_records)} LMP pricing records into fact_caiso_lmp...")
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO fact_caiso_lmp (node, event_timestamp, lmp_type, price_per_mwh, date)
                VALUES %s
                ON CONFLICT (node, event_timestamp, lmp_type) DO UPDATE SET 
                    price_per_mwh = EXCLUDED.price_per_mwh;
                """,
                lmp_records
            )
            conn.commit()
            logger.info("CAISO LMP bootstrap completed successfully!")
        else:
            logger.warning("No CAISO LMP data returned from API.")
            
    except Exception as e:
        logger.error(f"Failed to bootstrap CAISO LMP: {e}")
        conn.rollback()

    # ========================================================
    # 3. BUILD OPTIMIZED B-TREE INDEX ON DATABASE
    # ========================================================
    logger.info("Building time-series B-Tree indexes on database...")
    try:
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_caiso_lmp_time
            ON fact_caiso_lmp (node, event_timestamp DESC);
        """)
        conn.commit()
        logger.info("B-Tree composite indexes built successfully!")
    except Exception as e:
        logger.error(f"Failed to build indexes: {e}")
        conn.rollback()

    cursor.close()
    conn.close()
    logger.info("Historical bootstrapping complete!")

if __name__ == "__main__":
    bootstrap_caiso_data()
