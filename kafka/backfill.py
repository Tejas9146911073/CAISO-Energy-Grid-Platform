import os
import logging
import requests
import psycopg2
import pandas as pd
import gridstatus
from datetime import datetime, timezone
from dotenv import load_dotenv

# Configure Logging
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

def reconcile_caiso_data():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"Starting CAISO daily gap-filling and reconciliation for date: {today} UTC...")
    
    # Initialize CAISO client
    caiso = gridstatus.CAISO()
    start_time = pd.Timestamp(f"{today}T00:00:00Z")
    end_time = pd.Timestamp(f"{today}T23:59:59Z")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # ========================================================
    # 1. RECONCILE SYSTEM LOAD
    # ========================================================
    logger.info("Running Load reconciliation...")
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
            
            reconciled_load = 0
            for _, row in load_df.iterrows():
                ts = row["event_timestamp"].isoformat()
                actual = int(row["actual_load_mw"]) if not pd.isna(row["actual_load_mw"]) else 0
                forecast = int(row["forecast_load_mw"]) if not pd.isna(row["forecast_load_mw"]) else 0
                
                cursor.execute("""
                    INSERT INTO fact_caiso_load (event_timestamp, actual_load_mw, forecast_load_mw, date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_timestamp) DO UPDATE SET 
                        actual_load_mw = EXCLUDED.actual_load_mw,
                        forecast_load_mw = EXCLUDED.forecast_load_mw;
                """, (ts, actual, forecast, today))
                reconciled_load += 1
                
            conn.commit()
            logger.info(f"Reconciled {reconciled_load} load intervals successfully!")
        else:
            logger.warning("No load data returned for today.")
    except Exception as e:
        logger.error(f"Error during load reconciliation: {e}")
        conn.rollback()

    # ========================================================
    # 2. RECONCILE LMPs
    # ========================================================
    logger.info("Running LMP pricing reconciliation...")
    locations = ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]
    node_mapping = {
        "TH_NP15_GEN-APND": "TH_NP15",
        "TH_SP15_GEN-APND": "TH_SP15",
        "TH_ZP26_GEN-APND": "TH_ZP26"
    }
    
    try:
        lmp_df = caiso.get_lmp(
            start=start_time,
            end=end_time,
            market="REAL_TIME_5_MIN",
            locations=locations
        )
        
        if not lmp_df.empty:
            lmp_df["node_clean"] = lmp_df["Location"].map(node_mapping)
            
            reconciled_lmp = 0
            for _, row in lmp_df.iterrows():
                node = row["node_clean"]
                if pd.isna(node):
                    continue
                    
                ts = row["Time"].isoformat()
                
                # Upsert all pricing components
                for lmp_type, col in [("LMP", "LMP"), ("MCC", "Congestion"), ("MCL", "Loss")]:
                    cursor.execute("""
                        INSERT INTO fact_caiso_lmp (node, event_timestamp, lmp_type, price_per_mwh, date)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (node, event_timestamp, lmp_type) DO UPDATE SET 
                            price_per_mwh = EXCLUDED.price_per_mwh;
                    """, (node, ts, lmp_type, float(row[col]), today))
                    reconciled_lmp += 1
                    
            conn.commit()
            logger.info(f"Reconciled {reconciled_lmp} LMP pricing components successfully!")
        else:
            logger.warning("No LMP data returned for today.")
    except Exception as e:
        logger.error(f"Error during LMP reconciliation: {e}")
        conn.rollback()

    cursor.close()
    conn.close()
    logger.info("Daily grid reconciliation complete!")

if __name__ == "__main__":
    reconcile_caiso_data()
