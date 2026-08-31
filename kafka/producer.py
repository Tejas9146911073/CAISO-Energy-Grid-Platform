import os
import json
import time
import logging
import pandas as pd
import gridstatus
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load Credentials
load_dotenv()

# Aiven Kafka Credentials
BOOTSTRAP_SERVER = os.getenv("AIVEN_KAFKA_BOOTSTRAP_SERVER")
CA_CERT_PATH = os.getenv("AIVEN_KAFKA_CA_CERT_PATH")
KAFKA_USER = os.getenv("AIVEN_KAFKA_USERNAME", "avnadmin")
KAFKA_PASS = os.getenv("AIVEN_KAFKA_PASSWORD")

if not BOOTSTRAP_SERVER or not CA_CERT_PATH or not KAFKA_PASS:
    logger.error("Aiven Kafka credentials (server, cert path, or password) are missing in your .env file!")
    exit(1)

# Initialize Kafka Producer with SASL_SSL encryption for Aiven
logger.info("Initializing connection to Aiven Kafka using SASL_SSL...")
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASS,
        ssl_cafile=CA_CERT_PATH,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info("Connected to Aiven Kafka successfully!")
except Exception as e:
    logger.error(f"Failed to connect to Aiven Kafka: {e}")
    exit(1)

# Target nodes
locations = ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"]
node_mapping = {
    "TH_NP15_GEN-APND": "TH_NP15",
    "TH_SP15_GEN-APND": "TH_SP15",
    "TH_ZP26_GEN-APND": "TH_ZP26"
}

# Cache for deduplication
last_published_lmp_time = None
last_published_load_time = None

def main():
    global last_published_lmp_time, last_published_load_time
    logger.info("Initializing CAISO Real-Time Grid Producer...")
    caiso = gridstatus.CAISO()
    
    while True:
        try:
            # ========================================================
            # 1. POLL REAL-TIME LMPs
            # ========================================================
            lmp_df = caiso.get_lmp(
                date="today",
                market="REAL_TIME_5_MIN",
                locations=locations
            )
            
            if not lmp_df.empty:
                latest_time = lmp_df["Time"].max()
                latest_time_str = latest_time.isoformat()
                
                if last_published_lmp_time != latest_time_str:
                    logger.info(f"New CAISO LMP pricing interval detected: {latest_time_str}")
                    latest_lmp_rows = lmp_df[lmp_df["Time"] == latest_time]
                    
                    for _, row in latest_lmp_rows.iterrows():
                        node = node_mapping.get(row["Location"])
                        if not node:
                            continue
                            
                        for lmp_type, col in [("LMP", "LMP"), ("MCC", "Congestion"), ("MCL", "Loss")]:
                            message = {
                                "type": "LMP",
                                "node": node,
                                "timestamp": latest_time_str,
                                "lmp_type": lmp_type,
                                "price_per_mwh": float(row[col]),
                                "date": str(latest_time.date())
                            }
                            producer.send("stock-prices", value=message)
                            
                    logger.info(f"Published latest LMP pricing to Kafka for {locations}")
                    last_published_lmp_time = latest_time_str
                    
            # ========================================================
            # 2. POLL REAL-TIME GRID LOAD
            # ========================================================
            load_df = caiso.get_load(date="today")
            if not load_df.empty:
                latest_load_row = load_df.iloc[-1]
                time_col = "Time" if "Time" in latest_load_row else "Interval Start"
                latest_time = latest_load_row[time_col]
                latest_time_str = latest_time.isoformat()
                
                if last_published_load_time != latest_time_str:
                    logger.info(f"New CAISO Load interval detected: {latest_time_str}")
                    
                    actual = int(latest_load_row["Load"]) if "Load" in latest_load_row and not pd.isna(latest_load_row["Load"]) else 0
                    forecast = int(latest_load_row["Forecast"]) if "Forecast" in latest_load_row and not pd.isna(latest_load_row["Forecast"]) else actual
                    
                    message = {
                        "type": "LOAD",
                        "timestamp": latest_time_str,
                        "actual_load_mw": actual,
                        "forecast_load_mw": forecast,
                        "date": str(latest_time.date())
                    }
                    producer.send("stock-prices", value=message)
                    logger.info(f"Published latest Grid Load (Demand: {actual} MW) to Kafka")
                    last_published_load_time = latest_time_str

        except Exception as e:
            logger.error(f"Error during CAISO polling cycle: {e}")

        # Poll every 20 seconds
        time.sleep(20)

if __name__ == "__main__":
    main()
