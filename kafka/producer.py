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
API_KEY = os.getenv("OANDA_API_KEY") # Kept for Oanda check if needed, but not used here

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
                market="REAL_TIME_5_MIN",
                locations=locations,
                latest=True
            )
            
            if not lmp_df.empty:
                latest_time = lmp_df["Time"].iloc[0]
                latest_time_str = latest_time.isoformat()
                
                # Check if this is a new 5-minute pricing interval
                if last_published_lmp_time != latest_time_str:
                    logger.info(f"New CAISO LMP pricing interval detected: {latest_time_str}")
                    
                    for _, row in lmp_df.iterrows():
                        node = node_mapping.get(row["Location"])
                        if not node:
                            continue
                            
                        # Send Total Price, Congestion, and Loss component messages
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
            load_df = caiso.get_load(latest=True)
            if not load_df.empty:
                load_df = load_df.rename(columns={
                    "Time": "event_timestamp",
                    "Actual Load": "actual_load_mw",
                    "Load": "actual_load_mw",
                    "Forecast Load": "forecast_load_mw",
                    "Forecast": "forecast_load_mw"
                })
                
                latest_load_time = load_df["event_timestamp"].iloc[0]
                latest_load_time_str = latest_load_time.isoformat()
                
                # Check if this is a new Load interval
                if last_published_load_time != latest_load_time_str:
                    logger.info(f"New CAISO Load interval detected: {latest_load_time_str}")
                    
                    row = load_df.iloc[0]
                    actual = int(row["actual_load_mw"]) if not pd.isna(row["actual_load_mw"]) else 0
                    forecast = int(row["forecast_load_mw"]) if not pd.isna(row["forecast_load_mw"]) else 0
                    
                    message = {
                        "type": "LOAD",
                        "timestamp": latest_load_time_str,
                        "actual_load_mw": actual,
                        "forecast_load_mw": forecast,
                        "date": str(latest_load_time.date())
                    }
                    producer.send("stock-prices", value=message)
                    logger.info(f"Published latest Grid Load (Demand: {actual} MW) to Kafka")
                    last_published_load_time = latest_load_time_str

        except Exception as e:
            logger.error(f"Error during CAISO polling cycle: {e}")

        # Poll every 15 seconds to capture the 5-minute ticks immediately
        time.sleep(15)

if __name__ == "__main__":
    main()
