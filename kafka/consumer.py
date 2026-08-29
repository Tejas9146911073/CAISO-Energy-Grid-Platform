import os
import json
import logging
import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load Credentials
load_dotenv()

# Aiven Kafka configuration
BOOTSTRAP_SERVER = os.getenv("AIVEN_KAFKA_BOOTSTRAP_SERVER")
CA_CERT_PATH = os.getenv("AIVEN_KAFKA_CA_CERT_PATH")
KAFKA_USER = os.getenv("AIVEN_KAFKA_USERNAME", "avnadmin")
KAFKA_PASS = os.getenv("AIVEN_KAFKA_PASSWORD")

# Postgres configuration
DB_HOST = os.getenv("RDS_POSTGRES_HOST")
DB_USER = os.getenv("RDS_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("RDS_POSTGRES_PASSWORD")
DB_NAME = "postgres"

if not BOOTSTRAP_SERVER or not CA_CERT_PATH or not KAFKA_PASS:
    logger.error("Aiven Kafka credentials (server, cert path, or password) are missing in your .env file!")
    exit(1)

def get_postgres_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=5432
    )

# Connect to Aiven Kafka securely using SASL_SSL
logger.info("Connecting to Aiven Kafka using SASL_SSL...")
try:
    consumer = KafkaConsumer(
        "stock-prices",
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASS,
        ssl_cafile=CA_CERT_PATH,
        auto_offset_reset="earliest",
        group_id="postgres-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    logger.info("Connected to Aiven Kafka successfully!")
except Exception as e:
    logger.error(f"Failed to connect to Aiven Kafka: {e}")
    exit(1)

def main():
    logger.info("Starting PostgreSQL Consumer. Listening to Kafka...")
    
    for message in consumer:
        data = message.value
        ticker = data["ticker"]
        timestamp = data["timestamp"]
        open_val = data["open"]
        high_val = data["high"]
        low_val = data["low"]
        close_val = data["close"]
        volume = data["volume"]
        date_str = timestamp.split("T")[0]
        
        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()
            
            # Upsert into PostgreSQL (inserts new candles, does nothing if already exists)
            cursor.execute("""
                INSERT INTO fact_forex_candles (ticker, event_timestamp, open, high, low, close, volume, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, event_timestamp) DO NOTHING;
            """, (ticker, timestamp, open_val, high_val, low_val, close_val, volume, date_str))
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Loaded tick into PostgreSQL: {ticker} @ {timestamp}")
        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL: {e}")

if __name__ == "__main__":
    main()
