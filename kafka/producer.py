import os
import json
import time
import logging
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load Credentials
load_dotenv()
API_KEY = os.getenv("OANDA_API_KEY")
ENVIRONMENT = os.getenv("OANDA_ENVIRONMENT", "practice")

# Aiven Kafka Credentials
BOOTSTRAP_SERVER = os.getenv("AIVEN_KAFKA_BOOTSTRAP_SERVER")
CA_CERT_PATH = os.getenv("AIVEN_KAFKA_CA_CERT_PATH")
KAFKA_USER = os.getenv("AIVEN_KAFKA_USERNAME", "avnadmin")
KAFKA_PASS = os.getenv("AIVEN_KAFKA_PASSWORD")

if not API_KEY:
    logger.error("OANDA_API_KEY is missing in your .env file!")
    exit(1)
if not BOOTSTRAP_SERVER or not CA_CERT_PATH or not KAFKA_PASS:
    logger.error("Aiven Kafka credentials (server, cert path, or password) are missing in your .env file!")
    exit(1)

# Establish Oanda URL based on environment
if ENVIRONMENT.lower() == "live":
    BASE_URL = "https://api-fxtrade.oanda.com"
else:
    BASE_URL = "https://api-fxpractice.oanda.com"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# Fetch Oanda Account ID dynamically at startup
logger.info("Connecting to Oanda to resolve Account ID...")
try:
    accounts_url = f"{BASE_URL}/v3/accounts"
    response = requests.get(accounts_url, headers=headers)
    ACCOUNT_ID = response.json().get("accounts", [])[0]["id"]
    logger.info(f"Oanda Account ID resolved: {ACCOUNT_ID}")
except Exception as e:
    logger.error(f"Failed to connect to Oanda: {e}")
    exit(1)

def is_market_open():
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    hour = now.hour
    if weekday == 4 and hour >= 22:
        return False
    if weekday == 5:
        return False
    if weekday == 6 and hour < 22:
        return False
    return True

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

INSTRUMENTS = ["XAU_USD", "XAG_USD", "USD_JPY"]
GRANULARITY = "M5"
last_published_timestamps = {inst: None for inst in INSTRUMENTS}

def main():
    logger.info(f"Starting Oanda {GRANULARITY} state-aware poller for {INSTRUMENTS}...")
    while True:
        if is_market_open():
            for inst in INSTRUMENTS:
                try:
                    # Fetch last 2 candles
                    url = f"{BASE_URL}/v3/instruments/{inst}/candles?granularity={GRANULARITY}&count=2&price=M"
                    response = requests.get(url, headers=headers)
                    
                    if response.status_code == 200:
                        candles = response.json().get("candles", [])
                        if candles:
                            completed_candle = candles[0]
                            if completed_candle.get("complete") is True:
                                candle_time = completed_candle["time"]
                                
                                if last_published_timestamps[inst] != candle_time:
                                    open_val = float(completed_candle["mid"]["o"])
                                    high_val = float(completed_candle["mid"]["h"])
                                    low_val = float(completed_candle["mid"]["l"])
                                    close_val = float(completed_candle["mid"]["c"])
                                    volume = int(completed_candle["volume"])
                                    
                                    data = {
                                        "ticker": inst,
                                        "open": open_val,
                                        "high": high_val,
                                        "low": low_val,
                                        "close": close_val,
                                        "volume": volume,
                                        "timestamp": candle_time
                                    }
                                    
                                    producer.send("stock-prices", value=data)
                                    logger.info(f"New M5 candle -> {inst}: O={open_val:.4f}, H={high_val:.4f}, L={low_val:.4f}, C={close_val:.4f} (Time: {candle_time})")
                                    last_published_timestamps[inst] = candle_time
                    else:
                        logger.error(f"Error fetching {inst}: {response.status_code} {response.text}")
                except Exception as e:
                    logger.error(f"Exception polling {inst}: {e}")
            time.sleep(10)
        else:
            logger.info("Forex markets are closed (Weekend). Standby mode active. Checking again in 60s...")
            time.sleep(60)

if __name__ == "__main__":
    main()
