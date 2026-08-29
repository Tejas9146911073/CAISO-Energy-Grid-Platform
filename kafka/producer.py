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

if not API_KEY:
    logger.error("OANDA_API_KEY is missing in your .env file!")
    exit(1)
if not BOOTSTRAP_SERVER or not CA_CERT_PATH:
    logger.error("AIVEN_KAFKA_BOOTSTRAP_SERVER or AIVEN_KAFKA_CA_CERT_PATH is missing in your .env file!")
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

# 1. Fetch Oanda Account ID dynamically at startup
logger.info("Connecting to Oanda to resolve Account ID...")
try:
    accounts_url = f"{BASE_URL}/v3/accounts"
    response = requests.get(accounts_url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Failed to fetch Oanda accounts: {response.status_code} {response.text}")
        exit(1)
    
    accounts = response.json().get("accounts", [])
    if not accounts:
        logger.error("No Oanda accounts found under this API Key.")
        exit(1)
        
    ACCOUNT_ID = accounts[0]["id"]
    logger.info(f"Oanda Account ID resolved: {ACCOUNT_ID}")
except Exception as e:
    logger.error(f"Exception during Oanda lookup: {e}")
    exit(1)

def is_market_open():
    """Returns True if the global Forex market is open in UTC.
    Market closes Friday at 22:00 UTC and opens Sunday at 22:00 UTC.
    """
    now = datetime.now(timezone.utc)
    weekday = now.weekday()  # Mon=0, Fri=4, Sat=5, Sun=6
    hour = now.hour
    
    if weekday == 4:  # Friday
        if hour >= 22:
            return False
    elif weekday == 5:  # Saturday
        return False
    elif weekday == 6:  # Sunday
        if hour < 22:
            return False
            
    return True

# 2. Initialize Kafka Producer with SSL encryption for Aiven
logger.info("Initializing connection to Aiven Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol="SSL",
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
                                
                                # Check if we have already published this candle
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
                                    
                                    # Publish to Aiven Kafka topic 'stock-prices'
                                    producer.send("stock-prices", value=data)
                                    logger.info(f"New M5 candle -> {inst}: O={open_val:.4f}, H={high_val:.4f}, L={low_val:.4f}, C={close_val:.4f} (Time: {candle_time})")
                                    
                                    # Update cache
                                    last_published_timestamps[inst] = candle_time
                    else:
                        logger.error(f"Error fetching {inst}: {response.status_code} {response.text}")
                except Exception as e:
                    logger.error(f"Exception polling {inst}: {e}")
            
            # Poll every 10 seconds
            time.sleep(10)
        else:
            logger.info("Forex markets are closed (Weekend). Standby mode active. Checking again in 60s...")
            time.sleep(60)

if __name__ == "__main__":
    main()
