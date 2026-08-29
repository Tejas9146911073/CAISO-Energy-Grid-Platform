import os
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load credentials
load_dotenv()
API_KEY = os.getenv("OANDA_API_KEY")
ENVIRONMENT = os.getenv("OANDA_ENVIRONMENT", "practice")

DB_HOST = os.getenv("RDS_POSTGRES_HOST")
DB_USER = os.getenv("RDS_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("RDS_POSTGRES_PASSWORD")
DB_NAME = "postgres"

if ENVIRONMENT.lower() == "live":
    BASE_URL = "https://api-fxtrade.oanda.com"
else:
    BASE_URL = "https://api-fxpractice.oanda.com"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def backfill():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Starting 5-minute historical backfill and gap-filling for date: {today}...")
    
    tickers = ["XAU_USD", "XAG_USD", "USD_JPY"]
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            port=5432
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return
    
    for t in tickers:
        try:
            # Query Oanda REST for today's entire list of 5-minute candles
            url = f"{BASE_URL}/v3/instruments/{t}/candles?from={today}T00:00:00Z&to={today}T23:59:59Z&granularity=M5"
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                candles = res.json().get("candles", [])
                print(f"Retrieved {len(candles)} candles for {t}")
                for candle in candles:
                    open_val = float(candle["mid"]["o"])
                    high_val = float(candle["mid"]["h"])
                    low_val = float(candle["mid"]["l"])
                    close_val = float(candle["mid"]["c"])
                    volume = int(candle["volume"])
                    candle_time = candle["time"]
                    
                    # Upsert: overwrite fields if they exist to heal any data quality anomalies
                    cursor.execute("""
                        INSERT INTO fact_forex_candles (ticker, event_timestamp, open, high, low, close, volume, date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ticker, event_timestamp) 
                        DO UPDATE SET 
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume;
                    """, (t, candle_time, open_val, high_val, low_val, close_val, volume, today))
            else:
                print(f"Failed to fetch Oanda data for {t}: {res.status_code} {res.text}")
            conn.commit()
        except Exception as e:
            print(f"Failed to backfill {t}: {e}")
            
    cursor.close()
    conn.close()
    print("Backfill & Gap-filling completed successfully!")

if __name__ == "__main__":
    backfill()
