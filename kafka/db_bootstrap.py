import os
import time
import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# ========================================================
# DATABASE CONFIG (Reads from your .env file)
# ========================================================
load_dotenv()

DB_HOST = os.getenv("RDS_POSTGRES_HOST")
DB_USER = os.getenv("RDS_POSTGRES_USER")
DB_PASS = os.getenv("RDS_POSTGRES_PASSWORD")
DB_NAME = "postgres"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, port=5432
    )

# ========================================================
# OANDA CONFIG
# ========================================================
OANDA_API_KEY = os.getenv("OANDA_API_KEY")
ENVIRONMENT = os.getenv("OANDA_ENVIRONMENT", "practice")

if ENVIRONMENT.lower() == "live":
    BASE_URL = "https://api-fxtrade.oanda.com"
else:
    BASE_URL = "https://api-fxpractice.oanda.com"

headers = {
    "Authorization": f"Bearer {OANDA_API_KEY}",
    "Content-Type": "application/json"
}

instruments = ["XAU_USD", "XAG_USD", "USD_JPY"]
granularity = "M5"

# ========================================================
# BULK INSERT TO POSTGRESQL
# ========================================================
def save_df_to_postgres(df, ticker):
    if df.empty:
        return
        
    print(f"Bulk-inserting {len(df)} rows for {ticker} into PostgreSQL...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Add metadata columns matching our schema
    df["ticker"] = ticker
    df["date"] = df["datetime"].dt.date
    
    # Convert dataframe rows to tuples
    records = df[["ticker", "datetime", "open", "high", "low", "close", "volume", "date"]].values.tolist()
    
    # Fast bulk insertion using execute_values
    psycopg2.extras.execute_values(
        cursor,
        """
        INSERT INTO fact_forex_candles (ticker, event_timestamp, open, high, low, close, volume, date)
        VALUES %s
        ON CONFLICT (ticker, event_timestamp) DO NOTHING;
        """,
        records
    )
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully saved chunk to Database!")

# ========================================================
# FETCH MULTI-YEAR DATA CHUNKED
# ========================================================
def fetch_candles_chunked(instrument, granularity, start_dt, end_dt):
    all_data = pd.DataFrame()
    current_start = start_dt
    chunk_days = 7

    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=chunk_days), end_dt)
        print(f"[{instrument}] Fetching chunk: {current_start.date()} to {current_end.date()}")

        url = f"{BASE_URL}/v3/instruments/{instrument}/candles"
        params = {
            "from": current_start.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
            "to": current_end.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
            "granularity": granularity,
            "price": "M"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            candles = data.get("candles", [])

            if candles:
                candles_returned = len(candles)
                df = pd.DataFrame({
                    "datetime": pd.to_datetime([c["time"] for c in candles], utc=True),
                    "open": [float(c["mid"]["o"]) for c in candles],
                    "high": [float(c["mid"]["h"]) for c in candles],
                    "low": [float(c["mid"]["l"]) for c in candles],
                    "close": [float(c["mid"]["c"]) for c in candles],
                    "volume": [c["volume"] for c in candles],
                })

                all_data = pd.concat([all_data, df], ignore_index=True)

                if candles_returned >= 4900:
                    chunk_days = max(1, chunk_days // 2)
                    print(f"Near request limit, reducing chunk to {chunk_days} days")
                else:
                    chunk_days = min(7, chunk_days + 1)
            else:
                chunk_days = max(1, chunk_days // 2)

        except Exception as e:
            print(f"Error for chunk {current_start} to {current_end}: {e}")
            chunk_days = max(1, chunk_days // 2)

        current_start = current_end + timedelta(seconds=1)
        time.sleep(0.3)  # Respect API limits

    if not all_data.empty:
        all_data.drop_duplicates(subset=["datetime"], inplace=True)
        all_data.sort_values("datetime", inplace=True)

    return all_data

# ========================================================
# MAIN RUN
# ========================================================
def main():
    print("Starting Historical Bootstrap...")
    
    # 1. Loop through each instrument
    for ticker in instruments:
        print(f"\n==================== BOOTSTRAPPING {ticker} ====================")
        
        # Loop through each year from 2016 to 2026 (or 2027)
        for year in range(2016, 2027):
            start_dt = datetime(year, 1, 1)
            end_dt = min(datetime(year, 12, 31, 23, 59, 59), datetime.now())
            
            df_year = fetch_candles_chunked(ticker, granularity, start_dt, end_dt)
            save_df_to_postgres(df_year, ticker)
            
            if end_dt == datetime.now():
                break

    # 2. Build B-Tree Index at the very end
    print("\n==================== BUILDING B-TREE INDEX ====================")
    print("Building composite index idx_ticker_timestamp...")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ticker_timestamp 
        ON fact_forex_candles (ticker, event_timestamp DESC);
    """)
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Bootstrapping complete! Your database is fully populated and indexed!")

if __name__ == "__main__":
    main()
