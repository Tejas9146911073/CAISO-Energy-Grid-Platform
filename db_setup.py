import os
import psycopg2
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

DB_HOST = os.getenv("RDS_POSTGRES_HOST")
DB_USER = os.getenv("RDS_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("RDS_POSTGRES_PASSWORD")
DB_NAME = "postgres"  # Default RDS database name

def setup_database():
    if not DB_HOST or not DB_PASS:
        print("Error: RDS_POSTGRES_HOST or RDS_POSTGRES_PASSWORD is missing in your .env file!")
        return

    print(f"Connecting to AWS RDS PostgreSQL at {DB_HOST}...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            port=5432
        )
        cursor = conn.cursor()
        
        # 1. Create Fact Table with Composite Primary Key (prevents duplicate time-series entries)
        print("Creating table: fact_forex_candles...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_forex_candles (
                ticker VARCHAR(20) NOT NULL,
                event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                open NUMERIC(15, 5) NOT NULL,
                high NUMERIC(15, 5) NOT NULL,
                low NUMERIC(15, 5) NOT NULL,
                close NUMERIC(15, 5) NOT NULL,
                volume BIGINT NOT NULL,
                date DATE NOT NULL,
                PRIMARY KEY (ticker, event_timestamp)
            );
        """)
        
        # 2. Create Dimension Table
        print("Creating table: dim_stocks...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_stocks (
                ticker VARCHAR(20) PRIMARY KEY,
                company_name VARCHAR(100),
                sector VARCHAR(50),
                industry VARCHAR(50),
                asset_category VARCHAR(20)
            );
        """)
        
        # 3. Populate Asset Dimension profiles
        print("Populating dim_stocks with Forex & Commodity profiles...")
        dimensions = [
            ("XAU_USD", "Gold Spot USD", "Commodities", "Precious Metals", "Commodity"),
            ("XAG_USD", "Silver Spot USD", "Commodities", "Precious Metals", "Commodity"),
            ("USD_JPY", "USD to JPY Forex", "Forex", "Currency Pair", "Fiat")
        ]
        for ticker, name, sec, ind, cat in dimensions:
            cursor.execute("""
                INSERT INTO dim_stocks (ticker, company_name, sector, industry, asset_category)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO NOTHING;
            """, (ticker, name, sec, ind, cat))
            
        conn.commit()
        cursor.close()
        conn.close()
        print("AWS RDS PostgreSQL Database schema created successfully!")
        
    except Exception as e:
        print(f"Failed to setup database: {e}")

if __name__ == "__main__":
    setup_database()
