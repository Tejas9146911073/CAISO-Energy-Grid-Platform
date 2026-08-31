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
        
        # 1. Create Node Dimension Profile Table
        print("Creating table: dim_caiso_nodes...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_caiso_nodes (
                node_id VARCHAR(30) PRIMARY KEY,
                node_name VARCHAR(100) NOT NULL,
                location VARCHAR(50) NOT NULL,
                voltage_level_kv NUMERIC(5, 1) NOT NULL
            );
        """)

        # Populate Node Dimension Profiles (NP15, SP15, ZP26 transmission hubs)
        print("Populating dim_caiso_nodes with California Grid hubs...")
        dimensions = [
            ('TH_NP15', 'North Path 15 Transmission Hub', 'Northern California', 500.0),
            ('TH_SP15', 'South Path 15 Transmission Hub', 'Southern California', 500.0),
            ('TH_ZP26', 'Zone Path 26 Transmission Hub', 'Central California', 500.0)
        ]
        for node_id, name, loc, volt in dimensions:
            cursor.execute("""
                INSERT INTO dim_caiso_nodes (node_id, node_name, location, voltage_level_kv)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (node_id) DO NOTHING;
            """, (node_id, name, loc, volt))

        # 2. Create Fact CAISO LMP Table (Wholesale Prices)
        print("Creating table: fact_caiso_lmp...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_caiso_lmp (
                node VARCHAR(30) NOT NULL REFERENCES dim_caiso_nodes(node_id),
                event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                lmp_type VARCHAR(10) NOT NULL, -- 'LMP' (total price), 'MCC' (congestion), 'MCL' (loss)
                price_per_mwh NUMERIC(10, 2) NOT NULL, -- Price in USD/MWh
                date DATE NOT NULL,
                PRIMARY KEY (node, event_timestamp, lmp_type)
            );
        """)

        # 3. Create Fact CAISO Load Table (Grid Demand/Load)
        print("Creating table: fact_caiso_load...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_caiso_load (
                event_timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
                actual_load_mw INTEGER NOT NULL, -- Live Load demand in Megawatts
                forecast_load_mw INTEGER NOT NULL, -- Forecast demand in Megawatts
                date DATE NOT NULL
            );
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("AWS RDS PostgreSQL Database schema initialized successfully!")
        
    except Exception as e:
        print(f"Failed to setup database: {e}")

if __name__ == "__main__":
    setup_database()
