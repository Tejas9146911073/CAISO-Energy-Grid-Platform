import os
import logging
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load Credentials
load_dotenv()
DB_HOST = os.getenv("RDS_POSTGRES_HOST")
DB_USER = os.getenv("RDS_POSTGRES_USER", "postgres")
DB_PASS = os.getenv("RDS_POSTGRES_PASSWORD")
DB_NAME = "postgres"

app = FastAPI(
    title="CAISO Real-Time Energy Grid Data Services API",
    description="REST API delivering 5-minute real-time Locational Marginal Prices (LMPs) and System Load from CAISO.",
    version="1.0.0"
)

# Enable CORS for React Frontend (allows Vercel & local development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=5432
    )

@app.get("/")
def root():
    return {
        "status": "online",
        "service": "CAISO Real-Time Energy Grid Data Platform",
        "docs": "/docs"
    }

# ========================================================
# 1. PIPELINE HEALTH & STATUS ENDPOINT
# ========================================================
@app.get("/api/status")
def get_pipeline_status():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Get total counts and latest timestamp from fact_caiso_lmp
        cursor.execute("""
            SELECT 
                COUNT(*) as total_lmp_records,
                MAX(event_timestamp) as latest_lmp_time
            FROM fact_caiso_lmp;
        """)
        lmp_stat = cursor.fetchone()
        
        # Get total counts, latest timestamp, and latest non-zero demand from fact_caiso_load
        cursor.execute("""
            SELECT 
                COUNT(*) as total_load_records,
                MAX(event_timestamp) as latest_load_time,
                COALESCE(
                    (SELECT actual_load_mw FROM fact_caiso_load WHERE actual_load_mw > 0 ORDER BY event_timestamp DESC LIMIT 1),
                    (SELECT forecast_load_mw FROM fact_caiso_load WHERE forecast_load_mw > 0 ORDER BY event_timestamp DESC LIMIT 1),
                    0
                ) as current_demand_mw
            FROM fact_caiso_load;
        """)
        load_stat = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "pipeline_status": "HEALTHY",
            "ingestion_stream": "Aiven Kafka (SASL_SSL)",
            "database": "AWS RDS PostgreSQL (Time-Series B-Tree Indexed)",
            "caiso_pricing": {
                "total_records": lmp_stat["total_lmp_records"] if lmp_stat else 0,
                "latest_timestamp": str(lmp_stat["latest_lmp_time"]) if lmp_stat and lmp_stat["latest_lmp_time"] else None
            },
            "caiso_grid_load": {
                "total_records": load_stat["total_load_records"] if load_stat else 0,
                "latest_timestamp": str(load_stat["latest_load_time"]) if load_stat and load_stat["latest_load_time"] else None,
                "current_demand_mw": load_stat["current_demand_mw"] if load_stat else 0
            }
        }
    except Exception as e:
        logger.error(f"Error fetching status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========================================================
# 2. CAISO LMP PRICING ENDPOINT (FOR TIME-SERIES CHARTS)
# ========================================================
@app.get("/api/prices")
def get_caiso_prices(
    node: str = Query("TH_SP15", description="Node identifier: TH_NP15, TH_SP15, or TH_ZP26"),
    lmp_type: str = Query("LMP", description="Price component: LMP (Total), MCC (Congestion), or MCL (Loss)"),
    limit: int = Query(288, description="Number of 5-minute intervals to return (288 = 24 hours)")
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("""
            SELECT 
                node,
                event_timestamp,
                lmp_type,
                price_per_mwh,
                date
            FROM fact_caiso_lmp
            WHERE node = %s AND lmp_type = %s
            ORDER BY event_timestamp DESC
            LIMIT %s;
        """, (node, lmp_type, limit))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        data = [{
            "node": row["node"],
            "timestamp": row["event_timestamp"].isoformat(),
            "price_per_mwh": float(row["price_per_mwh"]),
            "lmp_type": row["lmp_type"]
        } for row in reversed(rows)]
        
        return {
            "node": node,
            "lmp_type": lmp_type,
            "count": len(data),
            "data": data
        }
    except Exception as e:
        logger.error(f"Error fetching prices: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========================================================
# 3. CAISO GRID LOAD (DEMAND VS FORECAST) ENDPOINT
# ========================================================
@app.get("/api/load")
def get_caiso_load(
    limit: int = Query(288, description="Number of intervals (288 = 24 hours)")
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("""
            SELECT 
                event_timestamp,
                actual_load_mw,
                forecast_load_mw,
                date
            FROM fact_caiso_load
            ORDER BY event_timestamp DESC
            LIMIT %s;
        """, (limit,))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        data = [{
            "timestamp": row["event_timestamp"].isoformat(),
            "actual_load_mw": row["actual_load_mw"],
            "forecast_load_mw": row["forecast_load_mw"]
        } for row in reversed(rows)]
        
        return {
            "count": len(data),
            "data": data
        }
    except Exception as e:
        logger.error(f"Error fetching load: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========================================================
# 4. GRID TRANSMISSION NODES METADATA
# ========================================================
@app.get("/api/nodes")
def get_caiso_nodes():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("SELECT node_id, node_name, location, voltage_level_kv FROM dim_caiso_nodes;")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return [{
            "node_id": r["node_id"],
            "node_name": r["node_name"],
            "location": r["location"],
            "voltage_level_kv": float(r["voltage_level_kv"])
        } for r in rows]
    except Exception as e:
        logger.error(f"Error fetching nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))
