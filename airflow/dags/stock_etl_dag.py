import os
import time
import requests
import tempfile
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def ingest_historical_csv(**context):
    """
    Batch ingestion: pulls ticker prices from Yahoo Finance and dumps CSV to MinIO RAW zone.
    """
    run_date = context['ds']
    # 1. UPDATED TICKERS
    tickers = ['^NSEI', 'RELIANCE.NS', 'HDFCBANK.NS', 'TATAMOTORS.NS', 'SUNPHARMA.NS']
    
    print(f"Running batch Yahoo Finance ingestion for run_date: {run_date}...")
    import yfinance as yf
    from minio import Minio
    
    # Initialize MinIO
    mc = Minio("minio:9000", access_key="admin", secret_key="password123", secure=False)
    
    all_data = []
    for ticker_sym in tickers:
        ticker = yf.Ticker(ticker_sym)
        # Pull 1 day historical data
        hist = ticker.history(start=run_date, end=(datetime.strptime(run_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"))
        for index, row in hist.iterrows():
            all_data.append({
                'ticker': ticker_sym,
                'price': float(row['Close']),
                'volume': int(row['Volume']),
                'timestamp': index.isoformat()
            })
            
    if not all_data:
        # 2. UPDATED FALLBACK VALUES FOR INDIAN STOCKS (exchanges closed on weekends/holidays)
        mock_prices = {
            '^NSEI': 22500.0, 
            'RELIANCE.NS': 2950.0, 
            'HDFCBANK.NS': 1450.0, 
            'TATAMOTORS.NS': 950.0, 
            'SUNPHARMA.NS': 1550.0
        }
        for t in tickers:
            all_data.append({
                'ticker': t,
                'price': mock_prices[t],
                'volume': 10000,
                'timestamp': f"{run_date}T12:00:00Z"
            })
            
    df = pd.DataFrame(all_data)
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
        df.to_csv(tmp_file.name, index=False)
        mc.fput_object(
            "stock-bucket", 
            f"raw/stock_prices/date={run_date}.csv", 
            tmp_file.name, 
            content_type="application/csv"
        )
    print("CSV ingestion completed.")

def submit_spark_batch_etl(**context):
    run_date = context['ds']
    url = "http://spark-master:6066/v1/submissions/create"
    
    payload = {
        "action": "CreateSubmissionRequest",
        "clientSparkVersion": "3.5.0",
        # UPDATE THIS LINE:
        "appArgs": ["/opt/spark-apps/batch_job.py", "", "--run-date", run_date], 
        "appResource": "file:///opt/spark-apps/batch_job.py",
        "environmentVariables": {"SPARK_ENV_LOADED": "1"},
        "mainClass": "org.apache.spark.deploy.PythonRunner",
        "sparkProperties": {
            "spark.driver.supervise": "false",
            "spark.app.name": "StockMarketBatchETL",
            "spark.submit.deployMode": "cluster",
            "spark.master": "spark://spark-master:7077",
            "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp/ivy2/cache -Divy.home=/tmp/ivy2",
            "spark.executor.extraJavaOptions": "-Divy.cache.dir=/tmp/ivy2/cache -Divy.home=/tmp/ivy2",
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4"
        }
    }
    
    resp = requests.post(url, json=payload)
    resp.raise_for_status()
    submission_id = resp.json().get("submissionId")
    print(f"Job submitted. Tracking ID: {submission_id}")
    
    # Poll status
    status_url = f"http://spark-master:6066/v1/submissions/status/{submission_id}"
    start_time = time.time()
    while time.time() - start_time < 300:
        status_resp = requests.get(status_url).json()
        state = status_resp.get("driverState")
        print(f"Driver State: {state}")
        if state == "FINISHED":
            return True
        elif state in ("FAILED", "ERROR", "KILLED"):
            raise AirflowException(f"Spark job failed: {state}")
        time.sleep(10)
    raise AirflowException("Job timeout reached.")

def sync_data_to_snowflake(**context):
    run_date = context['ds']
    
    from airflow.hooks.base import BaseHook
    try:
        # Load credentials directly from the Airflow Connection UI
        connection = BaseHook.get_connection("snowflake_default")
        sf_account = connection.extra_dejson.get("account") or connection.host
        sf_user = connection.login
        sf_password = connection.password
        sf_database = connection.extra_dejson.get("database") or "STOCK_DB"
        sf_schema = connection.schema or "PUBLIC"
        sf_warehouse = connection.extra_dejson.get("warehouse") or "COMPUTE_WH"
        sf_role = connection.extra_dejson.get("role")
        
        if not (sf_account and sf_user and sf_password):
            raise ValueError("Credentials are empty in the connection settings.")
            
    except Exception as e:
        print("=" * 70)
        print(f"Skipping Snowflake upload: Connection 'snowflake_default' is missing or incomplete ({e}).")
        print("To load data, configure 'snowflake_default' in Admin -> Connections in Airflow.")
        print("=" * 70)
        return
        
    from minio import Minio
    import snowflake.connector
    
    mc = Minio("minio:9000", access_key="admin", secret_key="password123", secure=False)
    
    # 1. Sync Fact Table
    print(f"Starting sync of Fact Table data for date {run_date}...")
    fact_prefix = f"silver/fact_stock_prices/date={run_date}/"
    objects = mc.list_objects("stock-bucket", prefix=fact_prefix, recursive=True)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        fact_files = []
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                fname = os.path.basename(obj.object_name)
                local_path = os.path.join(tmp_dir, fname)
                mc.fget_object("stock-bucket", obj.object_name, local_path)
                fact_files.append(local_path)
                
        if fact_files:
            conn = snowflake.connector.connect(
                account=sf_account, user=sf_user, password=sf_password,
                database=sf_database, schema=sf_schema,
                warehouse=sf_warehouse, role=sf_role
            )
            cursor = conn.cursor()
            try:
                cursor.execute(f"DELETE FROM FACT_STOCK_PRICES WHERE date = '{run_date}';")
                for f in fact_files:
                    cursor.execute(f"PUT file://{f} @%FACT_STOCK_PRICES AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")
                cursor.execute("""
                    COPY INTO FACT_STOCK_PRICES (TICKER, PRICE, VOLUME, EVENT_TIMESTAMP, DATE)
                     FROM (
                       SELECT 
                         $1:ticker::VARCHAR, 
                         $1:price::NUMBER(38,4), 
                         $1:volume::BIGINT, 
                         $1:event_timestamp::TIMESTAMP_NTZ, 
                         $1:date::DATE
                       FROM @%FACT_STOCK_PRICES
                     )
                     FILE_FORMAT = (TYPE = PARQUET)
                     PURGE = TRUE;
                    """)
                print("Fact table loaded successfully.")
            finally:
                cursor.close()
                conn.close()
                
    # 2. Sync Dim Table (SCD Type 2)
    dim_prefix = "silver/dim_stocks/"
    dim_objects = mc.list_objects("stock-bucket", prefix=dim_prefix, recursive=True)
    with tempfile.TemporaryDirectory() as tmp_dir:
        dim_files = []
        for obj in dim_objects:
            if obj.object_name.endswith('.parquet'):
                fname = os.path.basename(obj.object_name)
                local_path = os.path.join(tmp_dir, fname)
                mc.fget_object("stock-bucket", obj.object_name, local_path)
                dim_files.append(local_path)
                
        if dim_files:
            conn = snowflake.connector.connect(
                account=sf_account, user=sf_user, password=sf_password,
                database=sf_database, schema=sf_schema,
                warehouse=sf_warehouse, role=sf_role
            )
            cursor = conn.cursor()
            try:
                cursor.execute("TRUNCATE TABLE DIM_STOCKS;")
                for f in dim_files:
                    cursor.execute(f"PUT file://{f} @%DIM_STOCKS AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")
                cursor.execute("""
                    COPY INTO DIM_STOCKS (TICKER, COMPANY_NAME, SECTOR, INDUSTRY, MARKET_CAP_CATEGORY, START_DATE, END_DATE, IS_CURRENT)
                     FROM (
                       SELECT 
                         $1:ticker::VARCHAR, 
                         $1:company_name::VARCHAR, 
                         $1:sector::VARCHAR, 
                         $1:industry::VARCHAR, 
                         $1:market_cap_category::VARCHAR, 
                         $1:start_date::DATE, 
                         $1:end_date::DATE, 
                         $1:is_current::BOOLEAN
                       FROM @%DIM_STOCKS
                     )
                     FILE_FORMAT = (TYPE = PARQUET)
                     PURGE = TRUE;
                 """)
                print("Dimension table loaded successfully.")
            finally:
                cursor.close()
                conn.close()

with DAG(
    dag_id='real_time_stock_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    batch_csv_ingest = PythonOperator(
        task_id='ingest_yahoo_finance_csv',
        python_callable=ingest_historical_csv,
    )

    run_spark_batch = PythonOperator(
        task_id='trigger_spark_batch_etl',
        python_callable=submit_spark_batch_etl,
    )

    sync_snowflake = PythonOperator(
        task_id='sync_to_snowflake',
        python_callable=sync_data_to_snowflake,
    )

    batch_csv_ingest >> run_spark_batch >> sync_snowflake
    
