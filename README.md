# Real-Time Indian Stock Market Data Engineering Pipeline

An end-to-end, production-grade data engineering pipeline that ingests real-time stock data from the National Stock Exchange of India (NSE), processes it using a stream-and-batch hybrid (Kappa/Lambda) architecture, and stores it in a Snowflake Data Warehouse for analytical querying.

---

## 🏗️ System Architecture
The pipeline implements a modern data lakehouse design:

```
[ yfinance API ]
       │  (Scrapes NSE live prices every 2s)
       ▼
[ Kafka Producer ]
       │  (Publishes to "stock-prices" topic)
       ▼
[ Apache Kafka ] ◄─── [ Spark Structured Streaming ]
                           │ (Windowed stream ingestion)
                           ▼
                      [ MinIO Data Lake ]
                       ├── bronze/ (Raw JSON stream)
                       ├── raw/ (Airflow Batch CSVs)
                       └── silver/ (Spark Fact & Dim Parquet tables)
                           │
                           ▼
                      [ Apache Airflow ]
                       ├── Triggers Spark Batch ETL (Deduplication + SCD Type 2)
                       └── Syncs Silver Parquet files to Snowflake DWH
                           │
                           ▼
                      [ Snowflake DWH ]
                       ├── FACT_STOCK_PRICES
                       └── DIM_STOCKS (Type 2 Slowly Changing Dimension)
```

---

## 🛠️ Tech Stack & Infrastructure
* **Orchestration**: Apache Airflow 2.7.2 (running on Python 3.10)
* **Stream Processing**: Apache Spark 3.5.2 (Structured Streaming & Batch SparkSQL)
* **Message Broker**: Apache Kafka (Confluent Platform 7.5.0)
* **Object Storage / Data Lake**: MinIO (S3-compatible local storage)
* **Data Warehouse**: Snowflake Cloud Data Warehouse
* **Database (Metastore)**: PostgreSQL 15 (Airflow metadata backend)
* **Language**: Python 3.10 (with `yfinance`, `pyspark`, `minio`, `snowflake-connector-python`, and `pyarrow`)
* **Deployment**: Docker & Docker Compose

---

## 📁 Repository Structure
```
Real_Time_Stock_Data_Project/
├── airflow/
│   └── dags/
│       └── stock_etl_dag.py        # Airflow DAG (Ingestion, Batch Trigger, Snowflake Load)
├── kafka/
│   └── producer.py                 # Live NSE stock scraper & Kafka publisher
├── spark/
│   ├── streaming_job.py            # Spark Structured Streaming (Kafka -> MinIO Bronze)
│   ├── batch_job.py                # Spark Batch ETL (Deduplication & SCD Type 2)
│   ├── hadoop-aws-3.3.4.jar        # Pre-loaded S3 filesystem JAR
│   └── aws-java-sdk-bundle-1.12.262.jar # Pre-loaded S3 AWS SDK JAR
├── snowflake/
│   ├── schema.sql                  # Snowflake table creation & staging setup
│   └── analytics.sql               # Advanced DWH analytics & temporal joins
├── docker-compose.yml              # Cluster services configuration
├── requirements.txt                # Workspace Python requirements
└── README.md                       # Project documentation
```

---

## 🚀 Setup & Execution Instructions

### Prerequisites
* Docker & Docker Compose installed on your system.
* A Snowflake account (with credentials and account ID).

### Step 1: Clone and Spin Up the Infrastructure
1. Clone this repository to your local machine.
2. Spin up the containerized cluster (Kafka, Spark, MinIO, Airflow, Postgres):
   ```bash
   docker compose up -d
   ```
3. Verify that all services are running:
   ```bash
   docker compose ps
   ```

### Step 2: Set up Snowflake Schema
Log into your Snowflake account and execute the DDL queries inside `snowflake/schema.sql` to initialize your database (`STOCK_DB`), create the file format, and set up the tables (`FACT_STOCK_PRICES` and `DIM_STOCKS`).

### Step 3: Configure Airflow Connections
1. Access the Airflow UI at `http://localhost:8080` (Credentials: `admin`/`admin`).
2. Go to **Admin** ➡️ **Connections** ➡️ **Add a new record** (`+` button).
3. Add a Snowflake connection with the ID **`snowflake_default`**:
   * **Connection Type**: `Snowflake`
   * **Account**: `<your_snowflake_account_id>` (e.g. `BEFDQWH-BW87134`)
   * **Login**: `<username>`
   * **Password**: `<password>`
   * **Database**: `STOCK_DB`
   * **Warehouse**: `COMPUTE_WH`
   * **Schema**: `PUBLIC`
4. Click **Save**.

### Step 4: Start the Live Ingestion
1. Create a local virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Run the Kafka producer:
   ```bash
   python kafka/producer.py
   ```
   *Note: If the Indian stock exchanges are closed (weekends/nights), the producer automatically switches to simulating live market feeds so you can test the pipeline anytime.*

### Step 5: Trigger the Airflow DAG
1. In the Airflow UI, locate the **`real_time_stock_etl`** DAG.
2. Unpause the DAG and click **Trigger DAG**.
3. Watch the Grid View as Airflow:
   * Scrapes historical CSV data from `yfinance`.
   * Contacts the Spark Master to run the batch jobs.
   * Connects to Snowflake and copies the data into the tables.

---

## 📊 Analytical DWH Queries in Snowflake

Once the data is synced, run these queries inside your Snowflake worksheet:

### 1. 10-Tick Simple Moving Average (SMA)
Computes a rolling 10-tick moving average of the stock prices to identify market trends:
```sql
SELECT 
    ticker,
    event_timestamp,
    price,
    AVG(price) OVER (
        PARTITION BY ticker 
        ORDER BY event_timestamp 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS sma_10_ticks
FROM FACT_STOCK_PRICES;
```

### 2. SCD Type 2 Temporal Join
Joins historical transaction prices with the dimension table to fetch company metadata corresponding to the exact time the trade occurred:
```sql
SELECT 
    f.ticker,
    d.company_name,
    d.sector,
    f.price,
    f.event_timestamp
FROM FACT_STOCK_PRICES f
JOIN DIM_STOCKS d 
  ON f.ticker = d.ticker
 AND f.date >= d.start_date
 AND (d.end_date IS NULL OR f.date <= d.end_date)
ORDER BY f.event_timestamp DESC;
```
