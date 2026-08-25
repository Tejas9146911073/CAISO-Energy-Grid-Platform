import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, BooleanType

def build_spark_session():
    return SparkSession.builder \
        .appName("StockMarketBatchETL") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def process_fact_table(spark, run_date, bronze_path, raw_csv_path, silver_fact_path):
    print(f"Processing fact table for run_date: {run_date}...")
    
    # Read Bronze
    bronze_partition = f"{bronze_path}/date={run_date}"
    try:
        bronze_df = spark.read.parquet(bronze_partition)
    except Exception as e:
        print(f"Info: No bronze partition found or failed to read: {e}")
        bronze_df = None
    # Read Raw CSV
    csv_partition = f"{raw_csv_path}/date={run_date}.csv"
    try:
        csv_df = spark.read.option("header", "true").csv(csv_partition)
    except Exception as e:
        print(f"Info: No CSV file found or failed to read: {e}")
        csv_df = None

    records = []
    if bronze_df:
        records.append(bronze_df.select("ticker", "price", "volume", "timestamp"))
    if csv_df:
        records.append(csv_df.select(
            col("ticker"), 
            col("price").cast(DoubleType()), 
            col("volume").cast(LongType()), 
            col("timestamp")
        ))

    unified_df = records[0]
    for df in records[1:]:
        unified_df = unified_df.union(df)

    deduped_df = unified_df.dropDuplicates(["ticker", "timestamp"])

    fact_df = deduped_df.select(
        col("ticker").cast(StringType()),
        col("price").cast(DoubleType()),
        col("volume").cast(LongType()),
        col("timestamp").alias("event_timestamp"),
        lit(run_date).cast(DateType()).alias("date")
    )

    fact_df.write.mode("overwrite").parquet(f"{silver_fact_path}/date={run_date}")
    return True

def process_dim_table_scd2(spark, run_date, silver_dim_path):
    print("Running Slowly Changing Dimension (SCD Type 2) logic...")
    
    incoming_updates = [
        {"ticker": "^NSEI", "company_name": "Nifty 50 Index", "sector": "Indices", "industry": "Market Index", "market_cap_category": "Index"},
        {"ticker": "RELIANCE.NS", "company_name": "Reliance Industries Limited", "sector": "Energy/Conglomerate", "industry": "Oil & Gas / Retail", "market_cap_category": "Large-Cap"},
        {"ticker": "HDFCBANK.NS", "company_name": "HDFC Bank Limited", "sector": "Financial Services", "industry": "Private Bank", "market_cap_category": "Large-Cap"},
        {"ticker": "TATAMOTORS.NS", "company_name": "Tata Motors Limited", "sector": "Automotive", "industry": "Auto Manufacturers", "market_cap_category": "Mid-Cap"},
        {"ticker": "SUNPHARMA.NS", "company_name": "Sun Pharmaceutical Industries Limited", "sector": "Healthcare", "industry": "Pharmaceuticals", "market_cap_category": "Mid-Cap"}
    ]
    
    meta_schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("company_name", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("market_cap_category", StringType(), True)
    ])
    updates_df = spark.createDataFrame(incoming_updates, schema=meta_schema)

    try:
        existing_dim = spark.read.parquet(silver_dim_path)
    except Exception:
        empty_schema = StructType(meta_schema.fields + [
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True),
            StructField("is_current", BooleanType(), True)
        ])
        existing_dim = spark.createDataFrame([], schema=empty_schema)

    closed_records = existing_dim.filter(col("is_current") == False)
    active_records = existing_dim.filter(col("is_current") == True)

    joined_df = active_records.join(updates_df.alias("upd"), on="ticker", how="outer")

    is_modified = (
        (active_records["ticker"].isNotNull()) & (col("upd.ticker").isNotNull()) & 
        (
            (active_records["sector"] != col("upd.sector")) | 
            (active_records["market_cap_category"] != col("upd.market_cap_category"))
        )
    )
    is_new = (active_records["ticker"].isNull()) & (col("upd.ticker").isNotNull())

    expired_records = joined_df.filter(is_modified).select(
        col("ticker"),
        active_records["company_name"],
        active_records["sector"],
        active_records["industry"],
        active_records["market_cap_category"],
        active_records["start_date"],
        lit(run_date).cast(DateType()).alias("end_date"),
        lit(False).alias("is_current")
    )

    unchanged_records = joined_df.filter(
        (active_records["ticker"].isNotNull()) & (col("upd.ticker").isNotNull()) & (~is_modified)
    ).select(
        col("ticker"),
        active_records["company_name"],
        active_records["sector"],
        active_records["industry"],
        active_records["market_cap_category"],
        active_records["start_date"],
        active_records["end_date"],
        active_records["is_current"]
    )

    new_records = joined_df.filter(is_new | is_modified).select(
        col("upd.ticker").alias("ticker"),
        col("upd.company_name").alias("company_name"),
        col("upd.sector").alias("sector"),
        col("upd.industry").alias("industry"),
        col("upd.market_cap_category").alias("market_cap_category"),
        lit(run_date).cast(DateType()).alias("start_date"),
        lit(None).cast(DateType()).alias("end_date"),
        lit(True).alias("is_current")
    )

    final_dim = closed_records.union(expired_records).union(unchanged_records).union(new_records)
    final_dim.write.mode("overwrite").parquet(silver_dim_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", type=str, required=True)
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = "s3a://stock-bucket/bronze/stock_prices"
    raw_csv_path = "s3a://stock-bucket/raw/stock_prices"
    silver_fact_path = "s3a://stock-bucket/silver/fact_stock_prices"
    silver_dim_path = "s3a://stock-bucket/silver/dim_stocks"

    success = process_fact_table(spark, args.run_date, bronze_path, raw_csv_path, silver_fact_path)
    if success:
        process_dim_table_scd2(spark, args.run_date, silver_dim_path)

if __name__ == "__main__":
    main()
