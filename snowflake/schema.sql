CREATE DATABASE IF NOT EXISTS STOCK_DB;
USE DATABASE STOCK_DB;
USE SCHEMA PUBLIC;
-- Transactional Fact Table
CREATE TABLE IF NOT EXISTS FACT_STOCK_PRICES (
    ticker VARCHAR(10) NOT NULL,
    price NUMBER(38, 4) NOT NULL,
    volume BIGINT NOT NULL,
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    date DATE NOT NULL
);
-- Dimension Table tracking updates over time (SCD Type 2)
CREATE TABLE IF NOT EXISTS DIM_STOCKS (
    ticker VARCHAR(10) NOT NULL,
    company_name VARCHAR(100),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap_category VARCHAR(50),
    start_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);
    
