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
-- Query 2: Retrieve facts enriched dynamically with historical SCD Type 2 dimension values
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
    
