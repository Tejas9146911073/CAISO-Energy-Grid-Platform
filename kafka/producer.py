import time
import json
import random
from datetime import datetime
import yfinance as yf
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'stock_prices'
TICKERS = ['^NSEI', 'RELIANCE.NS', 'HDFCBANK.NS', 'TATAMOTORS.NS', 'SUNPHARMA.NS']
POLL_INTERVAL = 5 # seconds

def get_stock_data(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.basic_info
        price = info.get('last_price') or info.get('previous_close')
        volume = info.get('last_volume') or info.get('volume')
        
        if price is None or price <= 0:
            raise ValueError("No price retrieved")
            
        return {
            'ticker': ticker_symbol,
            'price': float(price),
            'volume': int(volume),
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        mock_prices = {
            '^NSEI': 22500.0, 'RELIANCE.NS': 2950.0, 'HDFCBANK.NS': 1450.0, 
            'TATAMOTORS.NS': 950.0, 'SUNPHARMA.NS': 1550.0
        }
        base_price = mock_prices.get(ticker_symbol, 100.0)
        price_change = base_price * random.uniform(-0.015, 0.015)
        mock_price = round(base_price + price_change, 2)
        mock_volume = random.randint(1000, 25000)
        
        return {
            'ticker': ticker_symbol,
            'price': float(mock_price),
            'volume': int(mock_volume),
            'timestamp': datetime.utcnow().isoformat(),
            'is_mock': True
        }

def main():
    print(f"Connecting to broker: {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer initialized successfully.")
    except Exception as e:
        print(f"CRITICAL: Failed to initialize producer: {e}")
        return

    print("Starting real-time streaming loop. Press Ctrl+C to stop.")
    try:
        while True:
            for ticker_symbol in TICKERS:
                data = get_stock_data(ticker_symbol)
                print(f"Publishing: {data}")
                producer.send(TOPIC_NAME, value=data)
            producer.flush()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping Producer...")
    finally:
        producer.close()

if __name__ == '__main__':
    main()
