import yfinance as yf
from pymongo import MongoClient
from datetime import datetime

# List of stock tickers and time intervals to retrieve historical data
tickers = ['XOM', 'CL=F']
intervals = ['1h', '1d', '1wk', '1mo', '3mo']

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['stock_data']

# Fetch and store historical data in respective interval collections
def fetch_and_store_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        for interval in intervals:
            # Fetch historical data for the given interval (maximum available data)
            if interval == '1h':
                data = stock.history(period='730d', interval=interval)  # Adjust period to be within last 730 days
            else:
                data = stock.history(period='max', interval=interval)
            
            collection_name = f'stock_data_{ticker}_{interval.replace(" ", "_")}'
            collection = db[collection_name]
            for index, row in data.iterrows():
                timestamp = index.strftime('%Y-%m-%d %H:%M:%S')
                record = {
                    "timestamp": timestamp,
                    "open": round(row['Open'], 3),
                    "high": round(row['High'], 3),
                    "low": round(row['Low'], 3),
                    "close": round(row['Close'], 3),
                    "volume": row['Volume']
                }
                # Insert data into MongoDB, avoiding duplicates
                collection.update_one({"timestamp": timestamp}, {"$set": record}, upsert=True)
    except Exception as e:
        print(f"An error occurred while fetching or storing data for {ticker}: {e}")

# Execution
if __name__ == '__main__':
    for ticker in tickers:
        fetch_and_store_data(ticker)
