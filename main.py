import yfinance as yf
from pymongo import MongoClient, errors
from datetime import datetime
import pandas as pd

# List of stock tickers and time intervals to retrieve historical data
tickers = ['XOM', 'CL=F']
intervals = ['1h', '1d', '1wk', '1mo', '3mo']

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['stock_data']

# Fetch and store historical data in respective interval collections
def fetch_and_store_data():
    combined_data = {interval: [] for interval in intervals}

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            for interval in intervals:
                if interval == '1h':
                    data = stock.history(period='730d', interval=interval)
                else:
                    data = stock.history(period='max', interval=interval)
                
                collection_name = f'stock_data_{ticker}_{interval.replace(" ", "_")}'
                collection = db[collection_name]
                for index, row in data.iterrows():
                    timestamp = index.strftime('%Y-%m-%d %H:%M:%S')
                    record = {
                        "timestamp": timestamp,
                        "ticker": ticker,
                        "interval": interval,
                        "open": round(row['Open'], 3),
                        "high": round(row['High'], 3),
                        "low": round(row['Low'], 3),
                        "close": round(row['Close'], 3),
                        "volume": row['Volume']
                    }
                    try:
                        collection.update_one({"timestamp": timestamp}, {"$set": record}, upsert=True)
                    except errors.OperationFailure as e:
                        print(f"Error updating collection {collection_name}: {e}")
                        collection = db.create_collection(collection_name)
                        collection.update_one({"timestamp": timestamp}, {"$set": record}, upsert=True)
                    combined_data[interval].append(record)
        except Exception as e:
            print(f"An error occurred while fetching or storing data for {ticker}: {e}")

    # Store combined data for each interval
    for interval, records in combined_data.items():
        combined_collection_name = f'combined_data_{interval.replace(" ", "_")}'
        combined_collection = db[combined_collection_name]
        for record in records:
            try:
                combined_collection.update_one(
                    {"timestamp": record["timestamp"], "ticker": record["ticker"]},
                    {"$set": record},
                    upsert=True
                )
            except errors.OperationFailure as e:
                print(f"Error updating combined collection {combined_collection_name}: {e}")
                combined_collection = db.create_collection(combined_collection_name)
                combined_collection.update_one(
                    {"timestamp": record["timestamp"], "ticker": record["ticker"]},
                    {"$set": record},
                    upsert=True
                )

# Execution
if __name__ == '__main__':
    fetch_and_store_data()
