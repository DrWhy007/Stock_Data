from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

import yfinance as yf
from dotenv import dotenv_values
from pymongo import MongoClient
from datetime import datetime, timedelta


args = {
 
    'owner': 'my_team',
    'start_date': days_ago(1)
}

dag = DAG(dag_id = 'collect_clf_data', default_args=args, schedule_interval=timedelta(seconds=3600))

# List of stock tickers and time intervals to retrieve historical data
ticker = 'CL=F'
intervals = ['1h', '1d', '1wk', '1mo', '3mo']

env = dotenv_values("../.env")

# Connect to MongoDB
client = MongoClient(env['MONGODB_HOST'], env['MONGODB_PORT'])
db = client['stock_data']


def fetch_and_store_data():
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


with dag:

    collecte_task = PythonOperator(
        task_id='collecte_task',
        python_callable = fetch_and_store_data
    )


    collecte_task