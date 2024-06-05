import yfinance as yf
from pymongo import MongoClient
from dotenv import dotenv_values

# List of stock tickers and time intervals to retrieve historical data
tickers = ['XOM', 'CL=F']
intervals = ['1h', '1d', '1wk', '1mo', '3mo']

env = dotenv_values(".env")

# Connect to MongoDB
client = MongoClient(env['MONGODB_HOST'], env['MONGODB_PORT'])
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


'''
# envoi des collections vers S3 https://awsacademy.instructure.com/courses/74365/modules/items/6677565
def send_to_s3():
    directory = "json_files"

    if not os.path.exists(directory):
        os.makedirs(directory)

    for c in db.list_collection_names():
        # print(c)
        cursor = db[c].find({})

        # save under project folder
        # with open(f'{directory}/{c}.json', 'w') as file:
            # json.dump(json.loads(dumps(cursor)), file)

        # save in s3
        s3 = boto3.resource(
            's3',
            region_name=env["REGION_NAME"],
            aws_access_key_id=env['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=env['AWS_SECRET_ACCESS_KEY'],
            aws_session_token=env['AWS_SESSION_TOKEN'],
        )

        content = bson.json_util.dumps(cursor).encode('UTF-8')
        # print(content)
        s3.Object(env['BUCKETNAME'], f'stock_data_{directory}/{c}.json').put(Body=content)
'''

# Execution
if __name__ == '__main__':
    for ticker in tickers:
        fetch_and_store_data(ticker)
