from dotenv import dotenv_values
from flask import Flask, render_template, request, jsonify
import plotly
import plotly.express as px
import pandas as pd
import json
from pymongo import MongoClient

app = Flask(__name__)

env = dotenv_values("../.env")

print(env)

# Connect to MongoDB
client = MongoClient(env['MONGODB_HOST'], env['MONGODB_PORT'])
db = client['stock_data']

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data', methods=['POST'])
def get_data():
    tickers = request.form.getlist('ticker')
    interval = request.form['interval']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    
    combined_data = pd.DataFrame()
    
    for ticker in tickers:
        # Fetch data from MongoDB
        collection_name = f'stock_data_{ticker}_{interval.replace(" ", "_")}'
        collection = db[collection_name]
        data = list(collection.find({
            "timestamp": {"$gte": start_date, "$lte": end_date}
        }))
        
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['ticker'] = ticker
        combined_data = pd.concat([combined_data, df])
    
    fig = px.line(combined_data, x='timestamp', y='close', color='ticker', title='Stock Prices')
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    return graphJSON

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
