import json
import requests
from kafka import KafkaProducer
import time

API_KEY = "TIV40O5G6D5FQQUV"
STOCK_SYMBOL = "AAPL"
KAFKA_BROKER = "b-1.your-msk-broker-url:9092,b-2.your-msk-broker-url:9092"
KAFKA_TOPIC = "stock_data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_stock_data():
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=1min&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    if "Time Series (1min)" in data:
        for timestamp, values in data["Time Series (1min)"].items():
            stock_data = {
                "symbol": STOCK_SYMBOL,
                "timestamp": timestamp,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            }
            producer.send(KAFKA_TOPIC, stock_data)
            print(f"Sent: {stock_data}")

while True:
    fetch_stock_data()
    time.sleep(60)  # 每分钟获取一次数据
