from confluent_kafka import Producer
import json
import requests
import time

# Kafka 配置
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "stock-data"

# Alpha Vantage API 配置（记得换成自己的 API Key）
ALPHA_VANTAGE_API_KEY = "TIV40O5G6D5FQQUV"
STOCK_SYMBOL = "AAPL"
API_URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_SYMBOL}&interval=1min&apikey={ALPHA_VANTAGE_API_KEY}"

# 创建 Kafka 生产者
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def fetch_stock_data():
    """ 获取实时股票数据 """
    response = requests.get(API_URL)
    data = response.json()
    
    if "Time Series (1min)" not in data:
        print("获取数据失败:", data)
        return None
    
    latest_time = max(data["Time Series (1min)"].keys())
    stock_info = data["Time Series (1min)"][latest_time]
    
    return {
        "symbol": STOCK_SYMBOL,
        "time": latest_time,
        "open": stock_info["1. open"],
        "high": stock_info["2. high"],
        "low": stock_info["3. low"],
        "close": stock_info["4. close"],
        "volume": stock_info["5. volume"]
    }

def delivery_report(err, msg):
    """ 检查 Kafka 消息是否成功发送 """
    if err is not None:
        print("消息发送失败:", err)
    else:
        print(f"消息发送到 {msg.topic()} [{msg.partition()}]")

# 发送数据到 Kafka
while True:
    stock_data = fetch_stock_data()
    if stock_data:
        producer.produce(KAFKA_TOPIC, key=STOCK_SYMBOL, value=json.dumps(stock_data), callback=delivery_report)
        producer.flush()
    
    time.sleep(60)  # 每 1 分钟获取一次数据
