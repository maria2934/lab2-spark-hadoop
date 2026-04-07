
import random
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession

# Инициализация Spark
spark = SparkSession.builder \
    .appName("GenerateCryptoDataset") \
    .getOrCreate()

# Фиксируем seed
random.seed(42)

# Категории
crypto_types = ["Bitcoin", "Ethereum", "Solana", "Cardano", "Polkadot"]
regions = ["North America", "Europe", "Asia", "South America", "Africa", "Oceania"]

# Информация по биржам
exchanges_info = {
    "Binance": {"founded_year": 2017, "hq": "Cayman Islands", "regulated": True},
    "Coinbase": {"founded_year": 2012, "hq": "USA", "regulated": True},
    "Kraken": {"founded_year": 2011, "hq": "USA", "regulated": True},
    "FTX": {"founded_year": 2019, "hq": "Bahamas", "regulated": False},
    "KuCoin": {"founded_year": 2017, "hq": "Seychelles", "regulated": False},
    "Bybit": {"founded_year": 2018, "hq": "Dubai", "regulated": False}
}
exchanges = list(exchanges_info.keys())

# Временные метки за 2025 год
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 12, 31)
total_minutes = int((end_date - start_date).total_seconds() // 60)
timestamps = [start_date + timedelta(minutes=random.randint(0, total_minutes)) for _ in range(100000)]

# Генерация 100000 строк
data = []
for i in range(1, 100001):
    base_price = {
        "Bitcoin": 30000 + random.uniform(-10000, 50000),
        "Ethereum": 1800 + random.uniform(-500, 3000),
        "Solana": 20 + random.uniform(-10, 100),
        "Cardano": 0.3 + random.uniform(-0.1, 0.5),
        "Polkadot": 5 + random.uniform(-2, 10)
    }
    coin = random.choice(crypto_types)
    price = round(base_price[coin], 2)
    exchange = random.choice(exchanges)
    exchange_data = exchanges_info[exchange]

    row = {
        "record_id": i,
        "timestamp": timestamps[i - 1],
        "crypto_type": coin,
        "exchange": exchange,
        "region": random.choice(regions),
        "price_usd": price,
        "volume_usd": round(price * random.uniform(10, 10000), 2),
        "hash_rate_ths": round(random.uniform(50.0, 300.0), 1),
        "blockchain_fees_usd": round(random.uniform(1.0, 50.0), 2),
        "is_anomaly": random.choice([True, False]),
        "energy_consumption_kw": round(random.uniform(100.0, 2000.0), 1),
        "founded_year": exchange_data["founded_year"],
        "hq": exchange_data["hq"],
        "regulated": exchange_data["regulated"]
    }
    data.append(row)

# Создание DataFrame и сохранение
df = spark.createDataFrame(data)
df.toPandas().to_csv("crypto_dataset.csv", index=False)

print("Dataset with 100000 rows (2025) saved as crypto_dataset.csv")

spark.stop()
