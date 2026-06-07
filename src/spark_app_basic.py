import time
import psutil
import os
import json
import sys
from pyspark.sql import SparkSession

# Считываем имя эксперимента из аргументов
EXP_NAME = sys.argv[1] if len(sys.argv) > 1 else "exp_1dn_base"
is_3dn = "3dn" in EXP_NAME

# Отслеживание пиковой RAM
process = psutil.Process()
peak_memory_mb = 0.0

def get_memory_mb():
    ram = process.memory_info().rss / 1024 / 1024
    global peak_memory_mb
    if ram > peak_memory_mb:
        peak_memory_mb = ram
    return ram

start_ram = get_memory_mb()
start_time = time.time()

spark = SparkSession.builder \
    .appName(f"Lab2_{EXP_NAME}") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Чтение данных из HDFS с inferSchema=True...")
df = spark.read.csv("hdfs://localhost:9000/lab2/data_2lab.csv", header=True, inferSchema=True)

row_count = df.count()
print(f"Загружено строк: {row_count}")
df.printSchema()
get_memory_mb()

print("Выполняем groupBy и avg(salary)...")
result = df.groupBy("department").avg("salary")
result.show()
get_memory_mb()

end_time = time.time()
execution_time = end_time - start_time
peak_ram = peak_memory_mb

print(f"\nЭксперимент {EXP_NAME} завершён:")
print(f"Время выполнения: {execution_time:.2f} сек")
print(f"Пиковая RAM (драйвер): {peak_ram:.1f} МБ")

# === СОХРАНЕНИЕ В JSON ===
results = {
    "Experiment": EXP_NAME,
    "Configuration": "3-optimized=false" if is_3dn else "1-optimized=false",
    "DataNodes": 3 if is_3dn else 1,
    "Optimized": False,
    "Total time (s)": round(execution_time, 3),
    "Memory (MB)": round(peak_ram, 3),
    "Rows processed": row_count,
    "Aggregation result": {row["department"]: round(row["avg(salary)"], 2) for row in result.collect()}
}

# Сохраняем строго под универсальным именем в корень проекта
with open("results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)

print(f"\n Результаты сохранены в JSON: results.json")

spark.stop()
