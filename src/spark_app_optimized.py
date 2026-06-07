import time
import psutil
import os
import json
import sys
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Считываем имя эксперимента из аргументов
EXP_NAME = sys.argv[1] if len(sys.argv) > 1 else "exp_1dn_optimized"
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
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Явная схема
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("age", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("experience_years", StringType(), True),
    StructField("department", StringType(), True),
    StructField("is_active", StringType(), True),
    StructField("join_date", StringType(), True),
    StructField("rating", StringType(), True)
])

print("Чтение данных из HDFS с явной схемой...")
df = spark.read.csv("hdfs://localhost:9000/lab2/data_2lab.csv", header=True, schema=schema)

row_count = df.count()
print(f"Загружено строк: {row_count}")
df.printSchema()
get_memory_mb()

print("Репартицируем на 4 партиции по 'department'...")
df_optimized = df.repartition(4, "department")
get_memory_mb()

print("Кэшируем данные в памяти (MEMORY_ONLY)...")
df_cached = df_optimized.persist(StorageLevel.MEMORY_ONLY)
get_memory_mb()

print("Активируем кэш через .count()...")
count_start = time.time()
df_cached.count()
count_time = time.time() - count_start
get_memory_mb()
print(f"Кэширование активировано: {row_count} строк за {count_time:.2f} сек")

num_partitions = df_cached.rdd.getNumPartitions()
print(f"Количество партиций после repartition: {num_partitions}")

print("\nПервый запуск groupBy.avg(salary)...")
result1_start = time.time()
result1 = df_cached.groupBy("department").avg("salary")
result1.show()
result1_time = time.time() - result1_start
get_memory_mb()

print("\nВторой запуск groupBy.avg(salary) — данные в кэше!")
result2_start = time.time()
result2 = df_cached.groupBy("department").avg("salary")
result2.show()
result2_time = time.time() - result2_start
get_memory_mb()

end_time = time.time()
execution_time = end_time - start_time
peak_ram = peak_memory_mb

print(f"\nЭксперимент {EXP_NAME} завершён:")
print(f"Общее время выполнения: {execution_time:.2f} сек")
print(f"Пиковая RAM (драйвер): {peak_ram:.1f} МБ")
print(f"Первый groupBy: {result1_time:.2f} сек")
print(f"Второй groupBy: {result2_time:.2f} сек")
print(f"Ускорение: {result1_time / result2_time:.2f}x")

# === СОХРАНЕНИЕ В JSON ===
results = {
    "Experiment": EXP_NAME,
    "Configuration": "3-optimized=true" if is_3dn else "1-optimized=true",
    "DataNodes": 3 if is_3dn else 1,
    "Optimized": True,
    "Total time (s)": round(execution_time, 3),
    "Memory (MB)": round(peak_ram, 3),
    "First groupBy time (s)": round(result1_time, 3),
    "Second groupBy time (s)": round(result2_time, 3),
    "Speedup (x)": round(result1_time / result2_time, 2),
    "Rows processed": row_count,
    "Aggregation result": {row["department"]: round(row["avg(salary)"], 2) for row in result1.collect()}
}

# Сохраняем строго под универсальным именем в корень проекта
with open("results.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)

print(f"\n Результаты сохранены в JSON: results.json")

df_cached.unpersist()
spark.stop()