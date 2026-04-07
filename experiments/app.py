import os
import time
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, max, min, desc, round

try:
    import psutil
    PSUTIL_AVAILABLE = True
    
    process = psutil.Process()
    peak_memory_mb = 0.0  

    def get_memory_mb():
        """Возвращает текущую RAM в MB"""
        ram = process.memory_info().rss / 1024 / 1024
        global peak_memory_mb
        if ram > peak_memory_mb:
            peak_memory_mb = ram
        return ram

    def log_memory(stage_name):
        """Логирует RAM на этапе и обновляет пик"""
        current = get_memory_mb()
        print(f"[{stage_name}] RAM: {current:.1f} MB (пик: {peak_memory_mb:.1f} MB)")

except ImportError:
    PSUTIL_AVAILABLE = False
    peak_memory_mb = "N/A"

    def get_memory_mb():
        return "N/A"

    def log_memory(stage_name):
        print(f"[{stage_name}] RAM: недоступно (psutil не установлен)")

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.showConsoleProgress=false pyspark-shell'

spark = SparkSession.builder \
    .appName("CryptoAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.ui.enabled", "true") \
    .config("spark.sql.ui.retainedExecutions", "5") \
    .config("spark.ui.retainedJobs", "5") \
    .config("spark.ui.retainedStages", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_counter = 0

def increment_job():
    global job_counter
    job_counter += 1
    current_ram = get_memory_mb()
    ram_str = f"{current_ram:.1f} MB" if isinstance(current_ram, float) else current_ram
    print(f"Job {job_counter}: Запуск операции | RAM: {ram_str}")

print("Начало выполнения Spark-приложения:", time.strftime("%Y-%m-%d %H:%M:%S"))

start_ram = get_memory_mb()
start_time = time.time()
log_memory("Старт приложения")

print("Чтение данных из HDFS: hdfs://localhost:9000/input/crypto_dataset.csv")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/input/crypto_dataset.csv")

row_count = df.count()
log_memory("После df.count()")
print(f"Успешно прочитано {row_count} строк")
print("Схема данных:")
df.printSchema()

print("Анализ 1: Средняя цена (USD) по типу криптовалюты")
increment_job()
avg_price = df.filter(col("price_usd").isNotNull()) \
    .groupBy("crypto_type") \
    .agg(round(avg("price_usd"), 2).alias("avg_price_usd")) \
    .orderBy(desc("avg_price_usd"))
avg_price.show()
log_memory("После первого show()")

print("Анализ 2: Количество аномальных записей (is_anomaly == true)")
anomalies = df.filter(col("is_anomaly") == True).count()
log_memory("После count() аномалий")
normal = df.filter(col("is_anomaly") == False).count()
print(f"Аномальные записи: {anomalies}")
print(f"Нормальные записи: {normal}")

print("Анализ 3: Количество записей по регионам")
increment_job()
region_stats = df.groupBy("region") \
    .agg(count("*").alias("record_count")) \
    .orderBy(desc("record_count"))
region_stats.show(truncate=False)
log_memory("После show() region_stats")

print("Анализ 4: Энергопотребление и комиссии (средние значения)")
increment_job()
energy_fees = df.agg(
    round(avg("energy_consumption_kw"), 2).alias("avg_energy_kw"),
    round(avg("blockchain_fees_usd"), 2).alias("avg_fees_usd"),
    round(sum("blockchain_fees_usd"), 2).alias("total_fees_usd")
)
energy_fees.show()
log_memory("После show() energy_fees")

print("Анализ 5: Статистика по объёму торгов (volume_usd)")
increment_job()
volume_stats = df.agg(
    round(avg("volume_usd"), 2).alias("avg_volume"),
    round(min("volume_usd"), 2).alias("min_volume"),
    round(max("volume_usd"), 2).alias("max_volume")
)
volume_stats.show()
log_memory("После show() volume_stats")

output_path = "hdfs://localhost:9000/output"
print(f"Сохранение результатов в: {output_path}")
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/output"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
increment_job()
avg_price.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/output/avg_price_by_crypto")
log_memory("После записи в HDFS")
print("Результаты сохранены в HDFS")

end_time = time.time()
duration = end_time - start_time
final_ram = get_memory_mb()

report = f"""
==================================================
ФИНАЛЬНЫЙ ОТЧЁТ ПО ВЫПОЛНЕНИЮ
==================================================
Время выполнения: {duration:.2f} секунд
Пиковая RAM: {peak_memory_mb:.1f} MB
Обработано строк: {row_count}
Всего запущено Jobs: {job_counter}
Spark UI доступен по адресу: http://localhost:4040
==================================================
"""

print(report)

with open("report.txt", "w") as f:
    f.write(report.strip())

print("Финальный отчёт сохранён в report.txt")

spark.stop()
print("SparkSession остановлен. Приложение завершено.")
