#!/bin/bash
# run_experiment.sh <1dn|3dn> <0|1> <exp_name>

CONFIG=$1
OPTIMIZE=$2
EXP_NAME=$3

EXP_DIR="$PROJECT_ROOT/experiments/$EXP_NAME"
mkdir -p "$EXP_DIR/results"

# Удаляем старый файл из корня
rm -f "$PROJECT_ROOT/results.json"

# Безопасное создание папки и загрузка файла
echo " Создание директории в HDFS..."
hdfs dfs -mkdir -p /lab2 || true
sleep 2

echo " Удаление старого файла из HDFS (если был)..."
hdfs dfs -rm -f /lab2/data_2lab.csv || true
sleep 2

echo " Загрузка датасета в HDFS..."
hdfs dfs -put "$PROJECT_ROOT/data/data_2lab.csv" /lab2/
sleep 3

# Проверяем, появился ли файл в HDFS на самом деле
if ! hdfs dfs -test -e /lab2/data_2lab.csv; then
  echo " [ВНИМАНИЕ] Файл не загрузился с первой попытки. Пробуем еще раз..."
  sleep 5
  hdfs dfs -put "$PROJECT_ROOT/data/data_2lab.csv" /lab2/ || true
fi

# Установка репликации
REPLICA=1
[ "$CONFIG" == "3dn" ] && REPLICA=3
echo " Установка репликации = $REPLICA"
hdfs dfs -setrep $REPLICA /lab2/data_2lab.csv || true

# Выбор скрипта
if [ "$OPTIMIZE" -eq 1 ]; then
  APP="spark_app_optimized.py"
else
  APP="spark_app_basic.py"
fi

echo " Запуск приложения: $APP для эксперимента: $EXP_NAME"

# Запуск Spark
spark-submit \
  --driver-memory 1g \
  --executor-memory 1g \
  "$PROJECT_ROOT/src/$APP" "$EXP_NAME" 2>&1 | tee "$EXP_DIR/results/spark.log"

# Копируем свежий файл результатов
if [ -f "$PROJECT_ROOT/results.json" ]; then
  cp "$PROJECT_ROOT/results.json" "$EXP_DIR/results/results.json"
  echo " [УСПЕХ] Файл результатов скопирован в $EXP_DIR/results/"
else
  echo " [ОШИБКА] Файл results.json не был создан приложением Spark!"
fi
