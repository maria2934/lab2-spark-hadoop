#!/bin/bash
# deploy.sh <1dn|3dn>

CONFIG=$1
HADOOP_CONF="$HADOOP_HOME/etc/hadoop"

echo " Настройка HDFS: $CONFIG"

if [ "$CONFIG" == "1dn" ]; then
  cp "$PROJECT_ROOT/config/hdfs-1dn/workers" "$HADOOP_CONF/workers"
elif [ "$CONFIG" == "3dn" ]; then
  cp "$PROJECT_ROOT/config/hdfs-3dn/workers" "$HADOOP_CONF/workers"
else
  echo " Неизвестный конфиг: $CONFIG"
  exit 1
fi

# 1. Мягкая остановка
stop-dfs.sh || true
sleep 2

# 2. ЖЕСТКАЯ ОЧИСТКА ДЛЯ MAC: убиваем любые зависшие процессы Hadoop в памяти
echo " Очистка портов и процессов DataNode/NameNode..."
pkill -9 -f proc_datanode || true
pkill -9 -f proc_namenode || true
pkill -9 -f proc_secondarynamenode || true
sleep 2

# Очищаем временные директории
rm -rf /tmp/hadoop-*

# Форматируем NameNode заново для чистоты эксперимента
echo " Форматирование NameNode..."
hdfs namenode -format -force -nonInteractive

# Запуск
echo " Запуск HDFS..."
start-dfs.sh

echo " Ожидание инициализации файловой системы..."
# Цикл проверки: ждем, пока команда hdfs dfs -mkdir начнет выполняться успешно
for i in {1..15}; do
  if hdfs dfs -mkdir -p /test_ready 2>/dev/null; then
    echo " [ОК] HDFS успешно отвечает на запросы!"
    hdfs dfs -rm -r /test_ready &>/dev/null
    break
  fi
  echo " Ждем готовности HDFS... (попытка $i из 15)"
  sleep 4
done

echo "--- Текущие процессы Java ---"
jps
echo "----------------------------"
