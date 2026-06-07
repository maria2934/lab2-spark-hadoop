#!/bin/bash
# run_all.sh — Запуск всех 4 экспериментов последовательно

set +e  

PROJECT_ROOT="$(pwd)"
SCRIPT_DIR="$PROJECT_ROOT/scripts"
export PROJECT_ROOT

echo " Запуск всех 4 экспериментов"
echo "===================================================="

# Строгий массив: config | optimize (0 или 1) | имя_эксперимента
experiments=(
  "1dn|0|exp_1dn_base"
  "1dn|1|exp_1dn_optimized"
  "3dn|0|exp_3dn_base"
  "3dn|1|exp_3dn_optimized"
)

for exp in "${experiments[@]}"; do
  IFS='|' read -r config optimize exp_name <<< "$exp"

  echo ""
  echo " ЭКСПЕРИМЕНТ: $exp_name"
  echo "   Конфиг: $config | Оптимизация: $([ $optimize -eq 1 ] && echo 'Да' || echo 'Нет')"
  echo "----------------------------------------------------"

  # ШАГ 1: Настройка HDFS через deploy.sh
  echo "  Настройка HDFS: $config"
  bash "$SCRIPT_DIR/deploy.sh" "$config"
  sleep 5

  # ШАГ 2: Запуск Spark эксперимента через run_experiment.sh
  echo " Запуск Spark приложения..."
  bash "$SCRIPT_DIR/run_experiment.sh" "$config" "$optimize" "$exp_name"

  # ШАГ 3: Очистка HDFS
  echo " Очистка: остановка HDFS"
  stop-dfs.sh || true

  echo " Ожидание 10 сек перед следующим..."
  sleep 10
done

echo ""
echo " ВСЕ ЭКСПЕРИМЕНТЫ ЗАВЕРШЕНЫ!"
echo " Генерация сводки..."
python3 "$SCRIPT_DIR/compare_results.py"

echo " Итоговые результаты: $PROJECT_ROOT/results/summary.json"
echo " Готово!"
