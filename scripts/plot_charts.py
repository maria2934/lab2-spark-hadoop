import json
import os
import matplotlib.pyplot as plt

# Путь к итоговому JSON
json_path = "results/summary.json"

if not os.path.exists(json_path):
    print(f"[ОШИБКА] Файл {json_path} не найден! Убедитесь, что вы запустили run_all.sh")
    exit(1)

with open(json_path, "r", encoding="utf-8") as f:
    summary_data = json.load(f)["summary"]

# Извлекаем метрики для визуализации
experiments = [e["Experiment"] for e in summary_data]
total_times = [e["Total time (s)"] for e in summary_data]
memories = [e["Memory (MB)"] for e in summary_data]

# Стиль графиков
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')

# График 1: Общее время выполнения
plt.figure(figsize=(10, 5))
bars_time = plt.bar(experiments, total_times, color=['#3498db', '#2ecc71', '#2980b9', '#27ae60'], edgecolor='black', width=0.6)
plt.title("Общее время выполнения экспериментов (меньше = лучше)", fontsize=14, fontweight='bold', pad=15)
plt.ylabel("Время (секунды)", fontsize=12)
plt.xticks(fontsize=10, rotation=10)
plt.ylim(0, max(total_times) * 1.2)

# Добавляем значения над столбцами
for bar in bars_time:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval + 0.1, f"{yval:.3f} s", ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig("results/chart_time.png", dpi=150)
plt.close()
print("[УСПЕХ] График времени сохранен в: results/chart_time.png")

# График 2: Пиковое потребление памяти драйвером
plt.figure(figsize=(10, 5))
bars_mem = plt.bar(experiments, memories, color=['#e67e22', '#e74c3c', '#d35400', '#c0392b'], edgecolor='black', width=0.6)
plt.title("Пиковое потребление RAM драйвером (меньше = лучше)", fontsize=14, fontweight='bold', pad=15)
plt.ylabel("Память (МБ)", fontsize=12)
plt.xticks(fontsize=10, rotation=10)
plt.ylim(0, max(memories) * 1.2)

# Добавляем значения над столбцами
for bar in bars_mem:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval + 1, f"{yval:.2f} MB", ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig("results/chart_memory.png", dpi=150)
plt.close()
print("[УСПЕХ] График памяти сохранен в: results/chart_memory.png")

# График 3: Сравнение первого и второго groupBy (эффект кэша)
opt_exps = [e["Experiment"] for e in summary_data if e["Optimized"]]
first_groupby = [e["First groupBy time (s)"] for e in summary_data if e["Optimized"]]
second_groupby = [e["Second groupBy time (s)"] for e in summary_data if e["Optimized"]]

if opt_exps:
    plt.figure(figsize=(9, 5))
    x = range(len(opt_exps))
    width = 0.35
    
    plt.bar([i - width/2 for i in x], first_groupby, width, label='Первый запуск (Без кэша)', color='#f1c40f', edgecolor='black')
    plt.bar([i + width/2 for i in x], second_groupby, width, label='Второй запуск (Из кэша)', color='#2ecc71', edgecolor='black')
    
    plt.title("Эффект кэширования: Первый vs Второй groupBy", fontsize=14, fontweight='bold', pad=15)
    plt.ylabel("Время выполнения (секунды)", fontsize=12)
    plt.xticks(x, opt_exps, fontsize=10)
    plt.legend(fontsize=11)
    
    plt.tight_layout()
    plt.savefig("results/chart_cache_effect.png", dpi=150)
    plt.close()
    print("[УСПЕХ] График эффекта кэширования сохранен в: results/chart_cache_effect.png")

