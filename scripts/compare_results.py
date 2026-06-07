import json
import os

def load_result(exp_name):
    # Путь к перемещенным файлам результатов
    path = f"experiments/{exp_name}/results/results.json"
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

experiments = [
    "exp_1dn_base",
    "exp_1dn_optimized",
    "exp_3dn_base",
    "exp_3dn_optimized"
]

results = []
for exp in experiments:
    data = load_result(exp)
    if data:
        results.append(data)

summary = {
    "summary": results,
    "total_experiments": len(results)
}

os.makedirs("results", exist_ok=True)
with open("results/summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, ensure_ascii=False)

print("\n[УСПЕХ] Сводная аналитика сохранена: results/summary.json")
