import subprocess
import os

# Helper để chạy một file và log
def run_script(path):
    print(f"\n🟢 Running: {path}")
    try:
        subprocess.run(["python", path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error in {path}: {e}")

base = os.path.join(os.path.dirname(__file__))

# === 1. Bronze Layer ===
bronze_dir = os.path.join(base, "bronze")
bronze_files = [
    "bronze_clothes_table.py",
    "bronze_customer_table.py",
    "bronze_gtrends_table.py",
    "bronze_price_table.py",
    "bronze_restock_table.py",
    "bronze_sales_table.py",
    "bronze_weather_table.py",
]

# === 2. Silver Layer ===
silver_dir = os.path.join(base, "silver")
silver_files = [
    "silver_clothes_table.py",
    "silver_customer_table.py",
    "silver_gtrends_table.py",
    "silver_price_table.py",
    "silver_restock_table.py",
    "silver_sales_table.py",
]

# === 3. Gold Layer ===
gold_dir = os.path.join(base, "gold")
gold_files = [
    "gold_demandforecast2to1_table.py",
    "gold_forecasted_table.py",
    "gold_imgembed_table.py",
    "gold_inventory_table.py",
    "gold_restock_table.py",
]

# === 4. Export to PostgreSQL ===
postgres_script = os.path.join(base, "postgre_sql", "lake2postgre.py")

# === RUN ALL ===
for f in bronze_files:
    run_script(os.path.join(bronze_dir, f))

for f in silver_files:
    run_script(os.path.join(silver_dir, f))

for f in gold_files:
    run_script(os.path.join(gold_dir, f))

run_script(postgres_script)

print("\n✅ All preprocessing completed successfully.")
