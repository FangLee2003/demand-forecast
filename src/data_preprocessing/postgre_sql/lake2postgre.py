import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

# 🧠 Tạo Spark session
spark = SparkSession.builder \
    .appName("ExportGoldToPostgres") \
    .getOrCreate()

# 🔐 Cấu hình PostgreSQL
PG_USER = "postgres"
PG_PASSWORD = "123456"
PG_HOST = "localhost"  # đổi thành 'host.docker.internal' nếu chạy từ container
PG_PORT = "5432"
PG_DB = "postgres"

pg_url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(pg_url)

# 📋 Danh sách bảng Gold và tên PostgreSQL tương ứng (không có tiền tố 'gold_')
gold_tables = {
    "gold_demandforecast2to1_table": "demandforecast2to1",
    "gold_forecasted_table": "forecasted",
    "gold_inventory_table": "inventory",
    "gold_restock_table": "restock",
    "gold_imgembed_table": "imgembed"
}

# 🔁 Export từng bảng Delta → PostgreSQL
for delta_table, pg_table in gold_tables.items():
    try:
        print(f"📤 Exporting '{delta_table}' to PostgreSQL table '{pg_table}'...")
        df_spark = spark.read.table(f"portfolio.end_to_end_demand_forecast.{delta_table}")
        df = df_spark.toPandas()
        df.to_sql(pg_table, engine, if_exists="replace", index=False)
        print(f"✅ Success: '{pg_table}' exported.")
    except Exception as e:
        print(f"❌ Error exporting {delta_table}: {str(e)}")

print("🏁 All Gold tables have been exported to PostgreSQL.")
