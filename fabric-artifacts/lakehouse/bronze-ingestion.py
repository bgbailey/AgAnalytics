# Databricks notebook source / Microsoft Fabric Notebook
# MAGIC %md
# # Bronze Layer Ingestion — AgriTech Analytics
# Reads raw Parquet files from OneLake Files/ and writes to Bronze Delta tables.
# Run this notebook after uploading historical data via `upload-historical-to-onelake.py`.
#
# ## Expected source structure (in Lakehouse Files/):
# ```
# Files/raw/iot_telemetry/greenhouse_id=brightharvest/date=2025-01-15/data.parquet
# Files/raw/weather/greenhouse_id=brightharvest/date=2025-01-15/data.parquet
# Files/raw/equipment/...
# Files/raw/daily_harvest/...
# Files/raw/energy/...
# Files/raw/shipments/...
# ```

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# ---------------------------------------------------------------------------
# Configuration — data types to ingest and their source paths
# Each entry maps a raw folder name to its target Bronze Delta table.
# ---------------------------------------------------------------------------
data_types = [
    "iot_telemetry",
    "weather",
    "equipment",
    "daily_harvest",
    "energy",
    "shipments",
]

# COMMAND ----------

# ---------------------------------------------------------------------------
# Ingest loop — read each Parquet dataset and write to a Bronze Delta table
# ---------------------------------------------------------------------------
row_counts = {}

for dt in data_types:
    source_path = f"Files/raw/{dt}"
    target_table = f"bronze_{dt}"

    print(f"▶ Ingesting {dt} from {source_path}...")

    try:
        df = spark.read.parquet(source_path)
    except Exception as e:
        print(f"  ⚠ Skipping {dt} — source not found or unreadable: {e}")
        continue

    # Stamp every row with ingestion metadata so lineage is traceable
    df = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", input_file_name())
    )

    # Full-reload write — safe for initial bulk load and idempotent re-runs.
    # Switch to .mode("append") for incremental / streaming-style ingestion.
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)

    count = df.count()
    row_counts[target_table] = count
    print(f"  ✔ {target_table}: {count:,} rows written")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("Bronze ingestion complete!")
print("=" * 60)
for table, count in row_counts.items():
    print(f"  {table:<30s} {count:>12,} rows")
print(f"\nTotal tables ingested: {len(row_counts)}")
