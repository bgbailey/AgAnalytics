# Databricks notebook source / Microsoft Fabric Notebook
# MAGIC %md
# # Silver Layer Transformation — AgriTech Analytics
# Reads Bronze Delta tables, applies cleaning, validation, deduplication,
# and enrichment. Outputs Silver Delta tables ready for Gold aggregation.
#
# ## Transformations applied per dataset
# | Dataset | Key transforms |
# |---------|---------------|
# | Sensors | Type-cast timestamps, range-filter bad readings, deduplicate, derive VPD |
# | Weather | Derive heat-index & wind-chill, add date/hour partitions |
# | Equipment | Boolean flags for heating/cooling/ventilating states |
# | Harvests | Filter negative weights, compute total weight |
# | Energy | Water-recycling percentage, date/hour partitions |
# | Shipments | Timestamp cast, add date column |

# COMMAND ----------

from pyspark.sql.functions import (
    avg, col, count, current_timestamp, exp, hour, lit, pow, sum,
    to_date, to_timestamp, when,
)

row_counts = {}

# ===================================================================
# SENSOR READINGS
# ===================================================================
print("▶ Transforming sensor readings...")
bronze_sensors = spark.read.table("bronze_iot_telemetry")

silver_sensors = (
    bronze_sensors
    .withColumn("timestamp", to_timestamp("timestamp"))
    .filter(col("timestamp").isNotNull())
    # Physical-range validation — reject clearly impossible values
    .filter(col("air_temperature").between(-10, 50))
    .filter(col("air_humidity").between(0, 100))
    .filter(col("co2_level").between(100, 5000))
    .filter(col("substrate_ph").between(3.0, 9.0))
    # Add date/hour columns for downstream time-based aggregation
    .withColumn("date", to_date("timestamp"))
    .withColumn("hour", hour("timestamp"))
    # One reading per zone per timestamp — discard late-arriving duplicates
    .dropDuplicates(["timestamp", "zone_id"])
)

# Derive VPD (Vapour Pressure Deficit) when the source column is missing.
# Formula: VPD = SVP × (1 − RH/100) where SVP = 0.6108 × e^(17.27T / (T + 237.3))
silver_sensors = silver_sensors.withColumn(
    "vpd_calculated",
    (
        0.6108
        * exp(17.27 * col("air_temperature") / (col("air_temperature") + 237.3))
    )
    * (1 - col("air_humidity") / 100),
)

silver_sensors.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_sensor_readings")
row_counts["silver_sensor_readings"] = silver_sensors.count()
print(f"  ✔ silver_sensor_readings: {row_counts['silver_sensor_readings']:,} rows")

# COMMAND ----------

# ===================================================================
# WEATHER
# ===================================================================
print("▶ Transforming weather data...")
bronze_weather = spark.read.table("bronze_weather")

silver_weather = (
    bronze_weather
    .withColumn("timestamp", to_timestamp("timestamp"))
    .filter(col("timestamp").isNotNull())
    .withColumn("date", to_date("timestamp"))
    .withColumn("hour", hour("timestamp"))
    # Heat index — meaningful above 27 °C (Rothfusz regression, simplified)
    .withColumn(
        "heat_index",
        when(
            col("outside_temperature") > 27,
            -8.785
            + 1.611 * col("outside_temperature")
            + 2.339 * col("outside_humidity")
            - 0.146 * col("outside_temperature") * col("outside_humidity"),
        ).otherwise(col("outside_temperature")),
    )
    # Wind chill — meaningful below 10 °C (Environment Canada formula)
    .withColumn(
        "wind_chill",
        when(
            col("outside_temperature") < 10,
            13.12
            + 0.6215 * col("outside_temperature")
            - 11.37 * pow(col("wind_speed"), 0.16)
            + 0.3965 * col("outside_temperature") * pow(col("wind_speed"), 0.16),
        ).otherwise(col("outside_temperature")),
    )
)

silver_weather.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_weather")
row_counts["silver_weather"] = silver_weather.count()
print(f"  ✔ silver_weather: {row_counts['silver_weather']:,} rows")

# COMMAND ----------

# ===================================================================
# EQUIPMENT STATES
# ===================================================================
print("▶ Transforming equipment states...")
bronze_equipment = spark.read.table("bronze_equipment")

silver_equipment = (
    bronze_equipment
    .withColumn("timestamp", to_timestamp("timestamp"))
    .filter(col("timestamp").isNotNull())
    .withColumn("date", to_date("timestamp"))
    # Derive boolean flags for common dashboard filters
    .withColumn("is_heating", col("heating_output") > 0)
    .withColumn("is_cooling", col("cooling_output") > 0)
    .withColumn("is_ventilating", col("vent_position") > 5)
)

silver_equipment.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_equipment")
row_counts["silver_equipment"] = silver_equipment.count()
print(f"  ✔ silver_equipment: {row_counts['silver_equipment']:,} rows")

# COMMAND ----------

# ===================================================================
# HARVESTS
# ===================================================================
print("▶ Transforming harvest data...")
bronze_harvests = spark.read.table("bronze_daily_harvest")

silver_harvests = (
    bronze_harvests
    .withColumn("date", to_date("date"))
    .filter(col("harvest_weight_kg") >= 0)
    .withColumn("total_weight_kg", col("harvest_weight_kg") + col("waste_kg"))
)

silver_harvests.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_harvests")
row_counts["silver_harvests"] = silver_harvests.count()
print(f"  ✔ silver_harvests: {row_counts['silver_harvests']:,} rows")

# COMMAND ----------

# ===================================================================
# ENERGY & UTILITIES
# ===================================================================
print("▶ Transforming energy data...")
bronze_energy = spark.read.table("bronze_energy")

silver_energy = (
    bronze_energy
    .withColumn("timestamp", to_timestamp("timestamp"))
    .withColumn("date", to_date("timestamp"))
    .withColumn("hour", hour("timestamp"))
    .withColumn(
        "water_recycling_pct",
        when(col("water_liters") > 0, col("water_recycled_liters") / col("water_liters") * 100)
        .otherwise(lit(0)),
    )
)

silver_energy.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_energy")
row_counts["silver_energy"] = silver_energy.count()
print(f"  ✔ silver_energy: {row_counts['silver_energy']:,} rows")

# COMMAND ----------

# ===================================================================
# SHIPMENTS / SUPPLY CHAIN
# ===================================================================
print("▶ Transforming shipment data...")
bronze_shipments = spark.read.table("bronze_shipments")

silver_shipments = (
    bronze_shipments
    .withColumn("ship_date", to_timestamp("ship_date"))
    .withColumn("date", to_date("ship_date"))
)

silver_shipments.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_shipments")
row_counts["silver_shipments"] = silver_shipments.count()
print(f"  ✔ silver_shipments: {row_counts['silver_shipments']:,} rows")

# COMMAND ----------

# ===================================================================
# Summary
# ===================================================================
print("\n" + "=" * 60)
print("Silver transformation complete!")
print("=" * 60)
for table, cnt in row_counts.items():
    print(f"  {table:<30s} {cnt:>12,} rows")
print(f"\nTotal Silver tables: {len(row_counts)}")
