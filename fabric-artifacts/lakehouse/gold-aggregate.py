# Databricks notebook source / Microsoft Fabric Notebook
# MAGIC %md
# # Gold Layer Aggregation — AgriTech Analytics
# Reads Silver tables and produces business-ready fact tables consumed by the
# Direct Lake semantic model. Each fact table maps 1-to-1 with a semantic model
# table so Power BI gets zero-copy, always-fresh data.
#
# ## Gold tables produced
# | Table | Grain | Purpose |
# |-------|-------|---------|
# | `fact_daily_harvest` | zone × day | Yield, quality grades, waste, revenue |
# | `fact_zone_daily_environment` | zone × day | Sensor aggregates, optimal-range hours |
# | `fact_daily_energy` | greenhouse × day | Utility consumption, cost per kg |
# | `fact_shipments` | order | Supply chain delivery, cold-chain compliance |
# | `fact_weekly_crop_health` | zone × week | Predicted yield & quality (ML proxy) |
# | `fact_anomaly_events` | event | Anomaly log (populated by anomaly engine) |

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import (
    abs as _abs, avg, col, count, date_trunc, lit, max, min, rand, sum, when,
)
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

row_counts = {}

# ===================================================================
# Zone → crop mapping (mirrors data-generator config)
# ===================================================================
ZONE_CROPS = {
    "BH-Z01": "baby_spinach",  "BH-Z02": "baby_spinach",
    "BH-Z03": "romaine",       "BH-Z04": "romaine",
    "BH-Z05": "arugula",       "BH-Z06": "arugula",
    "BH-Z07": "basil",         "BH-Z08": "basil",
    "MV-Z01": "cocktail_tomato", "MV-Z02": "cocktail_tomato",
    "MV-Z03": "bell_pepper",     "MV-Z04": "bell_pepper",
    "MV-Z05": "mini_cucumber",   "MV-Z06": "mini_cucumber",
    "MV-Z07": "strawberry",      "MV-Z08": "strawberry",
}

# Optimal temperature setpoints per crop (°C)
CROP_SETPOINTS = {
    "baby_spinach": 18.0, "romaine": 20.0, "arugula": 19.0, "basil": 24.0,
    "cocktail_tomato": 23.0, "bell_pepper": 22.0, "mini_cucumber": 24.0, "strawberry": 20.0,
}

# Utility cost assumptions (USD)
ELECTRICITY_RATE = 0.12   # per kWh
NATURAL_GAS_RATE = 0.45   # per m³

# COMMAND ----------

# ===================================================================
# FACT: DAILY HARVEST
# ===================================================================
print("▶ Building fact_daily_harvest...")
silver_harvests = spark.read.table("silver_harvests")

fact_harvest = silver_harvests.select(
    col("date").alias("harvest_date"),
    "zone_id",
    "greenhouse_id",
    "crop_id",
    "harvest_weight_kg",
    "harvest_units",
    "grade_a_pct",
    "grade_b_pct",
    "grade_c_pct",
    "waste_kg",
    "waste_pct",
    "days_to_harvest",
    "revenue_estimate_usd",
)

fact_harvest.write.format("delta").mode("overwrite").saveAsTable("fact_daily_harvest")
row_counts["fact_daily_harvest"] = fact_harvest.count()
print(f"  ✔ fact_daily_harvest: {row_counts['fact_daily_harvest']:,} rows")

# COMMAND ----------

# ===================================================================
# FACT: ZONE DAILY ENVIRONMENT
# Aggregates 30-second sensor readings to one row per zone per day.
# Joins with crop setpoints to compute optimal-range metrics.
# ===================================================================
print("▶ Building fact_zone_daily_environment...")
silver_sensors = spark.read.table("silver_sensor_readings")

# Build a small lookup DataFrame: zone_id → target_temp
zone_setpoint_rows = [
    Row(zone_id=z, target_temp=CROP_SETPOINTS[c]) for z, c in ZONE_CROPS.items()
]
zone_setpoints_df = spark.createDataFrame(zone_setpoint_rows)

# Aggregate sensors to daily grain
fact_env = silver_sensors.groupBy("date", "zone_id", "greenhouse_id").agg(
    avg("air_temperature").alias("avg_temp"),
    min("air_temperature").alias("min_temp"),
    max("air_temperature").alias("max_temp"),
    avg("air_humidity").alias("avg_humidity"),
    avg("co2_level").alias("avg_co2"),
    # DLI (Daily Light Integral) = Σ PAR × interval / 1 000 000
    # PAR is µmol m⁻² s⁻¹; interval is 30 s; result is mol m⁻² day⁻¹
    (sum("par_light") * 30 / 1_000_000).alias("total_dli"),
    avg("vpd").alias("avg_vpd"),
    avg("substrate_ec").alias("avg_ec"),
    avg("substrate_ph").alias("avg_ph"),
    count("*").alias("reading_count"),
)

# Join with setpoints and derive excursion metrics
fact_env = fact_env.join(zone_setpoints_df, "zone_id", "left")
fact_env = (
    fact_env
    .withColumn(
        "temp_excursion_hours",
        # Rough heuristic: when average deviates > 2 °C from setpoint,
        # the magnitude maps to proportional "excursion hours"
        when(
            _abs(col("avg_temp") - col("target_temp")) > 2.0,
            _abs(col("avg_temp") - col("target_temp")) * 2.0,
        ).otherwise(0.0),
    )
    .withColumn(
        "hours_in_optimal_range",
        lit(24.0) - col("temp_excursion_hours"),
    )
    .drop("target_temp")
)

fact_env.write.format("delta").mode("overwrite").saveAsTable("fact_zone_daily_environment")
row_counts["fact_zone_daily_environment"] = fact_env.count()
print(f"  ✔ fact_zone_daily_environment: {row_counts['fact_zone_daily_environment']:,} rows")

# COMMAND ----------

# ===================================================================
# FACT: DAILY ENERGY
# One row per greenhouse per day with utility totals and per-kg cost.
# ===================================================================
print("▶ Building fact_daily_energy...")
silver_energy = spark.read.table("silver_energy")

fact_energy = silver_energy.groupBy("date", "greenhouse_id").agg(
    sum("electricity_kwh").alias("electricity_kwh"),
    sum("natural_gas_m3").alias("natural_gas_m3"),
    sum("water_liters").alias("water_liters"),
    sum("water_recycled_liters").alias("water_recycled_liters"),
    sum("co2_purchased_kg").alias("co2_purchased_kg"),
)

# Estimated daily energy cost
fact_energy = fact_energy.withColumn(
    "energy_cost_usd",
    col("electricity_kwh") * ELECTRICITY_RATE + col("natural_gas_m3") * NATURAL_GAS_RATE,
)

# Join with harvest totals to derive cost-per-kg-of-produce
daily_yield = (
    spark.read.table("fact_daily_harvest")
    .groupBy(col("harvest_date").alias("date"), "greenhouse_id")
    .agg(sum("harvest_weight_kg").alias("total_kg"))
)

fact_energy = fact_energy.join(daily_yield, ["date", "greenhouse_id"], "left")
fact_energy = (
    fact_energy
    .withColumn(
        "energy_per_kg_produce",
        when(col("total_kg") > 0, col("energy_cost_usd") / col("total_kg"))
        .otherwise(lit(0)),
    )
    .drop("total_kg")
)

fact_energy.write.format("delta").mode("overwrite").saveAsTable("fact_daily_energy")
row_counts["fact_daily_energy"] = fact_energy.count()
print(f"  ✔ fact_daily_energy: {row_counts['fact_daily_energy']:,} rows")

# COMMAND ----------

# ===================================================================
# FACT: SHIPMENTS
# One row per order — supply chain delivery & cold-chain compliance.
# ===================================================================
print("▶ Building fact_shipments...")
silver_shipments = spark.read.table("silver_shipments")

fact_shipments = silver_shipments.select(
    col("date").alias("ship_date"),
    "order_id",
    "greenhouse_id",
    "customer_id",
    col("product_sku").alias("product_id"),
    "crop_id",
    "quantity_cases",
    lit(0).alias("quantity_ordered"),          # placeholder — join with orders table later
    "delivery_status",
    "cold_chain_compliant",
    "shelf_life_remaining_days",
    (col("delivery_status") == "delivered").alias("on_time_flag"),
)

fact_shipments.write.format("delta").mode("overwrite").saveAsTable("fact_shipments")
row_counts["fact_shipments"] = fact_shipments.count()
print(f"  ✔ fact_shipments: {row_counts['fact_shipments']:,} rows")

# COMMAND ----------

# ===================================================================
# FACT: WEEKLY CROP HEALTH
# Proxy table built from sensor weekly averages. In production this is
# populated by the ML-based CropSimulator model output.
# ===================================================================
print("▶ Building fact_weekly_crop_health...")
silver_sensors = spark.read.table("silver_sensor_readings")

weekly_env = (
    silver_sensors
    .withColumn("week_start", date_trunc("week", "date"))
    .groupBy("week_start", "zone_id", "greenhouse_id")
    .agg(
        avg("air_temperature").alias("avg_temp"),
        avg("par_light").alias("avg_par"),
    )
)

# Zone → crop lookup
zone_crop_rows = [Row(zone_id=z, crop_id=c) for z, c in ZONE_CROPS.items()]
zone_crop_df = spark.createDataFrame(zone_crop_rows)

weekly_health = weekly_env.join(zone_crop_df, "zone_id", "left")
weekly_health = (
    weekly_health
    # Placeholders — replace with actual model predictions once ML pipeline is wired
    .withColumn("avg_plant_height_cm", lit(25.0) + rand() * 15)
    .withColumn("avg_leaf_count", lit(8.0) + rand() * 6)
    .withColumn(
        "predicted_quality_grade",
        when(_abs(col("avg_temp") - lit(21)) < 2, lit("A"))
        .when(_abs(col("avg_temp") - lit(21)) < 4, lit("B"))
        .otherwise(lit("C")),
    )
    .withColumn("predicted_yield_kg", col("avg_par") * 0.5 + rand() * 50)
    .select(
        "week_start", "zone_id", "greenhouse_id", "crop_id",
        "avg_plant_height_cm", "avg_leaf_count",
        "predicted_quality_grade", "predicted_yield_kg",
    )
)

weekly_health.write.format("delta").mode("overwrite").saveAsTable("fact_weekly_crop_health")
row_counts["fact_weekly_crop_health"] = weekly_health.count()
print(f"  ✔ fact_weekly_crop_health: {row_counts['fact_weekly_crop_health']:,} rows")

# COMMAND ----------

# ===================================================================
# FACT: ANOMALY EVENTS (empty schema — populated by anomaly engine)
# ===================================================================
print("▶ Creating fact_anomaly_events (schema-only placeholder)...")
anomaly_schema = StructType([
    StructField("event_id", StringType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("greenhouse_id", StringType()),
    StructField("zone_ids", StringType()),
    StructField("anomaly_type", StringType()),
    StructField("severity", StringType()),
    StructField("root_cause", StringType()),
    StructField("estimated_loss_usd", DoubleType()),
    StructField("prevented_loss_usd", DoubleType()),
    StructField("response_time_minutes", DoubleType()),
    StructField("resolution_status", StringType()),
])
spark.createDataFrame([], anomaly_schema) \
    .write.format("delta").mode("overwrite").saveAsTable("fact_anomaly_events")
row_counts["fact_anomaly_events"] = 0
print("  ✔ fact_anomaly_events: schema created (0 rows — populated at runtime)")

# COMMAND ----------

# ===================================================================
# Summary
# ===================================================================
print("\n" + "=" * 60)
print("Gold aggregation complete!")
print("=" * 60)
for table, cnt in row_counts.items():
    print(f"  {table:<35s} {cnt:>12,} rows")
print(f"\nTotal Gold tables: {len(row_counts)}")
print("Tables are ready for the Direct Lake semantic model.")
