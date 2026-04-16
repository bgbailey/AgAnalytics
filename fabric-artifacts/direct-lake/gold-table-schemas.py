# Databricks notebook source / Microsoft Fabric Notebook
# ============================================================================
# Gold Layer Table Creation — AgriTech Analytics
# ============================================================================
#
# Run this notebook in Microsoft Fabric to create all 12 Gold-layer Delta
# tables in the attached Lakehouse.  These tables are consumed directly by
# the Direct Lake semantic model (zero-copy — no Import refresh needed).
#
# Tables created:
#   FACTS  (6): fact_daily_harvest, fact_zone_daily_environment,
#               fact_daily_energy, fact_shipments, fact_weekly_crop_health,
#               fact_anomaly_events
#   DIMS   (6): dim_date, dim_greenhouse, dim_zone, dim_crop,
#               dim_customer, dim_product
#
# Prerequisites:
#   • Attached Lakehouse (Gold layer)
#   • Spark runtime 3.5+ (Fabric default)
# ============================================================================

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ============================================================================
# Helper — write an empty Delta table from a schema
# ============================================================================

def create_delta_table(table_name: str, schema: StructType, partition_col: str = None):
    """Create (or replace) an empty Delta table in the attached Lakehouse."""
    df = spark.createDataFrame([], schema)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.saveAsTable(table_name)
    print(f"  ✅  {table_name} ({len(schema.fields)} cols"
          f"{', partitioned by ' + partition_col if partition_col else ''})")

# ============================================================================
#  FACT TABLES
# ============================================================================

print("=" * 60)
print("Creating FACT tables …")
print("=" * 60)

# ---------- fact_daily_harvest ----------

fact_daily_harvest_schema = StructType([
    StructField("harvest_date",         DateType(),    False),
    StructField("zone_id",              StringType(),  False),
    StructField("greenhouse_id",        StringType(),  False),
    StructField("crop_id",              StringType(),  False),
    StructField("harvest_weight_kg",    DoubleType(),  True),
    StructField("harvest_units",        IntegerType(), True),
    StructField("grade_a_pct",          DoubleType(),  True),
    StructField("grade_b_pct",          DoubleType(),  True),
    StructField("grade_c_pct",          DoubleType(),  True),
    StructField("waste_kg",             DoubleType(),  True),
    StructField("waste_pct",            DoubleType(),  True),
    StructField("days_to_harvest",      IntegerType(), True),
    StructField("revenue_estimate_usd", DoubleType(),  True),
])

create_delta_table("fact_daily_harvest", fact_daily_harvest_schema, "harvest_date")

# ---------- fact_zone_daily_environment ----------

fact_zone_daily_environment_schema = StructType([
    StructField("date",                   DateType(),    False),
    StructField("zone_id",                StringType(),  False),
    StructField("greenhouse_id",          StringType(),  False),
    StructField("avg_temp",               DoubleType(),  True),
    StructField("min_temp",               DoubleType(),  True),
    StructField("max_temp",               DoubleType(),  True),
    StructField("avg_humidity",           DoubleType(),  True),
    StructField("avg_co2",                DoubleType(),  True),
    StructField("total_dli",              DoubleType(),  True),
    StructField("avg_vpd",                DoubleType(),  True),
    StructField("avg_ec",                 DoubleType(),  True),
    StructField("avg_ph",                 DoubleType(),  True),
    StructField("hours_in_optimal_range", DoubleType(),  True),
    StructField("temp_excursion_hours",   DoubleType(),  True),
])

create_delta_table("fact_zone_daily_environment", fact_zone_daily_environment_schema, "date")

# ---------- fact_daily_energy ----------

fact_daily_energy_schema = StructType([
    StructField("date",                   DateType(),    False),
    StructField("greenhouse_id",          StringType(),  False),
    StructField("electricity_kwh",        DoubleType(),  True),
    StructField("natural_gas_m3",         DoubleType(),  True),
    StructField("water_liters",           DoubleType(),  True),
    StructField("water_recycled_liters",  DoubleType(),  True),
    StructField("co2_purchased_kg",       DoubleType(),  True),
    StructField("energy_cost_usd",        DoubleType(),  True),
    StructField("energy_per_kg_produce",  DoubleType(),  True),
])

create_delta_table("fact_daily_energy", fact_daily_energy_schema, "date")

# ---------- fact_shipments ----------

fact_shipments_schema = StructType([
    StructField("ship_date",                DateType(),    False),
    StructField("order_id",                 StringType(),  False),
    StructField("greenhouse_id",            StringType(),  False),
    StructField("customer_id",              StringType(),  False),
    StructField("product_id",               StringType(),  False),
    StructField("crop_id",                  StringType(),  False),
    StructField("quantity_cases",           IntegerType(), True),
    StructField("quantity_ordered",         IntegerType(), True),
    StructField("delivery_status",          StringType(),  True),
    StructField("cold_chain_compliant",     BooleanType(), True),
    StructField("shelf_life_remaining_days", IntegerType(), True),
    StructField("on_time_flag",             BooleanType(), True),
])

create_delta_table("fact_shipments", fact_shipments_schema, "ship_date")

# ---------- fact_weekly_crop_health ----------

fact_weekly_crop_health_schema = StructType([
    StructField("week_start",             DateType(),    False),
    StructField("zone_id",                StringType(),  False),
    StructField("greenhouse_id",          StringType(),  False),
    StructField("crop_id",                StringType(),  False),
    StructField("avg_plant_height_cm",    DoubleType(),  True),
    StructField("avg_leaf_count",         DoubleType(),  True),
    StructField("predicted_quality_grade", StringType(), True),
    StructField("predicted_yield_kg",     DoubleType(),  True),
])

create_delta_table("fact_weekly_crop_health", fact_weekly_crop_health_schema, "week_start")

# ---------- fact_anomaly_events ----------

fact_anomaly_events_schema = StructType([
    StructField("event_id",               StringType(),    False),
    StructField("start_time",             TimestampType(), False),
    StructField("end_time",               TimestampType(), True),
    StructField("greenhouse_id",          StringType(),    False),
    StructField("zone_ids",               StringType(),    True),
    StructField("anomaly_type",           StringType(),    True),
    StructField("severity",               StringType(),    True),
    StructField("root_cause",             StringType(),    True),
    StructField("estimated_loss_usd",     DoubleType(),    True),
    StructField("prevented_loss_usd",     DoubleType(),    True),
    StructField("response_time_minutes",  DoubleType(),    True),
    StructField("resolution_status",      StringType(),    True),
])

create_delta_table("fact_anomaly_events", fact_anomaly_events_schema)

print()

# ============================================================================
#  DIMENSION TABLES
# ============================================================================

print("=" * 60)
print("Creating DIMENSION tables …")
print("=" * 60)

# ---------- dim_date  (fully populated: 2024-04-01 → 2026-04-15) ----------

from datetime import date, timedelta

def _season(month: int) -> str:
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7, 8):
        return "Summer"
    if month in (9, 10, 11):
        return "Fall"
    return "Winter"

start_date = date(2024, 4, 1)
end_date   = date(2026, 4, 15)

date_rows = []
current = start_date
while current <= end_date:
    iso_cal = current.isocalendar()
    date_rows.append((
        current,                              # date_key
        current,                              # date
        current.strftime("%A"),               # day_name
        iso_cal[2],                           # day_of_week (1=Mon .. 7=Sun)
        iso_cal[1],                           # week_num
        current.strftime("%B"),               # month_name
        current.month,                        # month_num
        f"Q{(current.month - 1) // 3 + 1}",  # quarter
        current.year,                         # year
        current.year,                         # fiscal_year
        iso_cal[2] >= 6,                      # is_weekend (Sat=6, Sun=7)
        _season(current.month),               # season
        current.timetuple().tm_yday,          # day_of_year
    ))
    current += timedelta(days=1)

dim_date_schema = StructType([
    StructField("date_key",     DateType(),    False),
    StructField("date",         DateType(),    False),
    StructField("day_name",     StringType(),  False),
    StructField("day_of_week",  IntegerType(), False),
    StructField("week_num",     IntegerType(), False),
    StructField("month_name",   StringType(),  False),
    StructField("month_num",    IntegerType(), False),
    StructField("quarter",      StringType(),  False),
    StructField("year",         IntegerType(), False),
    StructField("fiscal_year",  IntegerType(), False),
    StructField("is_weekend",   BooleanType(), False),
    StructField("season",       StringType(),  False),
    StructField("day_of_year",  IntegerType(), False),
])

spark.createDataFrame(date_rows, dim_date_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_date")

print(f"  ✅  dim_date ({len(date_rows)} rows, 2024-04-01 → 2026-04-15)")

# ---------- dim_greenhouse ----------

greenhouse_rows = [
    ("brightharvest", "BrightHarvest Greens", "Rochelle",   "Illinois", "US",     41.92, -89.07, 200000, 8, "Hydroponic NFT"),
    ("mucci-valley",  "Mucci Valley Farms",   "Kingsville", "Ontario",  "Canada", 42.04, -82.74, 250000, 8, "Hydroponic Drip Irrigation"),
]

dim_greenhouse_schema = StructType([
    StructField("greenhouse_id",  StringType(),  False),
    StructField("name",           StringType(),  False),
    StructField("location_city",  StringType(),  False),
    StructField("location_state", StringType(),  False),
    StructField("country",        StringType(),  False),
    StructField("latitude",       DoubleType(),  True),
    StructField("longitude",      DoubleType(),  True),
    StructField("size_sqft",      IntegerType(), True),
    StructField("zone_count",     IntegerType(), True),
    StructField("growing_method", StringType(),  True),
])

spark.createDataFrame(greenhouse_rows, dim_greenhouse_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_greenhouse")

print(f"  ✅  dim_greenhouse ({len(greenhouse_rows)} rows)")

# ---------- dim_zone ----------

zone_rows = [
    # BrightHarvest Greens — 8 zones
    ("BH-Z01", "brightharvest", "Zone 1", 25000, "baby_spinach"),
    ("BH-Z02", "brightharvest", "Zone 2", 25000, "baby_spinach"),
    ("BH-Z03", "brightharvest", "Zone 3", 25000, "romaine"),
    ("BH-Z04", "brightharvest", "Zone 4", 25000, "romaine"),
    ("BH-Z05", "brightharvest", "Zone 5", 25000, "arugula"),
    ("BH-Z06", "brightharvest", "Zone 6", 25000, "arugula"),
    ("BH-Z07", "brightharvest", "Zone 7", 25000, "basil"),
    ("BH-Z08", "brightharvest", "Zone 8", 25000, "basil"),
    # Mucci Valley Farms — 8 zones
    ("MV-Z01", "mucci-valley", "Zone 1", 31250, "cocktail_tomato"),
    ("MV-Z02", "mucci-valley", "Zone 2", 31250, "cocktail_tomato"),
    ("MV-Z03", "mucci-valley", "Zone 3", 31250, "bell_pepper"),
    ("MV-Z04", "mucci-valley", "Zone 4", 31250, "bell_pepper"),
    ("MV-Z05", "mucci-valley", "Zone 5", 31250, "mini_cucumber"),
    ("MV-Z06", "mucci-valley", "Zone 6", 31250, "mini_cucumber"),
    ("MV-Z07", "mucci-valley", "Zone 7", 31250, "strawberry"),
    ("MV-Z08", "mucci-valley", "Zone 8", 31250, "strawberry"),
]

dim_zone_schema = StructType([
    StructField("zone_id",         StringType(),  False),
    StructField("greenhouse_id",   StringType(),  False),
    StructField("zone_name",       StringType(),  False),
    StructField("size_sqft",       IntegerType(), True),
    StructField("primary_crop_id", StringType(),  True),
])

spark.createDataFrame(zone_rows, dim_zone_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_zone")

print(f"  ✅  dim_zone ({len(zone_rows)} rows)")

# ---------- dim_crop ----------

crop_rows = [
    ("baby_spinach",    "Baby Spinach",     "leafy_green", 30,  18.0, 70.0,  4.40, 10),
    ("romaine",         "Romaine Lettuce",  "leafy_green", 35,  20.0, 65.0,  3.80, 12),
    ("arugula",         "Arugula",          "leafy_green", 25,  19.0, 68.0,  5.20,  8),
    ("basil",           "Basil",            "leafy_green", 28,  24.0, 60.0,  8.80,  7),
    ("cocktail_tomato", "Cocktail Tomato",  "vine_crop",   270, 23.0, 70.0,  5.50, 14),
    ("bell_pepper",     "Bell Pepper",      "vine_crop",   240, 22.0, 65.0,  4.20, 14),
    ("mini_cucumber",   "Mini Cucumber",    "vine_crop",   180, 24.0, 75.0,  3.60, 10),
    ("strawberry",      "Strawberry",       "berry",       300, 20.0, 65.0,  9.00,  5),
]

dim_crop_schema = StructType([
    StructField("crop_id",                 StringType(),  False),
    StructField("crop_name",               StringType(),  False),
    StructField("crop_category",           StringType(),  False),
    StructField("growth_cycle_days",       IntegerType(), True),
    StructField("optimal_temp_c",          DoubleType(),  True),
    StructField("optimal_humidity_pct",    DoubleType(),  True),
    StructField("market_price_per_kg_usd", DoubleType(),  True),
    StructField("shelf_life_days",         IntegerType(), True),
])

spark.createDataFrame(crop_rows, dim_crop_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_crop")

print(f"  ✅  dim_crop ({len(crop_rows)} rows)")

# ---------- dim_customer ----------

customer_rows = [
    ("freshmart",        "FreshMart Groceries", "Midwest US",    "US",     "daily"),
    ("greenleaf",        "GreenLeaf Markets",   "Northeast US",  "US",     "3x_weekly"),
    ("harvest-co",       "Harvest Co. Foods",   "Southeast US",  "US",     "3x_weekly"),
    ("maple-fresh",      "Maple Fresh",         "Ontario",       "Canada", "daily"),
    ("northern-harvest", "Northern Harvest",    "Quebec",        "Canada", "3x_weekly"),
    ("pacific-organics", "Pacific Organics",    "West Coast US", "US",     "weekly"),
]

dim_customer_schema = StructType([
    StructField("customer_id",        StringType(), False),
    StructField("name",               StringType(), False),
    StructField("region",             StringType(), True),
    StructField("country",            StringType(), True),
    StructField("delivery_frequency", StringType(), True),
])

spark.createDataFrame(customer_rows, dim_customer_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_customer")

print(f"  ✅  dim_customer ({len(customer_rows)} rows)")

# ---------- dim_product ----------

product_rows = [
    # Leafy greens (BrightHarvest Greens)
    ("PROD-BH-001", "baby_spinach",    "Baby Spinach 5oz Clamshell",     "clamshell", 0.142, "BH-BSP-5OZ"),
    ("PROD-BH-002", "romaine",         "Romaine Hearts 3-Pack Bag",      "bag",       0.340, "BH-ROM-3PK"),
    ("PROD-BH-003", "arugula",         "Arugula 5oz Clamshell",          "clamshell", 0.142, "BH-ARU-5OZ"),
    ("PROD-BH-004", "basil",           "Basil Living 2oz Clamshell",     "clamshell", 0.057, "BH-BAS-2OZ"),
    # Vine crops (Mucci Valley Farms)
    ("PROD-MV-001", "cocktail_tomato", "Cocktail Tomato 12oz Case",      "case",      0.340, "MV-TOM-12Z"),
    ("PROD-MV-002", "bell_pepper",     "Bell Pepper Tri-Color 3ct Case", "case",      0.680, "MV-PEP-3CT"),
    ("PROD-MV-003", "mini_cucumber",   "Mini Cucumber 1lb Bag",          "bag",       0.454, "MV-CUC-1LB"),
    ("PROD-MV-004", "strawberry",      "Strawberry 1lb Clamshell",       "clamshell", 0.454, "MV-STR-1LB"),
    # Variety packs
    ("PROD-VP-001", "arugula",         "Mixed Greens Medley 10oz Bag",   "bag",       0.284, "VP-MXG-10Z"),
    ("PROD-VP-002", "cocktail_tomato", "Mixed Veggies Harvest Case",     "case",      2.270, "VP-MXV-5LB"),
]

dim_product_schema = StructType([
    StructField("product_id",        StringType(), False),
    StructField("crop_id",           StringType(), False),
    StructField("product_name",      StringType(), False),
    StructField("package_type",      StringType(), True),
    StructField("package_weight_kg", DoubleType(), True),
    StructField("sku",               StringType(), True),
])

spark.createDataFrame(product_rows, dim_product_schema) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_product")

print(f"  ✅  dim_product ({len(product_rows)} rows)")

# ============================================================================
#  Summary
# ============================================================================

print()
print("=" * 60)
print("Gold-layer table creation complete!")
print("=" * 60)

all_tables = [
    "fact_daily_harvest", "fact_zone_daily_environment", "fact_daily_energy",
    "fact_shipments", "fact_weekly_crop_health", "fact_anomaly_events",
    "dim_date", "dim_greenhouse", "dim_zone", "dim_crop",
    "dim_customer", "dim_product",
]

print(f"\n  {len(all_tables)} tables created in attached Lakehouse:\n")
for t in all_tables:
    count = spark.table(t).count()
    print(f"    {t:40s}  {count:>6,} rows")

print("\n  These tables are ready for the Direct Lake semantic model.")
print("  No Import refresh needed — Power BI reads Delta files directly.")
