# Fabric notebook source
# Yield Prediction Model — AgriTech Analytics
# Trains a LightGBM regression model to predict daily harvest yield per zone
# Uses MLflow for experiment tracking and model registry
#
# Prerequisites:
#   - Gold-layer tables loaded in the attached Lakehouse:
#     fact_daily_harvest, fact_zone_daily_environment, dim_crop, dim_zone
#   - Fabric ML runtime (MLflow pre-configured)

# COMMAND ----------
# Setup and imports
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error

# Try LightGBM, fall back to sklearn GBM
try:
    from lightgbm import LGBMRegressor
    USE_LIGHTGBM = True
    print("Using LightGBM")
except ImportError:
    from sklearn.ensemble import GradientBoostingRegressor as LGBMRegressor
    USE_LIGHTGBM = False
    print("LightGBM not available — falling back to sklearn GradientBoostingRegressor")

# COMMAND ----------
# Load data from Gold layer
fact_harvest = spark.read.table("fact_daily_harvest").toPandas()
fact_env = spark.read.table("fact_zone_daily_environment").toPandas()
dim_crop = spark.read.table("dim_crop").toPandas()
dim_zone = spark.read.table("dim_zone").toPandas()

print(f"Harvest records: {len(fact_harvest):,}")
print(f"Environment records: {len(fact_env):,}")
print(f"Crops: {dim_crop['crop_id'].nunique()}")
print(f"Zones: {dim_zone['zone_id'].nunique()}")

# COMMAND ----------
# Feature Engineering
# Join harvest with environment data on the same day/zone/greenhouse
df = fact_harvest.merge(
    fact_env,
    left_on=["harvest_date", "zone_id", "greenhouse_id"],
    right_on=["date", "zone_id", "greenhouse_id"],
    how="inner",
)

# Add crop properties
df = df.merge(
    dim_crop[["crop_id", "crop_category", "optimal_temp_c", "growth_cycle_days"]],
    on="crop_id",
    how="left",
)

# Add zone size
df = df.merge(dim_zone[["zone_id", "size_sqft"]], on="zone_id", how="left")

# Create derived features
df["month"] = pd.to_datetime(df["harvest_date"]).dt.month
df["day_of_year"] = pd.to_datetime(df["harvest_date"]).dt.dayofyear
df["temp_deviation"] = abs(df["avg_temp"] - df["optimal_temp_c"])
df["is_leafy_green"] = (df["crop_category"] == "leafy_green").astype(int)
df["yield_per_sqft"] = df["harvest_weight_kg"] / df["size_sqft"]

# Rolling features (7-day averages) — capture recent environmental trends
df = df.sort_values(["zone_id", "harvest_date"])
for col in ["avg_temp", "total_dli", "avg_humidity", "avg_vpd"]:
    df[f"{col}_7d_avg"] = df.groupby("zone_id")[col].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    )

print(f"Training dataset: {len(df):,} rows, {df.columns.size} columns")

# COMMAND ----------
# Prepare training data
feature_cols = [
    # Current-day environment
    "avg_temp", "avg_humidity", "avg_co2", "total_dli", "avg_vpd",
    "avg_ec", "avg_ph", "temp_excursion_hours", "hours_in_optimal_range",
    # Calendar features
    "month", "day_of_year", "days_to_harvest",
    # Crop & zone features
    "temp_deviation", "is_leafy_green", "size_sqft",
    # Rolling 7-day environmental trends
    "avg_temp_7d_avg", "total_dli_7d_avg", "avg_humidity_7d_avg", "avg_vpd_7d_avg",
]
target = "harvest_weight_kg"

df_model = df[feature_cols + [target]].dropna()
X = df_model[feature_cols]
y = df_model[target]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
print(f"Training set: {len(X_train):,} rows")
print(f"Test set:     {len(X_test):,} rows")
print(f"Target mean:  {y.mean():.2f} kg")
print(f"Target std:   {y.std():.2f} kg")

# COMMAND ----------
# Train with MLflow tracking
mlflow.set_experiment("AgriTech-YieldPrediction")

with mlflow.start_run(run_name="lgbm-yield-v1") as run:
    # Hyperparameters
    params = {
        "n_estimators": 300,
        "learning_rate": 0.05,
        "max_depth": 8,
        "num_leaves": 63,
        "min_child_samples": 20,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
    }

    if USE_LIGHTGBM:
        model = LGBMRegressor(**params, verbosity=-1)
    else:
        # sklearn GBM doesn't support all LightGBM params
        model = LGBMRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=8,
            min_samples_leaf=20,
            subsample=0.8,
            random_state=42,
        )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    # Evaluation metrics
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100

    mlflow.log_params(params)
    mlflow.log_metrics({"rmse": rmse, "r2": r2, "mape": mape})
    mlflow.log_param("model_type", "LightGBM" if USE_LIGHTGBM else "sklearn-GBM")
    mlflow.log_param("feature_count", len(feature_cols))
    mlflow.log_param("training_rows", len(X_train))

    # Log model artifact with signature
    signature = infer_signature(X_train, y_pred[: len(X_train)])
    mlflow.sklearn.log_model(model, "yield-predictor", signature=signature)

    # Feature importance
    if hasattr(model, "feature_importances_"):
        importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
        mlflow.log_dict(importance, "feature_importance.json")

    print("=" * 50)
    print("  Yield Prediction Model — Training Results")
    print("=" * 50)
    print(f"  RMSE:  {rmse:.2f} kg")
    print(f"  R²:    {r2:.4f}")
    print(f"  MAPE:  {mape:.1f}%")
    print(f"  Run:   {run.info.run_id}")
    print("=" * 50)

    # Register model in MLflow Model Registry
    model_uri = f"runs:/{run.info.run_id}/yield-predictor"
    registered = mlflow.register_model(model_uri, "AgriTech-YieldPredictor")
    print(f"\nModel registered: {registered.name} (version {registered.version})")

# COMMAND ----------
# Feature Importance Visualization
if hasattr(model, "feature_importances_"):
    imp_df = (
        pd.DataFrame({"feature": feature_cols, "importance": model.feature_importances_})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )

    print("\nTop 10 Features by Importance:")
    print("-" * 45)
    for _, row in imp_df.head(10).iterrows():
        bar = "█" * int(row["importance"] / imp_df["importance"].max() * 30)
        print(f"  {row['feature']:<30s} {bar}")

# COMMAND ----------
# Residual Analysis — check for systematic bias by crop type
df_test = X_test.copy()
df_test["actual"] = y_test.values
df_test["predicted"] = y_pred
df_test["residual"] = df_test["actual"] - df_test["predicted"]
df_test["abs_pct_error"] = abs(df_test["residual"] / df_test["actual"]) * 100

print("\nResidual Summary:")
print(f"  Mean residual:      {df_test['residual'].mean():.3f} kg (should be ~0)")
print(f"  Std residual:       {df_test['residual'].std():.3f} kg")
print(f"  Median abs % error: {df_test['abs_pct_error'].median():.1f}%")

print("\nError by crop type (leafy green vs vine):")
for crop_flag, label in [(1, "Leafy Greens"), (0, "Vine Crops / Berry")]:
    subset = df_test[df_test["is_leafy_green"] == crop_flag]
    if len(subset) > 0:
        print(f"  {label}: MAPE = {subset['abs_pct_error'].mean():.1f}%, n = {len(subset):,}")
