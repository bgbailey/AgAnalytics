# Fabric notebook source
# Anomaly Classifier — AgriTech Analytics
# Trains a Random Forest classifier to categorize greenhouse anomaly events
# Uses MLflow for experiment tracking and model registry
#
# Classes:
#   hvac_failure       — Heating/cooling system failure (temp deviation)
#   nutrient_drift     — Slow pH or EC drift outside optimal range
#   irrigation_failure — Pump failure causing moisture drop
#   cold_chain_break   — Post-harvest refrigeration failure
#   unknown            — Unclassified anomaly
#
# Prerequisites:
#   - Gold-layer tables in attached Lakehouse:
#     fact_anomaly_events, fact_zone_daily_environment, dim_zone
#   - Fabric ML runtime (MLflow pre-configured)

# COMMAND ----------
# Setup and imports
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
    f1_score,
)
from sklearn.preprocessing import LabelEncoder

# COMMAND ----------
# Load data from Gold layer
fact_anomaly = spark.read.table("fact_anomaly_events").toPandas()
fact_env = spark.read.table("fact_zone_daily_environment").toPandas()
dim_zone = spark.read.table("dim_zone").toPandas()

print(f"Anomaly events: {len(fact_anomaly):,}")
print(f"Environment records: {len(fact_env):,}")
print(f"\nAnomaly type distribution:")
print(fact_anomaly["anomaly_type"].value_counts().to_string())

# COMMAND ----------
# Feature Engineering
# Extract the date from anomaly start_time for environment join
fact_anomaly["event_date"] = pd.to_datetime(fact_anomaly["start_time"]).dt.date
fact_anomaly["event_date"] = pd.to_datetime(fact_anomaly["event_date"])
fact_env["date"] = pd.to_datetime(fact_env["date"])

# Join anomaly events with environment data from that day
df = fact_anomaly.merge(
    fact_env,
    left_on=["event_date", "zone_id", "greenhouse_id"],
    right_on=["date", "zone_id", "greenhouse_id"],
    how="left",
)

# Add zone properties
df = df.merge(dim_zone[["zone_id", "size_sqft", "primary_crop_id"]], on="zone_id", how="left")

# Temporal features from the anomaly event
df["hour_of_day"] = pd.to_datetime(df["start_time"]).dt.hour
df["day_of_week"] = pd.to_datetime(df["start_time"]).dt.dayofweek
df["month"] = pd.to_datetime(df["start_time"]).dt.month
df["is_night"] = ((df["hour_of_day"] < 6) | (df["hour_of_day"] >= 22)).astype(int)
df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

# Anomaly signature features
df["duration_minutes"] = pd.to_numeric(df.get("duration_minutes", pd.Series(dtype=float)), errors="coerce")
df["affected_sensor_count"] = pd.to_numeric(df.get("affected_sensor_count", pd.Series(dtype=float)), errors="coerce")

# Temperature-related signals
df["temp_deviation"] = abs(df["avg_temp"] - df.get("setpoint_temp", df["avg_temp"]))
df["temp_rate_of_change"] = df.get("temp_rate_of_change", 0.0)

# Moisture/irrigation signals
df["moisture_drop"] = df.get("moisture_drop_pct", 0.0)

# pH/EC drift signals
df["ph_deviation"] = abs(df.get("avg_ph", 6.0) - 6.0)
df["ec_deviation"] = abs(df.get("avg_ec", 2.0) - 2.0)

# Boolean indicator columns for affected systems
for system in ["heating", "cooling", "irrigation", "nutrient", "cold_chain"]:
    col = f"affected_{system}"
    if col in df.columns:
        df[col] = df[col].astype(int)
    else:
        df[col] = 0

print(f"Feature-engineered dataset: {len(df):,} rows")

# COMMAND ----------
# Prepare training data
feature_cols = [
    # Environmental context
    "avg_temp", "avg_humidity", "avg_co2", "total_dli", "avg_vpd",
    "avg_ec", "avg_ph", "temp_excursion_hours", "hours_in_optimal_range",
    # Temporal features
    "hour_of_day", "day_of_week", "month", "is_night", "is_weekend",
    # Anomaly signature
    "duration_minutes", "affected_sensor_count",
    "temp_deviation", "moisture_drop", "ph_deviation", "ec_deviation",
    # Affected system indicators
    "affected_heating", "affected_cooling", "affected_irrigation",
    "affected_nutrient", "affected_cold_chain",
]
target = "anomaly_type"

# Keep only rows with valid target and features
ANOMALY_CLASSES = ["hvac_failure", "nutrient_drift", "irrigation_failure", "cold_chain_break", "unknown"]
df_model = df[df[target].isin(ANOMALY_CLASSES)].copy()
df_model = df_model[feature_cols + [target]].dropna(subset=feature_cols)

# Encode target labels
le = LabelEncoder()
le.fit(ANOMALY_CLASSES)
df_model["target_encoded"] = le.transform(df_model[target])

X = df_model[feature_cols].astype(float)
y = df_model["target_encoded"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Training set: {len(X_train):,} rows")
print(f"Test set:     {len(X_test):,} rows")
print(f"Classes:      {list(le.classes_)}")

# COMMAND ----------
# Train with MLflow tracking
mlflow.set_experiment("AgriTech-AnomalyClassification")

with mlflow.start_run(run_name="rf-anomaly-v1") as run:
    params = {
        "n_estimators": 200,
        "max_depth": 12,
        "min_samples_split": 5,
        "min_samples_leaf": 3,
        "max_features": "sqrt",
        "class_weight": "balanced",
        "random_state": 42,
    }

    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)

    # Metrics
    accuracy = accuracy_score(y_test, y_pred)
    f1_weighted = f1_score(y_test, y_pred, average="weighted")
    f1_macro = f1_score(y_test, y_pred, average="macro")

    # Cross-validation score
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X, y, cv=cv, scoring="f1_weighted")

    mlflow.log_params(params)
    mlflow.log_metrics({
        "accuracy": accuracy,
        "f1_weighted": f1_weighted,
        "f1_macro": f1_macro,
        "cv_f1_mean": cv_scores.mean(),
        "cv_f1_std": cv_scores.std(),
    })
    mlflow.log_param("num_classes", len(ANOMALY_CLASSES))
    mlflow.log_param("training_rows", len(X_train))

    # Log model
    signature = infer_signature(X_train, y_pred[: len(X_train)])
    mlflow.sklearn.log_model(model, "anomaly-classifier", signature=signature)

    # Log class label mapping
    mlflow.log_dict(
        {str(i): label for i, label in enumerate(le.classes_)},
        "class_labels.json",
    )

    # Feature importance
    if hasattr(model, "feature_importances_"):
        importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
        mlflow.log_dict(importance, "feature_importance.json")

    print("=" * 55)
    print("  Anomaly Classifier — Training Results")
    print("=" * 55)
    print(f"  Accuracy:        {accuracy:.4f}")
    print(f"  F1 (weighted):   {f1_weighted:.4f}")
    print(f"  F1 (macro):      {f1_macro:.4f}")
    print(f"  CV F1 (5-fold):  {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    print(f"  Run:             {run.info.run_id}")
    print("=" * 55)

    # Register model
    model_uri = f"runs:/{run.info.run_id}/anomaly-classifier"
    registered = mlflow.register_model(model_uri, "AgriTech-AnomalyClassifier")
    print(f"\nModel registered: {registered.name} (version {registered.version})")

# COMMAND ----------
# Confusion Matrix
class_names = list(le.classes_)
cm = confusion_matrix(y_test, y_pred)

print("\nConfusion Matrix:")
print(f"{'':>20s}", end="")
for name in class_names:
    print(f"{name:>18s}", end="")
print()
print("-" * (20 + 18 * len(class_names)))
for i, row_name in enumerate(class_names):
    print(f"{row_name:>20s}", end="")
    for j in range(len(class_names)):
        val = cm[i][j]
        marker = " ◄" if i == j else ""
        print(f"{val:>16d}{marker:>2s}", end="")
    print()

# COMMAND ----------
# Per-Class Classification Report
report = classification_report(y_test, y_pred, target_names=class_names, output_dict=True)

print("\nPer-Class Metrics:")
print(f"{'Class':<22s} {'Precision':>10s} {'Recall':>10s} {'F1-Score':>10s} {'Support':>10s}")
print("-" * 62)
for cls in class_names:
    metrics = report[cls]
    print(
        f"{cls:<22s} {metrics['precision']:>10.3f} {metrics['recall']:>10.3f} "
        f"{metrics['f1-score']:>10.3f} {metrics['support']:>10.0f}"
    )

# COMMAND ----------
# Feature Importance — top predictors for anomaly classification
if hasattr(model, "feature_importances_"):
    imp_df = (
        pd.DataFrame({"feature": feature_cols, "importance": model.feature_importances_})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )

    print("\nTop 10 Features by Importance:")
    print("-" * 50)
    for _, row in imp_df.head(10).iterrows():
        bar = "█" * int(row["importance"] / imp_df["importance"].max() * 30)
        print(f"  {row['feature']:<30s} {bar}")

# COMMAND ----------
# Misclassification Analysis — which anomaly types are hardest to distinguish?
df_test = X_test.copy()
df_test["actual"] = le.inverse_transform(y_test)
df_test["predicted"] = le.inverse_transform(y_pred)
df_test["correct"] = df_test["actual"] == df_test["predicted"]

misclassified = df_test[~df_test["correct"]]
if len(misclassified) > 0:
    print(f"\nMisclassified: {len(misclassified)} of {len(df_test)} ({len(misclassified)/len(df_test)*100:.1f}%)")
    print("\nMost common misclassification pairs:")
    pairs = (
        misclassified.groupby(["actual", "predicted"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    for _, row in pairs.head(5).iterrows():
        print(f"  {row['actual']} → {row['predicted']}: {row['count']} cases")
else:
    print("\nNo misclassifications in test set!")
