import os
import json
import duckdb
import joblib
import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score


def run_train(run_path):

    # ----- Load Features from DuckDB -----
    db_path = os.path.join(run_path, "batchops.duckdb")
    conn = duckdb.connect(db_path)

    df = conn.execute("SELECT * FROM features_customer").fetchdf()
    conn.close()

    if df.empty:
        raise ValueError("Feature table is empty. Cannot train.")

    # ----- Synthetic Target -----
    # Define high spender as > median spend
    median_spend = df["total_spend_last_90d"].median()
    df["target"] = (df["total_spend_last_90d"] > median_spend).astype(int)

    # Guard: ensure at least 2 classes
    if df["target"].nunique() < 2:
        df["target"] = np.random.randint(0, 2, size=len(df))

    # ----- Feature Columns -----
    feature_cols = [
        "txn_count_last_30d",
        "total_spend_last_90d",
        "missing_email_flag"
    ]

    # Keep customer_id separately
    customer_ids = df["customer_id"]

    X = df[feature_cols]
    y = df["target"]

    # ----- Train/Test Split (Include IDs) -----
    X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
        X,
        y,
        customer_ids,
        test_size=0.3,
        random_state=42
    )

    # ----- Train Model -----
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    # ----- Evaluate -----
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    roc_auc = roc_auc_score(y_test, y_proba)
    accuracy = accuracy_score(y_test, y_pred)

    # ----- Save Model -----
    model_dir = os.path.join(run_path, "model")
    os.makedirs(model_dir, exist_ok=True)

    model_path = os.path.join(model_dir, "model.pkl")
    joblib.dump(model, model_path)

    # ----- Save Metrics -----
    metrics = {
        "roc_auc": float(roc_auc),
        "accuracy": float(accuracy),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "features": feature_cols
    }

    metrics_path = os.path.join(run_path, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    # ----- Return Everything Needed for Inference -----
    return {
        "model": model,
        "X_test": X_test,
        "customer_ids_test": ids_test.reset_index(drop=True),
        "metrics": metrics
    }