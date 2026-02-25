import os
import json


def run_validation(data, run_path):

    dq_report = {
        "checks": {},
        "status": "PASS"
    }

    customers = data.get("customers")
    transactions = data.get("transactions")

    # ---- Check 1: Required Columns ----
    required_customer_cols = {"customer_id", "signup_date", "email"}
    required_txn_cols = {"customer_id", "txn_date", "amount"}

    if not required_customer_cols.issubset(set(customers.columns)):
        dq_report["checks"]["customers_required_columns"] = "FAIL"
        dq_report["status"] = "FAIL"
    else:
        dq_report["checks"]["customers_required_columns"] = "PASS"

    if not required_txn_cols.issubset(set(transactions.columns)):
        dq_report["checks"]["transactions_required_columns"] = "FAIL"
        dq_report["status"] = "FAIL"
    else:
        dq_report["checks"]["transactions_required_columns"] = "PASS"

    # ---- Check 2: Duplicate Customers ----
    duplicate_count = customers.duplicated(subset=["customer_id"]).sum()
    dq_report["checks"]["duplicate_customers"] = int(duplicate_count)

    if duplicate_count > 0:
        dq_report["status"] = "FAIL"

    # ---- Check 3: Missing Email Rate ----
    missing_email_rate = customers["email"].isna().mean()
    dq_report["checks"]["missing_email_rate"] = float(missing_email_rate)
    if missing_email_rate > 0.5:
        dq_report["checks"]["missing_email_status"] = "FAIL"
        dq_report["status"] = "FAIL"
    elif missing_email_rate > 0.2:
        dq_report["checks"]["missing_email_status"] = "WARN"
    else:
        dq_report["checks"]["missing_email_status"] = "PASS"

    # ---- Write JSON Report ----
    with open(os.path.join(run_path, "dq_report.json"), "w") as f:
        json.dump(dq_report, f, indent=2)

    # ---- Write Markdown Report ----
    md_content = f"""
# Data Quality Report

## Overall Status: {dq_report['status']}

## Checks
- Customers required columns: {dq_report['checks']['customers_required_columns']}
- Transactions required columns: {dq_report['checks']['transactions_required_columns']}
- Duplicate customers: {dq_report['checks']['duplicate_customers']}
- Missing email rate: {dq_report['checks']['missing_email_rate']:.2f}
"""

    with open(os.path.join(run_path, "dq_report.md"), "w") as f:
        f.write(md_content)

    return dq_report["status"]