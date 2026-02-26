import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

def generate_provider_data(provider_name, n_customers=1000):

    np.random.seed(42)
    today = datetime.today()  # <-- move here

    base_path = os.path.join("data", provider_name)
    os.makedirs(base_path, exist_ok=True)

    # ---- Customers ----
    customer_ids = np.arange(1, n_customers + 1)

    emails = []
    for cid in customer_ids:
        if np.random.rand() < 0.15:
            emails.append(None)
        else:
            emails.append(f"user{cid}@example.com")

    signup_dates = []
    for _ in customer_ids:
        days_ago = np.random.randint(30, 365)
        signup_date = today - timedelta(days=int(days_ago))
        signup_dates.append(signup_date.strftime("%Y-%m-%d"))

    customers_df = pd.DataFrame({
        "customer_id": customer_ids,
        "signup_date": signup_dates,
        "email": emails
    })

    customers_df.to_csv(os.path.join(base_path, "customers.csv"), index=False)

    # ---- Transactions ----
    transactions = []

    for cid in customer_ids:
        n_txns = np.random.randint(20, 50)

        for _ in range(n_txns):
            days_ago = np.random.randint(0, 180)
            txn_date = today - timedelta(days=int(days_ago))
            amount = np.round(np.random.lognormal(mean=3.0, sigma=1.0), 2)

            transactions.append({
                "transaction_id": f"{cid}_{random.randint(1000, 9999)}",
                "customer_id": cid,
                "txn_date": txn_date.strftime("%Y-%m-%d"),
                "amount": amount
            })

    transactions_df = pd.DataFrame(transactions)

    transactions_df.to_csv(os.path.join(base_path, "transactions.csv"), index=False)

    print(f"Generated data for {provider_name}")
    print(f"Customers: {len(customers_df)}")
    print(f"Transactions: {len(transactions_df)}")
if __name__ == "__main__":
    generate_provider_data("provider_a", n_customers=1000)
    generate_provider_data("provider_b", n_customers=800)