import duckdb
import os


def run_import(data, run_path):

    db_path = os.path.join(run_path, "batchops.duckdb")
    conn = duckdb.connect(db_path)

    customers = data["customers"]
    transactions = data["transactions"]

    # Register DataFrames as temporary views
    conn.register("customers_df", customers)
    conn.register("transactions_df", transactions)

    # Idempotency: drop if exists
    conn.execute("DROP TABLE IF EXISTS raw_customers")
    conn.execute("DROP TABLE IF EXISTS raw_transactions")

    # Create raw tables from registered views
    conn.execute("""
        CREATE TABLE raw_customers AS
        SELECT * FROM customers_df
    """)

    conn.execute("""
        CREATE TABLE raw_transactions AS
        SELECT * FROM transactions_df
    """)

    conn.close()

    return db_path