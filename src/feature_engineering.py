import duckdb
import os


def run_feature_etl(run_path, snapshot_date):

    db_path = os.path.join(run_path, "batchops.duckdb")
    conn = duckdb.connect(db_path)

    # Idempotency
    conn.execute("DROP TABLE IF EXISTS features_customer")

    query = f"""
    CREATE TABLE features_customer AS
    SELECT
        c.customer_id,
        DATE '{snapshot_date}' AS snapshot_date,

        COUNT(
            CASE
                WHEN CAST(t.txn_date AS DATE) >= DATE '{snapshot_date}' - INTERVAL 30 DAY
                THEN 1
            END
        ) AS txn_count_last_30d,

        SUM(
            CASE
                WHEN CAST(t.txn_date AS DATE) >= DATE '{snapshot_date}' - INTERVAL 90 DAY
                THEN t.amount
                ELSE 0
            END
        ) AS total_spend_last_90d,

        CASE
            WHEN c.email IS NULL OR c.email = ''
            THEN 1
            ELSE 0
        END AS missing_email_flag

    FROM raw_customers c
    LEFT JOIN raw_transactions t
        ON c.customer_id = t.customer_id

    GROUP BY c.customer_id, c.email
    """

    conn.execute(query)

    conn.close()