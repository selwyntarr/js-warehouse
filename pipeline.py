"""
ETL pipeline: reads orders/customers/products CSVs, validates the data,
and loads a star-schema SQLite warehouse (warehouse.db).

Idempotent: re-running will not duplicate rows (INSERT OR IGNORE on natural keys).
"""

import sqlite3
import warnings
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
DB_PATH = Path("warehouse.db")

KNOWN_STATUSES = {"completed", "pending", "returned"}


# ---------------------------------------------------------------------------
# 1. Read CSVs
# ---------------------------------------------------------------------------

def read_csv():
    customers = pd.read_csv(DATA_DIR / "customers.csv", dtype=str).apply(
        lambda col: col.str.strip() if col.dtype == object else col
    )
    products = pd.read_csv(DATA_DIR / "products.csv", dtype=str).apply(
        lambda col: col.str.strip() if col.dtype == object else col
    )
    orders = pd.read_csv(DATA_DIR / "orders.csv", dtype=str).apply(
        lambda col: col.str.strip() if col.dtype == object else col
    )

    # coerce numeric columns
    products["cost_price"] = pd.to_numeric(products["cost_price"], errors="coerce")
    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce")
    orders["unit_price"] = pd.to_numeric(orders["unit_price"], errors="coerce")

    return customers, products, orders


# ---------------------------------------------------------------------------
# 2. Validate
# ---------------------------------------------------------------------------

def validate(customers, products, orders):
    issues = 0

    # null checks on required fields
    for df_name, df, required in [
        ("customers", customers, ["customer_id", "segment"]),
        ("products", products, ["product_id", "category", "cost_price"]),
        ("orders", orders, ["order_id", "customer_id", "product_id", "quantity", "unit_price", "status"]),
    ]:
        null_mask = df[required].isnull().any(axis=1)
        if null_mask.any():
            warnings.warn(
                f"[{df_name}] {null_mask.sum()} row(s) have nulls in required fields — "
                f"these rows will be skipped.\n{df[null_mask]}"
            )
            issues += null_mask.sum()

    # quantity <= 0
    bad_qty = orders[orders["quantity"].notna() & (orders["quantity"] <= 0)]
    if not bad_qty.empty:
        warnings.warn(
            f"[orders] {len(bad_qty)} row(s) with quantity <= 0 "
            f"(order_ids: {bad_qty['order_id'].tolist()}). "
            "Keeping rows; revenue and profit will be 0 or negative."
        )

    # unknown status values
    unknown_status = orders[~orders["status"].isin(KNOWN_STATUSES)]
    if not unknown_status.empty:
        warnings.warn(
            f"[orders] {len(unknown_status)} row(s) with unrecognised status values: "
            f"{unknown_status['status'].unique().tolist()}"
        )
        issues += len(unknown_status)

    return issues


# ---------------------------------------------------------------------------
# 3. Create tables
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    sk_customer  INTEGER PRIMARY KEY,
    customer_id  TEXT    NOT NULL UNIQUE,
    name         TEXT,
    email        TEXT,
    city         TEXT,
    state        TEXT,
    signup_date  DATE,
    segment      TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    sk_product   INTEGER PRIMARY KEY,
    product_id   TEXT    NOT NULL UNIQUE,
    product_name TEXT,
    category     TEXT,
    subcategory  TEXT,
    cost_price   REAL
);

CREATE TABLE IF NOT EXISTS fact_orders (
    sk_order     INTEGER PRIMARY KEY,
    order_id     TEXT    NOT NULL UNIQUE,
    sk_customer  INTEGER REFERENCES dim_customer(sk_customer),
    sk_product   INTEGER REFERENCES dim_product(sk_product),
    order_date   DATE,
    quantity     INTEGER,
    unit_price   REAL,
    revenue      REAL,
    profit       REAL,
    status       TEXT,
    is_returned  INTEGER NOT NULL DEFAULT 0
);
"""


def create_tables(conn):
    conn.executescript(DDL)
    conn.commit()


# ---------------------------------------------------------------------------
# 4. Load dimensions
# ---------------------------------------------------------------------------

def load_dimensions(conn, customers, products):
    # drop rows with null required fields before inserting
    customers_clean = customers.dropna(subset=["customer_id"])
    products_clean = products.dropna(subset=["product_id"])

    customers_clean[
        ["customer_id", "name", "email", "city", "state", "signup_date", "segment"]
    ].to_sql("dim_customer", conn, if_exists="append", index=False, method=_insert_or_ignore)

    products_clean[
        ["product_id", "product_name", "category", "subcategory", "cost_price"]
    ].to_sql("dim_product", conn, if_exists="append", index=False, method=_insert_or_ignore)

    conn.commit()


def _insert_or_ignore(table, conn, keys, data_iter):
    """pandas to_sql method that uses INSERT OR IGNORE for idempotency."""
    cols = ", ".join(keys)
    placeholders = ", ".join(["?"] * len(keys))
    sql = f"INSERT OR IGNORE INTO {table.name} ({cols}) VALUES ({placeholders})"
    conn.executemany(sql, data_iter)


# ---------------------------------------------------------------------------
# 5. Load facts
# ---------------------------------------------------------------------------

def load_facts(conn, orders, products):
    orders_clean = orders.dropna(
        subset=["order_id", "customer_id", "product_id", "quantity", "unit_price", "status"]
    )

    # fetch surrogate keys
    sk_customers = pd.read_sql("SELECT sk_customer, customer_id FROM dim_customer", conn)
    sk_products = pd.read_sql(
        "SELECT sk_product, product_id, cost_price FROM dim_product", conn
    )

    # join to get surrogate keys + cost_price for profit
    df = (
        orders_clean
        .merge(sk_customers, on="customer_id", how="left")
        .merge(sk_products, on="product_id", how="left")
    )

    # warn about any orders with unresolved FK references
    unresolved = df[df["sk_customer"].isna() | df["sk_product"].isna()]
    if not unresolved.empty:
        warnings.warn(
            f"[orders] {len(unresolved)} row(s) could not be matched to a customer or product "
            f"and will be skipped: {unresolved['order_id'].tolist()}"
        )
        df = df.dropna(subset=["sk_customer", "sk_product"])

    df["revenue"] = df["quantity"] * df["unit_price"]
    df["profit"] = df["revenue"] - (df["quantity"] * df["cost_price"])
    df["is_returned"] = (df["status"] == "returned").astype(int)

    fact_cols = [
        "order_id", "sk_customer", "sk_product", "order_date",
        "quantity", "unit_price", "revenue", "profit", "status", "is_returned",
    ]
    df[fact_cols].to_sql(
        "fact_orders", conn, if_exists="append", index=False, method=_insert_or_ignore
    )
    conn.commit()


# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------

def main():
    print("Reading CSVs...")
    customers, products, orders = read_csv()

    print("Validating data...")
    issues = validate(customers, products, orders)
    if issues:
        print(f"  {issues} data issue(s) flagged (see warnings above).")

    print(f"Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)

    print("Creating tables...")
    create_tables(conn)

    print("Loading dimensions...")
    load_dimensions(conn, customers, products)

    print("Loading facts...")
    load_facts(conn, orders, products)

    # summary
    counts = {
        tbl: conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        for tbl in ("dim_customer", "dim_product", "fact_orders")
    }
    conn.close()

    print("\nLoad complete:")
    for tbl, n in counts.items():
        print(f"  {tbl}: {n} rows")


if __name__ == "__main__":
    main()
