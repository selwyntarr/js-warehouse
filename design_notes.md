# Schema Design Notes

## Star Schema Overview

One fact table capturing order-level events, joined to two dimension tables.
Date dimension is omitted (per scope simplification) — `order_date` is stored
directly on the fact table and year/month are extracted in SQL via `strftime()`.

---

## Tables

### `dim_customer`
Represents the customer who placed the order.

| Column | Type | Notes |
|---|---|---|
| `sk_customer` | INTEGER PK | Surrogate key (auto-increment) |
| `customer_id` | TEXT UNIQUE | Natural key from source CSV |
| `name` | TEXT | |
| `email` | TEXT | |
| `city` | TEXT | |
| `state` | TEXT | |
| `signup_date` | DATE | |
| `segment` | TEXT | `premium`, `standard`, or `new` |

### `dim_product`
Represents the product sold.

| Column | Type | Notes |
|---|---|---|
| `sk_product` | INTEGER PK | Surrogate key (auto-increment) |
| `product_id` | TEXT UNIQUE | Natural key from source CSV |
| `product_name` | TEXT | |
| `category` | TEXT | `Electronics`, `Apparel`, `Sports`, `Appliances` |
| `subcategory` | TEXT | |
| `cost_price` | REAL | Used to compute profit in the fact table |

### `fact_orders`
One row per order event. Contains all measures and foreign keys to dimensions.

| Column | Type | Notes |
|---|---|---|
| `sk_order` | INTEGER PK | Surrogate key (auto-increment) |
| `order_id` | TEXT UNIQUE | Natural key from source CSV |
| `sk_customer` | INTEGER FK | → `dim_customer.sk_customer` |
| `sk_product` | INTEGER FK | → `dim_product.sk_product` |
| `order_date` | DATE | Stored as `YYYY-MM-DD`; use `strftime()` for month/year grouping |
| `quantity` | INTEGER | |
| `unit_price` | REAL | |
| `revenue` | REAL | Derived: `quantity × unit_price` |
| `profit` | REAL | Derived: `revenue − (quantity × cost_price)` |
| `status` | TEXT | `completed`, `pending`, or `returned` |
| `is_returned` | INTEGER | `1` if `status = 'returned'`, else `0` — convenience flag for filtering |

---

## How It Supports the Business Questions

**Total revenue by product category per month**
```sql
SELECT strftime('%Y-%m', f.order_date) AS month, p.category, SUM(f.revenue)
FROM fact_orders f
JOIN dim_product p ON f.sk_product = p.sk_product
WHERE f.is_returned = 0
GROUP BY month, p.category;
```

**Return rate by customer segment**
```sql
SELECT c.segment,
       ROUND(100.0 * SUM(f.is_returned) / COUNT(*), 2) AS return_rate_pct
FROM fact_orders f
JOIN dim_customer c ON f.sk_customer = c.sk_customer
GROUP BY c.segment;
```

---

## Design Decisions & Trade-offs

- **No `dim_date`** — the spec explicitly allows storing `order_date` on the fact table at this scope. With more time I'd add a proper date dimension (year, month, quarter, day-of-week) to make time-based slicing faster and more readable.
- **Integer surrogate keys** — simpler than UUIDs for a local SQLite warehouse. In a distributed system UUIDs would be preferred to avoid key collisions across sources.
- **`is_returned` flag** — denormalized from `status` for query convenience. Keeps `WHERE` clauses simple and avoids repeated string comparisons.
- **`cost_price` on `dim_product`** — cost is a product attribute so it lives on the dimension. Profit is computed at ETL time and stored on the fact table for query performance.
