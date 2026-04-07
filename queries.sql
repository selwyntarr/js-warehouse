-- ============================================================
-- Query 1 — Top 3 Product Categories by Total Revenue
-- Excludes returned orders using the is_returned flag set during ETL.
-- Uses a window function RANK() so ties would both appear (vs LIMIT alone).
-- ============================================================
SELECT
    p.category,
    ROUND(SUM(f.revenue), 2)                    AS total_revenue,
    RANK() OVER (ORDER BY SUM(f.revenue) DESC)  AS rank
FROM fact_orders f
JOIN dim_product p ON f.sk_product = p.sk_product
WHERE f.is_returned = 0
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 3;


-- ============================================================
-- Query 2 — Return Rate by Customer Segment
-- Shows all segments ordered from highest to lowest return rate.
-- NULLIF(COUNT(*), 0) guards against divide-by-zero for any segment
-- that somehow has zero orders in the database.
-- ============================================================
SELECT
    c.segment,
    COUNT(*)                                                     AS total_orders,
    SUM(f.is_returned)                                          AS returned_orders,
    ROUND(100.0 * SUM(f.is_returned) / NULLIF(COUNT(*), 0), 2) AS return_rate_pct
FROM fact_orders f
JOIN dim_customer c ON f.sk_customer = c.sk_customer
GROUP BY c.segment
ORDER BY return_rate_pct DESC;
