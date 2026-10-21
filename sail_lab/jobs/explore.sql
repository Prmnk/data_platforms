-- SQL tour over the raw orders (run gen_orders.py first): external tables, joins, windows, EXPLAIN.
CREATE TABLE IF NOT EXISTS orders USING parquet LOCATION 's3://lake/raw/orders';
CREATE TABLE IF NOT EXISTS customers USING parquet LOCATION 's3://lake/raw/customers';

SHOW TABLES;

SELECT status, count(*) AS orders, CAST(sum(amount) AS decimal(14,2)) AS amount
FROM orders GROUP BY status ORDER BY orders DESC;

-- Monthly revenue per segment and its share of the month
WITH monthly AS (
  SELECT date_trunc('month', o.order_date) AS month, c.segment, sum(o.amount) AS revenue
  FROM orders o JOIN customers c USING (customer_id)
  WHERE o.status IN ('PAID', 'SHIPPED')
  GROUP BY 1, 2
)
SELECT month, segment, CAST(revenue AS decimal(14,2)) AS revenue,
       round(100 * revenue / sum(revenue) OVER (PARTITION BY month), 1) AS pct_of_month
FROM monthly ORDER BY month, segment;

EXPLAIN SELECT country, count(*) FROM customers GROUP BY country;
