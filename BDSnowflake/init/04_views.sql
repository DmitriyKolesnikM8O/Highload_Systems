CREATE VIEW view_top_products AS
SELECT p.product_name, SUM(f.total_price) as revenue
FROM fact_sales f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 5;

CREATE VIEW view_revenue_by_country AS
SELECT s.store_country, SUM(f.total_price) as total_revenue
FROM fact_sales f
JOIN dim_stores s ON f.store_id = s.store_id
GROUP BY s.store_country;
