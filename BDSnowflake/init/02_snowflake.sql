-- 1. Сначала создаем чистые измерения (только уникальные значения)
CREATE TABLE dim_suppliers AS
SELECT DISTINCT supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country
FROM mock_data;
ALTER TABLE dim_suppliers ADD COLUMN supplier_id SERIAL PRIMARY KEY;

CREATE TABLE dim_customers AS
SELECT DISTINCT customer_first_name, customer_last_name, customer_email, customer_age, customer_country, customer_postal_code
FROM mock_data;
ALTER TABLE dim_customers ADD COLUMN customer_id SERIAL PRIMARY KEY;

CREATE TABLE dim_sellers AS
SELECT DISTINCT seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code
FROM mock_data;
ALTER TABLE dim_sellers ADD COLUMN seller_id SERIAL PRIMARY KEY;

CREATE TABLE dim_stores AS
SELECT DISTINCT store_name, store_location, store_city, store_state, store_country, store_phone, store_email
FROM mock_data;
ALTER TABLE dim_stores ADD COLUMN store_id SERIAL PRIMARY KEY;

-- 2. Снежинка: Товары (привязываем к ID поставщика, используя LIMIT 1 для страховки от дублей)
CREATE TABLE dim_products AS
SELECT DISTINCT ON (m.product_name) 
    m.product_name, m.product_category, m.product_brand, m.product_price, s.supplier_id
FROM mock_data m
JOIN dim_suppliers s ON m.supplier_name = s.supplier_name;
ALTER TABLE dim_products ADD COLUMN product_id SERIAL PRIMARY KEY;

-- 3. Снежинка: Питомцы (привязываем к ID клиента)
CREATE TABLE dim_pets AS
SELECT DISTINCT ON (m.customer_pet_name, m.customer_email)
    m.customer_pet_name, m.customer_pet_type, m.customer_pet_breed, c.customer_id
FROM mock_data m
JOIN dim_customers c ON m.customer_email = c.customer_email;
ALTER TABLE dim_pets ADD COLUMN pet_id SERIAL PRIMARY KEY;

-- 4. ТАБЛИЦА ФАКТОВ (теперь делаем JOIN максимально аккуратно)
CREATE TABLE fact_sales AS
SELECT 
    m.sale_date,
    c.customer_id,
    sel.seller_id,
    p.product_id,
    st.store_id,
    m.sale_quantity::INT,
    m.sale_total_price::DECIMAL
FROM mock_data m
JOIN dim_customers c ON m.customer_email = c.customer_email
JOIN dim_sellers sel ON m.seller_email = sel.seller_email
JOIN dim_products p ON m.product_name = p.product_name
JOIN dim_stores st ON m.store_name = st.store_name;
