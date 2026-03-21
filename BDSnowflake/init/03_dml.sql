INSERT INTO dim_suppliers (supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country)
SELECT DISTINCT ON (supplier_name) 
    supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country 
FROM mock_data;

INSERT INTO dim_stores (store_name, store_location, store_city, store_state, store_country, store_phone, store_email)
SELECT DISTINCT ON (store_name) 
    store_name, store_location, store_city, store_state, store_country, store_phone, store_email 
FROM mock_data;

INSERT INTO dim_customers (customer_first_name, customer_last_name, customer_email, customer_age, customer_country, customer_postal_code)
SELECT DISTINCT ON (customer_email) 
    customer_first_name, customer_last_name, customer_email, customer_age::INT, customer_country, customer_postal_code 
FROM mock_data;

INSERT INTO dim_sellers (seller_first_name, seller_last_name, seller_email, store_id)
SELECT DISTINCT ON (m.seller_email) 
    m.seller_first_name, m.seller_last_name, m.seller_email, s.store_id
FROM mock_data m
JOIN dim_stores s ON m.store_name = s.store_name;

INSERT INTO dim_products (product_name, product_category, product_brand, product_price, supplier_id)
SELECT DISTINCT ON (m.product_name) 
    m.product_name, m.product_category, m.product_brand, m.product_price::DECIMAL, sup.supplier_id
FROM mock_data m
JOIN dim_suppliers sup ON m.supplier_name = sup.supplier_name;

INSERT INTO dim_pets (pet_name, pet_type, pet_breed, customer_id)
SELECT DISTINCT ON (m.customer_pet_name, m.customer_email) 
    m.customer_pet_name, m.customer_pet_type, m.customer_pet_breed, c.customer_id
FROM mock_data m
JOIN dim_customers c ON m.customer_email = c.customer_email;

INSERT INTO fact_sales (sale_date, customer_id, seller_id, product_id, store_id, quantity, total_price)
SELECT 
    m.sale_date::DATE,
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
