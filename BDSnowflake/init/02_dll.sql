CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR NOT NULL,
    supplier_contact VARCHAR,
    supplier_email VARCHAR,
    supplier_phone VARCHAR,
    supplier_address TEXT,
    supplier_city VARCHAR,
    supplier_country VARCHAR
);

CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR NOT NULL,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    store_email VARCHAR
);

CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_email VARCHAR UNIQUE,
    customer_age INT,
    customer_country VARCHAR,
    customer_postal_code VARCHAR
);

CREATE TABLE dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_email VARCHAR UNIQUE,
    store_id INT REFERENCES dim_stores(store_id) 
);

CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    product_category VARCHAR,
    product_brand VARCHAR,
    product_price DECIMAL(10,2),
    supplier_id INT REFERENCES dim_suppliers(supplier_id) 
);

CREATE TABLE dim_pets (
    pet_id SERIAL PRIMARY KEY,
    pet_name VARCHAR,
    pet_type VARCHAR,
    pet_breed VARCHAR,
    customer_id INT REFERENCES dim_customers(customer_id) 
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    customer_id INT REFERENCES dim_customers(customer_id),
    seller_id INT REFERENCES dim_sellers(seller_id),
    product_id INT REFERENCES dim_products(product_id),
    store_id INT REFERENCES dim_stores(store_id),
    quantity INT,
    total_price DECIMAL(12,2)
);
