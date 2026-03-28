CREATE TABLE mock_data (
    id DECIMAL NOT NULL, 
    customer_first_name VARCHAR NOT NULL, 
    customer_last_name VARCHAR NOT NULL, 
    customer_age DECIMAL NOT NULL, 
    customer_email VARCHAR NOT NULL, 
    customer_country VARCHAR NOT NULL, 
    customer_postal_code VARCHAR, 
    customer_pet_type VARCHAR NOT NULL, 
    customer_pet_name VARCHAR NOT NULL, 
    customer_pet_breed VARCHAR NOT NULL, 
    seller_first_name VARCHAR NOT NULL, 
    seller_last_name VARCHAR NOT NULL, 
    seller_email VARCHAR NOT NULL, 
    seller_country VARCHAR NOT NULL, 
    seller_postal_code VARCHAR, 
    product_name VARCHAR NOT NULL, 
    product_category VARCHAR NOT NULL, 
    product_price DECIMAL NOT NULL, 
    product_quantity DECIMAL NOT NULL, 
    sale_date DATE NOT NULL, 
    sale_customer_id DECIMAL NOT NULL, 
    sale_seller_id DECIMAL NOT NULL, 
    sale_product_id DECIMAL NOT NULL, 
    sale_quantity DECIMAL NOT NULL, 
    sale_total_price DECIMAL NOT NULL, 
    store_name VARCHAR NOT NULL, 
    store_location VARCHAR NOT NULL, 
    store_city VARCHAR NOT NULL, 
    store_state VARCHAR, 
    store_country VARCHAR NOT NULL, 
    store_phone VARCHAR NOT NULL, 
    store_email VARCHAR NOT NULL, 
    pet_category VARCHAR NOT NULL, 
    product_weight DECIMAL NOT NULL, 
    product_color VARCHAR NOT NULL, 
    product_size VARCHAR NOT NULL, 
    product_brand VARCHAR NOT NULL, 
    product_material VARCHAR NOT NULL, 
    product_description VARCHAR NOT NULL, 
    product_rating DECIMAL NOT NULL, 
    product_reviews DECIMAL NOT NULL, 
    product_release_date DATE NOT NULL, 
    product_expiry_date DATE NOT NULL, 
    supplier_name VARCHAR NOT NULL, 
    supplier_contact VARCHAR NOT NULL, 
    supplier_email VARCHAR NOT NULL, 
    supplier_phone VARCHAR NOT NULL, 
    supplier_address VARCHAR NOT NULL, 
    supplier_city VARCHAR NOT NULL, 
    supplier_country VARCHAR NOT NULL
);

COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (1).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (2).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (3).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (4).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (5).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (6).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (7).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (8).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
COPY mock_data FROM '/var/lib/postgresql/data_files/MOCK_DATA (9).csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

