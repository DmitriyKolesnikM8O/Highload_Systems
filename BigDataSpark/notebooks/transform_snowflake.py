from pyspark.sql import SparkSession, functions as F
import socket

driver_host = socket.gethostbyname('jupyter_pyspark')
spark = SparkSession.builder \
    .appName("ETL_Snowflake_Postgres") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", driver_host) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.extraClassPath", "/opt/spark/drivers/*") \
    .config("spark.driver.extraClassPath", "/opt/spark/drivers/*") \
    .config("spark.jars", "/opt/spark/drivers/postgresql-42.7.1.jar") \
    .getOrCreate()

db_url = "jdbc:postgresql://db:5432/lab_db"
db_params = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}

df = spark.read.jdbc(url=db_url, table="mock_data", properties=db_params)

dim_suppliers = df.select("supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country").dropDuplicates(["supplier_name"]).withColumn("supplier_id", F.monotonically_increasing_id())
dim_stores = df.select("store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email").dropDuplicates(["store_name"]).withColumn("store_id", F.monotonically_increasing_id())
dim_customers = df.select("customer_first_name", "customer_last_name", "customer_email", "customer_age", "customer_country", "customer_postal_code").dropDuplicates(["customer_email"]).withColumn("customer_id", F.monotonically_increasing_id())

dim_sellers = df.join(dim_stores, "store_name").select("seller_first_name", "seller_last_name", "seller_email", "store_id").dropDuplicates(["seller_email"]).withColumn("seller_id", F.monotonically_increasing_id())
dim_products = df.join(dim_suppliers, "supplier_name").select("product_name", "product_category", "product_brand", "product_price", "supplier_id").dropDuplicates(["product_name"]).withColumn("product_id", F.monotonically_increasing_id())

dim_pets = df.join(dim_customers, "customer_email").select("customer_pet_name", "customer_pet_type", "customer_pet_breed", "customer_id", "customer_email").dropDuplicates(["customer_pet_name", "customer_email"]).withColumn("pet_id", F.monotonically_increasing_id()).drop("customer_email")

fact_sales = df.alias("raw") \
    .join(dim_customers.alias("c"), "customer_email") \
    .join(dim_sellers.alias("sel"), "seller_email") \
    .join(dim_products.alias("p"), "product_name") \
    .join(dim_stores.alias("st"), "store_name") \
    .select(F.col("raw.sale_date").cast("date"), "c.customer_id", "sel.seller_id", "p.product_id", "st.store_id", F.col("raw.sale_quantity").cast("int"), F.col("raw.sale_total_price").cast("decimal(18,2)"))

tables = {"dim_suppliers": dim_suppliers, "dim_stores": dim_stores, "dim_customers": dim_customers, "dim_sellers": dim_sellers, "dim_products": dim_products, "dim_pets": dim_pets, "fact_sales": fact_sales}
for name, table_df in tables.items():
    table_df.write.jdbc(url=db_url, table=name, mode="overwrite", properties=db_params)

print("Антихайп!")
spark.stop()
