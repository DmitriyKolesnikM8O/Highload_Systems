from pyspark.sql import SparkSession, functions as F
import requests
import socket
import sys

try:
    driver_host = socket.gethostbyname('jupyter_pyspark')
except:
    driver_host = "0.0.0.0"

spark = SparkSession.builder \
    .appName("ClickHouse_Reporting_Job") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", driver_host) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.extraClassPath", "/opt/spark/drivers/*") \
    .config("spark.driver.extraClassPath", "/opt/spark/drivers/*") \
    .config("spark.jars", "/opt/spark/drivers/postgresql-42.7.1.jar") \
    .getOrCreate()


db_url = "jdbc:postgresql://db:5432/lab_db"
db_params = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}

clickhouse_url = "http://clickhouse_db:8123/"
auth_params = {"user": "user", "password": "password"}

def run_ch_query(query):
    """Помощник для отправки запросов в ClickHouse"""
    response = requests.post(clickhouse_url, params=auth_params, data=query)
    if response.status_code != 200:
        print(f"Ошибка ClickHouse: {response.text}")
    return response

print("Наверное у меня все получится...")


df_fact = spark.read.jdbc(url=db_url, table="fact_sales", properties=db_params)
df_prod = spark.read.jdbc(url=db_url, table="dim_products", properties=db_params)
df_cust = spark.read.jdbc(url=db_url, table="dim_customers", properties=db_params)
df_stor = spark.read.jdbc(url=db_url, table="dim_stores", properties=db_params)
df_supp = spark.read.jdbc(url=db_url, table="dim_suppliers", properties=db_params)

df_raw = spark.read.jdbc(url=db_url, table="mock_data", properties=db_params)


print("№1 отчет...")
rep1 = df_fact.join(df_prod, "product_id").groupBy("product_name", "product_category") \
    .agg(F.sum("sale_quantity").alias("total_qty"), F.sum("sale_total_price").alias("rev"), F.round(F.avg("product_price"), 2).alias("avg_p")) \
    .orderBy(F.desc("total_qty")).limit(10)

run_ch_query("CREATE TABLE IF NOT EXISTS reports.report_product_sales (product_name String, product_category String, total_qty Int64, rev Float64, avg_p Float64) ENGINE = MergeTree() ORDER BY product_name")
for r in rep1.collect():
    p_name = r.product_name.replace("'", "''")
    run_ch_query(f"INSERT INTO reports.report_product_sales VALUES ('{p_name}', '{r.product_category}', {r.total_qty}, {r.rev}, {r.avg_p})")


print("№2 отчет...")
rep2 = df_fact.join(df_cust, "customer_id").groupBy("customer_first_name", "customer_last_name", "customer_email", "customer_country") \
    .agg(F.sum("sale_total_price").alias("spent"), F.round(F.avg("sale_total_price"), 2).alias("avg_chk")) \
    .orderBy(F.desc("spent")).limit(10)

run_ch_query("CREATE TABLE IF NOT EXISTS reports.report_customer_sales (f_name String, l_name String, email String, country String, spent Float64, avg_chk Float64) ENGINE = MergeTree() ORDER BY email")
for r in rep2.collect():
    fn, ln = r.customer_first_name.replace("'", "''"), r.customer_last_name.replace("'", "''")
    run_ch_query(f"INSERT INTO reports.report_customer_sales VALUES ('{fn}', '{ln}', '{r.customer_email}', '{r.customer_country}', {r.spent}, {r.avg_chk})")

print("№3 отчет...")
rep3 = df_fact.withColumn("year", F.year("sale_date")).withColumn("month", F.month("sale_date")) \
    .groupBy("year", "month").agg(F.sum("sale_total_price").alias("rev"), F.count("sale_date").alias("cnt")) \
    .orderBy("year", "month")

run_ch_query("CREATE TABLE IF NOT EXISTS reports.report_time_sales (year Int32, month Int32, rev Float64, cnt Int64) ENGINE = MergeTree() ORDER BY (year, month)")
for r in rep3.collect():
    run_ch_query(f"INSERT INTO reports.report_time_sales VALUES ({r.year}, {r.month}, {r.rev}, {r.cnt})")

print("№4 отчет...")
rep4 = df_fact.join(df_stor, "store_id").groupBy("store_name", "store_city", "store_country") \
    .agg(F.sum("sale_total_price").alias("rev"), F.round(F.avg("sale_total_price"), 2).alias("avg_chk")) \
    .orderBy(F.desc("rev")).limit(5)

run_ch_query("CREATE TABLE IF NOT EXISTS reports.report_store_sales (name String, city String, country String, rev Float64, avg_chk Float64) ENGINE = MergeTree() ORDER BY name")
for r in rep4.collect():
    sn, sc = r.store_name.replace("'", "''"), r.store_city.replace("'", "''")
    run_ch_query(f"INSERT INTO reports.report_store_sales VALUES ('{sn}', '{sc}', '{r.store_country}', {r.rev}, {r.avg_chk})")

print("№5 отчет...")
rep5 = df_fact.join(df_prod, "product_id").join(df_supp, "supplier_id") \
    .groupBy("supplier_name", "supplier_country") \
    .agg(F.sum("sale_total_price").alias("rev"), F.round(F.avg("product_price"), 2).alias("avg_p")) \
    .orderBy(F.desc("rev")).limit(5)

run_ch_query("CREATE TABLE IF NOT EXISTS reports.report_supplier_sales (name String, country String, rev Float64, avg_p Float64) ENGINE = MergeTree() ORDER BY name")
for r in rep5.collect():
    sn = r.supplier_name.replace("'", "''")
    run_ch_query(f"INSERT INTO reports.report_supplier_sales VALUES ('{sn}', '{r.supplier_country}', {r.rev}, {r.avg_p})")

print("№6 отчет...")
rep6 = df_raw.groupBy("product_name", "product_category") \
    .agg(F.round(F.avg("product_rating"), 2).alias("rtg"), F.sum("product_reviews").cast("long").alias("revs")) \
    .orderBy(F.desc("rtg"))

run_ch_query("CREATE TABLE IF NOT EXISTS reports.report_product_quality (name String, cat String, rtg Float64, revs Int64) ENGINE = MergeTree() ORDER BY rtg")
for r in rep6.collect():
    pn = r.product_name.replace("'", "''")
    run_ch_query(f"INSERT INTO reports.report_product_quality VALUES ('{pn}', '{r.product_category}', {r.rtg}, {r.revs})")

print("Господи, я выжил!")
spark.stop()
