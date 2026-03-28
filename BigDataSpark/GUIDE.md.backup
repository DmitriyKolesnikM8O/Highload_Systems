## Что использовалось

```text
- Docker (24.x) - Контейнеризация всего проекта
- PostgreSQL (16-alpine) - Хранилище исходных данных и модели "Снежинка"
- ClickHouse (clickhouse/clickhouse-server) - Хранилище для итоговых отчетов
- Apache Spark (3.5.0 (кластер: master + worker)) - ETL-движок для трансформаций
- Jupyter Notebook (jupyter/pyspark-notebook:spark-3.5.0) - Среда выполнения PySpark-скриптов
- pgcli - CLI-клиент для PostgreSQL
```
**работа выполнялась на Kubuntu 22.04.5 LTS (ядро 6.8.0-101-generic) с Docker и всеми перечисленными инструментами. При использовании другой ОС команды могут незначительно отличаться.**


## Что вводить


1. Клоним репу
```code
git clone <url_репозитория>
cd BigDataSpark
```
2. Запускаем все
```code
docker compose up -d
```
3. Чекаем, что все ок
```code
docker ps
```
4. Чекаем, что PostgreSQL стартанула (около 10-15 секунд)
```code
docker logs -f postgres_db  # найти "database system is ready to accept connections"
```
5. Подключаемся (если ошибка, скорее всего нужно просто подождать, еще Postgres не стартанул)
```code
pgcli postgres://postgres:password@localhost:5432/lab_db
```
6. Внутри postgres чекаем, что есть табличка:
```code
SELECT COUNT(*) FROM mock_data;  # должно быть 10000, если Да - ОК
\q
```
7. Запуск трансформации в модель "Снежинка"
```code
docker exec -it jupyter_pyspark spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/drivers/postgresql-42.7.1.jar \
  /home/jovyan/work/transform_snowflake.py
  
  
  # В качестве проверки можно:
  pgcli postgres://postgres:password@localhost:5432/lab_db
  /dt # должны быть все таблички
```
8. Запуск генерации отчетов в ClickHouse
```code
docker exec -it jupyter_pyspark spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/drivers/postgresql-42.7.1.jar \
  /home/jovyan/work/create_reports.py
```
9. Проверка отчетов в ClickHouse (должно вывести 6 отчетов)
```code
docker exec -it clickhouse_db clickhouse-client \
  --user user --password password \
  --query "SELECT name FROM system.tables WHERE database='reports'"
```
10. Проверка данных одного из отчетов
```code
docker exec -it clickhouse_db clickhouse-client \
  --user user --password password \
  --query "SELECT * FROM reports.report_product_sales LIMIT 5"
```

## Навигация

```text
BigDataSpark/
├── data/               # 10 CSV файлов
├── init/               # Скрипт загрузки в Postgres (как в 1-й лабе)
├── notebooks/          # Скрипты + выгруженный код с Jupiter (просто как артефакт)
├── jars/               # Драйверы для Postgres и ClickHouse
└── docker-compose.yml
```

## Детальное описание

**Предварительно должны быть установлены драйверы для clickhose и postgres. У меня лежат в /jars**

Команды:
```code
- docker compose up -d //чтобы все поднялось
- http://localhost:8080 //там работает Spark
- sudo docker logs jupyter_pyspark //тут нужно найти url Jupiter(ссылка, где токен прописан)
- docker logs jupyter_pyspark 2>&1 | grep "http://127.0.0.1:8888/lab" // это альтернатива команде выше 
- pgcli postgres://postgres:password@localhost:5432/lab_db //чтобы убедиться, что mock_data таблица создалась
```

Способ загрузки идентичен тому, что был в 1 лабе: через скрипт *init/01_load.sql*, который автоматически выполняется при старте контейнера PostgreSQL.

В docker-compose.yml, помимо Postgres, появились: ClickHouse, Spark Master, Spark Worker, Jupiter Notebook
Кластер минимального размера: Master для координации и Worker, чтобы делать.

Соответственно после **docker-compose up -d** все запустилось. Spark развернут на: *http://localhost:8080* - воркер отображается. В Jupiter зашел по ссылки из логов, там, где токен: **docker logs jupyter_pyspark**.
Ну а Postgres для проверки подключился через все тот же pgcli из 1 лабы: **pgcli postgres://postgres:password@localhost:5432/lab_db**

Все работает, база из 10к строк собрана.


Пунты 7 теперь делал через Jupiter.
В частности:
```code
from pyspark.sql import functions as F

# 1. Какие категории товаров у нас есть?
print("--- Категории товаров ---")
df.select("product_category").distinct().show()

# 2. Сколько у нас уникальных магазинов?
unique_stores = df.select('store_name').distinct().count()
print(f"Количество уникальных магазинов: {unique_stores}")

# 3. Проверим на наличие пустых значений (null)
print("Проверка на NULL (цена и дата)")
df.select(
    F.count(F.when(F.col("product_price").isNull(), 1)).alias("null_prices"),
    F.count(F.when(F.col("sale_date").isNull(), 1)).alias("null_dates")
).show()

# 4. Топ-5 стран покупателей
print("Топ-5 стран по количеству покупателей")
df.groupBy("customer_country").count().orderBy(F.desc("count")).show(5)

# 5. Средняя цена товара по категориям питомцев
print("Средняя цена товара по категориям питомцев")
df.groupBy("pet_category").agg(F.round(F.avg("product_price"), 2).alias("avg_price")).show()
```

По сути - аналоги запросов из прошлой лабы, только теперь через Spark (как крутые программисты!). 
Данные те же, а значит и результаты будут такими же.

По этой же причине и "снежинка" строится также, только теперь Spark:
- Измерения первого уровня (связаны с фактами):
  - dim_customers — покупатели.
  - dim_sellers — сотрудники (продавцы).
  - dim_products — каталог товаров.
  - dim_stores — магазины.
- Измерения второго уровня (Снежинка):
  - dim_pets — инфа о питомцах (связана с dim_customers).
  -dim_suppliers — поставщики (связана с dim_products).
- Таблица фактов:
  - fact_sales — центральная таблица с метриками и внешними ключами. 

```code
from pyspark.sql import functions as F

# 1. dim_suppliers
dim_suppliers = df.select(
    "supplier_name", "supplier_contact", "supplier_email", 
    "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
).dropDuplicates(["supplier_name"]) \
 .withColumn("supplier_id", F.monotonically_increasing_id())

# 2. dim_stores
dim_stores = df.select(
    "store_name", "store_location", "store_city", 
    "store_state", "store_country", "store_phone", "store_email"
).dropDuplicates(["store_name"]) \
 .withColumn("store_id", F.monotonically_increasing_id())

# 3. dim_customers
dim_customers = df.select(
    "customer_first_name", "customer_last_name", "customer_email", 
    "customer_age", "customer_country", "customer_postal_code"
).dropDuplicates(["customer_email"]) \
 .withColumn("customer_id", F.monotonically_increasing_id())

# 4. dim_sellers
dim_sellers = df.join(dim_stores, "store_name") \
    .select("seller_first_name", "seller_last_name", "seller_email", "store_id") \
    .dropDuplicates(["seller_email"]) \
    .withColumn("seller_id", F.monotonically_increasing_id())

# 5. dim_products
dim_products = df.join(dim_suppliers, "supplier_name") \
    .select("product_name", "product_category", "product_brand", "product_price", "supplier_id") \
    .dropDuplicates(["product_name"]) \
    .withColumn("product_id", F.monotonically_increasing_id())

# 6. dim_pets
dim_pets = df.join(dim_customers, "customer_email") \
    .select("customer_pet_name", "customer_pet_type", "customer_pet_breed", "customer_id", "customer_email") \
    .dropDuplicates(["customer_pet_name", "customer_email"]) \
    .withColumn("pet_id", F.monotonically_increasing_id()) \
    .drop("customer_email")

# 7. fact_sales
fact_sales = df.alias("raw") \
    .join(dim_customers.alias("c"), "customer_email") \
    .join(dim_sellers.alias("sel"), "seller_email") \
    .join(dim_products.alias("p"), "product_name") \
    .join(dim_stores.alias("st"), "store_name") \
    .select(
        F.col("raw.sale_date").cast("date"),
        F.col("c.customer_id"),
        F.col("sel.seller_id"),
        F.col("p.product_id"),
        F.col("st.store_id"),
        F.col("raw.sale_quantity").cast("int"),
        F.col("raw.sale_total_price").cast("decimal(18,2)")
    )

print("запись в PostgreSQL...")

tables = {
    "dim_suppliers": dim_suppliers,
    "dim_stores": dim_stores,
    "dim_customers": dim_customers,
    "dim_sellers": dim_sellers,
    "dim_products": dim_products,
    "dim_pets": dim_pets,
    "fact_sales": fact_sales
}

for name, table_df in tables.items():
    table_df.write.jdbc(url=db_url, table=name, mode="overwrite", properties=db_params)
    print(f"Table {name} is written.")

print("Все таблицы Снежинки созданы")
```

Соответственно после этой команды: **pgcli postgres://postgres:password@localhost:5432/lab_db**.
И там видно, что все таблицы создались. Это пункты 8 и 9 соответственно


Далее пришла пора работать с ClickHouse. Это пункты 10 и 11.
Я что-то много мучался с jdbc-драйвером - он просто упорно шел на локалхост, а не куда надо, и по итогу использовал HTTP API ClickHouse:

- 1 отчет
```code
from pyspark.sql import functions as F
import requests
import json

db_url = "jdbc:postgresql://db:5432/lab_db"
db_params = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}

df_fact = spark.read.jdbc(url=db_url, table="fact_sales", properties=db_params)
df_prod = spark.read.jdbc(url=db_url, table="dim_products", properties=db_params)

df_joined = df_fact.join(df_prod, "product_id")

report_products = df_joined.groupBy("product_name", "product_category") \
    .agg(
        F.sum("sale_quantity").alias("total_sales_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.round(F.avg("product_price"), 2).alias("avg_product_price")
    ) \
    .orderBy(F.desc("total_sales_quantity")) \
    .limit(10)

data_to_insert = report_products.collect()

clickhouse_url = "http://clickhouse_db:8123/"
auth_params = {"user": "user", "password": "password"}

create_table_query = """
CREATE TABLE IF NOT EXISTS reports.report_product_sales (
    product_name String,
    product_category String,
    total_sales_quantity Int64,
    total_revenue Float64,
    avg_product_price Float64
) ENGINE = MergeTree()
ORDER BY product_name
"""

try:

    response = requests.post(
        clickhouse_url,
        params=auth_params,
        data=create_table_query
    )
    print(f"Создание таблицы: {response.status_code} - {response.text}")
    
    insert_template = "INSERT INTO reports.report_product_sales (product_name, product_category, total_sales_quantity, total_revenue, avg_product_price) VALUES"
    
    for row in data_to_insert:
        insert_query = f"""
        INSERT INTO reports.report_product_sales 
        (product_name, product_category, total_sales_quantity, total_revenue, avg_product_price) 
        VALUES ('{row.product_name}', '{row.product_category}', {row.total_sales_quantity}, {row.total_revenue}, {row.avg_product_price})
        """
        response = requests.post(
            clickhouse_url,
            params=auth_params,
            data=insert_query
        )
        if response.status_code != 200:
            print(f"Ошибка при вставке {row.product_name}: {response.text}")
    
    print("Данные УсПеШнО записаны в ClickHouse!")
    
    check_query = "SELECT * FROM reports.report_product_sales"
    response = requests.post(
        clickhouse_url,
        params=auth_params,
        data=check_query
    )
    print("\nДанные в ClickHouse:")
    print(response.text)
    
except Exception as e:
    print(f"Ошибка: {e}")
```

*тут есть нюанс: метод .collect - наверное на очень больших данных это плохо, но тут чтобы себе жизнь упростить прибегнул*

- 2 отчет
```code
report_customers = df_fact.join(df_customers, "customer_id") \
    .groupBy("customer_first_name", "customer_last_name", "customer_email", "customer_country") \
    .agg(
        F.sum("sale_total_price").alias("total_spent"),
        F.round(F.avg("sale_total_price"), 2).alias("avg_check")
    ).orderBy(F.desc("total_spent")).limit(10)

create_table_cust = """
CREATE TABLE IF NOT EXISTS reports.report_customer_sales (
    customer_first_name String,
    customer_last_name String,
    customer_email String,
    customer_country String,
    total_spent Float64,
    avg_check Float64
) ENGINE = MergeTree()
ORDER BY customer_email
"""

try:
    r = requests.post(clickhouse_url, params=auth_params, data=create_table_cust)
    if r.status_code == 200:
        print("report_customer_sales создана успешно.")
    else:
        print(f"Ошибка создания таблицы (Код {r.status_code}): {r.text}")


    if r.status_code == 200:
        for row in report_customers.collect():
            f_name = row.customer_first_name.replace("'", "''")
            l_name = row.customer_last_name.replace("'", "''")
            ins = f"INSERT INTO reports.report_customer_sales VALUES ('{f_name}', '{l_name}', '{row.customer_email}', '{row.customer_country}', {row.total_spent}, {row.avg_check})"
            requests.post(clickhouse_url, params=auth_params, data=ins)
        print("Данные в отчет №2 добавлены.")

except Exception as e:
    print(f"Критическая ошибка, поезд сделал бум: {e}")
```


- 3 отчет
```code
from pyspark.sql import functions as F

report_time = df_fact.withColumn("year", F.year("sale_date")) \
    .withColumn("month", F.month("sale_date")) \
    .groupBy("year", "month") \
    .agg(
        F.sum("sale_total_price").alias("monthly_revenue"),
        F.count("sale_date").alias("total_orders"),
        F.round(F.avg("sale_total_price"), 2).alias("avg_order_value")
    ) \
    .orderBy("year", "month")

data_time = report_time.collect()

create_table_time = """
CREATE TABLE IF NOT EXISTS reports.report_time_sales (
    year Int32,
    month Int32,
    monthly_revenue Float64,
    total_orders Int64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (year, month)
"""

try:
    requests.post(clickhouse_url, params=auth_params, data=create_table_time)
    for row in data_time:
        insert_query = f"INSERT INTO reports.report_time_sales VALUES ({row.year}, {row.month}, {row.monthly_revenue}, {row.total_orders}, {row.avg_order_value})"
        requests.post(clickhouse_url, params=auth_params, data=insert_query)
    print("Отчет №3 (Время) успешно записан в ClickHouse! Можно отдохнуть")
    report_time.show(5)
except Exception as e:
    print(f"Ошибка в отчете №3: {e}")
```

- 4 отчет
```code
report_stores = df_fact.join(df_stores, "store_id") \
    .groupBy("store_name", "store_city", "store_country") \
    .agg(
        F.sum("sale_total_price").alias("store_revenue"),
        F.round(F.avg("sale_total_price"), 2).alias("avg_check_store")
    ).orderBy(F.desc("store_revenue")).limit(5)

create_table_stores = """
CREATE TABLE IF NOT EXISTS reports.report_store_sales (
    store_name String,
    store_city String,
    store_country String,
    store_revenue Float64,
    avg_check_store Float64
) ENGINE = MergeTree()
ORDER BY store_name
"""

try:
    r = requests.post(clickhouse_url, params=auth_params, data=create_table_stores)
    if r.status_code == 200:
        print("Таблица report_store_sales создана успешно.")
        
        for row in report_stores.collect():
            s_name = row.store_name.replace("'", "''")
            s_city = row.store_city.replace("'", "''")
            ins = f"INSERT INTO reports.report_store_sales VALUES ('{s_name}', '{s_city}', '{row.store_country}', {row.store_revenue}, {row.avg_check_store})"
            requests.post(clickhouse_url, params=auth_params, data=ins)
        print("Данные в отчет №4 добавлены.")
    else:
        print(f"Ошибка создания мироздания: {r.text}")

except Exception as e:
    print(f"Критическая ошибка: {e}")
```

- 5 отчет
```code
df_suppliers = spark.read.jdbc(url=db_url, table="dim_suppliers", properties=db_params)
df_products = spark.read.jdbc(url=db_url, table="dim_products", properties=db_params)

df_sup_joined = df_fact.join(df_products, "product_id") \
                       .join(df_suppliers, "supplier_id")

report_suppliers = df_sup_joined.groupBy("supplier_name", "supplier_country") \
    .agg(
        F.sum("sale_total_price").alias("total_supplier_revenue"),
        F.round(F.avg("product_price"), 2).alias("avg_item_price")
    ) \
    .orderBy(F.desc("total_supplier_revenue")) \
    .limit(5) # ТЗ требует Топ-5 (кажется)

create_table_sup = """
CREATE TABLE IF NOT EXISTS reports.report_supplier_sales (
    supplier_name String,
    supplier_country String,
    total_supplier_revenue Float64,
    avg_item_price Float64
) ENGINE = MergeTree()
ORDER BY supplier_name
"""

try:
    r = requests.post(clickhouse_url, params=auth_params, data=create_table_sup)
    if r.status_code == 200:
        print("Таблица report_supplier_sales готова.")
        
        for row in report_suppliers.collect():
            sup_name = row.supplier_name.replace("'", "''")
            ins = f"INSERT INTO reports.report_supplier_sales VALUES ('{sup_name}', '{row.supplier_country}', {row.total_supplier_revenue}, {row.avg_item_price})"
            requests.post(clickhouse_url, params=auth_params, data=ins)
        print("Данные в отчет №5 успешно добавлены.")
    else:
        print(f"Ошибка ClickHouse: {r.text}")

except Exception as e:
    print(f"Ошибка: {e}")

report_suppliers.show()
```

- 6 отчет
```code
report_quality = df.groupBy("product_name", "product_category") \
    .agg(
        F.round(F.avg("product_rating"), 2).alias("avg_rating"),
        F.sum("product_reviews").cast("long").alias("total_reviews"),
        F.sum("sale_quantity").cast("long").alias("total_sales_volume")
    ) \
    .orderBy(F.desc("avg_rating"))

create_table_quality = """
CREATE TABLE IF NOT EXISTS reports.report_product_quality (
    product_name String,
    product_category String,
    avg_rating Float64,
    total_reviews Int64,
    total_sales_volume Int64
) ENGINE = MergeTree()
ORDER BY avg_rating
"""

try:
    r = requests.post(clickhouse_url, params=auth_params, data=create_table_quality)
    if r.status_code == 200:
        print("Таблица report_product_quality создана успешно.")
        
        for row in report_quality.collect():
            p_name = row.product_name.replace("'", "''")
            ins = f"INSERT INTO reports.report_product_quality VALUES ('{p_name}', '{row.product_category}', {row.avg_rating}, {row.total_reviews}, {row.total_sales_volume})"
            requests.post(clickhouse_url, params=auth_params, data=ins)
        print("Все данные в отчет №6 добавлены.")
    else:
        print(f"Ошибка создания таблицы: {r.text}")

except Exception as e:
    print(f"Ошибка: {e}")

report_quality.show(5)
```

В конце: **docker exec -it clickhouse_db clickhouse-client --user user --password password --query "SHOW TABLES FROM reports"** и видим, что все создалось.


После всего этого создал 2 файлика в notebooks/
- *transform_snowflake.py* - трансформация в снежнику в Postgres
- *create_reports.py* - создание отчетов в ClickHouse


**docker exec -it jupyter_pyspark spark-submit --master spark://spark-master:7077 --jars /opt/spark/drivers/postgresql-42.7.1.jar /home/jovyan/work/transform_snowflake.py**

В Postgres появятся все таблички

**docker exec -it jupyter_pyspark spark-submit --master spark://spark-master:7077 --jars /opt/spark/drivers/postgresql-42.7.1.jar /home/jovyan/work/create_reports.py**

В ClickHouse созданы отчеты

