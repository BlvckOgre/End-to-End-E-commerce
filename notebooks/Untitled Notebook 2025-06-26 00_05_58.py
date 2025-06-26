# Databricks notebook source
storage_account = "olistdatastoragedidi"
application_id = "d5a446b8-d660-4b47-9bfd-f722bd1258eb"
directory_id = "82cf0738-0fdf-4db4-8581-77234fc80486"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "-tv8Q~5uc-do.K6CmjWWKFiIVH4w2tew48mVAaGs")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

df_customers_dataset = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", True)\
    .load(f"abfss://olistdata@olistdatastoragedidi.dfs.core.windows.net/Bronze/olist_customers_dataset.csv")\
    .display()

# COMMAND ----------

base_path = "abfss://olistdata@olistdatastoragedidi.dfs.core.windows.net/Bronze/"
geolocation_path= base_path + "olist_geolocation_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
orders_path = base_path + "olist_orders_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"

df_customers = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(customers_path)
df_geolocation = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(geolocation_path)
df_payments = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(payments_path)
df_items = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(items_path)
df_reviews = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(reviews_path)
df_orders = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(orders_path)
df_products = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(products_path)
df_sellers = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(sellers_path)

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient
import pandas as pd

hostname = "givha.h.filess.io"
database = "projectolist_luckskill"
port = "27018"
username = "projectolist_luckskill"
password = "e324d0d238c5d543a4015c42d8c69dd56eb3479c"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

collection = mydatabase["product_categories"]
mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

mongo_data

# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, when


# COMMAND ----------

def clean_dataframe(df,name):
    print("Cleaning dataframe " + name)
    return df.dropDuplicates().na.drop('all')

df_orders = clean_dataframe(df_orders, "Orders")
display(df_orders)

# COMMAND ----------

df_orders = df_orders.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))\
    .withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_date")))\
    .withColumn("order_approved_at", to_date(col("order_approved_at")))\
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))\
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))\
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))

display(df_orders)

# COMMAND ----------

df_orders = df_orders.withColumn("actual_delivery_time", datediff("order_delivered_customer_date","order_purchase_timestamp"))
df_orders = df_orders.withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
df_orders = df_orders.withColumn("delay time", col("actual_delivery_time") - col("estimated_delivery_time"))

display(df_orders)


# COMMAND ----------

# MAGIC %md
# MAGIC ##joining
# MAGIC

# COMMAND ----------

orders_customer_df = df_orders.join(df_customers, df_orders.customer_id == df_customers.customer_id, "left")
orders_payments_df = orders_customer_df.join(df_payments, orders_customer_df.order_id == df_payments.order_id, "left")
orders_items_df = orders_payments_df.join(df_items, "order_id", "left")
orders_products_df = orders_items_df.join(df_products, orders_items_df.product_id == df_products.product_id, "left")
final_df = orders_products_df.join(df_sellers, orders_products_df.seller_id == df_sellers.seller_id, "left")




# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)
mongo_spark_df = spark.createDataFrame(mongo_data)
display(mongo_spark_df)

# COMMAND ----------

final_df = final_df.join(mongo_spark_df, "product_category_name", "left")
display(final_df)

# COMMAND ----------

final_reviews_df = final_df.join(df_reviews, "order_id", "left")
final_geolocation_df = final_reviews_df.join(df_geolocation, final_reviews_df.seller_zip_code_prefix == df_geolocation.geolocation_zip_code_prefix, "left")

display(final_geolocation_df)

# COMMAND ----------

def remove_duplicate_columns(df ):
    columns = df.columns
    seen_columns = set()
    columns_to_drop = []
    for column in columns:
       if column in seen_columns:
           columns_to_drop.append(column)
       else:
           seen_columns.add(column)
    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_geolocation_df = remove_duplicate_columns(final_geolocation_df)
final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

final_geolocation_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastoragedidi.dfs.core.windows.net/Silver")
final_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastoragedidi.dfs.core.windows.net/silver")