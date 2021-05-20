# Databricks notebook source
# MAGIC %md
# MAGIC #1. Download Kaggle data from Terminal
# MAGIC - pip install kaggle
# MAGIC - KAGGLE_CONFIG_DIR='/dbfs/Users/{your-email}/ecommerce-demo/' [assumes that you have your Kaggle token kaggle.json stored in this folder]
# MAGIC - export KAGGLE_CONFIG_DIR
# MAGIC - kaggle datasets download mkechinov/ecommerce-behavior-data-from-multi-category-store -p /dbfs/Users/{your-email}/ecommerce-demo/
# MAGIC 
# MAGIC More information on https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Convert downloaded data to Delta
# MAGIC Unzip downloaded file, then move it to desired folder

# COMMAND ----------

email = 'yourEmail'
basePath = f"/Users/{email}/ecommerce-demo"

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/Users/{email}/ecommerce-demo/ecommerce-behavior-data-from-multi-category-store.zip

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/2019-Oct.csv", f"{basePath}/2019-Oct.csv")  
dbutils.fs.mv("file:/databricks/driver/2019-Nov.csv", f"{basePath}/2019-Nov.csv")  

# COMMAND ----------

dbutils.fs.ls(f"{basePath}/")

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Save to Delta
# MAGIC - Write to Delta = save data as multiple Parquet files (Parquet = better than csv for performance, size), but process it as one table, and a lot more functionalities: https://delta.io/
# MAGIC - Use Spark to process data in parallel

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE ecommerce_demo

# COMMAND ----------

df = spark.read.option("header", "true").csv(f"{basePath}/*.csv")

df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable('ecommerce_demo.events')

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ecommerce_demo.events

# COMMAND ----------

# MAGIC %md
# MAGIC The 2 files add up to ~13GB
# MAGIC The Delta table is ~4.2GB (>3X gain vs .csv)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From ecommerce_demo.events Limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Prepare dimension table, raw table w/o category_code and streaming directory

# COMMAND ----------

# MAGIC %md
# MAGIC Create the table that contains the category_id -> category_code mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace Table ecommerce_demo.categories Using Delta AS
# MAGIC Select cast(category_id as bigint) as category_id, 
# MAGIC        max(category_code) as category 
# MAGIC From ecommerce_demo.events
# MAGIC Group By category_id

# COMMAND ----------

# MAGIC %md
# MAGIC We create a table without category_code for the purpose of the demo

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace Table ecommerce_demo.events_raw Using Delta AS
# MAGIC Select event_time,
# MAGIC        event_type,
# MAGIC        product_id,
# MAGIC        category_id,
# MAGIC        brand,
# MAGIC        price,
# MAGIC        user_id,
# MAGIC        user_session
# MAGIC From ecommerce_demo.events

# COMMAND ----------

# MAGIC %md
# MAGIC Make streaming directories

# COMMAND ----------

# Clean up old directories and tables
dbutils.fs.rm(f"{basePath}/Streaming", recurse=True)
dbutils.fs.rm(f"{basePath}/StreamingCheckpoint", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/ecommerce_demo.db/events_stream", recurse=True)

# Create directories for streaming input, and streaming checkpoints
dbutils.fs.mkdirs(f"{basePath}/Streaming")
dbutils.fs.mkdirs(f"{basePath}/StreamingCheckpoint")

# COMMAND ----------


