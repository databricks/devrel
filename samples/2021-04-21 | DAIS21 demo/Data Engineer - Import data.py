# Databricks notebook source
# MAGIC %md
# MAGIC #1. Download Kaggle data from Terminal
# MAGIC - pip install kaggle
# MAGIC - KAGGLE_CONFIG_DIR='/dbfs/Users/ecommerce-demo/' [assumes that you have your Kaggle token kaggle.json stored in this folder]
# MAGIC - export KAGGLE_CONFIG_DIR
# MAGIC - kaggle datasets download mkechinov/ecommerce-behavior-data-from-multi-category-store -p /dbfs/Users/ecommerce-demo/
# MAGIC 
# MAGIC More information on https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Convert downloaded data to Delta
# MAGIC Unzip downloaded file, then move it to desired folder

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/Users/ecommerce-demo/ecommerce-behavior-data-from-multi-category-store.zip

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/2019-Oct.csv", "dbfs:/Users/ecommerce-demo/2019-Oct.csv")  
dbutils.fs.mv("file:/databricks/driver/2019-Nov.csv", "dbfs:/Users/ecommerce-demo/2019-Nov.csv")  

# COMMAND ----------

dbutils.fs.ls('dbfs:/Users/ecommerce-demo/')

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Save to Delta
# MAGIC - Write to Delta = save data as multiple Parquet files (Parquet = better than csv for performance, size), but process it as one table, and a lot more functionalities: https://delta.io/
# MAGIC - Use Spark to process data in parallel

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE ecommerce_demo

# COMMAND ----------

df = spark.read.option("header", "true").csv("/Users/ecommerce-demo/*.csv")

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
# MAGIC #4. Structured streaming
# MAGIC We show here an example to process and save data in real-time:
# MAGIC - We iteratively save each hour of data as a csv file in a folder
# MAGIC - A structured streaming command automatically picks new files to append them to a Delta table. We use the **auto-loader**: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html
# MAGIC - We assume the category_code is not in the input data, and add it during the streaming step
# MAGIC 
# MAGIC More on structured streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#overview

# COMMAND ----------

# MAGIC %md
# MAGIC Create the table that contains the category_id -> category_code mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace Table ecommerce_demo.categories Using Delta AS
# MAGIC Select cast(category_id as bigint) as category_id, 
# MAGIC        max(category_code) as category_code 
# MAGIC From ecommerce_demo.events
# MAGIC Group By category_id;
# MAGIC 
# MAGIC OPTIMIZE ecommerce_demo.categories;

# COMMAND ----------

# MAGIC %md
# MAGIC Make streaming directories

# COMMAND ----------

# Clean up old directories and tables
dbutils.fs.rm("/Users/ecommerce-demo/Streaming", recurse=True)
dbutils.fs.rm("/Users/ecommerce-demo/StreamingCheckpoint", recurse=True)
dbutils.fs.rm('dbfs:/user/hive/warehouse/ecommerce_demo.db/events_stream', recurse=True)
# Create directories for streaming input, and streaming checkpoints
dbutils.fs.mkdirs("dbfs:/Users/ecommerce-demo/Streaming")
dbutils.fs.mkdirs("dbfs:/Users/ecommerce-demo/StreamingCheckpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC Provide schema of input data

# COMMAND ----------

from pyspark.sql.types import StructType
eventsSchema = StructType().add("event_time", "timestamp") \
                          .add("event_type", "string") \
                          .add("product_id", "integer") \
                          .add("category_id", "long") \
                          .add("category_code", "string") \
                          .add("brand", "string") \
                          .add("price", "float") \
                          .add("user_id", "integer") \
                          .add("user_session", "string")

# COMMAND ----------

# MAGIC %md
# MAGIC Start structured streaming

# COMMAND ----------

df_categories = table("ecommerce_demo.categories")

# Define input (need to provide path to input folder, format, region and schema)
df_wo_categoryCode = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.region", "us-west-2") \
  .schema(eventsSchema) \
  .load("/Users/ecommerce-demo/Streaming") \
  .select("event_time", "event_type", "product_id", "category_id", "brand", "price", "user_id", "user_session")

# Add category_code
df = df_wo_categoryCode \
  .join(df_categories, on=['category_id'], how='left') \
  .select("event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session")

# Define output (need to provide output table (if Delta) and checkpoint location)
df.writeStream.format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/Users/ecommerce-demo/StreamingCheckpoint") \
  .table('ecommerce_demo.events_stream')

# COMMAND ----------

# MAGIC %md
# MAGIC The next cell starts a process to simulate the arrival of data into the destination folder.<br>
# MAGIC Here we add one hour of data at each iteration

# COMMAND ----------

for i in range(10000):
  time = i*3600
  spark_df = spark.sql("Select * From ecommerce_demo.events where date_trunc('hour', event_time) = from_unixtime(to_unix_timestamp('2019-10-01', 'yyyy-MM-dd') + {}, 'yyyy-MM-dd HH')".format(time))
  pd_df = spark_df.toPandas()
  pd_df.to_csv('/dbfs/Users/ecommerce-demo/Streaming/{}.csv'.format(str(time)), index=False, header=False)

# COMMAND ----------


