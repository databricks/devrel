# Databricks notebook source
# MAGIC %md
# MAGIC #Auto-loader demo
# MAGIC We show here an example to process and save data in real-time:
# MAGIC - We iteratively save each hour of data as a csv file in a folder (Software Engineer notebook)
# MAGIC - A structured streaming command automatically picks new files to append them to a Delta table. We use the **auto-loader**: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html
# MAGIC - As the category_code is not in the input data, we add it during the streaming step
# MAGIC 
# MAGIC More on structured streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#overview

# COMMAND ----------

email = 'yourEmail'
basePath = f"/Users/{email}/ecommerce-demo"

# COMMAND ----------

# MAGIC %md
# MAGIC Provide schema of input data

# COMMAND ----------

from pyspark.sql.types import StructType
schema = StructType().add("event_time", "timestamp") \
                     .add("event_type", "string") \
                     .add("product_id", "integer") \
                     .add("category_id", "long") \
                     .add("brand", "string") \
                     .add("price", "float") \
                     .add("user_id", "integer") \
                     .add("user_session", "string")

# COMMAND ----------

# MAGIC %md
# MAGIC Dimension table

# COMMAND ----------

df_categories = table("ecommerce_demo.categories")

# COMMAND ----------

# MAGIC %md
# MAGIC Input stream

# COMMAND ----------

df_raw = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.validateOptions", "false") \
  .schema(schema) \
  .load(f"{basePath}/Streaming") \
  .select("event_time", "event_type", "product_id", "category_id", "brand", "price", "user_id", "user_session")

# COMMAND ----------

# MAGIC %md
# MAGIC Join with dimension table to add category

# COMMAND ----------

df = df_raw \
  .join(df_categories, on=['category_id'], how='left') \
  .select("event_time", "event_type", "product_id", "category_id", "category", "brand", "price", "user_id", "user_session")

# COMMAND ----------

# MAGIC %md
# MAGIC Write output

# COMMAND ----------

df.writeStream.format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", f"{basePath}/StreamingCheckpoint") \
  .table('ecommerce_demo.events_stream')

# COMMAND ----------


