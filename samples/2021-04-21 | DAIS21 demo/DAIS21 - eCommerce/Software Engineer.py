# Databricks notebook source
email = 'yourEmail'
email = 'yourEmail'
basePath = f"/Users/{email}/ecommerce-demo"

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook starts a process to simulate the arrival of data into the destination folder.<br>
# MAGIC Here we add one hour of data at each iteration

# COMMAND ----------

for i in range(10000):
  time = i*3600
  spark_df = spark.sql(f"""Select * 
                           From ecommerce_demo.events 
                           Where date_trunc('hour', event_time) = from_unixtime(to_unix_timestamp('2019-10-01', 'yyyy-MM-dd') + {time}, 'yyyy-MM-dd HH')""")
  pd_df = spark_df.toPandas()
  file_path = f"/dbfs{basePath}/Streaming/{time}.csv"
  pd_df.to_csv(file_path, index=False, header=False)
  print(f'Saved to {file_path}.')

# COMMAND ----------


