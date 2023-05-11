# Databricks notebook source
# MAGIC %md ## WKT Data
# MAGIC 
# MAGIC * This was copied up from databricks-cli versus wget. 
# MAGIC * Data from https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc, exported as CSV which has the WKT.
# MAGIC * Data on DBFS at `dbfs:/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv`

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

BASE_PATH_DBFS = f"{path}/nyctaxi/taxi_zones"
BRONZE_PATH = "dbfs:" + BASE_PATH_DBFS + "/bronze"

# COMMAND ----------

wktDF = (sqlContext.read.format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load("/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv")
        )

# COMMAND ----------

wktDF.write \
  .format("delta") \
  .mode("overwrite") \
  .save(BRONZE_PATH)

# COMMAND ----------

spark.sql("CREATE TABLE nyc_taxi_zones_bronze USING DELTA LOCATION '" + BRONZE_PATH + "'")
