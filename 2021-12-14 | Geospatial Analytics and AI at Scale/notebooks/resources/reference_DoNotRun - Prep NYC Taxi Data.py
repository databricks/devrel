# Databricks notebook source
# MAGIC %md # Prepare NYC Taxi Data

# COMMAND ----------

# MAGIC %md ## Read Raw Taxi CSV Files

# COMMAND ----------

!! DO NOT RUN

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget -P /dbfs/tmp/taxi-csv/ -i https://raw.githubusercontent.com/toddwschneider/nyc-taxi-data/master/setup_files/raw_data_urls.txt

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/taxi-csv/

# COMMAND ----------

# MAGIC %md ## Clean Green Cab data and write it to Delta

# COMMAND ----------

df_green = spark.read.format("csv").option("header", "true").load("tmp/taxi-csv/green_tripdata_*.csv")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
# 
df_green_silver = df_green.withColumnRenamed("VendorID","vendor_id")\
.withColumn("pickup_datetime",from_unixtime(unix_timestamp(col("lpep_pickup_datetime"))))\
.withColumn("dropoff_datetime",from_unixtime(unix_timestamp(col("Lpep_dropoff_datetime"))))\
.withColumnRenamed("Store_and_fwd_flag","store_and_forward")\
.withColumnRenamed("RateCodeID","rate_code_id")\
.withColumn("dropoff_latitude",col("Dropoff_latitude").cast(DoubleType()))\
.withColumn("dropoff_longitude",col("Dropoff_longitude").cast(DoubleType()))\
.withColumn("pickup_latitude",col("Pickup_latitude").cast(DoubleType()))\
.withColumn("pickup_longitude",col("Pickup_longitude").cast(DoubleType()))\
.withColumn("passener_count",col("Passenger_count").cast(IntegerType()))\
.withColumn("trip_distance",col("Trip_distance").cast(DoubleType()))\
.withColumn("fare_amount",col("Fare_amount").cast(DoubleType()))\
.withColumn("extra",col("Extra").cast(DoubleType()))\
.withColumn("mta_tax",col("MTA_tax").cast(DoubleType()))\
.withColumn("tip_amount",col("Tip_amount").cast(DoubleType()))\
.withColumn("ehail_fee",col("Ehail_fee").cast(DoubleType()))\
.withColumn("payment_type",col("Payment_type"))\
.withColumn("trip_type",col("Trip_type "))\
.withColumn("tolls_amount", col("Tolls_amount").cast(DoubleType()))\
.withColumn("total_amount", col("Total_amount").cast(DoubleType()))\
.select(["vendor_id","pickup_datetime","dropoff_datetime","store_and_forward","rate_code_id","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passener_count","trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee",  "total_amount", "payment_type", "trip_type"])


# VendorID:string               --> vendor_id string
# lpep_pickup_datetime:string   --> pickup_datetime datetime
# Lpep_dropoff_datetime:string  --> dropoff_datetime datetime
# Store_and_fwd_flag:string     --> store_and_forward string
# RateCodeID:string             --> rate_code_id string
# Pickup_longitude:string       --> pickup_longitude double
# Pickup_latitude:string        --> pickup_latitude double
# Dropoff_longitude:string      --> dropoff_longitude double
# Dropoff_latitude:string       --> dropoff_latitude double
# Passenger_count:string        --> passener_count integer
# Trip_distance:string          --> trip_distance double
# Fare_amount:string            --> fare_amount double
# Extra:string                  --> extra double
# MTA_tax:string                --> mta_tax double
# Tip_amount:string             --> tip_amount double
# Tolls_amount:string           --> tolls_amount double 
# Ehail_fee:string              --> ehail_fee double
# Total_amount:string           --> total_amount double
# Payment_type:string           --> payment_type string
# Trip_type:string              --> trip_type string


# COMMAND ----------

 df_green_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/ml/blogs/geospatial/delta/nyc-green")

# COMMAND ----------

df_green_silver.columns

# COMMAND ----------

display(df_green_silver)

# COMMAND ----------

# MAGIC %md ## Read Yellow Cab data and write it to Delta

# COMMAND ----------

df_yellow = spark.read.format("csv").option("header", "true").load("tmp/taxi-csv/yellow_tripdata_*.csv")

# COMMAND ----------

df_yellow_silver = df_yellow.withColumnRenamed("vendor_name","vendor_id")\
.withColumn("pickup_datetime",from_unixtime(unix_timestamp(col("Trip_Pickup_DateTime"))))\
.withColumn("dropoff_datetime",from_unixtime(unix_timestamp(col("Trip_Dropoff_DateTime"))))\
.withColumnRenamed("store_and_forward","store_and_forward")\
.withColumnRenamed("Rate_Code","rate_code_id")\
.withColumn("dropoff_latitude",col("End_Lat").cast(DoubleType()))\
.withColumn("dropoff_longitude",col("End_Lon").cast(DoubleType()))\
.withColumn("pickup_latitude",col("Start_Lat").cast(DoubleType()))\
.withColumn("pickup_longitude",col("Start_Lon").cast(DoubleType()))\
.withColumn("passener_count",col("Passenger_Count").cast(IntegerType()))\
.withColumn("trip_distance",col("Trip_Distance").cast(DoubleType()))\
.withColumn("fare_amount",col("Fare_Amt").cast(DoubleType()))\
.withColumn("extra",col("surcharge").cast(DoubleType()))\
.withColumn("mta_tax",col("mta_tax").cast(DoubleType()))\
.withColumn("tip_amount",col("Tip_Amt").cast(DoubleType()))\
.withColumn("tolls_amount",col("Tolls_Amt").cast(DoubleType()))\
.withColumn("total_amount", col("Total_Amt").cast(DoubleType()))\
.withColumn("payment_type",col("Payment_Type"))\
.select(["vendor_id","pickup_datetime","dropoff_datetime","store_and_forward","rate_code_id","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","passener_count","trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "total_amount", "payment_type"])





# vendor_name:string           --> vendor_id string
# Trip_Pickup_DateTime:string  --> pickup_datetime datetime
# Trip_Dropoff_DateTime:string --> dropoff_datetime datetime
# Passenger_Count:string       --> passenger_count integer
# Trip_Distance:string         --> trip_distance double
# Start_Lon:string             --> pickup_longitude double
# Start_Lat:string             --> pickup_latitude double
# Rate_Code:string             --> rate_code_id string
# store_and_forward:string     --> store_and_forward string
# End_Lon:string               --> dropoff_longitude double
# End_Lat:string               --> dropoff_latitude double
# Payment_Type:string          --> payment_type string
# Fare_Amt:string              --> fare_amount double
# surcharge:string             --> extra double
# mta_tax:string               --> mta_tax double
# Tip_Amt:string               --> trip_amount double
# Tolls_Amt:string             --> tolls_amount double
# Total_Amt:string             --> total_amount double 


# COMMAND ----------

display(df_yellow_silver)

# COMMAND ----------

df_yellow_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/ml/blogs/geospatial/delta/nyc-yellow")

# COMMAND ----------

# MAGIC %md ## Optmize Delta Tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE delta. `dbfs:/ml/blogs/geospatial/delta/nyc-yellow`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE delta. `dbfs:/ml/blogs/geospatial/delta/nyc-green`;

# COMMAND ----------

# MAGIC %md ## Count the records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from delta. `dbfs:/ml/blogs/geospatial/delta/nyc-green`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from delta. `dbfs:/ml/blogs/geospatial/delta/nyc-yellow`;

# COMMAND ----------

# MAGIC %md ## WKT Data
# MAGIC 
# MAGIC * This was copied up from databricks-cli versus wget. 
# MAGIC * Data from https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc, exported as CSV which has the WKT.
# MAGIC * Data on DBFS at `dbfs:/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv`

# COMMAND ----------

dbutils.fs.head("/ml/blogs/geospatial/nyc_taxi_zones.wkt.csv")

# COMMAND ----------

# MAGIC %md ## GeoJSON
# MAGIC 
# MAGIC * This was copied up from databricks-cli versus wget.
# MAGIC * Data from https://data.cityofnewyork.us/City-Government/Borough-Boundaries/tqmj-j8zm, exported as GeoJSON.
# MAGIC * Data on DBFS at `dbfs:/ml/blogs/geospatial/nyc_boroughs.geojson`

# COMMAND ----------

dbutils.fs.head('/ml/blogs/geospatial/nyc_boroughs.geojson')

# COMMAND ----------

# MAGIC %md ## Shapefile
# MAGIC 
# MAGIC * This was copied up from databricks-cli versus wget.
# MAGIC * Data from https://data.cityofnewyork.us/Housing-Development/Shapefiles-and-base-map/2k7f-6s2k, exported as Shapefiles.
# MAGIC * Data on DBFS at `dbfs:/ml/blogs/geospatial/shapefiles/nyc/nyc_buildings.*`

# COMMAND ----------

display(dbutils.fs.ls('/ml/blogs/geospatial/shapefiles/nyc/'))
