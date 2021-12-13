# Databricks notebook source
# MAGIC %md
# MAGIC The New York City Taxi and Limousine Commission (TLC) provides a [dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) of trips taken by taxis and for-hire vehicles in New York City.  
# MAGIC 
# MAGIC AWS hosts this dataset in an S3 bucket in the `us-east-1` region (more information [here]( https://registry.opendata.aws/nyc-tlc-trip-records-pds/)).
# MAGIC 
# MAGIC Within this dataset, there are records for Yellow taxis, Green taxis, and for-hire vehicles (e.g. Uber, Lyft).  
# MAGIC 
# MAGIC For our purposes, we are interested in the records that have geospatial coordinates associated.  This includes:
# MAGIC - Yellow trip data from January 2009 through June 2016
# MAGIC - Green trip data from August 2013 through June 2016
# MAGIC - (The FHV trip data does not include coordinates)

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

# DBTITLE 1,Configure Paths
BASE_PATH_DBFS = f"{path}/nyctaxi/green_tripdata"
SCHEMA_V1_RAW_PATH = BASE_PATH_DBFS + "/schema_v1"
SCHEMA_V2_RAW_PATH = BASE_PATH_DBFS + "/schema_v2"
BRONZE_PATH = "dbfs:" + BASE_PATH_DBFS + "/bronze"

# COMMAND ----------

dbutils.fs.mkdirs(SCHEMA_V1_RAW_PATH)
dbutils.fs.mkdirs(SCHEMA_V2_RAW_PATH)

# COMMAND ----------

with open("schema_v1_path.txt", "w") as file:
  file.write("/dbfs" + SCHEMA_V1_RAW_PATH)
  
with open("schema_v2_path.txt", "w") as file:
  file.write("/dbfs" + SCHEMA_V2_RAW_PATH)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC path=`cat schema_v1_path.txt`
# MAGIC 
# MAGIC cat <<EOT >> green_tripdata_v1.txt
# MAGIC green_tripdata_2013-08.csv
# MAGIC green_tripdata_2013-09.csv
# MAGIC green_tripdata_2013-10.csv
# MAGIC green_tripdata_2013-11.csv
# MAGIC green_tripdata_2013-12.csv
# MAGIC green_tripdata_2014-01.csv
# MAGIC green_tripdata_2014-02.csv
# MAGIC green_tripdata_2014-03.csv
# MAGIC green_tripdata_2014-04.csv
# MAGIC green_tripdata_2014-05.csv
# MAGIC green_tripdata_2014-06.csv
# MAGIC green_tripdata_2014-07.csv
# MAGIC green_tripdata_2014-08.csv
# MAGIC green_tripdata_2014-09.csv
# MAGIC green_tripdata_2014-10.csv
# MAGIC green_tripdata_2014-11.csv
# MAGIC green_tripdata_2014-12.csv
# MAGIC EOT
# MAGIC 
# MAGIC while read file; do
# MAGIC   wget -P $path http://s3.amazonaws.com/nyc-tlc/trip%20data/$file
# MAGIC done <green_tripdata_v1.txt

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC path=`cat schema_v2_path.txt`
# MAGIC 
# MAGIC cat <<EOT >> green_tripdata_v2.txt
# MAGIC green_tripdata_2015-01.csv
# MAGIC green_tripdata_2015-02.csv
# MAGIC green_tripdata_2015-03.csv
# MAGIC green_tripdata_2015-04.csv
# MAGIC green_tripdata_2015-05.csv
# MAGIC green_tripdata_2015-06.csv
# MAGIC green_tripdata_2015-07.csv
# MAGIC green_tripdata_2015-08.csv
# MAGIC green_tripdata_2015-09.csv
# MAGIC green_tripdata_2015-10.csv
# MAGIC green_tripdata_2015-11.csv
# MAGIC green_tripdata_2015-12.csv
# MAGIC green_tripdata_2016-01.csv
# MAGIC green_tripdata_2016-02.csv
# MAGIC green_tripdata_2016-03.csv
# MAGIC green_tripdata_2016-04.csv
# MAGIC green_tripdata_2016-05.csv
# MAGIC green_tripdata_2016-06.csv
# MAGIC EOT
# MAGIC 
# MAGIC while read file; do
# MAGIC   wget -P $path http://s3.amazonaws.com/nyc-tlc/trip%20data/$file
# MAGIC done <green_tripdata_v2.txt

# COMMAND ----------

schema_v1 = StructType([
  StructField("VendorID",IntegerType(),True),
  StructField("lpep_pickup_datetime",TimestampType(),True),
  StructField("Lpep_dropoff_datetime",TimestampType(),True),
  StructField("Store_and_fwd_flag",StringType(),True),
  StructField("RateCodeID",IntegerType(),True),
  StructField("Pickup_longitude",DoubleType(),True),
  StructField("Pickup_latitude",DoubleType(),True),
  StructField("Dropoff_longitude",DoubleType(),True),
  StructField("Dropoff_latitude",DoubleType(),True),
  StructField("Passenger_count",IntegerType(),True),
  StructField("Trip_distance",DoubleType(),True),
  StructField("Fare_amount",DoubleType(),True),
  StructField("Extra",DoubleType(),True),
  StructField("MTA_tax",DoubleType(),True),
  StructField("Tip_amount",DoubleType(),True),
  StructField("Tolls_amount",DoubleType(),True),
  StructField("Ehail_fee",StringType(),True),
  StructField("Total_amount",DoubleType(),True),
  StructField("Payment_Type",IntegerType(),True),
  StructField("Trip_type",IntegerType(),True),
])

green_tripdata_v1 = (spark.read
                     .format("csv")
                     .option("header", True)
                     .schema(schema_v1)
                     .load(f"dbfs:{SCHEMA_V1_RAW_PATH}/*.csv")
                     .write
                     .format("delta")
                     .mode("overwrite")
                     .save(BRONZE_PATH)
                    )

spark.sql("CREATE TABLE green_tripdata_bronze USING DELTA LOCATION '" + BRONZE_PATH + "'")

# COMMAND ----------

schema_v2 = StructType([
  StructField("VendorID",IntegerType(),True),
  StructField("lpep_pickup_datetime",TimestampType(),True),
  StructField("Lpep_dropoff_datetime",TimestampType(),True),
  StructField("Store_and_fwd_flag",StringType(),True),
  StructField("RateCodeID",IntegerType(),True),
  StructField("Pickup_longitude",DoubleType(),True),
  StructField("Pickup_latitude",DoubleType(),True),
  StructField("Dropoff_longitude",DoubleType(),True),
  StructField("Dropoff_latitude",DoubleType(),True),
  StructField("Passenger_count",IntegerType(),True),
  StructField("Trip_distance",DoubleType(),True),
  StructField("Fare_amount",DoubleType(),True),
  StructField("Extra",DoubleType(),True),
  StructField("MTA_tax",DoubleType(),True),
  StructField("Tip_amount",DoubleType(),True),
  StructField("Tolls_amount",DoubleType(),True),
  StructField("Ehail_fee",StringType(),True),
  StructField("improvement_surcharge",DoubleType(),True), # This is a new column!
  StructField("Total_amount",DoubleType(),True),
  StructField("Payment_Type",IntegerType(),True),
  StructField("Trip_type",IntegerType(),True),
])

green_tripdata_v2 = (spark.read
                     .format("csv")
                     .option("header", True)
                     .schema(schema_v2)
                     .load(f"dbfs:{SCHEMA_V2_RAW_PATH}/*.csv")
                     .write
                     .format("delta")
                     .mode("append")
                     .option("mergeSchema", "true")
                     .save(BRONZE_PATH)
                    )

# COMMAND ----------

spark.conf.set("spark.databricks.io.skipping.mdc.curve", "hilbert")

# COMMAND ----------

spark.sql(f"OPTIMIZE {dbName}.green_tripdata_bronze ZORDER BY Pickup_latitude, Pickup_longitude")

# COMMAND ----------

display(spark.sql(f"SELECT count(*) FROM {dbName}.green_tripdata_bronze"))

# COMMAND ----------

display(spark.sql(f"SELECT count(*) FROM {dbName}.green_tripdata_bronze WHERE improvement_surcharge IS null"))

# COMMAND ----------

display(spark.sql(f"SELECT count(*) FROM {dbName}.green_tripdata_bronze WHERE improvement_surcharge IS NOT null"))
