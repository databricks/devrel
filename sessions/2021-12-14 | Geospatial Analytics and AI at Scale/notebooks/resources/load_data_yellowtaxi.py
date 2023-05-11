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
BASE_PATH_DBFS = f"{path}/nyctaxi/yellow_tripdata"
RAW_PATH = BASE_PATH_DBFS + "/raw"
SCHEMA_V1_RAW_PATH = RAW_PATH + "/schema_v1"
BRONZE_PATH = "dbfs:" + BASE_PATH_DBFS + "/bronze"
dbutils.fs.mkdirs(SCHEMA_V1_RAW_PATH)

# COMMAND ----------

with open("schema_v1_path.txt", "w") as file:
  file.write("/dbfs" + SCHEMA_V1_RAW_PATH)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC path=`cat schema_v1_path.txt`
# MAGIC 
# MAGIC cat <<EOT >> yellow_tripdata_v1.txt
# MAGIC yellow_tripdata_2009-01.csv
# MAGIC yellow_tripdata_2009-02.csv
# MAGIC yellow_tripdata_2009-03.csv
# MAGIC yellow_tripdata_2009-04.csv
# MAGIC yellow_tripdata_2009-05.csv
# MAGIC yellow_tripdata_2009-06.csv
# MAGIC yellow_tripdata_2009-07.csv
# MAGIC yellow_tripdata_2009-08.csv
# MAGIC yellow_tripdata_2009-09.csv
# MAGIC yellow_tripdata_2009-10.csv
# MAGIC yellow_tripdata_2009-11.csv
# MAGIC yellow_tripdata_2009-12.csv
# MAGIC yellow_tripdata_2010-01.csv
# MAGIC yellow_tripdata_2010-02.csv
# MAGIC yellow_tripdata_2010-03.csv
# MAGIC yellow_tripdata_2010-04.csv
# MAGIC yellow_tripdata_2010-05.csv
# MAGIC yellow_tripdata_2010-06.csv
# MAGIC yellow_tripdata_2010-07.csv
# MAGIC yellow_tripdata_2010-08.csv
# MAGIC yellow_tripdata_2010-09.csv
# MAGIC yellow_tripdata_2010-10.csv
# MAGIC yellow_tripdata_2010-11.csv
# MAGIC yellow_tripdata_2010-12.csv
# MAGIC yellow_tripdata_2011-01.csv
# MAGIC yellow_tripdata_2011-02.csv
# MAGIC yellow_tripdata_2011-03.csv
# MAGIC yellow_tripdata_2011-04.csv
# MAGIC yellow_tripdata_2011-05.csv
# MAGIC yellow_tripdata_2011-06.csv
# MAGIC yellow_tripdata_2011-07.csv
# MAGIC yellow_tripdata_2011-08.csv
# MAGIC yellow_tripdata_2011-09.csv
# MAGIC yellow_tripdata_2011-10.csv
# MAGIC yellow_tripdata_2011-11.csv
# MAGIC yellow_tripdata_2011-12.csv
# MAGIC yellow_tripdata_2012-01.csv
# MAGIC yellow_tripdata_2012-02.csv
# MAGIC yellow_tripdata_2012-03.csv
# MAGIC yellow_tripdata_2012-04.csv
# MAGIC yellow_tripdata_2012-05.csv
# MAGIC yellow_tripdata_2012-06.csv
# MAGIC yellow_tripdata_2012-07.csv
# MAGIC yellow_tripdata_2012-08.csv
# MAGIC yellow_tripdata_2012-09.csv
# MAGIC yellow_tripdata_2012-10.csv
# MAGIC yellow_tripdata_2012-11.csv
# MAGIC yellow_tripdata_2012-12.csv
# MAGIC yellow_tripdata_2013-01.csv
# MAGIC yellow_tripdata_2013-02.csv
# MAGIC yellow_tripdata_2013-03.csv
# MAGIC yellow_tripdata_2013-04.csv
# MAGIC yellow_tripdata_2013-05.csv
# MAGIC yellow_tripdata_2013-06.csv
# MAGIC yellow_tripdata_2013-07.csv
# MAGIC yellow_tripdata_2013-08.csv
# MAGIC yellow_tripdata_2013-09.csv
# MAGIC yellow_tripdata_2013-10.csv
# MAGIC yellow_tripdata_2013-11.csv
# MAGIC yellow_tripdata_2013-12.csv
# MAGIC yellow_tripdata_2014-01.csv
# MAGIC yellow_tripdata_2014-02.csv
# MAGIC yellow_tripdata_2014-03.csv
# MAGIC yellow_tripdata_2014-04.csv
# MAGIC yellow_tripdata_2014-05.csv
# MAGIC yellow_tripdata_2014-06.csv
# MAGIC yellow_tripdata_2014-07.csv
# MAGIC yellow_tripdata_2014-08.csv
# MAGIC yellow_tripdata_2014-09.csv
# MAGIC yellow_tripdata_2014-10.csv
# MAGIC yellow_tripdata_2014-11.csv
# MAGIC yellow_tripdata_2014-12.csv
# MAGIC yellow_tripdata_2015-01.csv
# MAGIC yellow_tripdata_2015-02.csv
# MAGIC yellow_tripdata_2015-03.csv
# MAGIC yellow_tripdata_2015-04.csv
# MAGIC yellow_tripdata_2015-05.csv
# MAGIC yellow_tripdata_2015-06.csv
# MAGIC yellow_tripdata_2015-07.csv
# MAGIC yellow_tripdata_2015-08.csv
# MAGIC yellow_tripdata_2015-09.csv
# MAGIC yellow_tripdata_2015-10.csv
# MAGIC yellow_tripdata_2015-11.csv
# MAGIC yellow_tripdata_2015-12.csv
# MAGIC yellow_tripdata_2016-01.csv
# MAGIC yellow_tripdata_2016-02.csv
# MAGIC yellow_tripdata_2016-03.csv
# MAGIC yellow_tripdata_2016-04.csv
# MAGIC yellow_tripdata_2016-05.csv
# MAGIC EOT
# MAGIC 
# MAGIC while read file; do
# MAGIC   wget -P $path http://s3.amazonaws.com/nyc-tlc/trip%20data/$file
# MAGIC done <yellow_tripdata_v1.txt

# COMMAND ----------

schema_v1 = StructType([
  StructField("vendor_name",StringType(),True),
  StructField("Trip_Pickup_DateTime",TimestampType(),True),
  StructField("Trip_Dropoff_DateTime",TimestampType(),True),
  StructField("Passenger_Count",IntegerType(),True),  
  StructField("Trip_Distance",DoubleType(),True),
  StructField("Start_Lon",DoubleType(),True),
  StructField("Start_Lat",DoubleType(),True),
  StructField("Rate_Code",IntegerType(),True),
  StructField("store_and_forward",StringType(),True), 
  StructField("End_Lon",DoubleType(),True),
  StructField("End_Lat",DoubleType(),True),
  StructField("Payment_Type",StringType(),True),
  StructField("Fare_Amt",DoubleType(),True),
  StructField("surcharge",DoubleType(),True),
  StructField("mta_tax",DoubleType(),True),
  StructField("Tip_Amt",DoubleType(),True),
  StructField("Tolls_Amt",DoubleType(),True),
  StructField("Total_Amt",DoubleType(),True),
])

yellow_tripdata_v1 = (spark.read
                      .format("csv")
                      .option("header", True)
                      .schema(schema_v1)
                      .load(f"dbfs:{SCHEMA_V1_RAW_PATH}/yellow_tripdata_2009-*.csv")
                      .withColumnRenamed("vendor_name", "vendor_id")
                      .withColumnRenamed("Trip_Pickup_DateTime", "pickup_datetime")
                      .withColumnRenamed("Trip_Dropoff_DateTime", "dropoff_datetime")
                      .withColumnRenamed("Passenger_Count", "passenger_count")
                      .withColumnRenamed("Trip_Distance", "trip_distance")
                      .withColumnRenamed("Start_Lon", "pickup_longitude")
                      .withColumnRenamed("Start_Lat", "pickup_latitude")
                      .withColumnRenamed("Rate_Code", "rate_code")
                      .withColumnRenamed("store_and_forward", "store_and_fwd_flag")
                      .withColumnRenamed("End_Lon", "dropoff_longitude")
                      .withColumnRenamed("End_Lat", "dropoff_latitude")
                      .withColumnRenamed("Payment_Type", "payment_type")
                      .withColumnRenamed("Fare_Amt", "fare_amount")
                      .withColumnRenamed("Tip_Amt", "tip_amount")
                      .withColumnRenamed("Tolls_Amt", "tolls_amount")
                      .withColumnRenamed("Total_Amt", "total_amount")
                     )

# COMMAND ----------

schema_v2 = StructType([
  StructField("vendor_id",StringType(),True),
  StructField("pickup_datetime",TimestampType(),True),
  StructField("dropoff_datetime",TimestampType(),True),
  StructField("passenger_count",IntegerType(),True),  
  StructField("trip_distance",DoubleType(),True),
  StructField("pickup_longitude",DoubleType(),True),
  StructField("pickup_latitude",DoubleType(),True),
  StructField("rate_code",IntegerType(),True),
  StructField("store_and_fwd_flag",StringType(),True), 
  StructField("dropoff_longitude",DoubleType(),True),
  StructField("dropoff_latitude",DoubleType(),True),
  StructField("payment_type",StringType(),True),
  StructField("fare_amount",DoubleType(),True),
  StructField("surcharge",DoubleType(),True),
  StructField("mta_tax",DoubleType(),True),
  StructField("tip_amount",DoubleType(),True),
  StructField("tolls_amount",DoubleType(),True),
  StructField("total_amount",DoubleType(),True),
])

yellow_tripdata_v2 = (spark.read
                      .format("csv")
                      .option("header", "true")
                      .schema(schema_v2)
                      .load(f"dbfs:{SCHEMA_V1_RAW_PATH}/yellow_tripdata_201[0-4]*.csv")
                     )

# COMMAND ----------

union = yellow_tripdata_v1.union(yellow_tripdata_v2)
_ = (union.write
     .format("delta")
     .mode("overwrite")
     .save(BRONZE_PATH)
    )

spark.sql("CREATE TABLE yellow_tripdata_bronze USING DELTA LOCATION '" + BRONZE_PATH + "'")

# COMMAND ----------

spark.conf.set("spark.databricks.io.skipping.mdc.curve", "hilbert")

# COMMAND ----------

spark.sql(f"OPTIMIZE {dbName}.yellow_tripdata_bronze")

# COMMAND ----------

# MAGIC %md Cells below are optional 

# COMMAND ----------

schema_v3 = StructType([
  StructField("VendorID",StringType(),True),
  StructField("tpep_pickup_datetime",TimestampType(),True),
  StructField("tpep_dropoff_datetime",TimestampType(),True),
  StructField("passenger_count",IntegerType(),True),  
  StructField("trip_distance",DoubleType(),True),
  StructField("pickup_longitude",DoubleType(),True),
  StructField("pickup_latitude",DoubleType(),True),
  StructField("RateCodeID",IntegerType(),True),
  StructField("store_and_fwd_flag",StringType(),True), 
  StructField("dropoff_longitude",DoubleType(),True),
  StructField("dropoff_latitude",DoubleType(),True),
  StructField("payment_type",StringType(),True),
  StructField("fare_amount",DoubleType(),True),
  StructField("extra",DoubleType(),True),
  StructField("mta_tax",DoubleType(),True),
  StructField("tip_amount",DoubleType(),True),
  StructField("tolls_amount",DoubleType(),True),
  StructField("improvement_surcharge",DoubleType(),True),
  StructField("total_amount",DoubleType(),True),
])

yellow_tripdata_v3 = (spark.read
                      .format("csv")
                      .option("header", "true")
                      .schema(schema_v3)
                      .load(f"dbfs:{SCHEMA_V1_RAW_PATH}/yellow_tripdata_201[5-6]*.csv")
                      .withColumnRenamed("VendorID", "vendor_id")
                      .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
                      .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
                      .withColumnRenamed("RateCodeID", "rate_code")
                      .withColumnRenamed("extra", "surcharge")
                     )

_ = (yellow_tripdata_v3.write
     .format("delta")
     .mode("append")
     .option("mergeSchema",True)
     .save(BRONZE_PATH)
    )

# COMMAND ----------

print(SCHEMA_V1_RAW_PATH)

# COMMAND ----------

# MAGIC %fs ls /Users/christopher.chalcraft@databricks.com/geospatial/nyctaxi/yellow_tripdata/raw/schema_v1

# COMMAND ----------

spark.sql(f"OPTIMIZE {dbName}.yellow_tripdata_bronze")

# COMMAND ----------

display(spark.sql(f"SELECT count(*) FROM {dbName}.yellow_tripdata_bronze"))

# COMMAND ----------


