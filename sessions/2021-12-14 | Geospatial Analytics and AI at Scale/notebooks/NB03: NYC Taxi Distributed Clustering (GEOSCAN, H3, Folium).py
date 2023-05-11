# Databricks notebook source
# MAGIC %md # Distributed Clustering with GEOSCAN
# MAGIC 
# MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
# MAGIC 
# MAGIC Demonstrates [GEOSCAN](https://github.com/databrickslabs/geoscan) on the [nyctaxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset (green trips only).  See Antoine Amend's blog article "[Identifying Financial Fraud With Geospatial Clustering](https://databricks.com/blog/2021/04/13/identifying-financial-fraud-with-geospatial-clustering.html)" for more info on GEOSCAN.
# MAGIC 
# MAGIC __Libraries__
# MAGIC * Add the following maven coordinates to your cluster: `com.databricks.labs:geoscan:0.1` and `com.uber:h3:3.6.3`
# MAGIC * The first code cell will pip install a session scoped libraries for geoscan, h3, and folium
# MAGIC 
# MAGIC _This was run on DBR ML 7.3 with 3 worker nodes of AWS instance type i3.xlarge. The machine learning runtime is needed for the mlflow integration demonstrated._
# MAGIC 
# MAGIC __Authors__
# MAGIC * Initial: [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC * Additional: [Michael Johns](https://www.linkedin.com/in/michaeljohns2/) (mjohns@databricks.com)

# COMMAND ----------

# MAGIC %md ## Initial Config

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databrickslabs/geoscan.git#subdirectory=python h3==3.6.3 folium mlflow

# COMMAND ----------

# MAGIC %run ./resources/setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

points_df = spark.read.table('green_tripdata_bronze') \
  .withColumnRenamed("Pickup_longitude", "longitude") \
  .withColumnRenamed("Pickup_latitude", "latitude") \
  .drop("VendorID") \
  .drop("lpep_pickup_datetime") \
  .drop("Lpep_dropoff_datetime") \
  .drop("Store_and_fwd_flag") \
  .drop("RateCodeID") \
  .drop("Dropoff_longitude") \
  .drop("Dropoff_latitude") \
  .drop("Passenger_count") \
  .drop("Trip_distance") \
  .drop("Fare_amount") \
  .drop("Extra") \
  .drop("MTA_tax") \
  .drop("Tip_amount") \
  .drop("Tolls_amount") \
  .drop("Ehail_fee") \
  .drop("Total_amount") \
  .drop("Payment_type") \
  .drop("Trip_type") \
  .drop("improvement_surcharge")

num_points = points_df.count()
print("num_points: ", num_points)

#display(points_df)

# COMMAND ----------

import h3
from pyspark.sql.functions import udf
from pyspark.sql import functions as F

@udf("string")
def to_h3(lat, lng, precision):
  h = h3.geo_to_h3(lat, lng, precision)
  return h.upper()

# examine distribution at resolution 9
#display(
#  points_df
#    .groupBy(to_h3(F.col('latitude'), F.col('longitude'), F.lit(9)).alias('h3'))
#    .count()
#    .orderBy(F.desc('count'))
#)

# COMMAND ----------

# MAGIC %md ## Render HeatMap (Folium)
# MAGIC 
# MAGIC __Using a sample here for visualization__

# COMMAND ----------

heatmap_sample = points_df.sample(0.001)
heatmap_sample.count()

# COMMAND ----------

import folium
from folium import plugins

points = heatmap_sample.toPandas()[['latitude', 'longitude']]
nyc = folium.Map([40.75466940037548,-73.98365020751953], zoom_start=11, width='80%', height='100%')
folium.TileLayer('Stamen Toner').add_to(nyc)
nyc.add_child(plugins.HeatMap(points.to_numpy(), radius=12))
nyc

# COMMAND ----------

# MAGIC %md ## Geoscan

# COMMAND ----------

geoscan_sample = points_df.sample(0.00035)
geosscan_sample_count = geoscan_sample.count()
print("geosscan_sample_count: ", geosscan_sample_count)

# COMMAND ----------

from geoscan import Geoscan
import mlflow

with mlflow.start_run(run_name='GEOSCAN_NYCTAXI') as run:

  geoscan = Geoscan() \
    .setLatitudeCol('latitude') \
    .setLongitudeCol('longitude') \
    .setPredictionCol('cluster') \
    .setEpsilon(200) \
    .setMinPts(20)

  mlflow.log_param('epsilon', 200)
  mlflow.log_param('minPts', 20)

  model = geoscan.fit(geoscan_sample)
  mlflow.spark.log_model(model, "geoscan")
  run_id = run.info.run_id

geoJson = model.toGeoJson()
with open('/tmp/geoscan.geojson', 'w') as f:
  f.write(geoJson)

client = mlflow.tracking.MlflowClient()
client.log_artifact(run_id, "/tmp/geoscan.geojson")

# COMMAND ----------

# MAGIC %md ### Render GEOSCAN Cluster + Heat Map

# COMMAND ----------

folium.GeoJson(geoJson).add_to(nyc)
nyc

# COMMAND ----------

# TODO explore the other sampling method to smooth out skew and reincorporate the commands to display distribution

import random
from pyspark.sql.types import *

# we randomly select maximum 10 points within a same polygon of size 11 (30m)
def sample(latitudes, longitudes):
  l = list(zip(latitudes, longitudes))
  return random.sample(l, min(len(l), 10))

sample_schema = ArrayType(StructType([StructField("latitude", DoubleType()), StructField("longitude", DoubleType())]))
sample_udf = udf(sample, sample_schema)

sample_df = (
  points_df
    .groupBy(to_h3(F.col("latitude"), F.col("longitude"), F.lit(11)))
    .agg(F.collect_list(F.col("latitude")).alias("latitudes"), F.collect_list(F.col("longitude")).alias("longitudes"))
    .withColumn('sample', F.explode(sample_udf(F.col('latitudes'), F.col('longitudes'))))
    .select('sample.latitude', 'sample.longitude')
)
sample_df.cache().repartition(sc.defaultParallelism * 20)
sample_count = sample_df.count()

print("num_points: ", num_points)
print("sample_count:", sample_count)
print("sample %: ", (sample_count / num_points) * 100 )

#display(
#  sample_df
#    .groupBy(to_h3(F.col("latitude"), F.col("longitude"), F.lit(9)).alias("h3"))
#    .count()
#    .orderBy(F.desc("count"))
#)
