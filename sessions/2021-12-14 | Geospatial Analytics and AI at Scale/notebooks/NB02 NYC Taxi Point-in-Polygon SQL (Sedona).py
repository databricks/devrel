# Databricks notebook source
# MAGIC %md # Geospatial Processing with Sedona
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/apache/incubator-sedona/master/docs/image/sedona_logo.png" width="200"/>
# MAGIC 
# MAGIC Example point-in-polygon join implementation using Apache Sedona SQL API.
# MAGIC 
# MAGIC Ensure you are using a DBR with Spark 3.0, Java 1.8, Scala 2.12, and Python 3.7+ (tested on 7.3 LTS)
# MAGIC 
# MAGIC Link the following libraries to your cluster:
# MAGIC 
# MAGIC - **Maven Central**
# MAGIC     - `org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating`
# MAGIC     - `org.datasyslab:geotools-wrapper:geotools-24.0`
# MAGIC - **PyPi**
# MAGIC     - `apache-sedona==1.0.1`
# MAGIC     
# MAGIC _More on Sedona libraries and dependencies at https://github.com/apache/incubator-sedona/blob/sedona-1.0.1-incubating/docs/download/maven-coordinates.md_
# MAGIC 
# MAGIC This was run on a 15 node cluster using i3.xlarge AWS instance types.
# MAGIC 
# MAGIC __Authors__
# MAGIC * Initial: [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC * Additional: [Michael Johns](https://www.linkedin.com/in/michaeljohns2/) (mjohns@databricks.com)
# MAGIC 
# MAGIC __GEOINT 2021 Slides for Training Class [Databricks: Hands-On Scaled Spatial Processing using Popular Open Source Frameworks](https://docs.google.com/presentation/d/1oqEh-FD5Ls5xgo-OOEUbuzA2tMzNFQ8V/edit?usp=sharing&ouid=110699193143161784974&rtpof=true&sd=true) by Michael Johns__

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

# MAGIC %run ./resources/setup

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "1600")

# COMMAND ----------

import os

from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# COMMAND ----------

spark = SparkSession. \
    builder. \
    appName('Databricks Shell'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:geotools-24.0'). \
    getOrCreate()

# COMMAND ----------

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# MAGIC %md ## Point in Polygon Join Operation on 1% SAMPLE (Non-Optimized)
# MAGIC 
# MAGIC __For smaller data scales, can use Sedona directly with no concern of data + layout. In the example below, __

# COMMAND ----------

# MAGIC %md ### Data

# COMMAND ----------

#what is in the database currently
display(spark.sql(f"show tables from {dbName}")) 

# COMMAND ----------

# MAGIC %md ### Taxi Event Points
# MAGIC 
# MAGIC __Converting Lat/Lon cols to Point Geometry on the fly.__
# MAGIC 
# MAGIC > This is a 1% SAMPLE of the original 45M points (so 450K points)

# COMMAND ----------

points_df = spark.sql("""
  SELECT
    lpep_pickup_datetime,
    Passenger_count,
    Trip_distance,
    Tolls_amount,
    ST_Point(cast(Pickup_longitude as Decimal(24,20)), cast(Pickup_latitude as Decimal(24,20))) as pickup_point 
  FROM 
    nyctlc.green_tripdata_bronze""") \
  .sample(0.01) \
  .repartition(sc.defaultParallelism * 20) \
  .cache()
  
num_points_in_sample = points_df.count()

# COMMAND ----------

# MAGIC %md ### Taxi Zone Polygons
# MAGIC 
# MAGIC __Converting WKT to Polygon Geometries on the fly.__

# COMMAND ----------

polygon_df = spark.sql("""
  SELECT 
    zone, 
    ST_GeomFromWKT(the_geom) as geom 
  FROM 
    nyctlc.nyc_taxi_zones_bronze""") \
  .cache()

num_polygons = polygon_df.count()

# COMMAND ----------

# MAGIC %md ### Summary + Generate Temp Views

# COMMAND ----------

print("points: ", num_points_in_sample)
print("polygons: ", num_polygons)
print("worst case lookups: ", num_points_in_sample * num_polygons)

# COMMAND ----------

points_df.createOrReplaceTempView("points")
polygon_df.createOrReplaceTempView("polygons")

# COMMAND ----------

# MAGIC %md ### Perform `ST_Contains` Point-in-Polygon on 1% SAMPLE (Non-Optimized)
# MAGIC 
# MAGIC __RESULT on 15 worker nodes of AWS i3.xlarge instance type:__ 
# MAGIC 
# MAGIC > This is a 1% SAMPLE of the original 45M points (so 450K points) which takes around 2 minutes to complete

# COMMAND ----------

point_in_polygon = spark.sql("""
  SELECT 
    lpep_pickup_datetime,
    Passenger_count,
    Trip_distance,
    Tolls_amount,
    points.pickup_point, 
    polygons.zone 
  FROM 
    points, polygons 
  WHERE 
    ST_Contains(polygons.geom, points.pickup_point)"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC explain formatted select * from point_in_polygon
# MAGIC -- # point_in_polygon.explain()

# COMMAND ----------

# point_in_polygon.cache()

# COMMAND ----------

num_matched = point_in_polygon.count()
num_unmatched = num_points_in_sample - num_matched
print("Number of points matched: ", num_matched)
print("Number of points unmatched: ", num_unmatched)

# COMMAND ----------

display(point_in_polygon)

# COMMAND ----------

# MAGIC %md ## Analyze results

# COMMAND ----------

from pyspark.sql.functions import col
display(
  point_in_polygon
    .groupBy("zone")
      .count()
    .sort(col("count").desc())
)
