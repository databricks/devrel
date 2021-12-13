# Databricks notebook source
# MAGIC %md # Spatial Disaggregation with H3 + Kepler.gl
# MAGIC 
# MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
# MAGIC 
# MAGIC This Notebook presents a technique to "rasterize" a large point dataset for visualization in Kepler.gl **(eye candy in final cell)**.  This is a useful technique to visualize large spatial datasets with high fidelity.  We use an aggregation technique using [Uber’s Hexagonal Hierarchical Spatial Index (H3)](https://eng.uber.com/h3/).  In this example, we visualize 45M records from the [nyctaxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset __without sampling__. 
# MAGIC 
# MAGIC __Libraries__
# MAGIC * Add the following maven coordinates to your cluster: `com.uber:h3:3.6.3`
# MAGIC * The first code cell will pip install a session scoped libraries for h3 and keplergl
# MAGIC 
# MAGIC _This was run on DBR ML 7.3 with 3 worker nodes of AWS instance type i3.xlarge._
# MAGIC 
# MAGIC __Authors__
# MAGIC * Initial: [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC * Additional: [Michael Johns](https://www.linkedin.com/in/michaeljohns2/) (mjohns@databricks.com)

# COMMAND ----------

# MAGIC %md ## Initial Setup

# COMMAND ----------

# MAGIC %pip install h3==3.6.3 keplergl

# COMMAND ----------

# MAGIC %run ./resources/setup

# COMMAND ----------

import keplergl
from h3 import h3
import pandas as pd

import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, DoubleType

# COMMAND ----------

# MAGIC %md __Define H3 UDFs__

# COMMAND ----------

@udf
def h3_hex_udf(lat, lon, resolution):
  """
   UDF to get the H3 value based on lat/lon at a given resolution
  """
  return h3.geo_to_h3(lat, lon, resolution)

schema = StructType([
  StructField("lat", DoubleType(), False),
  StructField("lon", DoubleType(), False)
])

@udf(schema)
def h3_hex_centroid(hex):
  """
  Get the centroid from a h3 hex location
  """
  (lat, lon) = h3.h3_to_geo(hex)
  return {
    "lat": lat,
    "lon": lon
  }

# COMMAND ----------

# MAGIC %md ## Compute Aggregation
# MAGIC 
# MAGIC __This was run at H3 resolution=11, can go as fine as 15 if useful, see see: https://h3geo.org/docs/core-library/restable/.__

# COMMAND ----------

h3_resolution = 11

# COMMAND ----------

# MAGIC %md __Aggregation Steps__
# MAGIC 1. Read our point data, computing the H3 index value for each pickup
# MAGIC 1. Group by the H3 values and compute counts
# MAGIC 1. Add the centroid for each hex
# MAGIC 1. Rename columns and filter

# COMMAND ----------

print(dbName)

# COMMAND ----------

h3_aggregation = (
  spark.read.table(f"{dbName}.green_tripdata_bronze")
    
    # compute h3 grid location at a given resolution for each point
    .withColumn(
      "h3", 
      h3_hex_udf(sf.col("Pickup_latitude"), sf.col("Pickup_longitude"), sf.lit(h3_resolution)))
  
    # group by the h3 grid
    .groupBy("h3")
  
    # grab counts
    .count()
  
    # add the centroid  
    .withColumn("h3_centroid", h3_hex_centroid(sf.col("h3")))
  
    # rename columns  
    .select(
      "h3",
      "count",
      sf.col("h3_centroid.lat").alias("lat"),
      sf.col("h3_centroid.lon").alias("lon"),
      sf.log10("count").alias("log_count")
    )
  
    # ensure sparse representation
    .filter(sf.col("count") > sf.lit(0))
)

# COMMAND ----------

# MAGIC %md __Convert results to Pandas__

# COMMAND ----------

pandas_df = h3_aggregation.toPandas()

# COMMAND ----------

# MAGIC %md __Export results to CSV__

# COMMAND ----------

BASE_PATH_DBFS = f"{path}/nyctaxi/exports"
pandas_df.to_csv(f"{BASE_PATH_DBFS}/green_rollups/out_{h3_resolution}_.csv", index=False, header=True)
