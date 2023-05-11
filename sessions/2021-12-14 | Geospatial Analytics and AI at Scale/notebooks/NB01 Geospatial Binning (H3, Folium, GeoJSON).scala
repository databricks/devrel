// Databricks notebook source
// MAGIC %md 
// MAGIC # Simple Geospatial Binning with Uber's H3 library
// MAGIC 
// MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
// MAGIC 
// MAGIC - Demonstrates how to spatially index point data using Uber's Hexagonal Hierarchical Spatial Index (H3)
// MAGIC - More info on H3 here: https://eng.uber.com/h3/
// MAGIC - Approach has applicability to many uses cases including spatial aggregations, spatial smoothing, and efficient spatial joins
// MAGIC 
// MAGIC __Libraries__
// MAGIC * Add the following maven coordinates to your cluster: `com.uber:h3:3.6.3`
// MAGIC * Code cell will pip install a session scoped libraries for h3, folium, and geojson
// MAGIC 
// MAGIC _Example run on a 10 node cluster (`i3.xlarge` instances). Also, this defaults to a __Scala notebook but also includes Python and SQL cells as well as Bash!__
// MAGIC 
// MAGIC __Authors__
// MAGIC * Initial: [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
// MAGIC * Additional: [Michael Johns](https://www.linkedin.com/in/michaeljohns2/) (mjohns@databricks.com)

// COMMAND ----------

// MAGIC %run ./resources/setup

// COMMAND ----------

// DBTITLE 1,Imports
// Required: attach com.uber:h3:3.6.3 to cluster from Maven Central
import com.uber.h3core.H3Core

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Define UDF to convert coordinates to H3 index at a given precision
@transient lazy val h3 = new ThreadLocal[H3Core] {
  override def initialValue() = H3Core.newInstance()
}

def convertToH3Address(xLong: Double, yLat: Double, precision: Int): String = {
  h3.get.geoToH3Address(yLat, xLong, precision)
}

// Register UDF
val geoToH3Address: UserDefinedFunction = udf(convertToH3Address _)

// COMMAND ----------

// DBTITLE 1,Grab a handle NYC taxi trip Delta table
// val df_nyc = spark.table("nyctaxidelta")//.limit(1000000)
val df_nyc = spark.table("green_tripdata_bronze")//.limit(1000000)
  
  // ignore columns irrelevant to this example to maximize data skipping
  .drop("neighborhood")
  .drop("vendorId")
  .drop("dropoff_datetime")
  .drop("passenger_count")
  .drop("trip_distance")
  .drop("rateCodeId")
  .drop("store_fwd")
  .drop("dropoff_longitude")
  .drop("dropoff_latitude")
  .drop("payment_type")
  .drop("fare_amount")
  .drop("extra")
  .drop("mta_tax")
  .drop("tip_amount")
  .drop("tolls_amount")
  .drop("improvement_surcharge")
  .drop("total_amount")

//display(df_nyc)

// COMMAND ----------

display(df_nyc)

// COMMAND ----------

// DBTITLE 1,We have 45 million records
// MAGIC %python
// MAGIC display(spark.sql(f"SELECT count(*) FROM {dbName}.green_tripdata_bronze"))

// COMMAND ----------

// MAGIC %md H3 supports 16 distinct resolutions: https://h3geo.org/#/documentation/core-library/resolution-table

// COMMAND ----------

// default to number of cores on the machine
sc.defaultParallelism

// COMMAND ----------

// DBTITLE 1,Add H3 resolutions
val resolutions = Array.range(5, 10) // in this example we will index at 5 different resolutions

// column names
val colPrefix = "pickup_h3_res_"
val latCol = "pickup_latitude"
val lngCol = "pickup_longitude"

// fancy statement to add columns at various H3 resolutions in a concise manner
val df_nyc_h3 = resolutions.foldLeft(df_nyc)( 
  (df, res) => df.withColumn(colPrefix + res, geoToH3Address(col(lngCol), col(latCol), lit(res)))
)
.repartition(sc.defaultParallelism * 10) // smooth out any skew
.cache() // cache on the cluster

display(df_nyc_h3)

// COMMAND ----------

// DBTITLE 1,Register temp table
df_nyc_h3.createOrReplaceTempView("nyctaxideltaFullyIndexed")

// COMMAND ----------

// DBTITLE 1,Examine record counts at each distinct resolution
// MAGIC %sql
// MAGIC SELECT 
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_0 ) AS res0_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_1 ) AS res1_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_2 ) AS res2_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_3 ) AS res3_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_4 ) AS res4_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_5 ) AS res5_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_6 ) AS res6_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_7 ) AS res7_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_8 ) AS res8_count,
// MAGIC   COUNT(DISTINCT pickup_h3_res_9 ) AS res9_count
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_10 ) AS res10_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_11 ) AS res11_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_12 ) AS res12_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_13 ) AS res13_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_14 ) AS res14_count,
// MAGIC   -- COUNT(DISTINCT pickup_h3_res_15 ) AS res15_count
// MAGIC FROM nyctaxideltaFullyIndexed;

// COMMAND ----------

// DBTITLE 1,Examine distribution at resolution 8
// MAGIC %sql 
// MAGIC SELECT 
// MAGIC   pickup_h3_res_8, 
// MAGIC   count(pickup_h3_res_8) AS count 
// MAGIC FROM 
// MAGIC   nyctaxideltaFullyIndexed 
// MAGIC GROUP BY 
// MAGIC   pickup_h3_res_8 
// MAGIC ORDER BY 
// MAGIC   count DESC

// COMMAND ----------

// MAGIC %md 
// MAGIC - Note: we coded in Scala above for superior UDF performance, but we switch to Python below to leverage Folium for maps
// MAGIC - It's often beneficial to visualize geospatial data using a heatmap
// MAGIC - My libraries exist to render these types of visualizations client site, but they will choke on large scale data
// MAGIC - In these cases it's often beneficial to preprocess the data on the server, as we did above
// MAGIC - Let's visualize the hex data on a map
// MAGIC - Aggregating & visualizing data in the context of equally-sized hexagons (versus varying polygon boundaries) can reduce bias and produce more reliable visualizations

// COMMAND ----------

// DBTITLE 1,The H3 Python bindings require cmake
// MAGIC %sh
// MAGIC apt-get install -y cmake

// COMMAND ----------

// DBTITLE 1,Install H3 Python bindings
// MAGIC %sh
// MAGIC /databricks/python/bin/pip install h3 folium geojson

// COMMAND ----------

// DBTITLE 1,Imports
// MAGIC %python
// MAGIC 
// MAGIC from h3 import h3
// MAGIC import json
// MAGIC from geojson.feature import *
// MAGIC from folium import Map, GeoJson
// MAGIC import branca.colormap as cm
// MAGIC import folium

// COMMAND ----------

// DBTITLE 1,Generate a Pandas DataFrame
// MAGIC %python
// MAGIC 
// MAGIC # same query as above, but using PySpark
// MAGIC pyspark_df = spark.sql("SELECT pickup_h3_res_8 AS h3_idx, count(pickup_h3_res_8) AS count FROM nyctaxideltaFullyIndexed GROUP BY h3_idx ORDER BY count DESC")
// MAGIC 
// MAGIC # convert Spark DataFrame to Pandas DF
// MAGIC pandas_df = pyspark_df.toPandas()
// MAGIC 
// MAGIC # add a column that includes the GeoJSON polygon representation for each hexagon
// MAGIC # - this version of h3 doesn't accept first arg keyword `h3_address` in `h3.h3_to_geo_boundary(x, geo_json=True)`
// MAGIC pandas_df["geometry"] = pandas_df.h3_idx.apply(lambda x:
// MAGIC   {
// MAGIC     "type" : "Polygon",
// MAGIC     "coordinates" : [h3.h3_to_geo_boundary(x, geo_json=True)] 
// MAGIC   }
// MAGIC )
// MAGIC 
// MAGIC # uncomment to display pandas DF
// MAGIC #pandas_df

// COMMAND ----------

// DBTITLE 1,Define function to encapsulate Folium map creation logic
// MAGIC %python
// MAGIC 
// MAGIC def choropleth_map(df, border_color = 'black', fill_opacity = 0.3, initial_map = None, with_legend = True):
// MAGIC 
// MAGIC   min_value, max_value = df["count"].quantile([0.05,0.95]).apply(lambda x: round(x, 2))
// MAGIC   m = round(df["count"].mean(),2)
// MAGIC   
// MAGIC   if initial_map is None:
// MAGIC     initial_map = Map(
// MAGIC       location= [40.7128, -74.0060], 
// MAGIC       zoom_start=11, 
// MAGIC       tiles="cartodbpositron", 
// MAGIC       attr= '© <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors © <a href="http://cartodb.com/attributions#basemaps">CartoDB</a>' 
// MAGIC     )
// MAGIC     
// MAGIC   #color names accepted https://github.com/python-visualization/branca/blob/master/branca/_cnames.json
// MAGIC     custom_cm = cm.LinearColormap(
// MAGIC     ['green','yellow','red'], 
// MAGIC     vmin=min_value, 
// MAGIC     vmax=max_value
// MAGIC   )
// MAGIC   
// MAGIC   #create geojson data from dataframe
// MAGIC   list_features = []
// MAGIC   for i,row in df.iterrows():
// MAGIC     feature = Feature(geometry = row["geometry"] , id=row["h3_idx"], properties = {"value" : row["count"]})
// MAGIC     list_features.append(feature)    
// MAGIC   feat_collection = FeatureCollection(list_features)
// MAGIC   geojson_data = json.dumps(feat_collection)
// MAGIC       
// MAGIC   GeoJson(
// MAGIC     geojson_data,
// MAGIC     style_function=lambda feature: {
// MAGIC       'fillColor': custom_cm(feature['properties']['value']),
// MAGIC       'color': border_color,
// MAGIC       'weight': 1,
// MAGIC       'fillOpacity': fill_opacity 
// MAGIC     }, 
// MAGIC     name = "Choropleth"
// MAGIC   ).add_to(initial_map)
// MAGIC 
// MAGIC   #add legend (not recommended if multiple layers)
// MAGIC   if with_legend == True:
// MAGIC     custom_cm.add_to(initial_map)
// MAGIC   
// MAGIC   return initial_map

// COMMAND ----------

// DBTITLE 1,Show Map
// MAGIC %python
// MAGIC 
// MAGIC hex_map = choropleth_map(df = pandas_df)
// MAGIC hex_map

// COMMAND ----------

// MAGIC %md __Note: Reference <a href="$./NB05: NYC Taxi Spatial Disaggregation (H3, Kepler.gl)">NB05: Spatial Disaggregation with H3 + Kepler.gl</a> example notebook for more advanced visualization technique.__
