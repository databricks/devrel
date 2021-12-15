# Databricks notebook source
# MAGIC %md
# MAGIC Author: milos.colic@databricks.com

# COMMAND ----------

# DBTITLE 1,Lib Installation
# MAGIC %pip install geopandas h3 s2 shapely folium

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point Data Preparation (Trips Data)

# COMMAND ----------

# MAGIC %md
# MAGIC For our point data we will use New York City Taxis data. </br>
# MAGIC This data provides us a volume of 1.6B records and in addition it contains two different Point Features - pick up and drop off. </br>
# MAGIC Our Polygon features dataset will remain the New York City Neighbourhoods. 

# COMMAND ----------

# DBTITLE 1,Read NYC Taxis Data
taxi_trips = spark.read.format("delta").load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/")

# COMMAND ----------

# DBTITLE 1,Visualise a Sample of Data
display(taxi_trips)

# COMMAND ----------

# DBTITLE 1,Verify Row Count
taxi_trips.count()

# COMMAND ----------

# MAGIC %md
# MAGIC In order to perform Point In Polygon joins we will make sure our point data contains H3 indices at resolution 10. </br>
# MAGIC We will create an User Defined Function that will perform this for us and we will apply it to the data.

# COMMAND ----------

# DBTITLE 1,UDF for H3 encoding
import h3
from pyspark.sql import functions as F

@udf("string")
def lat_lng_2_h3(lat, lng, res):
  import h3
  try:
    result = h3.geo_to_h3(lat, lng, res)
    return result
  except:
    return None # invalid coordinates will result in null index value.

taxi_trips = taxi_trips.withColumn(
  "h3_pickup", lat_lng_2_h3(F.col("pickup_latitude"), F.col("pickup_longitude"), F.lit(10))
).withColumn(
  "h3_dropoff", lat_lng_2_h3(F.col("dropoff_latitude"), F.col("dropoff_longitude"), F.lit(10))
)

# COMMAND ----------

# DBTITLE 1,View that H3 columns
display(taxi_trips)

# COMMAND ----------

# DBTITLE 1,Define location for data storage (silver layer)
username = "milos.colic" #please update with a correct user
silver_data_location = f"Users/{username}/geospatial/workshop/data/silver"

# COMMAND ----------

# DBTITLE 1,Write data out to a delta table
dbutils.fs.mkdirs(f"dbfs:/{silver_data_location}/h3/trips")
taxi_trips.write.format("delta").save(f"/{silver_data_location}/h3/trips/")

# COMMAND ----------

# MAGIC %md
# MAGIC For convenience purpose we will generate a managed database in the metastore. </br>
# MAGIC We will register the tables with our data against this database. </br>
# MAGIC This simplifies the way we are handling the data at consumption time.

# COMMAND ----------

# DBTITLE 1,Create a database for our workshop
# MAGIC %sql
# MAGIC Create DATABASE if Not EXISTS GeospatialWorkshop

# COMMAND ----------

# DBTITLE 1,Use the database
# MAGIC %sql
# MAGIC use GeospatialWorkshop

# COMMAND ----------

# DBTITLE 1,Create the table that will contain trips with h3 indices
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_h3_trips
# MAGIC using delta
# MAGIC LOCATION '/Users/milos.colic/geospatial/workshop/data/silver/h3/trips' -- please updated the username value in the path

# COMMAND ----------

# MAGIC %md
# MAGIC Delta is able to physically colocate the similar data based on z-order. </br>
# MAGIC Since we know we will be serving PIP joins using h3 columns we will z-order data by both h3 columns we have created. 

# COMMAND ----------

# DBTITLE 1,ZOrder Data
# MAGIC %sql
# MAGIC optimize silver_h3_trips ZORDER by (h3_pickup, h3_dropoff) 

# COMMAND ----------

# DBTITLE 1,View Trips Data
taxi_trips = spark.read.table("GeospatialWorkshop.silver_h3_trips")
display(taxi_trips)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Polygon Data Preparation - Neighbourhoods Data

# COMMAND ----------

# DBTITLE 1,Fetch Neighbours Data
# MAGIC %sh wget -O /tmp/neighbourhoods.zip https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nynta2020_21d.zip

# COMMAND ----------

# DBTITLE 1,Unzip the Data
# MAGIC %sh 
# MAGIC rm -R /tmp/neighbourhoods/
# MAGIC mkdir /tmp/neighbourhoods/
# MAGIC unzip /tmp/neighbourhoods.zip -d /tmp/neighbourhoods

# COMMAND ----------

# DBTITLE 1,Move the Data to dbfs
username = "milos.colic" #please update with a correct user
raw_data_location = f"Users/{username}/geospatial/workshop/data/raw/"
dbutils.fs.mkdirs(f"dbfs:/{raw_data_location}/")
dbutils.fs.cp("file:/tmp/neighbourhoods/nynta2020_21d/nynta2020.prj", f"dbfs:/{raw_data_location}/neighbourhoods.prj")
dbutils.fs.cp("file:/tmp/neighbourhoods/nynta2020_21d/nynta2020.shx", f"dbfs:/{raw_data_location}/neighbourhoods.shx")
dbutils.fs.cp("file:/tmp/neighbourhoods/nynta2020_21d/nynta2020.shp", f"dbfs:/{raw_data_location}/neighbourhoods.shp")

# COMMAND ----------

# DBTITLE 1,Load Data to geopandas
import geopandas as gpd

neighborhoods_pd = gpd.read_file(f"/dbfs/{raw_data_location}/neighbourhoods.shp")
display(neighborhoods_pd.head())

# COMMAND ----------

# MAGIC %md
# MAGIC In order to prepare our polygon data we will be using two set representation via h3 decompositon. </br>
# MAGIC The only difference to what we discussed in the notebook 01 is that we will be using WKB representation for the chips. </br>
# MAGIC Binary representation of polygons will result in more optimal runtime handling of chips. </br>
# MAGIC In addition we will be returning a single array of structs instead of separate collections for core and border chips. </br>
# MAGIC This will simplify our join logic. 

# COMMAND ----------

# DBTITLE 1,H3 Two Set Representation
import shapely
import math
import h3

@udf("array<struct<index: string, is_border: boolean, chip: binary>>")
def h3_2set_polyfill(geometry, resolution):
  # we need to account for possibility of MultiPolygon shapes
  def to_h3(geometry):
    if 'MultiPolygon' in geometry.geom_type:
      geometry = list(geometry.geoms)
    else:
      geometry = [geometry]
      
    hex_set = set()
    for p in geometry:
      p_geojson = shapely.geometry.mapping(p)
      hex_set = hex_set.union(h3.polyfill_geojson(p_geojson, resolution))
      
    return hex_set
  
  # convenience method for converting an index to a chip of a polygon
  def get_result_struct(hex_i, polygon, dirty_set):
    if hex_i in dirty_set:
      hex_polygon = shapely.geometry.Polygon(h3.h3_to_geo_boundary(hex_i, geo_json=True))
      intersection = polygon.intersection(hex_polygon)
      if intersection.is_empty:
        return None
      elif intersection.equals(hex_polygon):
        return (hex_i, False, None)
      else:
        return (hex_i, True, intersection.wkb)
    else:
      return (hex_i, False, None)
  
  polygon = shapely.wkb.loads(bytes(geometry))
  
  # compute the buffer radius
  # we cannot use the hexagon side lenght due to curvatrure
  # we use minimum rotated rectange - we assume that the rectangle is near rectangle in shape
  # the alternative would be to iterate through boundary vertices and take the longest side
  cent = polygon.centroid.xy
  centroid_hex = h3.geo_to_h3(cent[1][0], cent[0][0], resolution)
  centroid_geom = shapely.geometry.Polygon(h3.h3_to_geo_boundary(centroid_hex, geo_json=True))
  radius = math.sqrt(centroid_geom.minimum_rotated_rectangle.area)/2
  
  # any index that may touch the boundary
  dirty = polygon.boundary.buffer(radius)
  
  original_set = to_h3(polygon)
  dirty_set = to_h3(dirty)
  
  result = [get_result_struct(hex_i, polygon, dirty_set) for hex_i in list(original_set.union(dirty_set))]
  
  return result

# COMMAND ----------

# MAGIC %md
# MAGIC Our methods expect wkb representation. When we have loaded geopandas dataframe into spark, all our geometries were loaded as wkt. </br>
# MAGIC We will create an udf to perform the necessary conversions. 

# COMMAND ----------

# DBTITLE 1,Convert WKT to WKB in the Input Data
from pyspark.sql import functions as F

@udf("binary")
def wkt_to_wkb(wkt_poly):
  from shapely import wkt as wkt_loader
  from shapely import wkb as wkb_writer
  
  geom = wkt_loader.loads(wkt_poly)
  wkb_poly = wkb_writer.dumps(geom, hex=False)
  
  return wkb_poly

neighborhoods_pd = neighborhoods_pd.to_crs("EPSG:4326")
neighborhoods_pd["wkt_polygon"] = neighborhoods_pd["geometry"].apply(lambda x: str(x.wkt))
neighborhoods_df = spark.createDataFrame(neighborhoods_pd[["wkt_polygon"]])
neighborhoods_df = neighborhoods_df.withColumn("wkb_polygon", wkt_to_wkb(F.col("wkt_polygon")))

# COMMAND ----------

# DBTITLE 1,View Conversions between WKT and WKB
display(neighborhoods_df)

# COMMAND ----------

# DBTITLE 1,Represent Geometries via H3 Two Set Decomposition
neighborhoods_df = neighborhoods_df.withColumn("chips", h3_2set_polyfill(F.col("wkb_polygon"), F.lit(10)))

# COMMAND ----------

# DBTITLE 1,View Decompositions
display(neighborhoods_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We will perform 2 different write operations with Neighbourhoods data. </br>
# MAGIC One option is to keep the data in compact format with the array data kept at storage but exploded at join time. </br>
# MAGIC The other option is to pre-explode the arrays, store the data in flat way and z order by the index values. </br>

# COMMAND ----------

# DBTITLE 1,Write out Neighbourhoods Data
dbutils.fs.mkdirs(f"dbfs:/{silver_data_location}/h3/neighbourhoods")

neighborhoods_df = neighborhoods_df.select(
  F.monotonically_increasing_id().alias("id"),
  F.col("wkb_polygon"),
  F.col("chips")
).cache()

neighborhoods_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"/{silver_data_location}/h3/neighbourhoods/compact/")
neighborhoods_df.select(
  F.col("id"),
  F.col("wkb_polygon"),
  F.explode("chips").alias("chip")
).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"/{silver_data_location}/h3/neighbourhoods/flat/")

# COMMAND ----------

# DBTITLE 1,Create Compact Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_h3_neighbourhoods_compact
# MAGIC using delta
# MAGIC LOCATION '/Users/milos.colic/geospatial/workshop/data/silver/h3/neighbourhoods/compact' -- please updated the username value in the path

# COMMAND ----------

# DBTITLE 1,Create Flat Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_h3_neighbourhoods_flat
# MAGIC using delta
# MAGIC LOCATION '/Users/milos.colic/geospatial/workshop/data/silver/h3/neighbourhoods/flat' -- please updated the username value in the path

# COMMAND ----------

# DBTITLE 1,Optimize Flat Table
# MAGIC %sql
# MAGIC OPTIMIZE silver_h3_neighbourhoods_flat ZORDER BY (chip.index)

# COMMAND ----------

# DBTITLE 1,View Compact Data
neighborhoods_compact = spark.read.table("GeospatialWorkshop.silver_h3_neighbourhoods_compact")
display(neighborhoods_compact)

# COMMAND ----------

# DBTITLE 1,View Flat Data
neighborhoods_flat = spark.read.table("GeospatialWorkshop.silver_h3_neighbourhoods_flat")
display(neighborhoods_flat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Polygon Data Preparation - Ranomized Polygons

# COMMAND ----------

neighborhoods_df = spark.read.table("GeospatialWorkshop.silver_h3_neighbourhoods_compact").select("id", "wkb_polygon")

# COMMAND ----------

# MAGIC %md
# MAGIC We will generate 2 polygon datasets of ~2.5M polygons to demonstrate polygon intersection joins. </br>
# MAGIC We will randomly translate existing polygons many times in order to generate a dataset with many to many replationships.

# COMMAND ----------

from shapely import wkb
from shapely import affinity
from pyspark.sql import functions as F

@udf("binary")
def translate_polygon(in_wkb):
  from shapely import wkb
  from shapely import affinity
  import random
  
  polygon = wkb.loads(bytes(in_wkb))
  x_d = random.uniform(-1, 1)
  y_d = random.uniform(-1.5, 1.5)
  translated = affinity.translate(polygon, xoff=x_d, yoff=y_d)
  return translated.wkb

@udf("array<int>")
def gerate_simple_array(size):
  return [i for i in range(size)]

# COMMAND ----------

translated = neighborhoods_df.withColumn(
  "id2", F.explode(gerate_simple_array(F.lit(100))) # generate 100 numers that we will use to explode the data
).withColumn(
  "id3", F.explode(gerate_simple_array(F.lit(100))) # generate 100 numers that we will use to explode the data
).repartition(
  320, F.rand() # repartition the data
).withColumn(
  "wkb_polygon",
  translate_polygon(F.col("wkb_polygon")) # randomly translate the data
).withColumn(
  "id", F.concat_ws("-", F.col("id"), F.col("id2"), F.col("id3")) # generate a single ID by combining the original ID with two new IDs
).drop("id2", "id3")

display(translated)

# COMMAND ----------

# MAGIC %md
# MAGIC We will run the "same" run operation twice, both data outputs will be different. </br>
# MAGIC This is due to radom translations and the fact we have not fixed the random seed.

# COMMAND ----------

translated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_1")

# COMMAND ----------

translated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_2")

# COMMAND ----------

# MAGIC %md
# MAGIC We will run the decomposition operation on both of these randomized datasets and store the data.

# COMMAND ----------

from pyspark.sql import functions as F

polygons1 = spark.read.format("delta").load(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_1")
polygons1 = polygons1.withColumn("chips", h3_2set_polyfill(F.col("wkb_polygon"), F.lit(8)))
polygons1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_1_decomposed")

# COMMAND ----------

from pyspark.sql import functions as F

polygons2 = spark.read.format("delta").load(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_2")
polygons2 = polygons2.withColumn("chips", h3_2set_polyfill(F.col("wkb_polygon"), F.lit(8)))
polygons2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_2_decomposed")

# COMMAND ----------


