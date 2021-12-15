# Databricks notebook source
# MAGIC %md
# MAGIC Author: milos.colic@databricks.com

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC One of the most common geo spatial queries is the "point in polygon". This is operation is expressed by the "contains" predicate between a polygon and a point. </br>
# MAGIC The answer to "contains" depends on the number of vertices a polygon has, or in simpler terms - the more complex the polygon is the more time/compute it takes to answer if a point lies inside it. </br>
# MAGIC This one fact makes it harder to efficiently parallelise "point in polygon" problem. </br>
# MAGIC The issue is many real life questions map very well to this definition, such as: 
# MAGIC <ul>
# MAGIC   <li> Which addresses correspond to which building? </li>
# MAGIC   <li> City crime maps (ie. Which crime occurences happen in which neighbourhood?) </li>
# MAGIC   <li> Flood risk analysis (ie. Which location falls inside area near water bodies?) </li>
# MAGIC   <li> Maritime vessels port of call (ie. Is a vessel inside a polygon defining the port area?) </li>
# MAGIC   <li> etc. </li>
# MAGIC </ul>

# COMMAND ----------

displayHTML("""
            <center><img src="https://databricks.com/wp-content/uploads/2021/10/Geospatial-Indexing-with-British-National-Grid-in-Pyspark-blog-img-1.jpg" width="700px"></center>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why is PIP hard at scale?
# MAGIC Parallel systems such as spark works best with balanced operations and balanced data when executing joins. </br>
# MAGIC When it comes to geometries - they more commonly represent unique features than repeatable shapes. </br>
# MAGIC This means each row of our data is more likely to take different time to process. </br>
# MAGIC If this was not enough, PIP is a complex relation and spark cannot easily optimize it out of the box. </br>
# MAGIC This is because spark cannot easily joins based on user defined functions (UDFs).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Divide and Conquer
# MAGIC We can actually get our way around both problems with a same tool. </br>
# MAGIC If we represent our geometries differently we can ensure they are much more balanced. </br>
# MAGIC In addition, we can ensure that spark can optimize large point of our PIP joins. </br>
# MAGIC The answer to our problems are Geo Spatial Index Systems.

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3
# MAGIC 
# MAGIC Efficient spatial queries at scale requires putting the geometries into a **spatial index**. </br>
# MAGIC Querying indexed data with geometric predicate pushdown can dramatically improve performance. </br>
# MAGIC **S2** and **H3** are two common spatial indexing techniques. </br>
# MAGIC H3 is a spatial index developed by UBER and it is based on hierarchical hexagons that envelop whole surface of the earth. <br>
# MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" width=300>
# MAGIC 
# MAGIC In this notebook we will use H3 together with Folium to represent our geometries in a new way to facilitate a more scalable compute. </br>

# COMMAND ----------

# DBTITLE 1,Lib Installation
# MAGIC %pip install geopandas h3 s2 shapely folium

# COMMAND ----------

# DBTITLE 1,Fetch Neighbours Data
# MAGIC %sh wget -O /tmp/neighbourhoods.zip https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nynta2020_21d.zip

# COMMAND ----------

# MAGIC %sh 
# MAGIC mkdir /tmp/neighbourhoods/
# MAGIC unzip /tmp/neighbourhoods.zip -d /tmp/neighbourhoods

# COMMAND ----------

# DBTITLE 1,Move the data to dbfs
username = "milos.colic" #please update with a correct user
raw_data_location = f"Users/{username}/geospatial/workshop/data/raw/"
dbutils.fs.mkdirs(f"dbfs:/{raw_data_location}/")
dbutils.fs.cp("file:/tmp/neighbourhoods/nynta2020_21d/nynta2020.prj", f"dbfs:/{raw_data_location}/neighbourhoods.prj")
dbutils.fs.cp("file:/tmp/neighbourhoods/nynta2020_21d/nynta2020.shx", f"dbfs:/{raw_data_location}/neighbourhoods.shx")
dbutils.fs.cp("file:/tmp/neighbourhoods/nynta2020_21d/nynta2020.shp", f"dbfs:/{raw_data_location}/neighbourhoods.shp")

# COMMAND ----------

# DBTITLE 1,Load data to geopandas
import geopandas as gpd

neighborhoods_pd = gpd.read_file(f"/dbfs/{raw_data_location}/neighbourhoods.shp")
display(neighborhoods_pd.head())

# COMMAND ----------

# MAGIC %md
# MAGIC Our geometries are provided in a different coordinate reference system (CRS) than we expected (ie. lat-long). </br>
# MAGIC We can easily project the data polygons to expected CRS.

# COMMAND ----------

neighborhoods_pd = neighborhoods_pd.to_crs("EPSG:4326")

# COMMAND ----------

neighborhoods_pd.plot(figsize=(15,15))

# COMMAND ----------

# DBTITLE 1,Create a directory for auxiliary outputs of Folium
# MAGIC %sh
# MAGIC mkdir source

# COMMAND ----------

import folium

map_h3 = folium.Map(location=[40.7, -74],tiles="cartodbpositron", zoom_start=13)
folium.GeoJson(data=neighborhoods_pd["geometry"].iloc[0:10]).add_to(map_h3)
map_h3.save("source/output.html")
map_h3

# COMMAND ----------

# DBTITLE 1,Get H3 Indices that represent the Geometry
import shapely
import h3

polygon = neighborhoods_pd.iloc[11]["geometry"]
geo_json_geom = shapely.geometry.mapping(polygon)
indices = h3.polyfill(geo_json_geom, 10, True)
list(indices)[0]

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
index_geoms = [h3.h3_to_geo_boundary(ind, True) for ind in indices]
index_geoms = [shapely.geometry.Polygon(ind) for ind in index_geoms]

# COMMAND ----------

# DBTITLE 1,Folium Style Functions
def pink_poly(feature):
  return {
    'fillColor': '#e6328c',
    'color' : '#e6328c',
    'weight' : 1,
    'fillOpacity' : 0.5
  }

def pink_poly_dark(feature):
  return {
    'fillColor': '#e6328c',
    'color' : '#e6328c',
    'weight' : 2,
    'fillOpacity' : 0.75
  }

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its H3 Indices
import folium

map_h3 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=neighborhoods_pd["geometry"].iloc[11]).add_to(map_h3)
for ind in index_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_h3)
map_h3.save("source/output.html")
map_h3

# COMMAND ----------

# MAGIC %md
# MAGIC H3 polyfill is based on centroid approach. Only indices whose centroid fall inside the polygon will be included in the representation set. </br>
# MAGIC That is a problem if we require a lossless representation of the geometry. </br>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lossless representation

# COMMAND ----------

# DBTITLE 1,Get S2 Indices that represent the Geometry
import shapely
from s2 import s2

polygon = neighborhoods_pd.iloc[11]["geometry"]
geo_json_geom = shapely.geometry.mapping(polygon)
indices = s2.polyfill(geo_json_geom, 16, True, True)
indices[0] 

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
index_geoms = [shapely.geometry.Polygon(ind["geometry"]) for ind in indices]

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its S2 Indices
import folium

map_s2 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=neighborhoods_pd["geometry"].iloc[11]).add_to(map_s2)
for ind in index_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_s2)
map_s2.save("source/output.html")
map_s2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hole Handling with S2

# COMMAND ----------

# DBTITLE 1,Polygon with a Hole Representation
import shapely
from s2 import s2

polygon = neighborhoods_pd.iloc[11]["geometry"].boundary.buffer(0.001)
geo_json_geom = shapely.geometry.mapping(polygon)
indices = s2.polyfill(geo_json_geom, 16, True, True)
indices[0] 

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
index_geoms = [shapely.geometry.Polygon(ind["geometry"]) for ind in indices]

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its S2 Indices
import folium

map_s2 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=polygon).add_to(map_s2)
for ind in index_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_s2)
map_s2.save("source/output.html")
map_s2

# COMMAND ----------

# MAGIC %md
# MAGIC S2 pythong bindings polyfill logic is ignoring holes inside a geometry. </br>
# MAGIC If you are handling simple geometries this may not be a problem. </br>
# MAGIC Chances are that for any real life applications this will be an issue. </br>
# MAGIC Latter in this notebook we will propose a polyfill method that solves this issue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lossless representation - H3 extension

# COMMAND ----------

# MAGIC %md
# MAGIC We can achieve compatible behaviour in H3 with a a simple trick. </br>
# MAGIC We will expand our original geometry just enough to include the centroids of blind spots. </br>
# MAGIC Just enough is equal to the raduis of the enclosing circle of an index at a given resolution.

# COMMAND ----------

# DBTITLE 1,H3 Polyfill Extended
import math

def h3_polyfill_extended(geometry, resolution):
  (cx, cy) = polygon.centroid.coords.xy # get centroid location
  centroid_ind = h3.geo_to_h3(cx[0], cy[0], resolution) # get h3 index containing the centroid
  centroid_geom = shapely.geometry.Polygon(h3.h3_to_geo_boundary(centroid_ind)) # get centroid index geometry
  radius = math.sqrt(centroid_geom.minimum_rotated_rectangle.area)/2 # find the radius of (pseudo) minimal enclosing circle (via side of the rotated enclosing square)
  geom_extended = geometry.buffer(distance=radius, resolution=1) # buffer the original geometry by the radius of minimum enclosing cicle
  geo_json_geom = shapely.geometry.mapping(geom_extended) 
  indices = h3.polyfill_geojson(geo_json_geom, resolution) # get the indices
  return indices 

# COMMAND ----------

# DBTITLE 1,Get H3 Extended Indices that represent the Geometry
polygon = neighborhoods_pd.iloc[11]["geometry"]
indices = h3_polyfill_extended(polygon, 10)
list(indices)[0]

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
index_geoms = [h3.h3_to_geo_boundary(ind, True) for ind in indices]
index_geoms = [shapely.geometry.Polygon(ind) for ind in index_geoms]

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its S2 Indices
import folium

map_h3 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=neighborhoods_pd["geometry"].iloc[11]).add_to(map_h3)
for ind in index_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_h3)
map_h3.save("source/output.html")
map_h3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Two Set Representation
# MAGIC When we obser our polygon representation via index set we can notice that some of the indices are fully contained by the geometry. </br>
# MAGIC For any point that falls into a such index we immediatelly know it is contained by the geometry without any additional computation. </br>
# MAGIC All the other indices (the indices that touch the border of the geometry) may contain points on either sides of the geometry.

# COMMAND ----------

# DBTITLE 1,H3 Two Set Representation
def h3_2set_polyfill(geometry, resolution): 
  def to_h3(geometry):
    # buffering can result in MultiPolygon geometries
    if 'MultiPolygon' in geometry.geom_type:
      geometry = list(geometry.geoms)
    else:
      geometry = [geometry]
      
    hex_set = set()
    for p in geometry:
      p_geojson = shapely.geometry.mapping(p)
      hex_set = hex_set.union(h3.polyfill_geojson(p_geojson, resolution))
      
    return hex_set
  
  def get_result_struct(hex_i, polygon, dirty_set):
    if hex_i in dirty_set:
      hex_polygon = shapely.geometry.Polygon(h3.h3_to_geo_boundary(hex_i, geo_json=True))
      intersection = polygon.intersection(hex_polygon)
      if intersection.is_empty: # does not represent any area of the original geometry
        return None
      elif intersection.equals(hex_polygon): # fully contained by the original geometry
        return (hex_i, False, None)
      else:
        return (hex_i, True, intersection.wkb) # partially contained by the original geometry
    else:
      return (hex_i, False, None) # fully contained by the original geometry (not in the dirty set)
  
  polygon = geometry # placeholder for when we are loading wkt/wkb in udfs
  
  # get centroid of the geometry
  # get centroid index
  # get centroid index geometry
  # compute radius based on teh minimum enclosing rectangel (radius of pseudo minimal enclosing circle)
  cent = polygon.centroid.xy
  centroid_hex = h3.geo_to_h3(cent[1][0], cent[0][0], resolution)
  centroid_geom = shapely.geometry.Polygon(h3.h3_to_geo_boundary(centroid_hex, geo_json=True))
  radius = math.sqrt(centroid_geom.minimum_rotated_rectangle.area)/2
  
  dirty = polygon.boundary.buffer(radius)
  
  original_set = to_h3(polygon)
  dirty_set = to_h3(dirty)
  
  result = [get_result_struct(hex_i, polygon, dirty_set) for hex_i in list(original_set.union(dirty_set))]
  result = [c for c in result if c is not None]
  
  return result

# COMMAND ----------

# DBTITLE 1,Get H3 Extended Indices that represent the Geometry
polygon = neighborhoods_pd.iloc[11]["geometry"]
result = h3_2set_polyfill(polygon, 10)
result[0]

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
core_geoms = [h3.h3_to_geo_boundary(chip[0], True) for chip in result if chip[1] == False]
core_geoms = [shapely.geometry.Polygon(ind) for ind in core_geoms]
border_geoms = [shapely.wkb.loads(chip[2]) for chip in result if chip[1] == True]

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its H3 Indices and Chips
import folium

map_h3 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=neighborhoods_pd["geometry"].iloc[11]).add_to(map_h3)
for ind in core_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_h3)
for ind in border_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly_dark).add_to(map_h3)
map_h3.save("source/output.html")
map_h3

# COMMAND ----------

polygon.type

# COMMAND ----------

# DBTITLE 1,S2 Two Set Representation
def s2_2set_polyfill(geometry, resolution): 
  def to_s2(geometry):
    # buffering can result in MultiPolygon geometries
    if 'MultiPolygon' in geometry.geom_type:
      geometry = list(geometry.geoms)
    else:
      geometry = [geometry]
      
    ind_set = set()
    for p in geometry:
      p_geojson = shapely.geometry.mapping(p)
      new_indices = s2.polyfill(p_geojson, resolution, True, True)
      new_indices = set([ind["id"] for ind in new_indices])
      ind_set = ind_set.union(new_indices)
      
    return ind_set
  
  def get_result_struct(index, polygon, dirty_set):
    if index in dirty_set:
      ind_polygon = shapely.geometry.Polygon(s2.s2_to_geo_boundary(index, True))
      intersection = polygon.intersection(ind_polygon)
      if intersection.is_empty: # does not represent any area of the original geometry
        return None
      elif intersection.equals(ind_polygon): # fully contained by the original geometry
        return (index, False, None)
      else:
        return (index, True, intersection.wkb) # partially contained by the original geometry
    else:
      return (index, False, None) # fully contained by the original geometry (not in the dirty set)
  
  polygon = geometry # placeholder for when we are loading wkt/wkb in udfs
  
  # get centroid of the geometry
  # get centroid index
  # get centroid index geometry
  # compute radius based on teh minimum enclosing rectangel (radius of pseudo minimal enclosing circle)
  cent = polygon.centroid.xy
  centroid_ind = s2.geo_to_s2(cent[1][0], cent[0][0], resolution)
  centroid_geom = shapely.geometry.Polygon(s2.s2_to_geo_boundary(centroid_ind, True))
  radius = math.sqrt(centroid_geom.minimum_rotated_rectangle.area)/2
  
  dirty = polygon.boundary.buffer(radius)
  
  original_set = to_s2(polygon)
  dirty_set = to_s2(dirty)
  
  result = [get_result_struct(ind, polygon, dirty_set) for ind in list(original_set.union(dirty_set))]
  result = [c for c in result if c is not None]
  
  return result

# COMMAND ----------

# DBTITLE 1,Get S2 Two Set Indices that represent the Geometry
polygon = neighborhoods_pd.iloc[11]["geometry"]
result = s2_2set_polyfill(polygon, 16)
result[0]

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
core_geoms = [s2.s2_to_geo_boundary(ind[0], True) for ind in result if ind[1] == False]
core_geoms = [shapely.geometry.Polygon(ind) for ind in core_geoms]
border_geoms = [shapely.wkb.loads(ind[2]) for ind in result if ind[1] == True]

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its S2 Indices and Chips
import folium

map_s2 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=neighborhoods_pd["geometry"].iloc[11]).add_to(map_s2)
for ind in core_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_s2)
for ind in border_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly_dark).add_to(map_s2)
map_s2.save("source/output.html")
map_s2

# COMMAND ----------

# MAGIC %md
# MAGIC ## What about Polygon Holes?

# COMMAND ----------

# DBTITLE 1,Get S2 Two Set Indices that represent the Geometry
polygon = neighborhoods_pd.iloc[11]["geometry"].boundary.buffer(0.002)
result = s2_2set_polyfill(polygon, 16)
result[0]

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
core_geoms = [s2.s2_to_geo_boundary(ind[0], True) for ind in result if ind[1] == False]
core_geoms = [shapely.geometry.Polygon(ind) for ind in core_geoms]
border_geoms = [shapely.wkb.loads(ind[2]) for ind in result if ind[1] == True]

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its S2 Indices and Chips
import folium

map_s2 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=polygon).add_to(map_s2)
for ind in core_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_s2)
for ind in border_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly_dark).add_to(map_s2)
map_s2.save("source/output.html")
map_s2

# COMMAND ----------

# DBTITLE 1,Get H3 Two Set Indices that represent the Geometry
polygon = neighborhoods_pd.iloc[11]["geometry"].boundary.buffer(0.002)
result = h3_2set_polyfill(polygon, 10)
result[0]

# COMMAND ----------

# DBTITLE 1,Convert indices to Polygons
core_geoms = [h3.h3_to_geo_boundary(ind[0], True) for ind in result if ind[1] == False]
core_geoms = [shapely.geometry.Polygon(ind) for ind in core_geoms]
border_geoms = [shapely.wkb.loads(ind[2]) for ind in result if ind[1] == True]

# COMMAND ----------

# DBTITLE 1,Plot Geometry and its H3 Indices and Chips
import folium

map_s2 = folium.Map(location=[40.7, -73.92],tiles="cartodbpositron", zoom_start=15)
folium.GeoJson(data=polygon).add_to(map_s2)
for ind in core_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly).add_to(map_s2)
for ind in border_geoms:
  folium.GeoJson(data=ind, style_function=pink_poly_dark).add_to(map_s2)
map_s2.save("source/output.html")
map_s2

# COMMAND ----------


