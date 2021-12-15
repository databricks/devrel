# Databricks notebook source
# MAGIC %md
# MAGIC Author: milos.colic@databricks.com

# COMMAND ----------

# DBTITLE 1,Lib Installation
# MAGIC %pip install h3 shapely folium keplergl==0.3.2

# COMMAND ----------

username = "milos.colic" #please update with a correct user
silver_data_location = f"Users/{username}/geospatial/workshop/data/silver"

# COMMAND ----------

# DBTITLE 1,Fetch Polygons 1 Data
polygons1 = spark.read.format("delta").load(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_1_decomposed")
display(polygons1)

# COMMAND ----------

# DBTITLE 1,Fetch Polygons 2 Data
polygons2 = spark.read.format("delta").load(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_2_decomposed")
display(polygons2)

# COMMAND ----------

# MAGIC %md
# MAGIC We define polygon intersection operation via chips as follows:
# MAGIC <ul>
# MAGIC   <li>Check if there are any matches that include a core index.
# MAGIC     <ul>
# MAGIC       <li>If we have core to core index match we know the two geometries are intersecting. (core indices are fully contained on both sides)</li>
# MAGIC       <li>If we have core to border index match we know the two geometries are intersecting. (core index is fully contained on one side)</li>
# MAGIC       <li>If we have border to border index match we need to run intersection on the index chips. (geometries may be non touching neighbours depending on the resolution)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>If there is at least one match involving a core index we return True without running any shapely operations.</li>
# MAGIC   <li>For the border to border matches we will iterate and short-circuit on the first True.</li>
# MAGIC </ul>
# MAGIC Iterating through border to border matches is a wise choice since near neighbours that arent intersecting should be a relatively rare event. </br>
# MAGIC Also if we have multiple border to border matches that do contain enough informations to answer the query it is enough to evaluate only one of them. </br>

# COMMAND ----------

import shapely

@udf("boolean")
def intersects(pairs):
  from shapely import wkb as wkb_io
  
  def geom_intersect(pair):
    (left, right) = pair
    if left["chip"] is None or right["chip"] is None:
      return False
    left_geom = wkb_io.loads(bytes(left["chip"]))
    right_geom = wkb_io.loads(bytes(right["chip"]))
    return left_geom.intersects(right_geom)
  
  flags = [~p["left"]["is_border"] or ~p["right"]["is_border"] for p in pairs]
  if sum(flags) > 0:
    return True
  else:
    for p in pairs:
      if p["left"]["is_border"] and p["right"]["is_border"]:
        if geom_intersect(p):
          return True
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC The join logic itself is defined as follows:
# MAGIC <ul>
# MAGIC   <li>Perform join via index id.</li>
# MAGIC   <li>Collect matches in a collection.</li>
# MAGIC   <li>Execute intersection logic discussed above.</li>
# MAGIC </ul>
# MAGIC This logic can be further optized via custom aggregation udf (UDAF - User Defined Aggregate Functions).</br>
# MAGIC This would reduce the amount of shuffled data and can speed up the queries.

# COMMAND ----------

from pyspark.sql import functions as F

def intersection_join(left, right):
  intersections = left.join(
    right,
    on = ["h3"],
    how = "inner"
  ).groupBy(
    left["id"],
    right["id"]
  ).agg(
    F.collect_set(F.struct(left["chip"].alias("left"), right["chip"].alias("right"))).alias("matches")
  ).withColumn(
    "intersects", intersects(F.col("matches"))
  ).drop("matches")
  return intersections

# COMMAND ----------

# MAGIC %md
# MAGIC We will run a set of different query sizes:
# MAGIC <ul>
# MAGIC   <li>10K x 10K</li>
# MAGIC   <li>100K x 100K</li>
# MAGIC   <li>500K x 500K</li>
# MAGIC   <li>1M x 1M</li>
# MAGIC </ul>

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(10000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(10000)

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(100000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(100000)

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(500000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(500000)

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(1000000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(1000000)

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------

display(intersections)

# COMMAND ----------

# MAGIC %md
# MAGIC We will select a few examples to show that our intersect operation indeed has run correctly. </br>

# COMMAND ----------

polygon1 = polygons1.where("id = '0-14-88'").select("id", "wkb_polygon").toPandas()
polygon2 = polygons2.where("id in ('60129542151-97-33', '60129542168-86-23', '60129542154-60-32', '51539607565-14-58', '51539607564-62-40')").select("id", "wkb_polygon").toPandas()

# COMMAND ----------

# DBTITLE 1,Convert WKT to WKB
def wkb_to_wkt(wkb_poly):
  from shapely import wkb as wkb_io
  geom = wkb_io.loads(wkb_poly)
  return geom.wkt.strip()

polygon1["wkt"] = polygon1["wkb_polygon"].apply(lambda x: wkb_to_wkt(x))
polygon2["wkt"] = polygon2["wkb_polygon"].apply(lambda x: wkb_to_wkt(x))

# COMMAND ----------

# DBTITLE 1,Extra steps for KeplerGL rendering
import keplergl
 
def displayKepler(map_instance, height):
  displayHTML(map_instance._repr_html_().decode("utf-8").replace(
      ".height||400", f".height||{height}"
  ))

# COMMAND ----------

# DBTITLE 1,Kepler Plot Configs
plot1_conf = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "nxtu8fe",
          "type": "hexagonId",
          "config": {
            "dataId": "hexes_res_10",
            "label": "id",
            "color": [
              18,
              147,
              154
            ],
            "columns": {
              "hex_id": "id"
            },
            "isVisible": True,
            "visConfig": {
              "opacity": 0.59,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "coverage": 1,
              "enable3d": False,
              "sizeRange": [
                0,
                500
              ],
              "coverageRange": [
                0,
                1
              ],
              "elevationScale": 5
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear",
            "coverageField": None,
            "coverageScale": "linear"
          }
        },
        {
          "id": "eeksejt",
          "type": "geojson",
          "config": {
            "dataId": "raw_polygon",
            "label": "raw_polygon",
            "color": [
              221,
              178,
              124
            ],
            "columns": {
              "geojson": "wkt_polygons"
            },
            "isVisible": True,
            "visConfig": {
              "opacity": 0.1,
              "strokeOpacity": 0.8,
              "thickness": 0.5,
              "strokeColor": [
                136,
                87,
                44
              ],
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radius": 10,
              "sizeRange": [
                0,
                10
              ],
              "radiusRange": [
                0,
                50
              ],
              "heightRange": [
                0,
                500
              ],
              "elevationScale": 5,
              "stroked": True,
              "filled": True,
              "enable3d": False,
              "wireframe": False
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "heightField": None,
            "heightScale": "linear",
            "radiusField": None,
            "radiusScale": "linear"
          }
        }
      ],
      "interactionConfig": {
        "tooltip": {
          "fieldsToShow": {
            "hexes_res_10": [
              {
                "name": "id",
                "format": None
              }
            ],
            "raw_polygon": [
              {
                "name": "id",
                "format": None
              }
            ]
          },
          "compareMode": False,
          "compareType": "absolute",
          "enabled": True
        },
        "brush": {
          "size": 2.2,
          "enabled": False
        },
        "geocoder": {
          "enabled": False
        },
        "coordinate": {
          "enabled": False
        }
      },
      "layerBlending": "normal",
      "splitMaps": [],
      "animationConfig": {
        "currentTime": None,
        "speed": 1
      }
    },
    "mapState": {
      "bearing": 0,
      "dragRotate": False,
      "latitude": 40.766233510042554,
      "longitude": -73.9219485077532,
      "pitch": 0,
      "zoom": 13.846148233536928,
      "isSplit": False
    },
    "mapStyle": {
      "styleType": "dark",
      "topLayerGroups": {},
      "visibleLayerGroups": {
        "label": True,
        "road": True,
        "border": False,
        "building": True,
        "water": True,
        "land": True,
        "3d building": False
      },
      "threeDBuildingColor": [
        9.665468314072013,
        17.18305478057247,
        31.1442867897876
      ],
      "mapStyles": {}
    }
  }
}

# COMMAND ----------

# DBTITLE 1,Plot Data
from keplergl import KeplerGl

m1 = KeplerGl(config=plot1_conf)
m1.add_data(data=polygon1[["id", "wkt"]], name = "polygon1")
m1.add_data(data=polygon2[["id", "wkt"]], name = "polygon2")

displayKepler(m1, 1000)
