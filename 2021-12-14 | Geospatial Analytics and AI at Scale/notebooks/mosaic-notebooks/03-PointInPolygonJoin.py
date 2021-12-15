# Databricks notebook source
# MAGIC %md
# MAGIC Author: milos.colic@databricks.com

# COMMAND ----------

# DBTITLE 1,Lib Installation
# MAGIC %pip install h3 shapely folium keplergl==0.3.2

# COMMAND ----------

# MAGIC %md
# MAGIC We will fetch our trips data and limit it to 10% (~160M records). </br>
# MAGIC The queries with full volume will take a bit longer to execute. </br>
# MAGIC For first passage through the content we recomend that you use the limit until you get familiarized with the content well.

# COMMAND ----------

# DBTITLE 1,Fetch Trips Data
# please remove sample call to execute at full volume
trips = spark.read.table("GeospatialWorkshop.silver_h3_trips").sample(fraction=0.1, seed=11)
display(trips)

# COMMAND ----------

# DBTITLE 1,Fetch Neighbourhoods Data
neighbourhoods = spark.read.table("GeospatialWorkshop.silver_h3_neighbourhoods_flat")
display(neighbourhoods)

# COMMAND ----------

# MAGIC %md
# MAGIC We define our PIP relation as a series of simple logical steps:
# MAGIC <ul>
# MAGIC   <li>Prepare the join column on both sides of the join.</li>
# MAGIC   <li>Mark the skew on the trips data side due to power law in the location.</li>
# MAGIC   <li>Join on index equality</li>
# MAGIC   <li>Any index that is marked as is_core (is_border==False) will contain true positive matches.</li>
# MAGIC   <li>Any index that is marked as is_border will require contains udf to be called.</li>
# MAGIC </ul>

# COMMAND ----------

# DBTITLE 1,Defin PIP join logic
from pyspark.sql import functions as F
import shapely

@udf("boolean")
def contains(chip_wkb, point_lat, point_lng):
  from shapely import wkb
  
  if chip_wkb is None: # null check
    return None 
  else: # execute geometry operation in shapely
    geom = wkb.loads(bytes(chip_wkb))
    point = shapely.geometry.Point(point_lat, point_lng)
    return geom.contains(point)

# we hint to the system that data is skewed on the index (most of the trips happen in Manhattan)
joined = trips.withColumn("h3", F.col("h3_pickup")).hint("skew", "h3").join(
  neighbourhoods.withColumn("h3", F.col("chip.index")),
  on = ["h3"],
  how = "inner"
).where(
  # it is important that we first filter on simple operations and only then on udfs
  # logic operations can short circuit - if first part is true second part is skipped
  ~F.col("chip.is_border") | contains(F.col("chip.chip"), F.col("pickup_latitude"), F.col("pickup_longitude"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC We will compute some aggreages in order to be able to show some results visualisations.</br>
# MAGIC We will fetch these aggregates as a pandas dataframe since keplergl will expect a pandas dataframe. </br>
# MAGIC We will also convert any wkb polygon to a wkt polygon since keplergl wont be able to render wkbs.

# COMMAND ----------

# DBTITLE 1,Compute Aggregates
total_spend = joined.groupBy(
  "id"
).agg(
  F.first("wkb_polygon").alias("wkb_polygon"),
  F.sum("total_amount").alias("spend")
)

# COMMAND ----------

# DBTITLE 1,Convert to Pandas
total_spend_pd = total_spend.toPandas() 

# COMMAND ----------

# DBTITLE 1,Convert to WKT
def wkb_to_wkt(wkb_poly):
  from shapely import wkb as wkb_io
  geom = wkb_io.loads(wkb_poly)
  return geom.wkt.strip()

total_spend_pd["wkt"] = total_spend_pd["wkb_polygon"].apply(lambda x: wkb_to_wkt(x))

# COMMAND ----------

# MAGIC %md
# MAGIC We do require some modifications to the way we call KeplerGL plotting due to iframe sandboxing. </br>
# MAGIC The method below will ensure that you can properly view the plot and control the height of the plot. </br>
# MAGIC Width of the plot is always set to 100% of the cell.

# COMMAND ----------

# DBTITLE 1,Extra steps for KeplerGL rendering
import keplergl
 
def displayKepler(map_instance, height):
  displayHTML(map_instance._repr_html_().decode("utf-8").replace(
      ".height||400", f".height||{height}"
  ))

# COMMAND ----------

# MAGIC %md
# MAGIC KeplerGL explext a configuration JSON for the color scheme and layer definitions. </br>
# MAGIC We have provided below an example for the configuration. 

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
m1.add_data(data=total_spend_pd[["id", "spend", "wkt"]], name = "spend")

displayKepler(m1, 1000)

# COMMAND ----------


