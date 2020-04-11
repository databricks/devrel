# Databricks notebook source
# MAGIC %md ## NYT COVID-19 Analysis
# MAGIC This notebook processes and performs quick analysis from the [New York Times COVID-19 dataset](https://github.com/nytimes/covid-19-data).  The data is updated in the `/databricks-datasets/COVID/covid-19-data/` location regularly so you can access the data directly.

# COMMAND ----------

# Standard Libraries
import io

# External Libraries
import requests
import numpy as np
import pandas as pd
import altair as alt
from vega_datasets import data

# topographical
topo_usa = 'https://vega.github.io/vega-datasets/data/us-10m.json'
topo_wa = 'https://raw.githubusercontent.com/deldersveld/topojson/master/countries/us-states/WA-53-washington-counties.json'
topo_king = 'https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/WA/King.geo.json'

# COMMAND ----------

# MAGIC %md ### Download Mapping County FIPS to lat, long_

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/dennylee/COVID/map_fips/ && wget -O /dbfs/tmp/dennylee/COVID/map_fips/countyfips_lat_long.csv https://raw.githubusercontent.com/dennyglee/tech-talks/master/datasets/countyfips_lat_long.csv && ls -al /dbfs/tmp/dennylee/COVID/map_fips/

# COMMAND ----------

# Create mapping of county FIPS to centroid long_ and lat
map_fips = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/dennylee/COVID/map_fips/countyfips_lat_long.csv")
map_fips = (map_fips
              .withColumnRenamed("STATE", "state")
              .withColumnRenamed("COUNTYNAME", "county")
              .withColumnRenamed("LAT", "lat")
              .withColumnRenamed("LON", "long_"))
map_fips.createOrReplaceTempView("map_fips")

# COMMAND ----------

# MAGIC %md ## Specify `nyt_daily` table
# MAGIC * Source: `/databricks-datasets/COVID/covid-19-data/`
# MAGIC * Contains the COVID-19 daily reports

# COMMAND ----------

nyt_daily = spark.read.option("inferSchema", True).option("header", True).csv("/databricks-datasets/COVID/covid-19-data/us-counties.csv")
nyt_daily.createOrReplaceTempView("nyt_daily")
display(nyt_daily)

# COMMAND ----------

date_zero = '2020-01-21T00:00:00.000+0000'
nyt_daynum = spark.sql("""
select f.fips, 100 + datediff(f.date, '""" + date_zero + """') as day_num, f.cases as confirmed, f.deaths, m.lat, m.long_ 
  from nyt_daily f
    left outer join map_fips m
      on m.county = f.county 
 where f.fips is not null""")
nyt_daynum.createOrReplaceTempView("nyt_daynum")

# pivot confirmed cases
confirmed = nyt_daynum.select("fips", "day_num", "confirmed").toPandas()
confirmed['day_num'] = confirmed['day_num'].astype(str)
confirmed['confirmed'] = confirmed['confirmed'].astype('int64')
confirmed = confirmed.pivot_table(index='fips', columns='day_num', values='confirmed', fill_value=0).reset_index()

# pivot deaths
deaths = nyt_daynum.select("lat", "long_", "day_num", "deaths").where("deaths > 0").toPandas()
deaths['day_num'] = deaths['day_num'].astype(str)
deaths['deaths'] = deaths['deaths'].astype('int64')
deaths = deaths.pivot_table(index=['lat', 'long_'], columns='day_num', values='deaths', fill_value=0).reset_index()

# Extract column names for slider
column_names = confirmed.columns.tolist()

# Remove first element (`fips`)
column_names.pop(0)

# Convert to int
column_values = [None] * len(column_names)
for i in range(0, len(column_names)): column_values[i] = int(column_names[i]) 

# COMMAND ----------

# MAGIC %md ## COVID-19 Confirmed Cases and Deaths by County Slider

# COMMAND ----------

# Disable max_rows to see more data
alt.data_transformers.disable_max_rows()

# Topographic information
us_states = alt.topo_feature(topo_usa, 'states')
us_counties = alt.topo_feature(topo_usa, 'counties')

# state borders
base_states = alt.Chart(us_states).mark_geoshape().encode(
  stroke=alt.value('lightgray'), fill=alt.value('white')
).properties(
  width=1200,
  height=960,
).project(
  type='albersUsa',
)

# Slider choices
min_day_num = column_values[0]
max_day_num = column_values[len(column_values)-1]
slider = alt.binding_range(min=min_day_num, max=max_day_num, step=5)
slider_selection = alt.selection_single(fields=['day_num'], bind=slider, name="day_num", init={'day_num':min_day_num})

# Confirmed cases by county
base_counties = alt.Chart(us_counties).mark_geoshape(
    stroke='black',
    strokeWidth=0.05
).project(
    type='albersUsa'
).transform_lookup(
    lookup='id',
    from_=alt.LookupData(confirmed, 'fips', column_names)  
).transform_fold(
    column_names, as_=['day_num', 'confirmed']
).transform_calculate(
    day_num = 'parseInt(datum.day_num)',
    confirmed = 'isValid(datum.confirmed) ? datum.confirmed : -1'
).encode(
    color = alt.condition(
        'datum.confirmed > 0',      
        alt.Color('confirmed:Q', scale=alt.Scale(domain=(1, 75000), type='symlog', scheme='lightgreyred')),
        alt.value('white')
      )  
).properties(
  # update figure title
  title=f'COVID-19 Confirmed Cases by County Between 1/21 to 4/6 (2020)'
).transform_filter(
    slider_selection
)

# deaths by long, latitude
points = alt.Chart(
  deaths
).mark_point(
  opacity=0.75, filled=True
).transform_fold(
  column_names, as_=['day_num', 'deaths']
).transform_calculate(
    day_num = 'parseInt(datum.day_num)',
    deaths = 'isValid(datum.deaths) ? datum.deaths : -1'  
).encode(
  longitude='long_:Q',
  latitude='lat:Q',
  size=alt.Size('deaths:Q', scale=alt.Scale(domain=(1, 500), type='symlog'), title='deaths'),
  color=alt.value('#BD595D'),
  stroke=alt.value('brown'),
).add_selection(
    slider_selection
).transform_filter(
    slider_selection
)

# confirmed cases (base_counties) and deaths (points)
(base_states + base_counties + points) 

# COMMAND ----------


