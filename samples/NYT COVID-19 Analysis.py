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

map_fips_dedup = spark.sql("""select fips, min(state) as state, min(county) as county, min(long_) as long_, min(lat) as lat from map_fips group by fips""")
map_fips_dedup.createOrReplaceTempView("map_fips_dedup")

# COMMAND ----------

# MAGIC %md ### Download 2019 Population Estimates

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/dennylee/COVID/population_estimates_by_county/ && wget -O /dbfs/tmp/dennylee/COVID/population_estimates_by_county/co-est2019-alldata.csv https://raw.githubusercontent.com/databricks/tech-talks/master/datasets/co-est2019-alldata.csv && ls -al /dbfs/tmp/dennylee/COVID/population_estimates_by_county/

# COMMAND ----------

map_popest_county = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/dennylee/COVID/population_estimates_by_county/co-est2019-alldata.csv")
map_popest_county.createOrReplaceTempView("map_popest_county")
fips_popest_county = spark.sql("select State * 1000 + substring(cast(1000 + County as string), 2, 3) as fips, STNAME, CTYNAME, census2010pop, POPESTIMATE2019 from map_popest_county")
fips_popest_county.createOrReplaceTempView("fips_popest_county")

# COMMAND ----------

# MAGIC %md ## Specify `nyt_daily` table
# MAGIC * Source: `/databricks-datasets/COVID/covid-19-data/`
# MAGIC * Contains the COVID-19 daily reports

# COMMAND ----------

nyt_daily = spark.read.option("inferSchema", True).option("header", True).csv("/databricks-datasets/COVID/covid-19-data/us-counties.csv")
nyt_daily.createOrReplaceTempView("nyt_daily")
display(nyt_daily)

# COMMAND ----------

# MAGIC %md # COVID-19 Cases and Deaths for Specific Counties
# MAGIC Focusing on two-week window around when educational facilites were closed
# MAGIC * Top 10 Washington State counties (3/13/2020)
# MAGIC * Top 10 NY State counties (3/18/2020)
# MAGIC The queries are using the US Census Population Estimates for 2019

# COMMAND ----------

# WA State 2 week window
wa_state_window = spark.sql("""
SELECT date, 100 + datediff(date, '2020-03-06T00:00:00.000+0000') as day_num, county, fips, cases, deaths, 100000.*cases/population_estimate AS cases_per_100Kpop, 100000.*deaths/population_estimate AS deaths_per_100Kpop
  from (
SELECT CAST(f.date AS date) AS date, f.county, f.fips, SUM(f.cases) AS cases, SUM(f.deaths) AS deaths, MAX(p.POPESTIMATE2019) AS population_estimate 
  FROM nyt_daily f 
    JOIN fips_popest_county p
      ON p.fips = f.fips
 WHERE f.state = 'Washington' 
   AND date BETWEEN '2020-03-06T00:00:00.000+0000' AND '2020-03-20T00:00:00.000+0000'
 GROUP BY f.date, f.county, f.fips
) a""")
wa_state_window.createOrReplaceTempView("wa_state_window")

# NY State 2 week window
ny_state_window = spark.sql("""
SELECT date, 100 + datediff(date, '2020-03-11T00:00:00.000+0000') as day_num, county, fips, cases, deaths, 100000.*cases/population_estimate AS cases_per_100Kpop, 100000.*deaths/population_estimate AS deaths_per_100Kpop
  FROM (
SELECT CAST(f.date AS date) AS date, f.county, p.fips, SUM(f.cases) as cases, SUM(f.deaths) as deaths, MAX(p.POPESTIMATE2019) AS population_estimate  
  FROM nyt_daily f 
    JOIN fips_popest_county p
      ON p.fips = coalesce(f.fips, 36061)
 WHERE f.state = 'New York' 
   AND date BETWEEN '2020-03-11T00:00:00.000+0000' AND '2020-03-25T00:00:00.000+0000'
 GROUP BY f.date, f.county, p.fips
) a""")
ny_state_window.createOrReplaceTempView("ny_state_window")

# NY State 2 week window (-1 week)
ny_state_window_m1 = spark.sql("""
SELECT date, 100 + datediff(date, '2020-03-06T00:00:00.000+0000') as day_num, county, fips, cases, deaths, 100000.*cases/population_estimate AS cases_per_100Kpop, 100000.*deaths/population_estimate AS deaths_per_100Kpop
  FROM (
SELECT CAST(f.date AS date) AS date, f.county, p.fips, SUM(f.cases) as cases, SUM(f.deaths) as deaths, MAX(p.POPESTIMATE2019) AS population_estimate  
  FROM nyt_daily f 
    JOIN fips_popest_county p
      ON p.fips = coalesce(f.fips, 36061)
 WHERE f.state = 'New York' 
   AND date BETWEEN '2020-03-06T00:00:00.000+0000' AND '2020-03-20T00:00:00.000+0000'
 GROUP BY f.date, f.county, p.fips
) a""")
ny_state_window_m1.createOrReplaceTempView("ny_state_window_m1")

# COMMAND ----------

# MAGIC %md ## COVID-19 Cases for WA and NY Counties

# COMMAND ----------

# DBTITLE 1,WA State Confirmed Cases 3/6 - 3/20 - Educational Facilities Closed 3/13
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases 
# MAGIC   FROM wa_state_window f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases) as cases FROM wa_state_window GROUP BY county ORDER BY cases DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county

# COMMAND ----------

# DBTITLE 1,NY State Confirmed Cases 3/11 - 3/25 - Educational Facilities Closed 3/18
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases 
# MAGIC   FROM ny_state_window f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases) as cases FROM ny_state_window GROUP BY county ORDER BY cases DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county

# COMMAND ----------

# DBTITLE 1,NY State Confirmed Cases 3/6 - 3/20 - Educational Facilities Closed 3/18
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases 
# MAGIC   FROM ny_state_window_m1 f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases) as cases FROM ny_state_window_m1 GROUP BY county ORDER BY cases DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county

# COMMAND ----------

# MAGIC %md ## COVID-19 Cases per 100K people for WA and NY Counties
# MAGIC The concern with the above graphs is while it provides you the total number of cases, it's hard to compare WA state and NY state due to density.  While not perfect, a better way to look at these numbers would be to review this data as a proportion of population estimates.
# MAGIC 
# MAGIC Let's look at these values by a percentage of the population; the numbers used are the 2019 US Census estimates of county populations.
# MAGIC 
# MAGIC *Note, reviewing the top 10 counties by case (vs. % of cases)* 

# COMMAND ----------

# DBTITLE 1,WA State Confirmed Cases per 100K people 3/6 - 3/20 - Educational Facilities Closed 3/13
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases_per_100Kpop 
# MAGIC   FROM wa_state_window f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases) as cases FROM wa_state_window GROUP BY county ORDER BY cases DESC LIMIT 10  
# MAGIC     ) x ON x.county = f.county

# COMMAND ----------

# DBTITLE 1,NY State Confirmed Cases 3/11 - 3/25 - Educational Facilities Closed 3/18
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases_per_100Kpop 
# MAGIC   FROM ny_state_window f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases) as cases FROM ny_state_window GROUP BY county ORDER BY cases DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county

# COMMAND ----------

# DBTITLE 1,NY State Confirmed Cases 3/6 - 3/20 - Educational Facilities Closed 3/18
# MAGIC %sql
# MAGIC SELECT f.date, f.county, f.cases_per_100Kpop 
# MAGIC   FROM ny_state_window_m1 f
# MAGIC   JOIN (
# MAGIC       SELECT county, sum(cases) as cases FROM ny_state_window_m1 GROUP BY county ORDER BY cases DESC LIMIT 10
# MAGIC     ) x ON x.county = f.county

# COMMAND ----------

# MAGIC %md ## Visualize Cases by State Choropleth Maps
# MAGIC * Join the data with `map_fips_dedup` to obtain the county centroid lat, long_

# COMMAND ----------

# Extract Day Number and county centroid lat, long_
wa_daynum = spark.sql("""select f.fips, f.county, f.date, f.day_num, cases as confirmed, cast(f.cases_per_100Kpop as int) as confirmed_per100K, deaths, cast(f.deaths_per_100Kpop as int) as deaths_per100K, m.lat, m.long_ from wa_state_window f join map_fips_dedup m on m.fips = f.fips""")
wa_daynum.createOrReplaceTempView("wa_daynum")
ny_daynum = spark.sql("""select cast(f.fips as int) as fips, f.county, f.date, f.day_num, cases as confirmed, cast(f.cases_per_100Kpop as int) as confirmed_per100K, deaths, cast(f.deaths_per_100Kpop as int) as deaths_per100K, m.lat, m.long_ from ny_state_window f join map_fips_dedup m on m.fips = f.fips""")
ny_daynum.createOrReplaceTempView("ny_daynum")
ny_daynum_m1 = spark.sql("""select cast(f.fips as int) as fips, f.county, f.date, f.day_num, cases as confirmed, cast(f.cases_per_100Kpop as int) as confirmed_per100K, deaths, cast(f.deaths_per_100Kpop as int) as deaths_per100K, m.lat, m.long_ from ny_state_window_m1 f join map_fips_dedup m on m.fips = f.fips""")
ny_daynum_m1.createOrReplaceTempView("ny_daynum_m1")

# COMMAND ----------

# Obtain Topography
topo_usa = 'https://vega.github.io/vega-datasets/data/us-10m.json'
topo_wa = 'https://raw.githubusercontent.com/deldersveld/topojson/master/countries/us-states/WA-53-washington-counties.json'
topo_ny = 'https://raw.githubusercontent.com/deldersveld/topojson/master/countries/us-states/NY-36-new-york-counties.json'
us_counties = alt.topo_feature(topo_usa, 'counties')
wa_counties = alt.topo_feature(topo_wa, 'cb_2015_washington_county_20m')
ny_counties = alt.topo_feature(topo_ny, 'cb_2015_new_york_county_20m')

# COMMAND ----------

# Review WA
confirmed_wa = wa_daynum.select("fips", "day_num", "date", "confirmed", "confirmed_per100K", "county").where("confirmed > 0").toPandas()
confirmed_wa['date'] = confirmed_wa['date'].astype(str)
deaths_wa = wa_daynum.select("lat", "long_", "day_num", "date", "deaths", "deaths_per100K", "county").where("deaths > 0").toPandas()
deaths_wa['date'] = deaths_wa['date'].astype(str)

# Review NY
confirmed_ny = ny_daynum.select("fips", "day_num", "date", "confirmed", "confirmed_per100K", "county").where("confirmed > 0").toPandas()
confirmed_ny['date'] = confirmed_ny['date'].astype(str)
deaths_ny = ny_daynum.select("lat", "long_", "day_num", "date", "deaths", "deaths_per100K", "county").where("deaths > 0").toPandas()
deaths_ny['date'] = deaths_ny['date'].astype(str)

# Review NY -1 week
confirmed_ny_m1 = ny_daynum_m1.select("fips", "day_num", "date", "confirmed", "confirmed_per100K", "county").where("confirmed > 0").toPandas()
confirmed_ny_m1['date'] = confirmed_ny_m1['date'].astype(str)
deaths_ny_m1 = ny_daynum_m1.select("lat", "long_", "day_num", "date", "deaths", "deaths_per100K", "county").where("deaths > 0").toPandas()
deaths_ny_m1['date'] = deaths_ny_m1['date'].astype(str)

# COMMAND ----------

# State Choropleth Map Visualization Function
def map_state(curr_day_num, state_txt, state_counties, confirmed, confirmed_min, confirmed_max, deaths, deaths_min, deaths_max):
  # Get date_str
  date_str = confirmed[confirmed['day_num'] == 101]['date'].head(1).item()
  
  # State
  base_state = alt.Chart(state_counties).mark_geoshape(
      fill='white',
      stroke='lightgray',
  ).properties(
      width=800,
      height=600,
  ).project(
      type='mercator'
  )

  # counties
  base_state_counties = alt.Chart(us_counties).mark_geoshape(
  ).transform_lookup(
    lookup='id',
    from_=alt.LookupData(confirmed[(confirmed['confirmed_per100K'] > 0) & (confirmed['day_num'] == curr_day_num)], 'fips', ['confirmed_per100K', 'confirmed', 'county', 'date', 'fips'])  
  ).encode(
     color=alt.Color('confirmed_per100K:Q', scale=alt.Scale(type='log', domain=[confirmed_min, confirmed_max]), title='Confirmed per 100K'),
    tooltip=[
      alt.Tooltip('fips:O'),
      alt.Tooltip('confirmed:Q'),
      alt.Tooltip('confirmed_per100K:Q'),
      alt.Tooltip('county:N'),
      alt.Tooltip('date:N'),
    ],
  )

  # deaths by long, latitude
  points = alt.Chart(deaths[(deaths['deaths_per100K'] > 0) & (deaths['day_num'] == curr_day_num)]).mark_point(opacity=0.75, filled=True).encode(
    longitude='long_:Q',
    latitude='lat:Q',
    size=alt.Size('sum(deaths_per100K):Q', scale=alt.Scale(type='symlog', domain=[deaths_min, deaths_max]), title='Deaths per 100K'),
    color=alt.value('#BD595D'),
    stroke=alt.value('brown'),
    tooltip=[
      alt.Tooltip('lat'),
      alt.Tooltip('long_'),
      alt.Tooltip('deaths'),
      alt.Tooltip('county:N'),      
      alt.Tooltip('date:N'),      
    ],
  ).properties(
    # update figure title
    title=f'COVID-19 {state_txt} Confirmed Cases and Deaths per 100K by County [{curr_day_num}, {date_str}]'
  )

  return (base_state + base_state_counties + points)


# COMMAND ----------

# MAGIC %md 
# MAGIC | Factors | WA | NY | 
# MAGIC | ------- | -- | -- | 
# MAGIC | Educational Facilities Closed | 3/13/2020 | 3/18/2020 |
# MAGIC | Day 00 | 3/6/2020 | 3/11/2020 |
# MAGIC | Day 14 | 3/20/2020 | 3/25/2020 | 
# MAGIC | Max Cases | 794 | 20011 |
# MAGIC | Max Deaths | 68 | 280 |
# MAGIC | Max Cases per 100K | 50.55 | 1222.97 | 
# MAGIC | Max Deaths per 100K | 3.27 | 17.11 |

# COMMAND ----------

# MAGIC %md ### WA State (cases and deaths per 100K)

# COMMAND ----------

map_state(101, 'WA', wa_counties, confirmed_wa, 1, 60, deaths_wa, 1, 5)

# COMMAND ----------

map_state(107, 'WA', wa_counties, confirmed_wa, 1, 60, deaths_wa, 1, 5)

# COMMAND ----------

map_state(114, 'WA', wa_counties, confirmed_wa, 1, 60, deaths_wa, 1, 5)

# COMMAND ----------

# MAGIC %md ### NY State (cases and deaths per 100K)

# COMMAND ----------

map_state(101, 'NY', ny_counties, confirmed_ny, 1, 1500, deaths_ny, 1, 20)

# COMMAND ----------

map_state(107, 'NY', ny_counties, confirmed_ny, 1, 1500, deaths_ny, 1, 20)

# COMMAND ----------

map_state(114, 'NY', ny_counties, confirmed_ny, 1, 1500, deaths_ny, 1, 20)

# COMMAND ----------

# MAGIC %md ## COVID-19 Confirmed Cases and Deaths by WA and NY County Slider

# COMMAND ----------

# State Choropleth Map Visualization Function
def map_state_slider(state_txt, state_counties, confirmed, confirmed_min, confirmed_max, deaths, deaths_min, deaths_max, state_fips):
  # Pivot confirmed data by day_num
  confirmed_pv = confirmed[['fips', 'day_num', 'confirmed']].copy()
  confirmed_pv['fips'] = confirmed_pv['fips'].astype(str)
  confirmed_pv['day_num'] = confirmed_pv['day_num'].astype(str)
  confirmed_pv['confirmed'] = confirmed_pv['confirmed'].astype('int64')
  confirmed_pv = confirmed_pv.pivot_table(index='fips', columns='day_num', values='confirmed', fill_value=0).reset_index()

  # Pivot deaths data by day_num
  deaths_pv = deaths[['lat', 'long_', 'day_num', 'deaths']].copy()
  deaths_pv['day_num'] = deaths_pv['day_num'].astype(str)
  deaths_pv['deaths'] = deaths_pv['deaths'].astype('int64')
  deaths_pv = deaths_pv.pivot_table(index=['lat', 'long_'], columns='day_num', values='deaths', fill_value=0).reset_index()

  # Extract column names for slider
  column_names = confirmed_pv.columns.tolist()

  # Remove first element (`fips`)
  column_names.pop(0)

  # Convert to int
  column_values = [None] * len(column_names)
  for i in range(0, len(column_names)): column_values[i] = int(column_names[i]) 
  
  # Disable max_rows to see more data
  alt.data_transformers.disable_max_rows()

  # Topographic information
  us_states = alt.topo_feature(topo_usa, 'states')
  us_counties = alt.topo_feature(topo_usa, 'counties')

  # state county boundaries
  base_state = alt.Chart(state_counties).mark_geoshape(
      fill='white',
      stroke='lightgray',
  ).properties(
      width=800,
      height=600,
  ).project(
      type='mercator'
  )

  # Slider choices
  min_day_num = column_values[0]
  max_day_num = column_values[len(column_values)-1]
  slider = alt.binding_range(min=min_day_num, max=max_day_num, step=1)
  slider_selection = alt.selection_single(fields=['day_num'], bind=slider, name="day_num", init={'day_num':min_day_num})


  # Confirmed cases by county
  base_state_counties = alt.Chart(us_counties).mark_geoshape(
      stroke='black',
      strokeWidth=0.05
  ).transform_lookup(
      lookup='id',
      from_=alt.LookupData(confirmed_pv, 'fips', column_names)  
  ).transform_fold(
      column_names, as_=['day_num', 'confirmed']
  ).transform_calculate(
      state_id = "(datum.id / 1000)|0",
      day_num = 'parseInt(datum.day_num)',
      confirmed = 'isValid(datum.confirmed) ? datum.confirmed : -1'
  ).encode(
      color = alt.condition(
          'datum.confirmed > 0',      
          alt.Color('confirmed:Q', scale=alt.Scale(domain=(confirmed_min, confirmed_max), type='symlog')),
          alt.value('white')
        )  
  ).properties(
    # update figure title
    title=f'COVID-19 WA State Confirmed Cases by County'
  ).transform_filter(
      (alt.datum.state_id)==state_fips
  ).transform_filter(
      slider_selection
  )

  # deaths by long, latitude
  points = alt.Chart(
    deaths_pv
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
    size=alt.Size('deaths:Q', scale=alt.Scale(domain=(deaths_min, deaths_max), type='symlog'), title='deaths'),
    color=alt.value('#BD595D'),
    stroke=alt.value('brown'),
  ).add_selection(
      slider_selection
  ).transform_filter(
      slider_selection
  )

  # confirmed cases (base_counties) and deaths (points)
  return (base_state + base_state_counties + points) 

# COMMAND ----------

# MAGIC %md 
# MAGIC | Factors | WA | NY | 
# MAGIC | ------- | -- | -- | 
# MAGIC | Educational Facilities Closed | 3/13/2020 | 3/18/2020 |
# MAGIC | Day 00 | 3/6/2020 | 3/11/2020 |
# MAGIC | Day 14 | 3/20/2020 | 3/25/2020 | 
# MAGIC | Max Cases | 794 | 20011 |
# MAGIC | Max Deaths | 68 | 280 |
# MAGIC | Max Cases per 100K | 50.55 | 1222.97 | 
# MAGIC | Max Deaths per 100K | 3.27 | 17.11 |

# COMMAND ----------

map_state_slider('WA', wa_counties, confirmed_wa, 1, 800, deaths_wa, 1, 70, 53)

# COMMAND ----------

map_state_slider('NY', ny_counties, confirmed_ny, 1, 21000, deaths_ny, 1, 300, 36)

# COMMAND ----------

# MAGIC %md ## COVID-19 Confirmed Cases and Deaths by WA and NY County Map and Graph

# COMMAND ----------

# map_state_graph
def map_state_graph(state_txt, state_counties, confirmed, confirmed_min, confirmed_max, deaths, deaths_min, deaths_max, state_fips):
  
  # pivot confirmed cases (by date)
  confirmed_pv2 = confirmed[['fips', 'date', 'confirmed']].copy()
  confirmed_pv2['fips'] = confirmed_pv2['fips'].astype(str)
  confirmed_pv2['date'] = confirmed_pv2['date'].astype(str)
  confirmed_pv2['confirmed'] = confirmed_pv2['confirmed'].astype('int64')
  confirmed_pv2 = confirmed_pv2.pivot_table(index='fips', columns='date', values='confirmed', fill_value=0).reset_index()

  # pivot deaths
  deaths_pv2 = deaths[['lat', 'long_', 'date', 'deaths']].copy()
  deaths_pv2['date'] = deaths_pv2['date'].astype(str)
  deaths_pv2['deaths'] = deaths_pv2['deaths'].astype('int64')
  deaths_pv2 = deaths_pv2.pivot_table(index=['lat', 'long_'], columns='date', values='deaths', fill_value=0).reset_index()

  # Extract column names for slider
  column_names2 = confirmed_pv2.columns.tolist()

  # Remove first element (`fips`)
  column_names2.pop(0)

  # date selection
  pts = alt.selection(type="single", encodings=['x'])

  # State
  base_state = alt.Chart(state_counties).mark_geoshape(
      fill='white',
      stroke='lightgray',
  ).properties(
      width=800,
      height=600,
  ).project(
      type='mercator'
  )

  # State Counties
  base_state_counties = alt.Chart(us_counties).mark_geoshape(
    stroke='black',
    strokeWidth=0.05,
  ).transform_lookup(
    lookup='id',
   from_=alt.LookupData(confirmed_pv2, 'fips', column_names2)
   ).transform_fold(
     column_names2, as_=['date', 'confirmed']
  ).transform_calculate(
      state_id = "(datum.id / 1000)|0",
      date = 'datum.date',
      confirmed = 'isValid(datum.confirmed) ? datum.confirmed : -1'
  ).encode(
       color = alt.condition(
          'datum.confirmed > 0',      
          alt.Color('confirmed:Q', scale=alt.Scale(domain=(confirmed_min, confirmed_max), type='symlog')),
          alt.value('white')
        )  
  ).transform_filter(
    pts
  ).transform_filter(
      (alt.datum.state_id)==state_fips
  )

  # Bar Graph
  bar = alt.Chart(confirmed).mark_bar().encode(
      x='date:N',
      y='confirmed_per100K:Q',
      color=alt.condition(pts, alt.ColorValue("steelblue"), alt.ColorValue("grey"))
  ).properties(
      width=800,
      height=200,
      title='Confirmed Cases per 100K'
  ).add_selection(pts)

  # Deaths
  points = alt.Chart(deaths).mark_point(opacity=0.75, filled=True).encode(
    longitude='long_:Q',
    latitude='lat:Q',
    size=alt.Size('sum(deaths):Q', scale=alt.Scale(domain=[deaths_min, deaths_max]), title='Deaths'),
    color=alt.value('#BD595D'),
    stroke=alt.value('brown'),
    tooltip=[
      alt.Tooltip('lat'),
      alt.Tooltip('long_'),
      alt.Tooltip('deaths'),
      alt.Tooltip('county:N'),      
      alt.Tooltip('date:N'),      
    ],
  ).properties(
    # update figure title
    title=f'COVID-19 Confirmed Cases and Deaths by County'
  ).transform_filter(
      pts
  )

  return (base_state + base_state_counties + points) & bar

# COMMAND ----------

map_state_graph('WA', wa_counties, confirmed_wa, 1, 800, deaths_wa, 1, 70, 53)

# COMMAND ----------

map_state_graph('NY', ny_counties, confirmed_ny, 1, 21000, deaths_ny, 1, 300, 36)

# COMMAND ----------

map_state_graph('NY', ny_counties, confirmed_ny_m1, 1, 4500, deaths_ny, 1, 70, 36)

# COMMAND ----------

