# Databricks notebook source
# MAGIC %md ## Johns Hopkins CSSE COVID-19 Analysis
# MAGIC This notebook processes and performs quick analysis from the [2019 Novel Coronavirus COVID-19 (2019-nCoV) Data Repository by Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19).  The data is updated in the `/databricks-datasets/COVID/CSSEGISandData/` location regularly so you can access the data directly.

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

# MAGIC %md ## Specify `jhu_daily` table
# MAGIC * Source: `/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports/`
# MAGIC * Contains the COVID-19 daily reports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType
schema = StructType([
  StructField('FIPS', IntegerType(), True), 
  StructField('Admin2', StringType(), True),
  StructField('Province_State', StringType(), True),  
  StructField('Country_Region', StringType(), True),  
  StructField('Last_Update', TimestampType(), True),  
  StructField('Lat', DoubleType(), True),  
  StructField('Long_', DoubleType(), True),
  StructField('Confirmed', IntegerType(), True), 
  StructField('Deaths', IntegerType(), True), 
  StructField('Recovered', IntegerType(), True), 
  StructField('Active', IntegerType(), True),   
  StructField('Combined_Key', StringType(), True),  
  StructField('process_date', DateType(), True),    
])

# Create initial empty Spark DataFrame based on preceding schema
jhu_daily = spark.createDataFrame([], schema)

# COMMAND ----------

# MAGIC %md ## Loops Through Each File
# MAGIC The following code snippet processes each file to:
# MAGIC * Extract out the filename which is needed to know which date the data is referring
# MAGIC * The schema of the files change over time so we need slightly different logic to insert data for each different schema

# COMMAND ----------

import os
import pandas as pd
import glob
from pyspark.sql.functions import input_file_name, lit, col

# Creates a list of all csv files
globbed_files = glob.glob("/dbfs/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports/*.csv") 
#globbed_files = glob.glob("/dbfs/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports/04*.csv")

i = 0
for csv in globbed_files:
  # Filename
  source_file = csv[5:200]
  process_date = csv[100:104] + "-" + csv[94:96] + "-" + csv[97:99]
  
  # Read data into temporary dataframe
  df_tmp = spark.read.option("inferSchema", True).option("header", True).csv(source_file)
  df_tmp.createOrReplaceTempView("df_tmp")

  # Obtain schema
  schema_txt = ' '.join(map(str, df_tmp.columns)) 
  
  # Three schema types (as of 2020-04-08) 
  schema_01 = "Province/State Country/Region Last Update Confirmed Deaths Recovered" # 01-22-2020 to 02-29-2020
  schema_02 = "Province/State Country/Region Last Update Confirmed Deaths Recovered Latitude Longitude" # 03-01-2020 to 03-21-2020
  schema_03 = "FIPS Admin2 Province_State Country_Region Last_Update Lat Long_ Confirmed Deaths Recovered Active Combined_Key" # 03-22-2020 to
  
  # Insert data based on schema type
  if (schema_txt == schema_01):
    df_tmp = (df_tmp
                .withColumn("FIPS", lit(None).cast(IntegerType()))
                .withColumn("Admin2", lit(None).cast(StringType()))
                .withColumn("Province_State", col("Province/State"))
                .withColumn("Country_Region", col("Country/Region"))
                .withColumn("Last_Update", col("Last Update"))
                .withColumn("Lat", lit(None).cast(DoubleType()))
                .withColumn("Long_", lit(None).cast(DoubleType()))
                .withColumn("Active", lit(None).cast(IntegerType()))
                .withColumn("Combined_Key", lit(None).cast(StringType()))
                .withColumn("process_date", lit(process_date))
                .select("FIPS", 
                        "Admin2", 
                        "Province_State", 
                        "Country_Region", 
                        "Last_Update", 
                        "Lat", 
                        "Long_", 
                        "Confirmed", 
                        "Deaths", 
                        "Recovered", 
                        "Active", 
                        "Combined_Key", 
                        "process_date")
               )
    jhu_daily = jhu_daily.union(df_tmp)
  elif (schema_txt == schema_02):
    df_tmp = (df_tmp
                .withColumn("FIPS", lit(None).cast(IntegerType()))
                .withColumn("Admin2", lit(None).cast(StringType()))
                .withColumn("Province_State", col("Province/State"))
                .withColumn("Country_Region", col("Country/Region"))
                .withColumn("Last_Update", col("Last Update"))
                .withColumn("Lat", col("Latitude"))
                .withColumn("Long_", col("Longitude"))
                .withColumn("Active", lit(None).cast(IntegerType()))
                .withColumn("Combined_Key", lit(None).cast(StringType()))
                .withColumn("process_date", lit(process_date))
                .select("FIPS", 
                        "Admin2", 
                        "Province_State", 
                        "Country_Region", 
                        "Last_Update", 
                        "Lat", 
                        "Long_", 
                        "Confirmed", 
                        "Deaths", 
                        "Recovered", 
                        "Active", 
                        "Combined_Key", 
                        "process_date")
               )
    jhu_daily = jhu_daily.union(df_tmp)

  elif (schema_txt == schema_03):
    df_tmp = df_tmp.withColumn("process_date", lit(process_date))
    jhu_daily = jhu_daily.union(df_tmp)
  else:
    print("Schema may have changed")
    raise
  
  # print out the schema being processed by date
  print("%s | %s" % (process_date, schema_txt))

# COMMAND ----------

jhu_daily.createOrReplaceTempView("jhu_daily")
display(jhu_daily)

# COMMAND ----------

#%sh
#rm -fR /dbfs/tmp/dennylee/COVID/jhu_daily/

# COMMAND ----------

# # Saving jhu_daily table
# file_path = '/tmp/dennylee/COVID/jhu_daily/'
# jhu_daily.repartition(4).write.format("parquet").save(file_path)

# COMMAND ----------

# MAGIC %md ## Download 2019 Population Estimates

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/dennylee/COVID/population_estimates_by_county/ && wget -O /dbfs/tmp/dennylee/COVID/population_estimates_by_county/co-est2019-alldata.csv https://raw.githubusercontent.com/databricks/tech-talks/master/datasets/co-est2019-alldata.csv && ls -al /dbfs/tmp/dennylee/COVID/population_estimates_by_county/

# COMMAND ----------

map_popest_county = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/dennylee/COVID/population_estimates_by_county/co-est2019-alldata.csv")
map_popest_county.createOrReplaceTempView("map_popest_county")
fips_popest_county = spark.sql("select State * 1000 + substring(cast(1000 + County as string), 2, 3) as fips, STNAME, CTYNAME, census2010pop, POPESTIMATE2019 from map_popest_county")
fips_popest_county.createOrReplaceTempView("fips_popest_county")

# COMMAND ----------

# MAGIC %md ## Include Population Estimates
# MAGIC Create `jhu_daily_pop` to include population estimates; note, by including population estimates we're limiting our dataset from 3/22 onwards as the data prior to 3/22 does not contain `FIPS` information.

# COMMAND ----------

jhu_daily_pop = spark.sql("""
SELECT f.FIPS, f.Admin2, f.Province_State, f.Country_Region, f.Last_Update, f.Lat, f.Long_, f.Confirmed, f.Deaths, f.Recovered, f.Active, f.Combined_Key, f.process_date, p.POPESTIMATE2019 
  FROM jhu_daily f
    JOIN fips_popest_county p
      ON p.fips = f.FIPS
""")
jhu_daily_pop.createOrReplaceTempView("jhu_daily_pop")

# COMMAND ----------

# MAGIC %md ## Initial Exploratory Data Analysis

# COMMAND ----------

# MAGIC %md #### Reviewing the confirmed cases and deaths for NY and King Counties

# COMMAND ----------

# MAGIC %sql
# MAGIC select process_date, Admin2, Confirmed, Deaths, Recovered, Active from jhu_daily where Province_State in ('New York') and Admin2 in ('New York City')

# COMMAND ----------

# MAGIC %sql
# MAGIC select process_date, Admin2, Confirmed, Deaths, Recovered, Active from jhu_daily where Province_State in ('Washington') and Admin2 in ('King')

# COMMAND ----------

# MAGIC %md #### Reviewing the confirmed cases and deaths in proportion to the population for NY and King Counties

# COMMAND ----------

# MAGIC %sql
# MAGIC select process_date, Admin2, 100000.*Confirmed/POPESTIMATE2019 as Confirmed_per100K, 100000.*Deaths/POPESTIMATE2019 as Deaths_per100K, Recovered, Active from jhu_daily_pop where Province_State in ('New York') and Admin2 in ('New York City')

# COMMAND ----------

# MAGIC %sql
# MAGIC select process_date, Admin2, 100000.*Confirmed/POPESTIMATE2019 as Confirmed_per100K, 100000.*Deaths/POPESTIMATE2019 as Deaths_per100K, Recovered, Active from jhu_daily_pop where Province_State in ('Washington') and Admin2 in ('King')

# COMMAND ----------

# MAGIC %md ## COVID-19 Confirmed Cases and Deaths by County

# COMMAND ----------

# Create `usa` dataframe
df_usa = spark.sql("select fips, cast(100000.*Confirmed/POPESTIMATE2019 as int) as confirmed_per100K, cast(100000.*Deaths/POPESTIMATE2019 as int) as deaths_per100K, recovered, active, lat, long_, admin2 as county, province_state as state, process_date, cast(replace(process_date, '-', '') as integer) as process_date_num from jhu_daily_pop where lat is not null and long_ is not null and fips is not null and (lat <> 0 and long_ <> 0)")
df_usa.createOrReplaceTempView("df_usa")

# Convert latest date of data to pandas DataFrame
pdf_usa = df_usa.toPandas()
pdf_usa['confirmed_per100K'] = pdf_usa['confirmed_per100K'].astype('int32')
pdf_usa['deaths_per100K'] = pdf_usa['deaths_per100K'].astype('int32')

# COMMAND ----------

def map_usa_cases(curr_date):
  # Obtain altair topographic information
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


  # confirmed cases by county
  base_counties = alt.Chart(us_counties).mark_geoshape().encode(
    color=alt.Color('confirmed:Q', scale=alt.Scale(type='log'), title='Confirmed'),
  ).transform_lookup(
    lookup='id',
    from_=alt.LookupData(pdf_usa[(pdf_usa['confirmed'] > 0) & (pdf_usa['process_date'] == curr_date)], 'fips', ['confirmed'])  
  )

  # deaths by long, latitude
  points = alt.Chart(pdf_usa[(pdf_usa['deaths'] > 0) & (pdf_usa['process_date'] == curr_date)]).mark_point(opacity=0.75, filled=True).encode(
    longitude='long_:Q',
    latitude='lat:Q',
    size=alt.Size('sum(deaths):Q', scale=alt.Scale(type='symlog'), title='deaths'),
    color=alt.value('#BD595D'),
    stroke=alt.value('brown'),
    tooltip=[
      alt.Tooltip('state', title='state'), 
      alt.Tooltip('county', title='county'), 
      alt.Tooltip('confirmed', title='confirmed'),
      alt.Tooltip('deaths', title='deaths'),       
    ],
  ).properties(
    # update figure title
    title=f'COVID-19 Confirmed Cases and Deaths by County {curr_date}'
  )

  # display graph
  return (base_states + base_counties + points)

# COMMAND ----------

def map_usa_cases(curr_date):
  # Obtain altair topographic information
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


  # confirmed cases by county
  base_counties = alt.Chart(us_counties).mark_geoshape().encode(
    color=alt.Color('confirmed_per100K:Q', scale=alt.Scale(domain=(1, 7500), type='log'), title='Confirmed per 100K'),
  ).transform_lookup(
    lookup='id',
    from_=alt.LookupData(pdf_usa[(pdf_usa['confirmed_per100K'] > 0) & (pdf_usa['process_date'] == curr_date)], 'fips', ['confirmed_per100K'])  
  )

  # deaths by long, latitude
  points = alt.Chart(pdf_usa[(pdf_usa['deaths_per100K'] > 0) & (pdf_usa['process_date'] == curr_date)]).mark_point(opacity=0.75, filled=True).encode(
    longitude='long_:Q',
    latitude='lat:Q',
     size=alt.Size('deaths_per100K:Q', scale=alt.Scale(domain=(1, 1000), type='log'), title='deaths_per100K'),
     #size=alt.Size('deaths_per100K:Q', title='deaths_per100K'),
     color=alt.value('#BD595D'),
     stroke=alt.value('brown'),
    tooltip=[
      alt.Tooltip('state', title='state'), 
      alt.Tooltip('county', title='county'), 
      alt.Tooltip('confirmed_per100K', title='confirmed'),
      alt.Tooltip('deaths_per100K', title='deaths'),       
    ],
  ).properties(
    # update figure title
    title=f'COVID-19 Confirmed Cases and Deaths by County (by 100K) {curr_date}'
  )

   # display graph
  return (base_states + base_counties + points)

# COMMAND ----------

# Starting Date (2020-03-22)
map_usa_cases('2020-03-22')

# COMMAND ----------

# Latest Date (2020-04-14)
map_usa_cases('2020-04-14')

# COMMAND ----------

# MAGIC %md ## COVID-19 Confirmed Cases and Deaths by County Slider

# COMMAND ----------

# Create `usa_confirmed` dataframe 
process_date_zero = spark.sql("select min(process_date) from df_usa where fips is not null").collect()[0][0]
df_usa_conf = spark.sql("""
select fips, 100 + datediff(process_date, '""" + process_date_zero + """') as day_num, confirmed_per100K
  from (
     select fips, process_date, max(confirmed_per100K) as confirmed_per100K
       from df_usa
      group by fips, process_date
) x """)
df_usa_conf.createOrReplaceTempView("df_usa_conf")

# Convert to Pandas
pdf_usa_conf = df_usa_conf.toPandas()
pdf_usa_conf['day_num'] = pdf_usa_conf['day_num'].astype(str)
pdf_usa_conf['confirmed_per100K'] = pdf_usa_conf['confirmed_per100K'].astype('int64')
pdf_usa_conf = pdf_usa_conf.pivot_table(index='fips', columns='day_num', values='confirmed_per100K', fill_value=0).reset_index()

# Create `usa_deaths` datasframe
df_usa_deaths = spark.sql("""
select lat, long_, 100 + datediff(process_date, '""" + process_date_zero + """') as day_num, deaths_per100K
  from (
     select lat, long_, process_date, max(deaths_per100K) as deaths_per100K
       from df_usa
      group by lat, long_, process_date
) x """)
df_usa_deaths.createOrReplaceTempView("df_usa_deaths")

# Covnert to pandas
pdf_usa_deaths = df_usa_deaths.toPandas()
pdf_usa_deaths['day_num'] = pdf_usa_deaths['day_num'].astype(str)
pdf_usa_deaths['deaths_per100K'] = pdf_usa_deaths['deaths_per100K'].astype('int64')
pdf_usa_deaths = pdf_usa_deaths.pivot_table(index=['lat', 'long_'], columns='day_num', values='deaths_per100K', fill_value=0).reset_index()

# Extract column names for slider
column_names = pdf_usa_conf.columns.tolist()

# Remove first element (`fips`)
column_names.pop(0)

# Convert to int
column_values = [None] * len(column_names)
for i in range(0, len(column_names)): column_values[i] = int(column_names[i]) 

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
slider = alt.binding_range(min=min_day_num, max=max_day_num, step=1)
slider_selection = alt.selection_single(fields=['day_num'], bind=slider, name="day_num", init={'day_num':max_day_num})

# Confirmed cases by county
base_counties = alt.Chart(us_counties).mark_geoshape(
    stroke='black',
    strokeWidth=0.05
).project(
    type='albersUsa'
).transform_lookup(
    lookup='id',
    from_=alt.LookupData(pdf_usa_conf, 'fips', column_names)  
).transform_fold(
    column_names, as_=['day_num', 'confirmed_per100K']
).transform_calculate(
    day_num = 'parseInt(datum.day_num)',
    confirmed_per100K = 'isValid(datum.confirmed_per100K) ? datum.confirmed_per100K : -1'
).encode(
    color = alt.condition(
        'datum.confirmed_per100K > 0',      
        alt.Color('confirmed_per100K:Q', scale=alt.Scale(domain=(1, 7500), type='log')),
        alt.value('white')
      )  
).transform_filter(
    slider_selection
# ).add_selection(
#     slider_selection
)

# deaths by long, latitude
points = alt.Chart(
  pdf_usa_deaths
).mark_point(
  opacity=0.75, filled=True
).transform_fold(
  column_names, as_=['day_num', 'deaths_per100K']
).transform_calculate(
    day_num = 'parseInt(datum.day_num)',
    deaths_per100K = 'isValid(datum.deaths_per100K) ? datum.deaths_per100K : -1'  
).encode(
  longitude='long_:Q',
  latitude='lat:Q',
  size=alt.Size('deaths_per100K:Q', scale=alt.Scale(domain=(1, 1000), type='log'), title='deaths_per100K'),
  color=alt.value('#BD595D'),
  stroke=alt.value('brown'),
).properties(
  # update figure title
  title=f'COVID-19 Confirmed Cases and Deaths by County (per 100K) Between 3/22 to 4/14 (2020)'
).add_selection(
    slider_selection
).transform_filter(
    slider_selection
)

# confirmed cases (base_counties) and deaths (points)
(base_states + base_counties + points) 

# COMMAND ----------

