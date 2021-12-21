# Databricks notebook source
# MAGIC %md
# MAGIC ## Import Data
# MAGIC [More info around pyspark.pandas](https://databricks.com/blog/2021/10/04/pandas-api-on-upcoming-apache-spark-3-2.html)

# COMMAND ----------

import pyspark.pandas as pd
#import databricks.koalas as pd # for spark less than 3.2

data_file = "/mnt/training/airbnb-sf-listings.csv"
airbnb_sf_listings = pd.read_csv( data_file, quotechar='"', escapechar='"' )
display(airbnb_sf_listings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the data

# COMMAND ----------

airbnb_sf_listings.describe()

# COMMAND ----------

airbnb_sf_listings['price'].plot.hist(100)

# COMMAND ----------

airbnb_spark_df = airbnb_sf_listings.to_spark() #conversion from dataframe to spark

dbutils.data.summarize(airbnb_spark_df) #Data profiling with Spark dataframes

# COMMAND ----------

from pandas_profiling import ProfileReport

airbnb_pandas_df = airbnb_sf_listings.to_pandas() #convert spark dataframe to pandas

df_profile = ProfileReport(airbnb_pandas_df, title="Profiling Report", progress_bar=False, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)
