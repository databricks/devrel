# Databricks notebook source
# MAGIC %md
# MAGIC ##Import Data

# COMMAND ----------

import pyspark.pandas as pd

data_file = "/mnt/training/airbnb-sf-listings.csv"
airbnb_sf_listings = pd.read_csv( data_file, quotechar='"', escapechar='"' )
display(airbnb_sf_listings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Featurization

# COMMAND ----------

# combine id columns
airbnb_sf_listings['id'] = airbnb_sf_listings['host_id'] +'-'+ airbnb_sf_listings['id']
airbnb_sf_listings = airbnb_sf_listings.drop(['host_id', 'neighbourhood_group'])

# COMMAND ----------

display(airbnb_sf_listings)

# COMMAND ----------

# index categorical variables
def index_categorical( cat_col ):
  # get distinct category labels
  cat_labels = cat_col.dropna().drop_duplicates().to_list()
  # create inverse mapping from labels to index
  cat_map = dict((l,i) for i, l in enumerate(cat_labels))
  # transform the column by applying the mapping
  return cat_col.map(cat_map)

cat_columns = ['neighbourhood', 'room_type']
indexed_listings = airbnb_sf_listings
for col in cat_columns:
  idx_col = col+'_idx'
  indexed_listings[idx_col] = pd.to_numeric(index_categorical(indexed_listings[col])).astype(int)

# COMMAND ----------

feature_cols = ['id', 'neighbourhood_idx', 'room_type_idx', 'price', 'minimum_nights', 'number_of_reviews', 'reviews_per_month', 'calculated_host_listings_count', 'availability_365' ]
featurized = indexed_listings[feature_cols]
featurized = featurized.dropna()

# COMMAND ----------

display(featurized)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a Feature Table
# MAGIC [Databricks Feature Store](https://docs.databricks.com/applications/machine-learning/feature-store/index.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS airbnb;

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()
feature_table_name = 'airbnb.features'

# create table if it doesn't exist
try:
  fs.get_feature_table(feature_table_name)
except:
  featurized_fs = fs.create_feature_table(
    name = feature_table_name,
    keys = 'id',
    schema = featurized.spark.schema(),
    description = 'These features are derived from the airbnb.features table in the lakehouse.  I created dummy variables for the categorical columns, cleaned up their names, and dropped the neighbourhood_group_idx .  No aggregations were performed.'
  )

fs.write_table(
  name = feature_table_name,
  df = featurized.to_spark(),
  mode = 'merge'
)

# COMMAND ----------


