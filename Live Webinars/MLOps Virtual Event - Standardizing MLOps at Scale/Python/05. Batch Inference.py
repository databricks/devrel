# Databricks notebook source
# MAGIC %md
# MAGIC ## Get the stage of the model to fetch

# COMMAND ----------

model_name = 'Airbnb_Model'

# define a dropdown widget to set the stage
dbutils.widgets.dropdown("stage", "Production", ["Staging", "Production"])

# get the stage to be retrieved
model_stage = dbutils.widgets.get("stage")
print(f"Fetching latest {model_stage} version of the {model_name} model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find latest production version of our model

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

registry_uri = 'databricks://mlops_webinar:CMR'
client = MlflowClient(registry_uri=registry_uri)

all_model_versions = client.search_model_versions("name = '%s'" % model_name)
model_version = max([int(mv.version) for mv in all_model_versions if mv.current_stage == model_stage])
print(f"Latest {model_stage} version of the {model_name} model is: {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch model from central model registry
# MAGIC ### Pull it down as a Spark UDF

# COMMAND ----------

import mlflow
import mlflow.pyfunc

# build model uri
scope = 'mlops_webinar'
key = 'CMR'
model_uri = f"models://{scope}:{key}@databricks/{model_name}/{model_stage}"

# fetch model as spark UDF
model_udf = mlflow.pyfunc.spark_udf( spark, model_uri )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream data from feature table

# COMMAND ----------

feature_table_name = "airbnb.features"

features_df = spark.read.format("delta").table(feature_table_name).drop("price")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate streaming inferences

# COMMAND ----------

import pyspark.sql.functions as fn

predictions_df = ( features_df.withColumn("Prediction", model_udf())
                              .withColumn("PredictedAt", fn.current_timestamp())
                              .withColumn("ModelVersion", fn.lit(model_version)) )

# COMMAND ----------

#display(predictions_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write out predictions

# COMMAND ----------

table_name = "airbnb.predictions"
base_path = "/tmp/mlops_webinar"
dest_path = f"{base_path}/delta/predictions"
chkpt_path = f"{base_path}/checkpoints/predictions"

( predictions_df.drop("content")
                .write
                .format("delta")
                .outputMode("append")
                .save(dest_path) )


spark.sql(f"""
create table if not exists {table_name} 
using delta
location '{dest_path}'
""")

# COMMAND ----------


