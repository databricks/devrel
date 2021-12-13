# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

demo = "geospatial"
dbutils.widgets.text("demo", demo, "demo")

# COMMAND ----------

from pyspark.sql.functions import rand, input_file_name, from_json, col
from pyspark.sql.types import *

# COMMAND ----------

# import mlflow
# import mlflow.spark
# from mlflow.utils.file_utils import TempDir

from time import sleep
import re
import seaborn as sn
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

def clean_string(a: str) -> str:
  return re.sub('[^A-Za-z0-9]+', '', a).lower()

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
username = clean_string(user.partition('@')[0])
print("Created variables:")
print("user: {}".format(user))
print("username: {}".format(username))
dbName = re.sub(r'\W+', '_', username) + "_" + demo
path = f"/Users/{user}/{demo}"
dbutils.widgets.text("path", path, "path")
dbutils.widgets.text("dbName", dbName, "dbName")
print(f"path (default path): {path}")
spark.sql("""create database if not exists {} LOCATION '{}/{}/tables' """.format(dbName, demo, path))
spark.sql("""USE {}""".format(dbName))
print("dbName (using database): {}".format(dbName))

# COMMAND ----------


