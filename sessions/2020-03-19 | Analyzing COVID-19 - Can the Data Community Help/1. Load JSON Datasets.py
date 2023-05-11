# Databricks notebook source
# MAGIC %md 
# MAGIC ## 1. Load JSON Dataset
# MAGIC ### COVID-19 Open Research Dataset Challenge (CORD-19) Working Notebooks
# MAGIC 
# MAGIC This is a working notebook for the [COVID-19 Open Research Dataset Challenge (CORD-19)](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge) to help you jump start your analysis of the CORD-19 dataset.  
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/3648/1*596Ur1UdO-fzQsaiGPrNQg.png" width="900"/>
# MAGIC 
# MAGIC Attributions:
# MAGIC * The licenses for each dataset used for this workbook can be found in the *all _ sources _ metadata csv file* which is included in the [downloaded dataset](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge/download).  
# MAGIC * For the 2020-03-03 dataset: 
# MAGIC   * `comm_use_subset`: Commercial use subset (includes PMC content) -- 9000 papers, 186Mb
# MAGIC   * `noncomm_use_subset`: Non-commercial use subset (includes PMC content) -- 1973 papers, 36Mb
# MAGIC   * `biorxiov_medrxiv`: bioRxiv/medRxiv subset (pre-prints that are not peer reviewed) -- 803 papers, 13Mb
# MAGIC * When using Databricks or Databricks Community Edition, a copy of this dataset has been made available at `/databricks-datasets/COVID/CORD-19`
# MAGIC * This notebook is freely available to share, licensed under [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/us/)

# COMMAND ----------

# MAGIC %md #### Configure Path Variables
# MAGIC You can find the CORD-19 (2020-03-13) dataset available at: `/databricks-datasets/COVID/CORD-19/2020-03-13/`

# COMMAND ----------

# Configure Paths in Python
comm_use_subset_path = "/databricks-datasets/COVID/CORD-19/2020-03-13/comm_use_subset/comm_use_subset/"
noncomm_use_subset_path = "/databricks-datasets/COVID/CORD-19/2020-03-13/noncomm_use_subset/noncomm_use_subset/"
biorxiv_medrxiv_path = "/databricks-datasets/COVID/CORD-19/2020-03-13/biorxiv_medrxiv/biorxiv_medrxiv/"
json_schema_path = "/databricks-datasets/COVID/CORD-19/2020-03-13/json_schema.txt"

# COMMAND ----------

# Configure Path as Shell Enviroment Variables
import os
os.environ['comm_use_subset_path']=''.join(comm_use_subset_path)
os.environ['noncomm_use_subset_path']=''.join(noncomm_use_subset_path)
os.environ['biorxiv_medrxiv_path']=''.join(biorxiv_medrxiv_path)
os.environ['json_schema_path']=''.join(json_schema_path)

# COMMAND ----------

# MAGIC %md #### Review JSON Schema
# MAGIC The schema for these datasets is defined by `json_schema.txt` as noted in the following cell.

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat /dbfs$json_schema_path

# COMMAND ----------

# MAGIC %fs ls /tmp/dennylee/

# COMMAND ----------

# MAGIC %md #### Configure Parquet Path Variables
# MAGIC Save the data in Parquet format at: `/tmp/dennylee/COVID/CORD-19/2020-03-13/`

# COMMAND ----------

# Configure Parquet Paths in Python
comm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/comm_use_subset.parquet"
noncomm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/noncomm_use_subset.parquet"
biorxiv_medrxiv_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/biorxiv_medrxiv/biorxiv_medrxiv.parquet"

# Configure Path as Shell Enviroment Variables
os.environ['comm_use_subset_pq_path']=''.join(comm_use_subset_pq_path)
os.environ['noncomm_use_subset_pq_path']=''.join(noncomm_use_subset_pq_path)
os.environ['biorxiv_medrxiv_pq_path']=''.join(biorxiv_medrxiv_pq_path)

# COMMAND ----------

# MAGIC %md #### Read comm_use_subset JSON Files
# MAGIC As these are correctly formed JSON files, you can use `spark.read.json` to read these files.  Note, you will need to specify the *multiline* option.

# COMMAND ----------

# comm_use_subset
comm_use_subset = spark.read.option("multiLine", True).json(comm_use_subset_path)
comm_use_subset.printSchema()

# COMMAND ----------

# Number of records (originally number of JSON documents)
comm_use_subset.count()

# COMMAND ----------

# MAGIC %md ##### Validate number of files

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs$comm_use_subset_path | wc -l

# COMMAND ----------

# MAGIC %md #### Save comm_use_subset JSON Files
# MAGIC It takes awhile to read the files, so let's save them as Parquet format to improve query performance

# COMMAND ----------

# Get number of partitions
comm_use_subset.rdd.getNumPartitions()

# COMMAND ----------

# Write out in Pqarquet format in four partitions 
# Note, this cluster has four nodes 
comm_use_subset.repartition(4).write.format("parquet").mode("overwrite").save(comm_use_subset_pq_path)

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lsgA /dbfs/tmp/dennylee/COVID/CORD-19/2020-03-13/comm_use_subset.parquet

# COMMAND ----------

# Reread files
comm_use_subset = spark.read.format("parquet").load(comm_use_subset_pq_path)

# COMMAND ----------

# Number of records (originally number of JSON documents)
comm_use_subset.count()

# COMMAND ----------

# MAGIC %md #### Read noncomm_use_subset JSON Files
# MAGIC As these are correctly formed JSON files, you can use `spark.read.json` to read these files.  Note, you will need to specify the *multiline* option.

# COMMAND ----------

# noncomm_use_subset
noncomm_use_subset = spark.read.option("multiLine", True).json(noncomm_use_subset_path)
noncomm_use_subset.printSchema()

# COMMAND ----------

# Number of records (originally number of JSON documents)
noncomm_use_subset.count()

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs$noncomm_use_subset_path | wc -l

# COMMAND ----------

# MAGIC %md #### Save noncomm_use_subset JSON Files
# MAGIC It takes awhile to read the files, so let's save them as Parquet format to improve query performance

# COMMAND ----------

# Get number of partitions
noncomm_use_subset.rdd.getNumPartitions()

# COMMAND ----------

# Write out in Parquet format in four partitions 
# Note, this cluster has four nodes 
noncomm_use_subset.repartition(4).write.format("parquet").mode("overwrite").save(noncomm_use_subset_pq_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lsgA /dbfs$noncomm_use_subset_pq_path

# COMMAND ----------

# Reread files
noncomm_use_subset = spark.read.format("parquet").load(noncomm_use_subset_pq_path)

# COMMAND ----------

# Number of records (originally number of JSON documents)
noncomm_use_subset.count()

# COMMAND ----------

# MAGIC %md #### Read biorxiv_medrxiv JSON Files
# MAGIC As these are correctly formed JSON files, you can use `spark.read.json` to read these files.  Note, you will need to specify the *multiline* option.

# COMMAND ----------

# biorxiv_medrxiv
biorxiv_medrxiv = spark.read.option("multiLine", True).json(biorxiv_medrxiv_path)
biorxiv_medrxiv.count()

# COMMAND ----------

# Write out in Parquet format in four partitions 
# Note, this cluster has four nodes 
biorxiv_medrxiv.repartition(4).write.format("parquet").mode("overwrite").save(biorxiv_medrxiv_pq_path)

# COMMAND ----------

# Re-read files
biorxiv_medrxiv = spark.read.format("parquet").load(biorxiv_medrxiv_pq_path)
biorxiv_medrxiv.count()

# COMMAND ----------

# MAGIC %md From this point forward, we can read the Parquet files instead of the original JSON files for faster performance.

# COMMAND ----------

