# Databricks notebook source
# MAGIC %md # Diving into Delta Lake: Unpacking the Transaction Log
# MAGIC 
# MAGIC This notebook refers to the following [blog](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html) and [tech talk](https://databricks.zoom.us/webinar/register/WN_E6u5D7NQSpCdWsEk-u8IGA); it is a modified version of the [Delta Lake Tutorial: Spark + AI Summit 2019 EU](https://github.com/delta-io/delta/tree/master/examples/tutorials/saiseu19).
# MAGIC <br/>&nbsp;
# MAGIC 
# MAGIC ### Steps to run this notebook
# MAGIC 
# MAGIC You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well.
# MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 6.1 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. <br/>&nbsp;
# MAGIC 
# MAGIC ### Source Data for this notebook
# MAGIC 
# MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

# COMMAND ----------

# MAGIC %md <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
# MAGIC 
# MAGIC An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC * **Open format**: Stored as Parquet format in blob storage.
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 

# COMMAND ----------

# MAGIC %md ## Explore data as a Parquet table

# COMMAND ----------

# MAGIC %md #####Download the sampled Lending Club data

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/sais_eu_19_demo/loans/ && wget -O /dbfs/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet  https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet && ls -al  /dbfs/tmp/sais_eu_19_demo/loans/ 

# COMMAND ----------

# MAGIC %md **Setup and configuration**

# COMMAND ----------

import os, shutil

# Configurations necessary for running of Databricks Community Edition
spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

demo_path = "/sais_eu_19_demo/"

if os.path.exists("/dbfs" + demo_path):
  print("Deleting path " + demo_path)
  shutil.rmtree("/dbfs" + demo_path)
  print("Deleted " + demo_path)

# COMMAND ----------

# MAGIC %md #####Create the parquet table "loans_parquet"

# COMMAND ----------

import os
import shutil
from pyspark.sql.functions import * 

parquet_path = "/sais_eu_19_demo/loans_parquet"

# Delete a new parquet table with the parquet file
if os.path.exists("/dbfs" + parquet_path):
  print("Deleting path " + parquet_path)
  shutil.rmtree("/dbfs" + parquet_path)
  
# Create a new parquet table with the parquet file
spark.read.format("parquet").load("/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet") \
  .write.format("parquet").save(parquet_path)
print("Created a Parquet table at " + parquet_path)

# Create a view on the table called loans_parquet
spark.read.format("parquet").load(parquet_path).createOrReplaceTempView("loans_parquet")
print("Defined view 'loans_parquet'")


# COMMAND ----------

# MAGIC %md #####Let's explore this parquet table.
# MAGIC 
# MAGIC *Schema of the table*
# MAGIC - load_id - unique id for each loan
# MAGIC - funded_amnt - principal amount of the loan funded to the loanee
# MAGIC - paid_amnt - amount from the principle that has been paid back (ignoring interests)
# MAGIC - addr_state - state where this loan was funded

# COMMAND ----------

spark.sql("select * from loans_parquet").show(20)

# COMMAND ----------

# MAGIC %md **How many records does it have?**

# COMMAND ----------

spark.sql("select count(*) from loans_parquet").show()

# COMMAND ----------

dbutils.notebook.exit("stop") # Stop the notebook before the streaming cell, in case of a "run all" 

# COMMAND ----------

# MAGIC %md **Let's start appending some new data to it using Structured Streaming.**
# MAGIC 
# MAGIC We will generate a stream of data from with randomly generated loan ids and amounts. 
# MAGIC In addition, we are going to define a few more useful utility functions.

# COMMAND ----------

import random
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *


def random_checkpoint_dir(): 
  return "/sais_eu_19_demo/chkpt/%s" % str(random.randint(0, 10000))

# User-defined function to generate random state

states = ["CA", "TX", "NY", "WA"]

@udf(returnType=StringType())
def random_state():
  return str(random.choice(states))

# Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
def generate_and_append_data_stream(table_format, table_path):
  
  stream_data = spark.readStream.format("rate").option("rowsPerSecond", 5).load() \
    .withColumn("loan_id", 10000 + col("value")) \
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer")) \
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000)) \
    .withColumn("addr_state", random_state()) \

  query = stream_data.writeStream \
    .format(table_format) \
    .option("checkpointLocation", random_checkpoint_dir()) \
    .trigger(processingTime = "10 seconds") \
    .start(table_path)

  return query

# Function to stop all streaming queries 
def stop_all_streams():
  # Stop all the streams
  print("Stopping all streams")
  for s in spark.streams.active:
    s.stop()
  print("Stopped all streams")
  print("Deleting checkpoints")  
  dbutils.fs.rm("/sais_eu_19_demo/chkpt/", True)
  print("Deleted checkpoints")

# COMMAND ----------

# MAGIC %md **Let's start a new stream to append data to the Parquet table**

# COMMAND ----------

stream_query = generate_and_append_data_stream(
    table_format = "parquet", 
    table_path = parquet_path)

# COMMAND ----------

# MAGIC %md **Let's see if the data is being added to the table or not**.

# COMMAND ----------

spark.read.format("parquet").load(parquet_path).count()

# COMMAND ----------

# MAGIC %md ** What happens when we try to add a second stream?**

# COMMAND ----------

stream_query2 = generate_and_append_data_stream(
    table_format = "parquet", 
    table_path = parquet_path)

# COMMAND ----------

# MAGIC %md **Where did our existing 14705 rows go? Let's see the data once again**

# COMMAND ----------

spark.read.format("parquet").load(parquet_path).show() # wrong schema!

# COMMAND ----------

# MAGIC %md **Where did the two new columns `timestamp` and `value` come from? What happened here!**
# MAGIC 
# MAGIC What really happened is that when the streaming query started adding new data to the Parquet table, it did not properly account for the existing data in the table. Furthermore, the new data files that written out accidentally had two extra columns in the schema. Hence, when reading the table, the 2 different schema from different files were merged together, thus unexpectedly modifying the schema of the table.
# MAGIC 
# MAGIC 
# MAGIC Before we move on, **if you are running on Databricks Community Edition, definitely stop the streaming queries.** 
# MAGIC 
# MAGIC You free account in Databricks Community Edition has quota limits on the number of files and we do not want to hit that quote limit by running the streaming queries for too long.

# COMMAND ----------

stop_all_streams()

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Batch + stream processing and schema enforcement with Delta Lake
# MAGIC Let's understand Delta Lake solves these particular problems (among many others). We will start by creating a Delta table from the original data.

# COMMAND ----------

# Configure Delta Lake Silver Path
delta_path = "/sais_eu_19_demo/loans_delta"

# Configurations necessary for running of Databricks Community Edition
spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

# Remove folder if it exists
print("Deleting directory " + delta_path)
dbutils.fs.rm(delta_path, recurse=True)

# Create the Delta table with the same loans data
spark.read.format("parquet").load("/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet") \
  .write.format("delta").save(delta_path)
print("Created a Delta table at " + delta_path)

spark.read.format("delta").load(delta_path).createOrReplaceTempView("loans_delta")
print("Defined view 'loans_delta'")

# COMMAND ----------

# MAGIC %md **Let's see the data once again.**

# COMMAND ----------

spark.sql("select count(*) from loans_delta").show()

# COMMAND ----------

spark.sql("select * from loans_delta").show()

# COMMAND ----------

# MAGIC %md ###  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Enforcement

# COMMAND ----------

# MAGIC %md **Let's run a streaming count(*) on the table so that the count updates automatically**

# COMMAND ----------

spark.readStream.format("delta").load(delta_path).createOrReplaceTempView("loans_delta_stream")
display(spark.sql("select count(*) from loans_delta_stream"))

# COMMAND ----------

# MAGIC %md **Now let's try writing the streaming appends once again**

# COMMAND ----------

stream_query_2 = generate_and_append_data_stream(table_format = "delta", table_path = delta_path)

# COMMAND ----------

# MAGIC %md The writes were blocked because the schema of the new data did not match the schema of table (see the exception details). See more information about how it works [here](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html).
# MAGIC 
# MAGIC **Now, let's fix the streaming query by selecting the columns we want to write.**

# COMMAND ----------

from pyspark.sql.functions import *

# Generate a stream of randomly generated load data and append to the parquet table
def generate_and_append_data_stream_fixed(table_format, table_path):
    
  stream_data = spark.readStream.format("rate").option("rowsPerSecond", 50).load() \
    .withColumn("loan_id", 10000 + col("value")) \
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer")) \
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000)) \
    .withColumn("addr_state", random_state()) \
    .select("loan_id", "funded_amnt", "paid_amnt", "addr_state")   # *********** FIXED THE SCHEMA OF THE GENERATED DATA *************

  query = stream_data.writeStream \
    .format(table_format) \
    .option("checkpointLocation", random_checkpoint_dir()) \
    .trigger(processingTime="10 seconds") \
    .start(table_path)

  return query


# COMMAND ----------

# MAGIC %md **Now we can successfully write to the table. Note the count in the above streaming query increasing as we write to this table.**

# COMMAND ----------

stream_query_2 = generate_and_append_data_stream_fixed(table_format = "delta", table_path = delta_path)

# COMMAND ----------

# MAGIC %md **Scroll back up to see the numbers change in the `readStream` as more data is being appended by the `writeStream`.** 
# MAGIC 
# MAGIC **In fact, we can run multiple concurrent streams writing to that table, it will work together.**

# COMMAND ----------

stream_query_3 = generate_and_append_data_stream_fixed(table_format = "delta", table_path = delta_path)

# COMMAND ----------

# MAGIC %md Just for sanity check, let's query as a batch
# MAGIC 
# MAGIC Note, you can run a read stream, two write streams, and read in batch - concurrently!

# COMMAND ----------

display(spark.sql("select count(*) from loans_delta"))

# COMMAND ----------

# MAGIC %md **Again, remember to stop all the streaming queries.**

# COMMAND ----------

stop_all_streams()

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Audit Delta Lake Table History
# MAGIC All changes to the Delta table are recorded as commits in the table's transaction log. As you write into a Delta table or directory, every operation is automatically versioned. You can use the HISTORY command to view the table's history. For more information, check out the [docs](https://docs.delta.io/latest/delta-utility.html#history).

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lt /dbfs/sais_eu_19_demo/loans_delta/_delta_log/

# COMMAND ----------

from delta.tables import *
delta_path = "/sais_eu_19_demo/loans_delta"

deltaTable = DeltaTable.forPath(spark, delta_path)
display(deltaTable.history())

# COMMAND ----------

# MAGIC %sh 
# MAGIC head /dbfs/sais_eu_19_demo/loans_delta/_delta_log/00000000000000000011.json

# COMMAND ----------

# MAGIC %md ** With Time Travel, you can query previous versions **

# COMMAND ----------

currentVersion = deltaTable.history(1).select("version").collect()[0][0]

v01 = spark.read.format("delta").option("versionAsOf", 1).load(delta_path).count()
v11 = spark.read.format("delta").option("versionAsOf", 11).load(delta_path).count()
vno = spark.read.format("delta").option("versionAsOf", currentVersion).load(delta_path).count()
print("loans_delta table counts:\n Initial [%s] \n Version 11 [%s] \n Current Version [%s]" % (v01, v11, vno))

# COMMAND ----------

# MAGIC %md ** But, there are a lot of files because of the data versioning.**

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lt /dbfs/sais_eu_19_demo/loans_delta/

# COMMAND ----------

# MAGIC %md %md ###  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Compact files
# MAGIC 
# MAGIC If you continuously write data to a Delta table, it will over time accumulate a large number of files, especially if you add data in small batches. This can have an adverse effect on the efficiency of table reads, and it can also affect the performance of your file system. Ideally, a large number of small files should be rewritten into a smaller number of larger files on a regular basis. This is known as compaction.
# MAGIC 
# MAGIC You can compact a table by repartitioning it to smaller number of files. In addition, you can specify the option `dataChange` to be false indicates that the operation does not change the data, only rearranges the data layout. This would ensure that other concurrent operations are minimally affected due to this compaction operation.

# COMMAND ----------

numFiles = 4

(spark.read
   .format("delta")
   .load(delta_path)
   .repartition(numFiles)
   .write
   .option("dataChange", "false")
   .format("delta")
   .mode("overwrite")
   .save(delta_path))

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lt /dbfs/sais_eu_19_demo/loans_delta/ | wc -l

# COMMAND ----------

# This is an anti-pattern
spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
deltaTable.vacuum(retentionHours = 0)

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lt /dbfs/sais_eu_19_demo/loans_delta/

# COMMAND ----------

# MAGIC %md #Join the community!
# MAGIC 
# MAGIC 
# MAGIC * [Delta Lake on GitHub](https://github.com/delta-io/delta)
# MAGIC * [Delta Lake Slack Channel](https://delta-users.slack.com/) ([Registration Link](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA))
# MAGIC * [Public Mailing List](https://groups.google.com/forum/#!forum/delta-users)