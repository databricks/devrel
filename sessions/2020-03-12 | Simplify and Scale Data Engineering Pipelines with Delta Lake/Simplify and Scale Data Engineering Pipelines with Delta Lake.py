# Databricks notebook source
# MAGIC %md # Simplify and Scale Data Engineering Pipelines with Delta Lake
# MAGIC 
# MAGIC Delta Lake: An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
# MAGIC 
# MAGIC 
# MAGIC * **Open format**: Stored as Parquet format in blob storage.
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 
# MAGIC 
# MAGIC ### Source:
# MAGIC This notebook is a modified version of the [SAIS EU 2019 Delta Lake Tutorial](https://github.com/delta-io/delta/tree/master/examples/tutorials/saiseu19).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps to run this notebook
# MAGIC 
# MAGIC You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well.
# MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 6.1 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. <br/>&nbsp;
# MAGIC 
# MAGIC ### Source Data for this notebook
# MAGIC 
# MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

# COMMAND ----------

# MAGIC %md ## Explore data as a Parquet table
# MAGIC * Initially, let's start by exploring the data as a Parquet table.  
# MAGIC * As we progress, we will showcase how Delta Lake improves up on Parquet.

# COMMAND ----------

# MAGIC %md #### Download the sampled Lending Club data

# COMMAND ----------

# MAGIC %sh rm -rf /dbfs/tmp/sais_eu_19_demo/ && mkdir -p /dbfs/tmp/sais_eu_19_demo/loans/ && wget -O /dbfs/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet  https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet && ls -al  /dbfs/tmp/sais_eu_19_demo/loans/ 

# COMMAND ----------

# MAGIC %md #### Create the parquet table "loans_parquet"

# COMMAND ----------

from pyspark.sql.functions import * 

parquet_path = "/tmp/sais_eu_19_demo/loans/"

# Create a view on the table called loans_parquet
spark.read.format("parquet").load(parquet_path).createOrReplaceTempView("loans_parquet")
print("Defined view 'loans_parquet'")

# COMMAND ----------

# MAGIC %md #### Let's explore this parquet table.
# MAGIC 
# MAGIC *Schema of the table*
# MAGIC 
# MAGIC | Column Name | Description |
# MAGIC | ----------- | ----------- | 
# MAGIC | load_id | unique id for each loan |
# MAGIC | funded_amnt | principal amount of the loan funded to the loanee |
# MAGIC | paid_amnt | amount from the principle that has been paid back (ignoring interests) |
# MAGIC | addr_state | state where this loan was funded |

# COMMAND ----------

spark.sql("select * from loans_parquet").show(20)

# COMMAND ----------

# MAGIC %md #### How many records does it have?

# COMMAND ----------

spark.sql("select count(*) from loans_parquet").show()

# COMMAND ----------

dbutils.notebook.exit("stop") # Stop the notebook before the streaming cell, in case of a "run all" 

# COMMAND ----------

# MAGIC %md #### Let's start appending some new data to it using Structured Streaming
# MAGIC 
# MAGIC We will generate a stream of data from with randomly generated loan ids and amounts. 
# MAGIC In addition, we are going to define a few more useful utility functions.

# COMMAND ----------

import random
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *


def random_checkpoint_dir(): 
  return "/tmp/sais_eu_19_demo/chkpt/%s" % str(random.randint(0, 10000))

# User-defined function to generate random state

states = ["CA", "TX", "NY", "WA"]

@udf(returnType=StringType())
def random_state():
  return str(random.choice(states))

# Function to start a streaming query with a stream of randomly generated data and append to the parquet table
def generate_and_append_data_stream(table_format, table_path):

  stream_data = (spark.readStream.format("rate").option("rowsPerSecond", 5).load() 
    .withColumn("loan_id", 10000 + col("value")) 
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer")) 
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000)) 
    .withColumn("addr_state", random_state()))

  query = (stream_data.writeStream 
    .format(table_format) 
    .option("checkpointLocation", random_checkpoint_dir()) 
    .trigger(processingTime = "10 seconds") 
    .start(table_path))

  return query

# Function to stop all streaming queries 
def stop_all_streams():
  # Stop all the streams
  print("Stopping all streams")
  for s in spark.streams.active:
    s.stop()
  print("Stopped all streams")
  print("Deleting checkpoints")  
  dbutils.fs.rm("/tmp/sais_eu_19_demo/chkpt/", True)
  print("Deleted checkpoints")

# COMMAND ----------

# MAGIC %md #### Let's start a new stream to append data to the Parquet table

# COMMAND ----------

stream_query = generate_and_append_data_stream(
    table_format = "parquet", 
    table_path = parquet_path)

# COMMAND ----------

# MAGIC %md #### Let's see if the data is being added to the table or not

# COMMAND ----------

spark.read.format("parquet").load(parquet_path).count()

# COMMAND ----------

# MAGIC %md #### What happens if we try to add a second writeStream?

# COMMAND ----------

stream_query2 = generate_and_append_data_stream(
    table_format = "parquet", 
    table_path = parquet_path)

# COMMAND ----------

# MAGIC %md #### Where did our existing 14705 rows go? Let's see the data once again

# COMMAND ----------

spark.read.format("parquet").load(parquet_path).show() # wrong schema!

# COMMAND ----------

# MAGIC %md #### Where did the two new columns `timestamp` and `value` come from? What happened here!
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

# MAGIC %md ### Problems with Parquet format
# MAGIC 
# MAGIC Parquet is only a data layout format within a single file, does not provide any guarantees across an entire table of many parquet files.
# MAGIC 
# MAGIC #### 1. No schema enforcement 
# MAGIC Schema is not enforced when writing leading to dirty and often corrupted data.
# MAGIC 
# MAGIC #### 2. No interoperatbility between batch and streaming workloads
# MAGIC Apache Spark's Parquet streaming sink does not maintain enough metadata such that batch workload can seamlessly interact with batch workloads.

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Batch + stream processing and schema enforcement with Delta Lake
# MAGIC Let's understand Delta Lake solves these particular problems (among many others). We will start by creating a Delta table from the original data.

# COMMAND ----------

# MAGIC %sh rm -rf /dbfs/tmp/sais_eu_19_demo/ && mkdir -p /dbfs/tmp/sais_eu_19_demo/loans/ && wget -O /dbfs/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet  https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet && ls -al  /dbfs/tmp/sais_eu_19_demo/loans/ && ls -al  /dbfs/tmp/sais_eu_19_demo/

# COMMAND ----------

spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

# Configure Delta Lake Silver Path
delta_path = "/tmp/sais_eu_19_demo/loans_delta"

# Configurations necessary for running of Databricks Community Edition
spark.sql("set spark.sql.shuffle.partitions = 1")
spark.sql("set spark.databricks.delta.snapshotPartitions = 1")

# Remove folder if it exists
print("Deleting directory " + delta_path)
dbutils.fs.rm(delta_path, recurse=True)

# Create the Delta table with the same loans data
spark.read.format("parquet").load(parquet_path) \
  .write.format("delta").save(delta_path)
print("Created a Delta table at " + delta_path)

spark.read.format("delta").load(delta_path).createOrReplaceTempView("loans_delta")
print("Defined view 'loans_delta'")


# COMMAND ----------

# MAGIC %md #### Let's see the data once again

# COMMAND ----------

spark.sql("select count(*) from loans_delta").show()

# COMMAND ----------

spark.sql("select * from loans_delta").show()

# COMMAND ----------

# MAGIC %md #### Let's run a streaming count(*) on the table so that the count updates automatically

# COMMAND ----------

spark.readStream.format("delta").load(delta_path).createOrReplaceTempView("loans_delta_stream")
display(spark.sql("select count(*) from loans_delta_stream"))

# COMMAND ----------

# MAGIC %md #### Now let's try writing the streaming appends once again

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
    
  stream_data = (spark.readStream.format("rate").option("rowsPerSecond", 50).load() 
    .withColumn("loan_id", 10000 + col("value")) 
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer")) 
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000)) 
    .withColumn("addr_state", random_state()) 
    .select("loan_id", "funded_amnt", "paid_amnt", "addr_state")   # *********** FIXED THE SCHEMA OF THE GENERATED DATA *************
    )

  query = (stream_data.writeStream 
    .format(table_format) 
    .option("checkpointLocation", random_checkpoint_dir()) 
    .trigger(processingTime="10 seconds") 
    .start(table_path))

  return query

# COMMAND ----------

# MAGIC %md #### Now we can successfully write to the table. Note the count in the above streaming query increasing as we write to this table.

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

spark.sql("select count(*) from loans_delta").show()

# COMMAND ----------

# MAGIC %md #### Again, remember to stop all the streaming queries.

# COMMAND ----------

stop_all_streams()

# COMMAND ----------

# MAGIC %md #### Let's take a look at the file system

# COMMAND ----------

# MAGIC %fs ls /tmp/sais_eu_19_demo/loans_delta

# COMMAND ----------

# MAGIC %fs ls /tmp/sais_eu_19_demo/loans_delta/_delta_log/

# COMMAND ----------

# MAGIC %sh
# MAGIC head /dbfs/tmp/sais_eu_19_demo/loans_delta/_delta_log/00000000000000000026.json

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`/tmp/sais_eu_19_demo/loans_delta`

# COMMAND ----------

# MAGIC %python
# MAGIC (spark.read.format("delta") \
# MAGIC   .option("versionAsOf", 0) \
# MAGIC   .load(delta_path)
# MAGIC   .count())

# COMMAND ----------



# COMMAND ----------

# MAGIC %python
# MAGIC deltaTable = DeltaTable.forPath(spark, delta_path)
# MAGIC deltaTable.delete("funded_amnt = paid_amnt")

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs/tmp/sais_eu_19_demo/loans_delta/*.parquet | wc -l

# COMMAND ----------

# MAGIC %md **Let's check the number of fully paid loans once again.**

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %md **Note**: Because we were able to easily `DELETE` the data, the above value should be `null`.

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Audit Delta Lake Table History
# MAGIC All changes to the Delta table are recorded as commits in the table's transaction log. As you write into a Delta table or directory, every operation is automatically versioned. You can use the HISTORY command to view the table's history. For more information, check out the [docs](https://docs.delta.io/latest/delta-utility.html#history).

# COMMAND ----------

# MAGIC %scala
# MAGIC val deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC deltaTable.history().show()

# COMMAND ----------

# MAGIC %python
# MAGIC deltaTable = DeltaTable.forPath(spark, delta_path)
# MAGIC deltaTable.history().show()

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Travel back in time
# MAGIC Delta Lake’s time travel feature allows you to access previous versions of the table. Here are some possible uses of this feature:
# MAGIC 
# MAGIC * Auditing Data Changes
# MAGIC * Reproducing experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC You can query by using either a timestamp or a version number using Python, Scala, and/or SQL syntax. For this examples we will query a specific version using the Python syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html) and the [docs](https://docs.delta.io/latest/delta-batch.html#deltatimetravel).
# MAGIC 
# MAGIC **Let's query the table's state before we deleted the data, which still contains the fully paid loans.**

# COMMAND ----------

# MAGIC %scala
# MAGIC val previousVersion = deltaTable.history(1).select("version").first().getLong(0) - 1
# MAGIC 
# MAGIC spark.read.format("delta")
# MAGIC   .option("versionAsOf", previousVersion)
# MAGIC   .load(deltaPath)
# MAGIC   .createOrReplaceTempView("loans_delta_pre_delete")
# MAGIC 
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %python
# MAGIC previousVersion = deltaTable.history(1).select("version").first()[0] - 1
# MAGIC 
# MAGIC spark.read.format("delta") \
# MAGIC   .option("versionAsOf", previousVersion) \
# MAGIC   .load(delta_path) \
# MAGIC   .createOrReplaceTempView("loans_delta_pre_delete") \
# MAGIC 
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %md **We see the same number of fully paid loans that we had seen before delete.**

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Rollback
# MAGIC With Delta Lake’s time travel feature allows you to rollback to a previous versions of the table.
# MAGIC 
# MAGIC You can query by using either a timestamp or a version number using Python, Scala, and/or SQL syntax. For this examples we will query a specific version using the Python syntax.  For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html) and the [docs](https://docs.delta.io/latest/delta-batch.html#deltatimetravel).
# MAGIC 
# MAGIC **Let's query the table and get the fully paid loans back**

# COMMAND ----------

# Count before rollback
spark.sql("SELECT COUNT(1) FROM loans_delta").show()

# COMMAND ----------

# Count of previous Version
spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC # Rollback
# MAGIC spark.read.format("delta")
# MAGIC   .option("versionAsOf", previousVersion) 
# MAGIC   .load(delta_path) 
# MAGIC   .write.format("delta") 
# MAGIC   .mode("overwrite") 
# MAGIC   .save(delta_path)

# COMMAND ----------

# MAGIC %python
# MAGIC # Rollback
# MAGIC spark.read.format("delta") \
# MAGIC   .option("versionAsOf", previousVersion) \
# MAGIC   .load(delta_path) \
# MAGIC   .write.format("delta") \
# MAGIC   .mode("overwrite") \
# MAGIC   .save(delta_path)

# COMMAND ----------

# MAGIC %python
# MAGIC (spark.read.format("delta") \
# MAGIC   .option("versionAsOf", 19) \
# MAGIC   .load(delta_path)
# MAGIC   .count())

# COMMAND ----------

# Count after rollback
spark.sql("SELECT COUNT(1) FROM loans_delta").show()

# COMMAND ----------

# Deleted data is back
spark.sql("SELECT COUNT(1) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

deltaTable.history().show()

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Vacuum old versions of Delta Lake tables
# MAGIC 
# MAGIC While it's nice to be able to time travel to any previous version, sometimes you want actually delete the data from storage completely for reducing storage costs or for compliance reasons (example, GDPR).
# MAGIC The Vacuum operation deletes data files that have been removed from the table for a certain amount of time. For more information, check out the [docs](https://docs.delta.io/latest/delta-utility.html#vacuum).

# COMMAND ----------

# MAGIC %md By default, `vacuum()` retains all the data needed for the last 7 days. For this example, since this table does not have 7 days worth of history, we will retain 0 hours, which means to only keep the latest state of the table.

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs/tmp/sais_eu_19_demo/loans_delta/*.parquet | wc -l

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
deltaTable.vacuum(retentionHours = 0)

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs/tmp/sais_eu_19_demo/loans_delta/*.parquet | wc -l

# COMMAND ----------

# MAGIC %md **Same query as before, but it now fails**

# COMMAND ----------

spark.read.format("delta").option("versionAsOf", previousVersion).load(delta_path).createOrReplaceTempView("loans_delta_pre_delete")
spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Upsert into Delta Lake table using Merge
# MAGIC You can upsert data from an Apache Spark DataFrame into a Delta Lake table using the merge operation. This operation is similar to the SQL MERGE command but has additional support for deletes and extra conditions in updates, inserts, and deletes. For more information checkout the [docs](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge).

# COMMAND ----------

# MAGIC %md #### Upsert with Parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC 
# MAGIC #### Upsert using with Delta Lake
# MAGIC 
# MAGIC 1-step process: 
# MAGIC 1. [Use `Merge` operation](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)

# COMMAND ----------

# MAGIC %fs ls /tmp/sais_eu_19_demo/

# COMMAND ----------

spark.read.format("parquet").load(parquet_path).where("loan_id < 3").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("set spark.sql.shuffle.partitions = 1")
# MAGIC spark.sql("set spark.databricks.delta.snapshotPartitions = 1")
# MAGIC 
# MAGIC // Configure Delta Lake Silver Path
# MAGIC val deltaSmallPath = "/sais_eu_19_demo/loans_delta_small"
# MAGIC 
# MAGIC // Remove folder if it exists
# MAGIC println("Deleting directory " + deltaSmallPath)
# MAGIC dbutils.fs.rm(deltaSmallPath, recurse=true)
# MAGIC 
# MAGIC // Create the Delta table with the same loans data
# MAGIC spark.read.format("parquet").load(parquetPath)
# MAGIC   .where("loan_id < 3")
# MAGIC   .write.format("delta").save(deltaSmallPath)
# MAGIC println("Created a Delta table at " + deltaSmallPath)
# MAGIC 
# MAGIC spark.read.format("delta").load(deltaSmallPath).createOrReplaceTempView("loans_delta_small")
# MAGIC println("Defined view 'loans_delta_small'")

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("set spark.sql.shuffle.partitions = 1")
# MAGIC spark.sql("set spark.databricks.delta.snapshotPartitions = 1")
# MAGIC 
# MAGIC # Configure Delta Lake Silver Path
# MAGIC delta_small_path = "/tmp/sais_eu_19_demo/loans_delta_small"
# MAGIC 
# MAGIC # Remove folder if it exists
# MAGIC print("Deleting directory " + delta_small_path)
# MAGIC dbutils.fs.rm(delta_small_path, recurse=True)
# MAGIC 
# MAGIC # Create the Delta table with the same loans data
# MAGIC spark.read.format("parquet").load(parquet_path) \
# MAGIC   .where("loan_id < 3") \
# MAGIC   .write.format("delta").save(delta_small_path)
# MAGIC print("Created a Delta table at " + delta_small_path)
# MAGIC 
# MAGIC spark.read.format("delta").load(delta_small_path).createOrReplaceTempView("loans_delta_small")
# MAGIC print("Defined view 'loans_delta_small'")

# COMMAND ----------

# MAGIC %md #### Let's focus only on a part of the loans_delta table

# COMMAND ----------

spark.sql("select * from loans_delta_small order by loan_id").show()

# COMMAND ----------

# MAGIC %md **Now, let's say we got some new loan information**
# MAGIC 1. Duplicate loan_id = 1 was added to the change table due to a delay in processing
# MAGIC 1. Existing loan_id = 2 has been fully repaid. The corresponding row needs to be updated.
# MAGIC 1. New loan_id = 3 has been funded in CA. This is need to be inserted as a new row.

# COMMAND ----------

# MAGIC %scala
# MAGIC val loanUpdates = Seq(
# MAGIC   (1, 1000, 361.19, "WA"), // duplicate information    
# MAGIC   (2, 1000, 1000.0, "TX"), // existing loan's paid_amnt updated, loan paid in full
# MAGIC   (3, 2000, 0.0, "CA"))    // new loan details
# MAGIC   .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")
# MAGIC 
# MAGIC loanUpdates.show()

# COMMAND ----------

# MAGIC %python
# MAGIC cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state']
# MAGIC items = [
# MAGIC   (1, 1000, 361.19, 'WA'), # duplicate information  
# MAGIC   (2, 1000, 1000.0, 'TX'), # existing loan's paid_amnt updated, loan paid in full
# MAGIC   (3, 2000, 0.0, 'CA')     # new loan details
# MAGIC ]
# MAGIC 
# MAGIC loan_updates = spark.createDataFrame(items, cols)
# MAGIC 
# MAGIC loan_updates.show()

# COMMAND ----------

# MAGIC %md **Merge can upsert this in a single atomic operation.**
# MAGIC 
# MAGIC SQL `MERGE` command can do both `UPDATE` and `INSERT`.
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC MERGE INTO target t
# MAGIC USING source s
# MAGIC WHEN MATCHED THEN UPDATE SET ...
# MAGIC WHEN NOT MATCHED THEN INSERT ....
# MAGIC ```
# MAGIC 
# MAGIC Since Apache Spark's SQL parser does not have support for parsing MERGE SQL command, we have provided programmatic APIs in Python to perform the same operation with the same semantics as the SQL command.

# COMMAND ----------

# MAGIC %scala
# MAGIC import io.delta.tables.DeltaTable
# MAGIC val deltaTable = DeltaTable.forPath(spark, deltaSmallPath)
# MAGIC 
# MAGIC deltaTable.alias("t").merge(
# MAGIC   loanUpdates.alias("s"), 
# MAGIC   "t.loan_id = s.loan_id")
# MAGIC   .whenMatched.updateAll()
# MAGIC   .whenNotMatched.insertAll()
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %python
# MAGIC from delta.tables import *
# MAGIC 
# MAGIC delta_table = DeltaTable.forPath(spark, delta_small_path)
# MAGIC 
# MAGIC (delta_table.alias("t").merge(
# MAGIC     loan_updates.alias("s"), 
# MAGIC     "t.loan_id = s.loan_id") 
# MAGIC   .whenMatchedUpdateAll() 
# MAGIC   .whenNotMatchedInsertAll() 
# MAGIC   .execute())

# COMMAND ----------

spark.sql("select * from loans_delta_small order by loan_id").show()

# COMMAND ----------

# MAGIC %md **Note the changes in the table**
# MAGIC - Existing loan_id = 2 should have been updated with paid_amnt set to 1000. 
# MAGIC - New loan_id = 3 have been inserted.

# COMMAND ----------

# MAGIC %md <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
# MAGIC <br/>
# MAGIC ## Tutorial Summary
# MAGIC 
# MAGIC #### Full support for batch and streaming workloads
# MAGIC * Delta Lake allows batch and streaming workloads to concurrently read and write to Delta Lake tables with full ACID transactional guarantees.
# MAGIC 
# MAGIC #### Schema enforcement and schema evolution
# MAGIC * Delta Lake provides the ability to specify your schema and enforce it. This helps ensure that the data types are correct and required columns are present, preventing bad data from causing data corruption.
# MAGIC 
# MAGIC #### Table History and Time Travel
# MAGIC * Delta Lake transaction log records details about every change made to data providing a full audit trail of the changes. 
# MAGIC * You can query previous snapshots of data enabling developers to access and revert to earlier versions of data for audits, rollbacks or to reproduce experiments.
# MAGIC 
# MAGIC #### Delete data and Vacuum old versions
# MAGIC * Delete data from tables using a predicate.
# MAGIC * Fully remove data from previous versions using Vaccum to save storage and satisfy compliance requirements.
# MAGIC 
# MAGIC #### Upsert data using Merge
# MAGIC * Upsert data into tables from batch and streaming workloads
# MAGIC * Use extended merge syntax for advanced usecases like data deduplication, change data capture, SCD type 2 operations, etc.

# COMMAND ----------

# MAGIC %md ## Join the community!
# MAGIC 
# MAGIC 
# MAGIC * [Delta Lake on GitHub](https://github.com/delta-io/delta)
# MAGIC * [Delta Lake Slack Channel](https://delta-users.slack.com/) ([Registration Link](https://join.slack.com/t/delta-users/shared_invite/enQtNTY1NDg0ODcxOTI1LWJkZGU3ZmQ3MjkzNmY2ZDM0NjNlYjE4MWIzYjg2OWM1OTBmMWIxZTllMjg3ZmJkNjIwZmE1ZTZkMmQ0OTk5ZjA))
# MAGIC * [Public Mailing List](https://groups.google.com/forum/#!forum/delta-users)
