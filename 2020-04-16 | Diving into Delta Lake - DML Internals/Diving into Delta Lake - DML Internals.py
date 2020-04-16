# Databricks notebook source
# MAGIC %md # Diving into Delta Lake: DML Internals
# MAGIC 
# MAGIC This notebook is a modified version of the [SAIS EU 2019 Delta Lake Tutorial](https://github.com/delta-io/delta/tree/master/examples/tutorials/saiseu19). The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ### Steps to run this notebook
# MAGIC You can run this notebook in a Databricks environment. Specifically, this notebook has been designed to run in [Databricks Community Edition](http://community.cloud.databricks.com/) as well.
# MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 6.5 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. <br/>&nbsp;

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
# MAGIC 
# MAGIC An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC * **Open format**: Stored as Parquet format in blob storage.
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Loading data in Delta Lake table
# MAGIC 
# MAGIC First let’s, read this data and save it as a Delta Lake table.

# COMMAND ----------

# MAGIC %sh rm -rf /dbfs/tmp/sais_eu_19_demo/ && mkdir -p /dbfs/tmp/sais_eu_19_demo/loans/ && wget -O /dbfs/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet  https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet && ls -al  /dbfs/tmp/sais_eu_19_demo/loans/ 

# COMMAND ----------

spark.sql("set spark.sql.shuffle.partitions = 1")

# Configure source data path (TODO: update this path after loading the data into Databricks Datasets)
# sourcePath = "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"
sourcePath = "/tmp/sais_eu_19_demo/loans/SAISEU19-loan-risks.snappy.parquet"


# Configure Delta Lake Path
deltaPath = "/tmp/loans_delta"

# Remove folder if it exists
dbutils.fs.rm(deltaPath, recurse=True)

# Create the Delta table with the same loans data
(spark.read.format("parquet").load(sourcePath) 
  .write.format("delta").save(deltaPath))

spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")
print("Defined view 'loans_delta'")

# COMMAND ----------

# MAGIC %md Let's explore the data.

# COMMAND ----------

spark.sql("SELECT count(*) FROM loans_delta").show()

# COMMAND ----------

spark.sql("SELECT * FROM loans_delta LIMIT 5").show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Review Underlying Files
# MAGIC * Review the underlying `parquet` files
# MAGIC * Initial log

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lt /dbfs/tmp/loans_delta/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lt /dbfs/tmp/loans_delta/_delta_log/

# COMMAND ----------

# MAGIC %md #### Review initial log

# COMMAND ----------

j0 = spark.read.json("/tmp/loans_delta/_delta_log/00000000000000000000.json")

# COMMAND ----------

# Commit Information
display(j0.select("commitInfo").where("commitInfo is not null"))

# COMMAND ----------

# Add Information
display(j0.select("add").where("add is not null"))

# COMMAND ----------

# Metadata Information
display(j0.select("metadata").where("metadata is not null"))

# COMMAND ----------

jsonStr = j0.select("metadata.schemaString").where("metadata is not null").collect()[0][0]
df = spark.read.json(sc.parallelize([jsonStr]))
display(df)

# COMMAND ----------

# MAGIC %md ## Review Loans by State

# COMMAND ----------

# MAGIC %sql
# MAGIC select addr_state, sum(funded_amnt)/1000000 as funded_amnt from loans_delta where funded_amnt <> paid_amnt group by addr_state

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Updating data 
# MAGIC 
# MAGIC You can update the data that matches a predicate from a Delta Lake table. Let's say we want to update all the fully paid loans for `WA` state.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM loans_delta WHERE addr_state = 'WA' and funded_amnt <> paid_amnt

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, deltaPath)
deltaTable.update("addr_state = 'WA'", { "paid_amnt": "funded_amnt" } ) 

# COMMAND ----------



# COMMAND ----------

display(deltaTable.history())

# COMMAND ----------

# MAGIC %sql
# MAGIC select addr_state, sum(funded_amnt)/1000000 as funded_amnt from loans_delta where funded_amnt <> paid_amnt group by addr_state

# COMMAND ----------

# MAGIC %md ##### Review Underlying Files

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lt /dbfs/tmp/loans_delta/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lt /dbfs/tmp/loans_delta/_delta_log/

# COMMAND ----------

# MAGIC %md ##### Review Transaction Log

# COMMAND ----------

j1 = spark.read.json("/tmp/loans_delta/_delta_log/00000000000000000001.json")

# COMMAND ----------

# Commit Information
display(j1.select("commitInfo").where("commitInfo is not null"))

# COMMAND ----------

# Remove Information
display(j1.select("remove").where("remove is not null"))

# COMMAND ----------

# Remove Information
display(j1.select("add").where("add is not null"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Upserting change data to a table using merge
# MAGIC A common use cases is Change Data Capture (CDC), where you have to replicate row changes made in an OLTP table to another table for OLAP workloads. To continue with our loan data example, say we have another table of new loan information, some of which are new loans and others are updates to existing loans. In addition, let’s say this changes table has the same schema as the loan_delta table. You can upsert these changes into the table using the DeltaTable.merge() operation which is based on the MERGE SQL command.

# COMMAND ----------

# MAGIC %md
# MAGIC #### INSERT or UPDATE parquet: 7-step process
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
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loans_delta where addr_state = 'NY' and loan_id < 30

# COMMAND ----------

# MAGIC %md Let's say we have some changes to this data, one loan has been paid off, and another new loan has been added.

# COMMAND ----------

cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state', 'closed']

items = [
  (11, 1000, 1000.0, 'NY', True),   # loan paid off
  (12, 1000, 0.0, 'NY', False),     # new loan
  (28, 1200, 84.45, 'NY', False)    # duplicate loan
]

loanUpdates = spark.createDataFrame(items, cols)

# COMMAND ----------

# MAGIC %md Now, let's update the table with the change data using the `merge` operation.

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, deltaPath)

(deltaTable
  .alias("t")
  .merge(loanUpdates.alias("s"), "t.loan_id = s.loan_id") 
  .whenMatchedUpdateAll() 
  .whenNotMatchedInsertAll() 
  .execute())

# COMMAND ----------

# MAGIC %md Let's see whether the table has been updated.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loans_delta where addr_state = 'NY' and loan_id < 30

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Auditing data changes with operation history
# MAGIC 
# MAGIC All changes to the Delta table are recorded as commits in the table's transaction log. As you write into a Delta table or directory, every operation is automatically versioned. You can use the HISTORY command to view the table's history.

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, deltaPath)
display(deltaTable.history())

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lt /dbfs/tmp/loans_delta/

# COMMAND ----------

j2 = spark.read.json("/tmp/loans_delta/_delta_log/00000000000000000002.json")

# COMMAND ----------

# Commit Information
display(j2.select("commitInfo").where("commitInfo is not null"))

# COMMAND ----------

# Add Information
display(j2.select("add").where("add is not null"))

# COMMAND ----------

# Remove Information
display(j2.select("remove").where("remove is not null"))

# COMMAND ----------


