-- Databricks notebook source
-- MAGIC %md # SCD Type 2 Demo using Delta Lake MERGE INTO
-- MAGIC 
-- MAGIC ## Overview
-- MAGIC 
-- MAGIC The slowly changing dimension type two (SCD Type 2) is a classic data warehouse and star schema mainstay. This structure enables 'as-of' analytics over the point in time facts stored in the fact table(s). Customer, device, product, store, supplier are typical dimensions in a data warehouse. Facts such as orders and sales link dimensions together at the point in time the fact occured. However, dimensions, such as customers might change over time, customers may move, they may get reclassified as to their market segment as in this hypothetical demo scenario. The dimension is termed 'slowly changing' because the dimension doesn't change on a regular schedule.
-- MAGIC ![Delta Lake w/ Dimensional Schema Production](https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/DeltaLake/deltalake-dimensional-modeling.png)
-- MAGIC When the data system receives the new customer information, the new customer dimension record is created. If an existing customer record of a matching key value exists, instead of over-writing the current record (a type 1 update) the current record is marked as in-active while preserving any foriegn key relationships to fact tables. The currrent record is also given an end date signifying the last date(time) the record was meaningful. A new record with the new information is inserted, with a first effect date (or start date) set to the effective date of the incoming customer information, often the job date(time) of the job running the load. The end date of the new record is set to `NULL` to signify the end date is not known, or is set to a rather large (e.g. 12/31/9999) value to make coding between clauses easier.   Thus the count of active customer records remains the same. 
-- MAGIC 
-- MAGIC Additional constraints:
-- MAGIC * It's important to perform the update and insert in the same transaction to maintain accuracy of the dimension table.
-- MAGIC * Additional criteria include that there is only one natural key and active or current indicate is true and any point in time.
-- MAGIC * The surrogate key must be unique within the table. Fact tables will join to to the surrogate key value (in most cases).
-- MAGIC 
-- MAGIC Big data systems have struggeled with the ability to update the prior customer record. To achieve scale and a lockless architecture, direct updates to storage were not permitted. Delta Lake changes this.
-- MAGIC 
-- MAGIC Delta Lake with ACID transactions make it much easier to reliably to perform `UPDATE`, `DELETE` and Upsert/Merge operations. Delta Lake introduces the `MERGE INTO` operator to perform Upsert/Merge operations on Delta Lake tables
-- MAGIC 
-- MAGIC This demo will walk through loading a Customer dimension table and then selecting a percentage of the data to migrate to a 'SPORTS' market segment. We will then upsert/merge those modified records back into the customer_dim table and display the before & after summary of the market segments.
-- MAGIC 
-- MAGIC ### References
-- MAGIC * [Merge SQL](https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html)
-- MAGIC * [Merge Examples](https://docs.databricks.com/delta/delta-update.html#merge-examples)
-- MAGIC * [Wikipedia Article on SCD Type 2 Slowly Changing Dimension](https://en.wikipedia.org/w/index.php?title=Slowly_changing_dimension)
-- MAGIC * [Kimball Group on Slowly Changing Dimensions, Part 2](https://www.kimballgroup.com/2008/09/slowly-changing-dimensions-part-2/)

-- COMMAND ----------

-- MAGIC %md ## Wikipedia SCD Type 2 Definition
-- MAGIC Taken from: https://en.wikipedia.org/w/index.php?title=Slowly_changing_dimension
-- MAGIC 
-- MAGIC ### Type 2: add new row
-- MAGIC This method tracks historical data by creating multiple records for a given natural key in the dimensional tables with separate surrogate keys and/or different version numbers. Unlimited history is preserved for each insert.
-- MAGIC 
-- MAGIC #### For example, 
-- MAGIC i) if the supplier relocates to Illinois the version numbers will be incremented sequentially:
-- MAGIC 
-- MAGIC | Supplier_Key | Supplier_Code | Supplier_Name | Supplier | Version |
-- MAGIC |---|---|---|---|---|
-- MAGIC | 123 | ABC | Acme Supply Co | CA | 0 |
-- MAGIC | 124 | ABC | Acme Supply Co | IL | 1 |
-- MAGIC 
-- MAGIC 
-- MAGIC ii) Another method is to add 'effective date' columns.
-- MAGIC 
-- MAGIC | Supplier_Key | Supplier_Code | Supplier_Name | Supplier_State | Start_Date | End_Date |
-- MAGIC | --- | --- | --- | --- | --- | --- |
-- MAGIC | 123 |	ABC | Acme Supply Co | CA | 2000-01-01T00:00:00 | 2004-12-22T00:00:00 |
-- MAGIC | 124 | ABC | Acme Supply Co | IL | 2004-12-22T00:00:00 | NULL |
-- MAGIC The Start date/time of the second row is equal to the End date/time of the previous row. The null End_Date in row two indicates the current tuple version. A standardized surrogate high date (e.g. 9999-12-31) may instead be used as an end date, so that the field can be included in an index, and so that null-value substitution is not required when querying.
-- MAGIC 
-- MAGIC iii) And a third method uses an effective date and a current flag.
-- MAGIC 
-- MAGIC | Supplier_Key | Supplier_Code | Supplier_Name | Supplier_State | Effective_Date | Current_Flag |
-- MAGIC | --- | --- | --- | --- | --- | --- |
-- MAGIC | 123 | ABC | Acme Supply Co | CA | 2000-01-01T00:00:00 | N |
-- MAGIC | 124 | ABC | Acme Supply Co | IL | 2004-12-22T00:00:00 | Y |
-- MAGIC The Current_Flag value of 'Y' indicates the current tuple version.
-- MAGIC 
-- MAGIC Transactions that reference a particular surrogate key (Supplier_Key) are then permanently bound to the time slices defined by that row of the slowly changing dimension table. An aggregate table summarizing facts by state continues to reflect the historical state, i.e. the state the supplier was in at the time of the transaction; no update is needed. To reference the entity via the natural key, it is necessary to remove the unique constraint making Referential integrity by DBMS impossible.
-- MAGIC 
-- MAGIC If there are retroactive changes made to the contents of the dimension, or if new attributes are added to the dimension (for example a Sales_Rep column) which have different effective dates from those already defined, then this can result in the existing transactions needing to be updated to reflect the new situation. This can be an expensive database operation, so Type 2 SCDs are not a good choice if the dimensional model is subject to change.[1]

-- COMMAND ----------

-- MAGIC %md ## Setup
-- MAGIC * setup `current_user()` function
-- MAGIC * remove existing silver table
-- MAGIC * remove remove silver data folder
-- MAGIC * add notebook widgets that act as job parameter inputs

-- COMMAND ----------

-- MAGIC %run ./etl_setup

-- COMMAND ----------

-- DBTITLE 1,Add Widgets
-- MAGIC %python
-- MAGIC import datetime
-- MAGIC 
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC username = spark.conf.get("spark.databricks.username")
-- MAGIC database = username.split('@')[0].translate(str.maketrans("'@.-", '____')) + "_gold"
-- MAGIC db = database
-- MAGIC 
-- MAGIC dbutils.widgets.text("username",username)
-- MAGIC dbutils.widgets.text("db", database)
-- MAGIC 
-- MAGIC # set job date time once for consistent timestamps across all jobs & rows
-- MAGIC dbutils.widgets.text("job_dt",str(datetime.datetime.utcnow()))

-- COMMAND ----------

-- DBTITLE 1,Reset customer table
DROP TABLE IF EXISTS $db.customer;

-- COMMAND ----------

-- DBTITLE 1,Remove underlying data files and transaction log
-- MAGIC %fs rm -r /home/douglas.moore@databricks.com/databases/douglas_moore_gold.db/customer 

-- COMMAND ----------

-- DBTITLE 1,Add database
CREATE DATABASE IF NOT EXISTS $db COMMENT 'silver layer'  LOCATION 'dbfs:/home/$username/databases/$db.db';
DESCRIBE DATABASE $db

-- COMMAND ----------

-- MAGIC %md ## Explore data
-- MAGIC * Look at file(s) and sizes
-- MAGIC * Examine content of file
-- MAGIC * Count records
-- MAGIC * Split file (for demo purposes)

-- COMMAND ----------

-- MAGIC %sh ls -lh /dbfs/databricks-datasets/tpch/data-001/customer

-- COMMAND ----------

-- MAGIC %sh wc -l /dbfs/databricks-datasets/tpch/data-001/customer/c*.tbl

-- COMMAND ----------

-- MAGIC %sh head -5 /dbfs/databricks-datasets/tpch/data-001/customer/c*.tbl

-- COMMAND ----------

-- DBTITLE 1,Split data set (for demo purposes only)
-- MAGIC %sh 
-- MAGIC mkdir -p /dbfs/tmp/douglas.moore@databricks.com/customer_bronze
-- MAGIC cd /dbfs/tmp/douglas.moore@databricks.com/customer_bronze
-- MAGIC split -d --lines=100000 --additional-suffix=.csv /dbfs/databricks-datasets/tpch/data-001/customer/customer.tbl 
-- MAGIC ls -al

-- COMMAND ----------

-- MAGIC %md ## Load data from bronze to silver
-- MAGIC * parse CSV (Pipe separated file)
-- MAGIC * map field names to target field names
-- MAGIC * map source field types to target field types
-- MAGIC * append provinence (audit) info to each record
-- MAGIC * create external table on top of delta lake table directory
-- MAGIC * `COPY INTO` will track which files have already been loaded

-- COMMAND ----------

-- DBTITLE 1,Load initial customer dimension values
COPY INTO delta.`dbfs:/home/douglas.moore@databricks.com/databases/$db.db/customer` -- Target folder
FROM (
  SELECT CAST(_c0 as BIGINT) as ID,
         _c1 as CUSTKEY,
         _c2 as NAME,
         CAST(_c3 AS INT) as ADDRESSKEY,
         _c4 as PHONE,
         CAST(_c5 as DECIMAL(10,2)) as ACCTBAL,
         _c6 as MKTSEGMENT,
         _c7 as COMMENT,
         1 as ISCURRENT,
         CAST('1970' as TIMESTAMP) as STARTDT,
         CAST(null as TIMESTAMP)   as ENDDT,
         input_file_name()         as SRC_FILE,     -- Audit column
         current_user()            as UPDATEUSER,   -- Audit column
         CAST('$job_dt' AS TIMESTAMP) CREATEDT,     -- Audit column
         CAST('$job_dt' AS TIMESTAMP) UPDATEDT      -- Audit column
   FROM 'dbfs:/tmp/douglas.moore@databricks.com/customer_bronze/x0[0-6].csv'
   )
FILEFORMAT = CSV
FORMAT_OPTIONS('sep'='|','inferSchema'='true','header'='false', 'mergeSchema'='true')
COPY_OPTIONS ('force' = 'true');
-- set force = false for normal incremental ingestion

-- COMMAND ----------

-- DBTITLE 1,Create schema to external table to easily reference database table
CREATE TABLE IF NOT EXISTS $db.customer_dim USING DELTA LOCATION 'dbfs:/home/douglas.moore@databricks.com/databases/$db.db/customer';

-- COMMAND ----------

DESCRIBE extended $db.customer_dim

-- COMMAND ----------

select * from $db.customer_dim limit 10

-- COMMAND ----------

-- DBTITLE 1,Display original market segments found in customer dimension
SELECT MKTSEGMENT, count(*) as Count
FROM $db.customer_dim VERSION AS OF 0
GROUP BY MKTSEGMENT
ORDER BY MKTSEGMENT

-- COMMAND ----------

-- DBTITLE 1,Look at transaction history
describe history $db.customer_dim

-- COMMAND ----------

-- MAGIC %md ## Create a change set
-- MAGIC * The change set is the set of new records arriving that is changing the Customer dimension table
-- MAGIC * In this example, the market segment is 'slowly changing' for n% of the existing customers as a result of a hypothetical rewrite of the market segmentation rules
-- MAGIC * We need to update our customer dimension with the new customer records.
-- MAGIC * We need to preserve history and past transactions associated with the customer records

-- COMMAND ----------

-- DBTITLE 1,Market segment change by a percentage % of the customers
DROP TABLE IF EXISTS $db.change_set;
CREATE TABLE $db.change_set AS
  SELECT DISTINCT -- Make sure there are no duplicates
    ID,
    CUSTKEY,
    NAME,
    ADDRESSKEY,
    PHONE,
    ACCTBAL,
    'SPORTS' MKTSEGMENT, -- new MKTSEGMENT
    COMMENT,
    1 as ISCURRENT,
    SRC_FILE,     -- Audit column
    CAST('$job_dt' AS TIMESTAMP) STARTDT,
    CAST(null as TIMESTAMP) ENDDT,
    current_user()               UPDATEUSER,   -- Audit column
    CAST('$job_dt' AS TIMESTAMP) CREATEDT,     -- Audit column
    CAST('$job_dt' AS TIMESTAMP) UPDATEDT      -- Audit column
  FROM $db.customer_dim TABLESAMPLE ( 10.0 PERCENT);

-- COMMAND ----------

-- DBTITLE 1,Validate change set, count duplicates. Expect zero duplicates.
select count(*) from (select count(*), custkey from $db.change_set group by custkey having count(*) > 1)

-- COMMAND ----------

-- DBTITLE 1,Display change set
SELECT MKTSEGMENT, count(*) as Count 
FROM $db.change_set
GROUP BY MKTSEGMENT
ORDER BY MKTSEGMENT

-- COMMAND ----------

SELECT * from $db.change_set limit 10

-- COMMAND ----------

-- DBTITLE 1,Verify what's going to change
select isnull(mergekey), count(*) FROM( 
  SELECT -- UPDATE
      cs.CUSTKEY as MERGEKEY,
      cs.*
    FROM $db.change_set cs
  UNION ALL
    SELECT -- INSERT
      NULL as MERGEKEY,
      cs.*
    FROM $db.change_set cs
    JOIN $db.customer_dim c 
      ON c.CUSTKEY = cs.CUSTKEY 
     AND c.ISCURRENT = 1
   WHERE NOT
        ( c.NAME = cs.NAME
      AND c.PHONE = cs.PHONE
      AND c.ADDRESSKEY = cs.ADDRESSKEY 
      AND c.MKTSEGMENT = cs.MKTSEGMENT 
      AND c.ACCTBAL = cs.ACCTBAL
        )
) X group by isnull(mergekey)

-- COMMAND ----------

-- MAGIC %md ## Upsert / Merge
-- MAGIC * Upsert/merge the changed records into to the Customer dimension table.
-- MAGIC * `MERGE INTO` is based on matching the `MERGEKEY` and the Customer dimension `CUSTKEY`
-- MAGIC * Mark current record as retired, set `ENDDT` to job_dt, set `ISCURRENT = 0`
-- MAGIC * Insert new record with new record values

-- COMMAND ----------

-- DBTITLE 1,Upsert / Merge change set into customer dimension table
MERGE INTO $db.customer_dim AS c
  USING
  ( 
    SELECT -- UPDATE
      cs.CUSTKEY as MERGEKEY,
      cs.*
    FROM $db.change_set cs
  UNION ALL
    SELECT -- INSERT
      NULL as MERGEKEY,
      cs.*
    FROM $db.change_set cs
    JOIN $db.customer_dim c 
      ON c.CUSTKEY = cs.CUSTKEY
     AND c.ISCURRENT = 1
   WHERE NOT  -- ignore records with no changes, don't insert these
        ( c.NAME = cs.NAME
      AND c.PHONE = cs.PHONE
      AND c.ADDRESSKEY = cs.ADDRESSKEY 
      AND c.MKTSEGMENT = cs.MKTSEGMENT 
      AND c.ACCTBAL = cs.ACCTBAL
        )
  ) u
ON c.CUSTKEY = u.MERGEKEY -- Match record condition
WHEN MATCHED 
    AND c.ISCURRENT = 1 
    AND -- And if any of the attributes have changed
      NOT  ( c.NAME = u.NAME
      AND c.PHONE = u.PHONE
      AND c.ADDRESSKEY = u.ADDRESSKEY 
      AND c.MKTSEGMENT = u.MKTSEGMENT 
      AND c.ACCTBAL = u.ACCTBAL
        )
  THEN UPDATE SET -- Update fields on 'old' records
    ISCURRENT = 0, 
    ENDDT = CAST('$job_dt' as TIMESTAMP)
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md ## Analyze results of Upsert / Merge

-- COMMAND ----------

describe history $db.customer_dim

-- COMMAND ----------

OPTIMIZE $db.customer_dim
ZORDER BY userId

-- COMMAND ----------

-- DBTITLE 1,Examine results to ensure MERGE INTO logic was idempotent
SELECT 'new' as version, ISCURRENT, STARTDT, ENDDT, count(*) from $db.customer_dim VERSION AS OF 1 group by version, ISCURRENT, STARTDT, ENDDT
UNION ALL
SELECT 'old' as version, ISCURRENT, STARTDT, ENDDT, count(*) from $db.customer_dim VERSION AS OF 0 group by version, ISCURRENT, STARTDT, ENDDT
ORDER BY version asc, ISCURRENT DESC, STARTDT DESC, ENDDT DESC NULLS FIRST

-- COMMAND ----------

-- DBTITLE 1,Check final current customer records (should be same as start)
SELECT COUNT(*) from $db.customer_dim WHERE ISCURRENT = 1

-- COMMAND ----------

-- DBTITLE 1,Check for duplicates (should be none)
SELECT count(*), CUSTKEY, ISCURRENT FROM $db.customer_dim WHERE ISCURRENT = 1 GROUP BY CUSTKEY,ISCURRENT HAVING count(*) > 2

-- COMMAND ----------

-- DBTITLE 1,Show changes in market segment *as-of* a past date and a future date
SELECT version, MKTSEGMENT, count(*) FROM
(
  SELECT 'new' as version, MKTSEGMENT from $db.customer_dim WHERE CAST('2030' AS TIMESTAMP) between STARTDT and coalesce(ENDDT, CAST('9999' as TIMESTAMP))
  UNION ALL
  SELECT 'old' as version, MKTSEGMENT from $db.customer_dim WHERE CAST('2000' AS TIMESTAMP) between STARTDT and coalesce(ENDDT, CAST('9999' as TIMESTAMP))
) X
GROUP BY version, MKTSEGMENT
ORDER BY MKTSEGMENT, version desc

-- COMMAND ----------

-- DBTITLE 1,Visualize changes to market segment with the help of time travel
SELECT 'new' version, MKTSEGMENT, count(*) as Count 
FROM $db.customer_dim VERSION AS OF 1
WHERE ISCURRENT = 1
GROUP BY MKTSEGMENT
UNION ALL
SELECT 'old' version, MKTSEGMENT, count(*) as Count 
FROM $db.customer_dim VERSION AS OF 0
WHERE ISCURRENT = 1
GROUP BY MKTSEGMENT
ORDER BY MKTSEGMENT, version desc

-- COMMAND ----------

OPTIMIZE <table>
zorder primary_key, secondary_key

-- COMMAND ----------

VACUUM <table_name> RETAIN 720 hours
