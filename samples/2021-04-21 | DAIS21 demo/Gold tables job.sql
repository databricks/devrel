-- Databricks notebook source
set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create business aggregated table

-- COMMAND ----------

Create Or Replace Table ecommerce_demo.category_insights Using Delta AS
Select category_code,       
       date(event_time) as date,
       sum(price) as sales
From ecommerce_demo.events
Where category_code is not null and event_type='purchase'
Group By category_code, date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Feature table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""Select user_id, concat(category_code, '_', event_type) as category_event
-- MAGIC              From ecommerce_demo.events
-- MAGIC              Where category_code is not null
-- MAGIC              Group By user_id, category_event""")\
-- MAGIC .groupBy("user_id").pivot("category_event").agg(expr("1")).na.fill(0)\
-- MAGIC .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ecommerce_demo.user_features")

-- COMMAND ----------


