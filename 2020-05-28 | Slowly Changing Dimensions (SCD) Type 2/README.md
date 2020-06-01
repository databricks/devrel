[Notebooks for the 'Slowly Changing Dimensions (SCD) Type 2' online tech talk](https://www.youtube.com/watch?v=HZWwZG07hzQ)

# SCD Type 2 Demo using Delta Lake MERGE INTO

## Overview

The slowly changing dimension type two (SCD Type 2) is a classic data warehouse and star schema mainstay. This structure enables 'as-of' analytics over the point in time facts stored in the fact table(s). Customer, device, product, store, supplier are typical dimensions in a data warehouse. Facts such as orders and sales link dimensions together at the point in time the fact occured. However, dimensions, such as customers might change over time, customers may move, they may get reclassified as to their market segment as in this hypothetical demo scenario. The dimension is termed 'slowly changing' because the dimension doesn't change on a regular schedule.
![Delta Lake w/ Dimensional Schema Production](https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/DeltaLake/deltalake-dimensional-modeling.png)
When the data system receives the new customer information, the new customer dimension record is created. If an existing customer record of a matching key value exists, instead of over-writing the current record (a type 1 update) the current record is marked as in-active while preserving any foriegn key relationships to fact tables. The currrent record is also given an end date signifying the last date(time) the record was meaningful. A new record with the new information is inserted, with a first effect date (or start date) set to the effective date of the incoming customer information, often the job date(time) of the job running the load. The end date of the new record is set to `NULL` to signify the end date is not known, or is set to a rather large (e.g. 12/31/9999) value to make coding between clauses easier.   Thus the count of active customer records remains the same. 

Additional constraints:
* It's important to perform the update and insert in the same transaction to maintain accuracy of the dimension table.
* Additional criteria include that there is only one natural key and active or current indicate is true and any point in time.
* The surrogate key must be unique within the table. Fact tables will join to to the surrogate key value (in most cases).

Big data systems have struggeled with the ability to update the prior customer record. To achieve scale and a lockless architecture, direct updates to storage were not permitted. Delta Lake changes this.

Delta Lake with ACID transactions make it much easier to reliably to perform `UPDATE`, `DELETE` and Upsert/Merge operations. Delta Lake introduces the `MERGE INTO` operator to perform Upsert/Merge operations on Delta Lake tables

This demo will walk through loading a Customer dimension table and then selecting a percentage of the data to migrate to a 'SPORTS' market segment. We will then upsert/merge those modified records back into the customer_dim table and display the before & after summary of the market segments.

### References
* [Merge SQL](https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html)
* [Merge Examples](https://docs.databricks.com/delta/delta-update.html#merge-examples)
* [Wikipedia Article on SCD Type 2 Slowly Changing Dimension](https://en.wikipedia.org/w/index.php?title=Slowly_changing_dimension)
* [Kimball Group on Slowly Changing Dimensions, Part 2](https://www.kimballgroup.com/2008/09/slowly-changing-dimensions-part-2/)
