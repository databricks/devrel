## Tech Talk: Faster Spark SQL: Adaptive Query Execution in Databricks

2020-12-03 | [Watch the video](https://youtu.be/bQ33bwUE-ms) | This folder contains the notebooks used in this tutorial.

Over the years, there has been extensive and continuous effort on improving Spark SQL's query optimizer and planner, in order to generate high quality query execution plans. One of the biggest improvements is the cost-based optimization framework that collects and leverages a variety of data statistics (e.g., row count, number of distinct values, NULL values, max/min values, etc.) to help Spark make better decisions in picking the most optimal query plan.

Examples of these cost-based optimizations include choosing the right join type (broadcast-hash-join vs. sort-merge-join), selecting the correct build side in a hash-join, or adjusting the join order in a multi-way join. However, chances are data statistics can be out of date and cardinality estimates can be inaccurate, which may lead to a less optimal query plan. Adaptive Query Execution, new in Spark 3.0, now looks to tackle such issues by re-optimizing and adjusting query plans based on runtime statistics collected in the process of query execution. This talk is going to introduce the adaptive query execution framework along with a few optimizations it employs to address some major performance challenges the industry faces when using Spark SQL. We will illustrate how these statistics-guided optimizations work to accelerate execution through query examples. Finally, we will share the significant performance improvement we have seen on the TPC-DS benchmark with Adaptive Query Execution.

Check out this technical blog Allison and Maryann wrote: https://databricks.com/blog/2020/10/21/faster-sql-adaptive-query-execution-in-databricks.html

### Speakers ###

Maryann Xue
Maryann is a staff software engineer at Databricks, committer and PMC member of Apache Calcite and Apache Phoenix. Previously, she worked on a number of big data and compiler projects at Intel.

Allison Wang
Allison is a software engineer at Databricks, primarily focusing on Spark SQL. Previously she was on the data team at Robinhood. She holds a Bachelor’s degree in Computer Science from Carnegie Mellon University.