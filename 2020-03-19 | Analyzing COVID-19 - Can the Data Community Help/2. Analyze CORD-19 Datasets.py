# Databricks notebook source
# MAGIC %md 
# MAGIC ## 2. Analyze CORD-19 Datasets
# MAGIC ### COVID-19 Open Research Dataset Challenge (CORD-19) Working Notebooks
# MAGIC 
# MAGIC This is a working notebook for the [COVID-19 Open Research Dataset Challenge (CORD-19)](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge) to help you jump start your analysis of the CORD-19 dataset.  
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/3648/1*596Ur1UdO-fzQsaiGPrNQg.png" width="700"/>
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

# MAGIC %md #### Configure Parquet Path Variables
# MAGIC Save the data in Parquet format at: `/tmp/dennylee/COVID/CORD-19/2020-03-13/`

# COMMAND ----------

# Configure Parquet Paths in Python
comm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/comm_use_subset.parquet"
noncomm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/noncomm_use_subset.parquet"
biorxiv_medrxiv_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/biorxiv_medrxiv/biorxiv_medrxiv.parquet"
json_schema_path = "/databricks-datasets/COVID/CORD-19/2020-03-13/json_schema.txt"

# Configure Path as Shell Enviroment Variables
import os
os.environ['comm_use_subset_pq_path']=''.join(comm_use_subset_pq_path)
os.environ['noncomm_use_subset_pq_path']=''.join(noncomm_use_subset_pq_path)
os.environ['biorxiv_medrxiv_pq_path']=''.join(biorxiv_medrxiv_pq_path)
os.environ['json_schema_path']=''.join(json_schema_path)

# COMMAND ----------

# MAGIC %md #### Read Parquet Files
# MAGIC As these are correctly formed JSON files, you can use `spark.read.json` to read these files.  Note, you will need to specify the *multiline* option.

# COMMAND ----------

# Reread files
comm_use_subset = spark.read.format("parquet").load(comm_use_subset_pq_path)
noncomm_use_subset = spark.read.format("parquet").load(noncomm_use_subset_pq_path)
biorxiv_medrxiv = spark.read.format("parquet").load(biorxiv_medrxiv_pq_path)

# COMMAND ----------

# Count number of records
comm_use_subset_cnt = comm_use_subset.count()
noncomm_use_subset_cnt = noncomm_use_subset.count()
biorxiv_medrxiv_cnt = biorxiv_medrxiv.count()

# Print out
print ("comm_use_subset: %s, noncomm_use_subset: %s, biorxiv_medrxiv: %s" % (comm_use_subset_cnt, noncomm_use_subset_cnt, biorxiv_medrxiv_cnt))

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat /dbfs$json_schema_path

# COMMAND ----------

comm_use_subset.createOrReplaceTempView("comm_use_subset")
comm_use_subset.printSchema()

# COMMAND ----------

# MAGIC %md #### Extract Authors
# MAGIC To determine the source geographic location of these papers, let's extract the author metadata to create the `paperAuthorLocation` temporary view.

# COMMAND ----------

# MAGIC %sql
# MAGIC select paper_id, metadata.title, metadata.authors, metadata from comm_use_subset limit 10

# COMMAND ----------

paperAuthorLocation = spark.sql("""
select paper_id, 
       title,  
       authors.affiliation.location.addrLine as addrLine, 
       authors.affiliation.location.country as country, 
       authors.affiliation.location.postBox as postBox,
       authors.affiliation.location.postCode as postCode,
       authors.affiliation.location.region as region,
       authors.affiliation.location.settlement as settlement
  from (
    select a.paper_id, a.metadata.title as title, b.authors
      from comm_use_subset a
        left join (
            select paper_id, explode(metadata.authors) as authors from comm_use_subset 
            ) b
           on b.paper_id = a.paper_id  
  ) x
""")
paperAuthorLocation.createOrReplaceTempView("paperAuthorLocation")

# COMMAND ----------

# MAGIC %md #### Author Country Data Issues
# MAGIC There are some issues with the `authors.affiliation.location.country` information such as a value of `USA,USA,USA,USA`

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC   from (
# MAGIC     select paper_id, metadata.title as title, explode(metadata.authors) as authors from comm_use_subset 
# MAGIC   ) a
# MAGIC where authors.affiliation.location.country like '%USA, USA, USA, USA%'

# COMMAND ----------

# MAGIC %md ### Clean Up the Data
# MAGIC Let's work on cleaning up the author country data

# COMMAND ----------

# MAGIC %md #### Review paperAuthorLocation
# MAGIC A quick review of the `paperAuthorLocation` temporary view.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from paperAuthorLocation limit 200

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1), count(distinct paper_id) as papers from paperAuthorLocation

# COMMAND ----------

# MAGIC %md #### Extract country data
# MAGIC Extract country data (`paperCountries`) from the `paperAuthorLocation` temporary view.

# COMMAND ----------

paperCountries = spark.sql("""select distinct country from paperAuthorLocation""")
paperCountries.createOrReplaceTempView("paperCountries")

# COMMAND ----------

# MAGIC %md #### Use pycountry
# MAGIC Use `pycountry` to extract the alpha_3 code for each country

# COMMAND ----------

# import
import pycountry

# Look up alpha_3 country code (using pycountry)
def get_alpha_3(country):
    try_alpha_3 = -1
    try:
        try_alpha_3 = pycountry.countries.search_fuzzy(country)[0].alpha_3
    except:
        print("Unknown Country")
    return try_alpha_3

# Register UDF
spark.udf.register("get_alpha_3", get_alpha_3)

# COMMAND ----------

# from pyspark.sql.functions import pandas_udf, PandasUDFType

# # Use pandas_udf to define a Pandas UDF
# @pandas_udf('double', PandasUDFType.SCALAR)
# # Input/output are both a pandas.Series of doubles

# def pandas_plus_one(v):
#     return v + 1

# df.withColumn('v2', pandas_plus_one(df.v))

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, get_alpha_3(country) as alpha_3 from paperCountries

# COMMAND ----------

# MAGIC %md #### Steps to clean up country data

# COMMAND ----------

# Step 1: Extract alpha_3 for easily identifiable countries
paperCountries_s01 = spark.sql("""select country, get_alpha_3(country) as alpha_3 from paperCountries""")
paperCountries_s01.cache()
paperCountries_s01.createOrReplaceTempView("paperCountries_s01")

# COMMAND ----------

# Step 2: Extract alpha_3 for splittable identifiable countries (e.g. "USA, USA, USA", "Sweden, Norway", etc)
paperCountries_s02 = spark.sql("""
select country, splitCountry as country_cleansed, get_alpha_3(ltrim(rtrim(splitCountry))) as alpha_3
  from (
select country, explode(split(regexp_replace(country, "[^a-zA-Z, ]+", ""), ',')) as splitCountry
  from paperCountries_s01
 where alpha_3 = '-1'
 ) x
""")
paperCountries_s02.cache()
paperCountries_s02.createOrReplaceTempView("paperCountries_s02")

# COMMAND ----------

# Step 3: Extract yet to be identified countries (per steps 1 and 2) 
paperCountries_s03 = spark.sql("""select country, ltrim(rtrim(country_cleansed)) as country_cleansed, get_alpha_3(country_cleansed) from paperCountries_s02 where alpha_3 = -1""")
paperCountries_s03.cache()
paperCountries_s03.createOrReplaceTempView("paperCountries_s03")

# COMMAND ----------

# Step 4: Identify country by settlement
paperCountries_s04 = spark.sql("""
select distinct m.country_cleansed, f.settlement, get_alpha_3(f.settlement) as alpha_3
  from paperAuthorLocation f
    inner join paperCountries_s03 m
      on m.country = f.country
""")
paperCountries_s04.cache()
paperCountries_s04.createOrReplaceTempView("paperCountries_s04")

# COMMAND ----------

 # Step 5: Build new mapping
map_country_cleansed = spark.sql("""select distinct country_cleansed, alpha_3 from paperCountries_s04 where alpha_3 <> '-1'""")
map_country_cleansed.cache()
map_country_cleansed.createOrReplaceTempView("map_country_cleansed")

# COMMAND ----------

# Step 6: Update paperCountries_s03 using the mapping from step 5
paperCountries_s06 = spark.sql("""
select f.country, f.country_cleansed, m.alpha_3
  from paperCountries_s03 f
    left join map_country_cleansed m
      on m.country_cleansed = f.country_cleansed
 where m.alpha_3 is not null      
""")
paperCountries_s06.cache()
paperCountries_s06.createOrReplaceTempView("paperCountries_s06")

# COMMAND ----------

# MAGIC %md #### Build up map_country 
# MAGIC Build up map_country based on the previous pipeline processing.

# COMMAND ----------

map_country = spark.sql("""
select country, alpha_3 from paperCountries_s01 where alpha_3 <> '-1'
union all
select country, alpha_3 from paperCountries_s02 where alpha_3 <> '-1'
union all
select country, alpha_3 from paperCountries_s06
""")
map_country.cache()
map_country.createOrReplaceTempView("map_country")

# COMMAND ----------

# MAGIC %md #### Build paperCountryMapped
# MAGIC Put this all together to map the paper and alpha_3 geo location

# COMMAND ----------

paperCountryMapped = spark.sql("""
select p.paper_id, p.title, p.addrLine, p.country, p.postBox, p.postCode, p.region, p.settlement, m.alpha_3
 from paperAuthorLocation p
   left outer join map_country m
     on m.country = p.country
""")
paperCountryMapped.cache()
paperCountryMapped.createOrReplaceTempView("paperCountryMapped")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from paperCountryMapped limit 100

# COMMAND ----------

# MAGIC %md #### paperCountryMapped Descriptive Statistics

# COMMAND ----------

(ep_no, edp_no) = spark.sql("select count(1), count(distinct paper_id) from paperCountryMapped where country is null and settlement is null").collect()[0]
(ep_geo, edp_geo) = spark.sql("select count(1), count(distinct paper_id) from paperCountryMapped where country is not null or settlement is not null").collect()[0]
(ep_a3, edp_a3) = spark.sql("select count(1), count(distinct paper_id) from paperCountryMapped where alpha_3 is not null").collect()[0]
print("Distinct Papers with No Geographic Information: %s" % edp_no)
print("Distinct Papers with Some Geographic Information: %s" % edp_geo)
print("Distinct Papers with Identified Alpha_3 codes: %s" % edp_a3)

# COMMAND ----------

# MAGIC %md ### Visualize Paper Country Mapping
# MAGIC Map out the author country for each paper; note multiple authors per paper so there will be some double counting.

# COMMAND ----------

# MAGIC %sql
# MAGIC select alpha_3, count(distinct paper_id) 
# MAGIC   from paperCountryMapped 
# MAGIC  where alpha_3 is not null
# MAGIC  group by alpha_3