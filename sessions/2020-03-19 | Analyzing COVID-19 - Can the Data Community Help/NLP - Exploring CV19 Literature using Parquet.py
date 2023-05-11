# Databricks notebook source
# MAGIC %md 
# MAGIC ## Explore CORD-19 Datasets using NLP Methods
# MAGIC ### COVID-19 Open Research Dataset Challenge (CORD-19) Working Notebooks
# MAGIC 
# MAGIC This is a working notebook for the [COVID-19 Open Research Dataset Challenge (CORD-19)](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge) to help you jump start your NLP analysis of the CORD-19 dataset.  
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/3648/1*596Ur1UdO-fzQsaiGPrNQg.png" width="700"/>
# MAGIC 
# MAGIC Attributions:
# MAGIC * The licenses for each dataset used for this workbook can be found in the *all _ sources _ metadata csv file* which is included in the [downloaded dataset](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge/download) 
# MAGIC * When using Databricks or Databricks Community Edition, a copy of this dataset has been made available at `/databricks-datasets/COVID/CORD-19`
# MAGIC * This notebook is freely available to share, licensed under [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/us/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Parquet Path Variables
# MAGIC Save the data in Parquet format at: `/tmp/dennylee/COVID/CORD-19/2020-03-13/`

# COMMAND ----------

# Configure Parquet Paths in Python
comm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/comm_use_subset.parquet"
noncomm_use_subset_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/noncomm_use_subset.parquet"
biorxiv_medrxiv_pq_path = "/tmp/dennylee/COVID/CORD-19/2020-03-13/biorxiv_medrxiv/biorxiv_medrxiv.parquet"

# COMMAND ----------

# MAGIC %md #### Read Parquet Files

# COMMAND ----------

comm_use_subset = spark.read.format("parquet").load(comm_use_subset_pq_path)
noncomm_use_subset = spark.read.format("parquet").load(noncomm_use_subset_pq_path)
biorxiv_medrxiv = spark.read.format("parquet").load(biorxiv_medrxiv_pq_path)

# COMMAND ----------

# Count number of records
comm_use_subset_cnt = comm_use_subset.count()
noncomm_use_subset_cnt = noncomm_use_subset.count()
biorxiv_medrxiv_cnt = biorxiv_medrxiv.count()

# Print out
print (f"comm_use_subset: {comm_use_subset_cnt}, noncomm_use_subset: {noncomm_use_subset_cnt}, biorxiv_medrxiv: {biorxiv_medrxiv_cnt}")

# COMMAND ----------

comm_use_subset.show(3)

# COMMAND ----------

 comm_use_subset.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's generate a WordCloud from all the titles
# MAGIC 
# MAGIC You need to have `wordcloud==1.5` installed.

# COMMAND ----------

comm_use_subset.select("metadata.title").show(3)

# COMMAND ----------

from wordcloud import WordCloud, STOPWORDS 
import matplotlib.pyplot as plt 

def wordcloud_draw(text, color = 'white'):
    """
    Plots wordcloud of string text after removing stopwords
    """
    cleaned_word = " ".join([word for word in text.split()])
    wordcloud = WordCloud(stopwords=STOPWORDS,
                      background_color=color,
                      width=1000,
                      height=1000
                     ).generate(cleaned_word)
    plt.figure(1,figsize=(8, 8))
    plt.imshow(wordcloud)
    plt.axis('off')
    display(plt.show())

# COMMAND ----------

from pyspark.sql.functions import concat_ws, collect_list

all_title_df = comm_use_subset.agg(concat_ws(", ", collect_list(comm_use_subset['metadata.title'])).alias('all_titles'))
display(all_title_df)

# COMMAND ----------

wordcloud_draw(str(all_title_df.select('all_titles').collect()[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Round 2 - remove some non-meaningful words

# COMMAND ----------

def custom_wordcloud_draw(text, color = 'white'):
    """
    Plots wordcloud of string text after removing stopwords
    """
    cleaned_word = " ".join([word for word in text.split()])
    wordcloud = WordCloud(stopwords= STOPWORDS.update(['using', 'based', 'analysis', 'study', 'research', 'viruses']),
                      background_color=color,
                      width=1000,
                      height=1000
                     ).generate(cleaned_word)
    plt.figure(1,figsize=(8, 8))
    plt.imshow(wordcloud)
    plt.axis('off')
    display(plt.show())

# COMMAND ----------

custom_wordcloud_draw(str(all_title_df.select('all_titles').collect()[0]))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Generate Summaries from Abstracts

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we are going to use a summarizer model trained on BERT that initially was used to summarize lectures! 
# MAGIC 
# MAGIC Refer to this [paper](https://arxiv.org/abs/1906.04165) to read more about it. 
# MAGIC 
# MAGIC To use this library, you need to install `bert-extractive-summarizer` using PyPi. 

# COMMAND ----------

from summarizer import Summarizer

# take the first abstract
abstract1 = str(comm_use_subset.select("abstract.text").first())
abstract1

# COMMAND ----------

abstract2 = str(comm_use_subset.select("abstract.text").take(2)[1])
abstract2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train Summarizer Model using `min_length` parameter

# COMMAND ----------

model = Summarizer()
abstract1_summary = model(str(abstract1), min_length=20)

full_abstract1 = ''.join(abstract1_summary)
print(full_abstract1)

# COMMAND ----------

abstract2_summary = model(str(abstract2), min_length=20)

full_abstract2 = ''.join(abstract2_summary)
print(full_abstract2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train Summarizer Model using `max_length` parameter

# COMMAND ----------

summary_executive = model(str(abstract1), max_length=250)

full_exec_summary = ''.join(summary_executive)
print(full_exec_summary)

# COMMAND ----------

summary_executive2 = model(str(abstract2), max_length=250)

full_exec_summary2 = ''.join(summary_executive2)
print(full_exec_summary2)
