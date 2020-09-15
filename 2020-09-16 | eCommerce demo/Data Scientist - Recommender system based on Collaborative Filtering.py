# Databricks notebook source
# MAGIC %md
# MAGIC #Collaborative Filtering
# MAGIC Collaborative Filtering is the most common technique used when it comes to building intelligent recommender systems that can learn to give better recommendations as more information about users is collected. <br>
# MAGIC Collaborative filtering is a technique that can filter out items that a user might like on the basis of reactions by similar users.<br>
# MAGIC It works by searching a large group of people and finding a smaller set of users with tastes similar to a particular user. It looks at the items they like and combines them to create a ranked list of suggestions.<br>
# MAGIC More info: https://realpython.com/build-recommendation-engine-collaborative-filtering/

# COMMAND ----------

# MAGIC %md
# MAGIC Simplistic example:
# MAGIC - Alice bought a smartphone and a tablet
# MAGIC - Bob bought a smartphone and headphones
# MAGIC - A collaborative filtering algorithm will recommend headphones to Alice and a tablet to Bob

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Create a 'ratings' table for users and products by month
# MAGIC - We split the data by month. October will be used to train the model and November to test it.
# MAGIC - We do not have explicit ratings, so we will convert the event_type into (implicit) ratings: view -> 1, cart -> 5, purchase -> 10 <br>
# MAGIC 
# MAGIC |User|Category|Month|Rating|Count|
# MAGIC |---|---|---|---|---|
# MAGIC |Alice|Smartphone|10|1|5|
# MAGIC |Bob|Shoes|11|10|10|

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Or Replace Temporary View ratings_by_user_category_month AS
# MAGIC Select cast(user_id as bigint) as user,                -- The user needs to be an int for the algorithm
# MAGIC        abs(hash(category_code)) as category,           -- The category needs to be an int, so we hash the category_code
# MAGIC        month(event_time) as month,
# MAGIC        first(category_code) as category_name,
# MAGIC        max(case when event_type = 'view' then 1
# MAGIC                 when event_type = 'cart' then 5
# MAGIC                 when event_type = 'purchase'then 10 
# MAGIC                 else 0 end) as rating,                 -- If a user does multiple actions on an item, the rating is based on the highest action
# MAGIC        count(*) as cnt                                 -- The count will not be used in the algorithm
# MAGIC From ecommerce_demo.events
# MAGIC Where category_code is not null
# MAGIC Group By user, category, month;

# COMMAND ----------

# MAGIC %md
# MAGIC - We select only users with actions with at least 2 categories each month
# MAGIC - We select only categories with at least 100 users each month <br>
# MAGIC This data is then saved in ecommerce_demo.ratings as a Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Or Replace Temporary View top_categories AS
# MAGIC Select category
# MAGIC From ratings_by_user_category_month
# MAGIC Group By category
# MAGIC Having count(distinct case when month = 10 then user else Null end) >= 100 and 
# MAGIC        count(distinct case when month = 11 then user else Null end) >= 100;
# MAGIC 
# MAGIC 
# MAGIC Create Or Replace Temporary View top_users AS
# MAGIC Select user
# MAGIC From ratings_by_user_category_month
# MAGIC Inner Join top_categories Using(category)
# MAGIC Group By user
# MAGIC Having count(distinct case when month = 10 then category else Null end) >= 2 and 
# MAGIC        count(distinct case when month = 11 then category else Null end) >= 2;
# MAGIC 
# MAGIC 
# MAGIC Create Or Replace Temporary View ratings AS
# MAGIC Select A.user, 
# MAGIC        A.category, 
# MAGIC        A.month, 
# MAGIC        A.category_name, 
# MAGIC        A.rating, 
# MAGIC        A.cnt 
# MAGIC From ratings_by_user_category_month A
# MAGIC Inner Join top_users Using(User)
# MAGIC Inner Join top_categories Using(category);
# MAGIC 
# MAGIC 
# MAGIC Create or Replace Table ecommerce_demo.ratings Using Delta AS
# MAGIC Select * From ratings;
# MAGIC 
# MAGIC OPTIMIZE ecommerce_demo.ratings;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From ecommerce_demo.ratings Limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC The filtering for top categories and users keeps most of the categories, but only a small fraction of the users (~7%) who are active in both October and November.<br> Those users are responsible for 25% of the activity though.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select 'Before Filtering' as population,
# MAGIC        count(distinct user_id) as numUsers,
# MAGIC        count(distinct category_code) as numCategories,
# MAGIC        count(*) as numEvents
# MAGIC From ecommerce_demo.events
# MAGIC UNION
# MAGIC Select 'After Filtering' as population,
# MAGIC        count(distinct user) as numUsers,
# MAGIC        count(distinct category) as numCategories,
# MAGIC        sum(cnt) as numEvents
# MAGIC From ecommerce_demo.ratings

# COMMAND ----------

# MAGIC %md
# MAGIC There is a strong class imbalance in the ratings, but the weights reduce the imbalance in practice.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select rating,
# MAGIC        count(*) as cnt
# MAGIC From ecommerce_demo.ratings
# MAGIC Group By rating
# MAGIC Order By rating

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Enhance the 'ratings' table with inactive events
# MAGIC - The actions of view, cart and purchase all show a more or less strong interest for the products (positive class).
# MAGIC - We need a negative class that shows the absence of interest.
# MAGIC - The absence of events is not recorded. Thus, we will create this "rating=0" class from (user, category, month) tuples w/o events.<br>
# MAGIC 
# MAGIC |User|Category|Month|Rating|Count|
# MAGIC |---|---|---|---|---|
# MAGIC |Alice|Smartphone|10|1|5|
# MAGIC |Bob|Shoes|11|10|10|
# MAGIC |Alice|Shoes|10|0|0|
# MAGIC |Bob|Smartphone|11|0|0|

# COMMAND ----------

# MAGIC %md
# MAGIC - Given the number of users, categories and months, we have ~94M possible pairs, with 3.1M out of them who are in a positive class. That means the negative class is much larger, with ~91M (user, category) pairs.
# MAGIC - We will then take a sample of the negative class to have a good balance between negative and positive classes
# MAGIC - We will do the following in Python

# COMMAND ----------

# Pandas Dataframe of users
df_users = spark.sql("""Select user
                        From ecommerce_demo.ratings
                        Group By user""").toPandas()

# Pandas Dataframe of categories
df_categories = spark.sql("""Select category
                             From ecommerce_demo.ratings
                             Group By category""").toPandas()

# Pandas Dataframe of (user, catgeory, month) in positive class
df_positiveClass = spark.sql("""Select user, 
                                       category, 
                                       month 
                                From ecommerce_demo.ratings""").toPandas()

# Convert DataFrames to Python lists (and set of tuples for (user, category, month))
users = list(df_users['user'])
categories = list(df_products['category'])
positiveClass = df_positiveClass.values.tolist()
positiveClassSet = set(tuple(x) for x in positiveClass)
print('There are {} (user, category, month) tuples in the positive class.'.format(len(df_positiveClass)))

# COMMAND ----------

# MAGIC %md
# MAGIC - We take a random sample of 4M (user, category, month) tuples
# MAGIC - After removing tuples in the positive class and de-duplication, we have 3.78M elements in the negative class (rating=0)

# COMMAND ----------

import numpy as np
import pandas as pd

length = 4000000
months = [10, 11]

# Create random vectors
randomUsers = np.random.choice(users, length)
randomCategories = np.random.choice(categories, length)
randomMonth = np.random.choice(months, length)

# Remove items in positive class and de-dup (list(set()))
negativeClass = []
for i in range(length):
  item = (randomUsers[i], randomCategories[i], randomMonth[i])
  if item not in positiveClassSet:
    negativeClass.append(item)
negativeClassDedup = list(set(negativeClass))

print('{} items in the negative class before de-dup.'.format(len(negativeClass)))
print('{} items in the negative class after de-dup.'.format(len(negativeClassDedup)))

# Convert result in a Spark DataFrame
negativeClass_df = pd.DataFrame(negativeClassDedup, columns = ['user', 'category', 'month'])
spark.createDataFrame(negativeClass_df).registerTempTable('negative_class')

# COMMAND ----------

# MAGIC %md
# MAGIC Create full table as union of positive and negative classes

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Or Replace Temporary View categories As 
# MAGIC Select category, 
# MAGIC        max(category_name) as category_name 
# MAGIC From ecommerce_demo.ratings 
# MAGIC Group By category;
# MAGIC 
# MAGIC Create Or Replace Temporary View ratings_with_negative AS
# MAGIC Select * From ecommerce_demo.ratings
# MAGIC UNION ALL
# MAGIC (Select A.user,
# MAGIC         A.category,
# MAGIC         A.month,
# MAGIC         B.category_name,
# MAGIC         0 as rating,
# MAGIC         0 as cnt
# MAGIC From negative_class A
# MAGIC Left JOIN categories B On A.category = B.category)

# COMMAND ----------

# MAGIC %md
# MAGIC Add rating averages for 'base models' and create full ratings table as **ecommerce_demo.ratings_full**:
# MAGIC - Global average
# MAGIC - Average by user (when users are more or less active)
# MAGIC - Average by category (when categories are more or less popular)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace Temporary View globalAvg AS
# MAGIC Select avg(rating) as globalAvg 
# MAGIC From ratings_with_negative
# MAGIC Where month = 10;
# MAGIC 
# MAGIC Create or Replace Temporary View userAvg AS
# MAGIC Select user, 
# MAGIC        avg(rating) as userAvg 
# MAGIC From ratings_with_negative
# MAGIC Where month = 10
# MAGIC Group By user;
# MAGIC 
# MAGIC Create or Replace Temporary View categoryAvg AS
# MAGIC Select category, 
# MAGIC        avg(rating) as categoryAvg 
# MAGIC From ratings_with_negative
# MAGIC Where month = 10
# MAGIC Group By category;
# MAGIC 
# MAGIC 
# MAGIC Create or Replace Table ecommerce_demo.ratings_full Using Delta AS
# MAGIC Select A.*,
# MAGIC        B.globalAvg,
# MAGIC        C.userAvg,
# MAGIC        D.categoryAvg
# MAGIC From ratings_with_negative A
# MAGIC Left Join globalAvg B
# MAGIC Left Join userAvg C Using(user)
# MAGIC Left Join categoryAvg D Using(category);
# MAGIC 
# MAGIC OPTIMIZE ecommerce_demo.ratings_full;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From ecommerce_demo.ratings_full
# MAGIC Limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC Select rating,
# MAGIC        sum(case when month = 10 then 1 else 0 end) as numOctober,
# MAGIC        sum(case when month = 11 then 1 else 0 end) as numNovember
# MAGIC From ecommerce_demo.ratings_full
# MAGIC Group By rating
# MAGIC Order By rating

# COMMAND ----------

# MAGIC %md
# MAGIC #Train and test model
# MAGIC - We use the ALS (Alternating least Squares) algorithm from pyspark (https://spark.apache.org/docs/latest/ml-collaborative-filtering.html)
# MAGIC - We use October data for training and November data for testing
# MAGIC - We first evaluate the performance of base models (globalAvg, userAvg, categoryAvg)
# MAGIC - Then we train and test models and optimize the performance based on regParam, maxIter and rank.
# MAGIC - We use MAE (mean absolute error) to evaluate the model performance, as a general metric. It's probably a terrible metric to evaluate the business value of the model.

# COMMAND ----------

training = spark.sql("Select * From ecommerce_demo.ratings_full Where month = 10")
testing = spark.sql("Select * From ecommerce_demo.ratings_full Where month = 11")

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
import time

def run_als(als, training, testing, name, i):
  model = als.fit(training)  
  predictions = model.transform(testing)
  evaluator = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol="prediction")
  mae = evaluator.evaluate(predictions)
  print("{}={} in {} seconds, MAE = {}".format(name, i, int(time.time() - t), round(mae, 3)))
  return predictions

# COMMAND ----------

# MAGIC %md
# MAGIC - The user average behavior in October is a poor predictor of November (people who buy in October don't buy in November).
# MAGIC - The category average in October is a good predictor of November (popular categories in October are still popular in November).

# COMMAND ----------

for col in ['globalAvg', 'userAvg', 'categoryAvg']:
  evaluator = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol=col)
  mae = evaluator.evaluate(testing)
  print(col, round(mae, 3))

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize regParam:
# MAGIC - First span broad range using exp technique: [1, 3, 10, 30, 100...]
# MAGIC - Then use a finer range

# COMMAND ----------

for i in [0.01, 0.03, 0.1, 0.3, 1, 3, 10]:  
  t = time.time()
  als = ALS(rank=10, maxIter=10, regParam=i, userCol="user", itemCol="category", ratingCol="rating", coldStartStrategy="drop")
  run_als(als, training, testing, 'regParam', i)

# COMMAND ----------

for i in [0.2, 0.3, 0.4, 0.5]:  
  t = time.time()
  als = ALS(rank=10, maxIter=10, regParam=i, userCol="user", itemCol="category", ratingCol="rating", coldStartStrategy="drop")
  run_als(als, training, testing, 'regParam', i)

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize maxIter

# COMMAND ----------

for i in [1, 5, 10, 20, 40]:  
  t = time.time()
  als = ALS(rank=10, maxIter=i, regParam=0.4, userCol="user", itemCol="category", ratingCol="rating", coldStartStrategy="drop")
  run_als(als, training, testing, 'maxIter', i)

# COMMAND ----------

# MAGIC %md
# MAGIC Optimize rank

# COMMAND ----------

for i in [10, 20, 50, 100]:  
  t = time.time()
  als = ALS(rank=i, maxIter=20, regParam=0.4, userCol="user", itemCol="category", ratingCol="rating", coldStartStrategy="drop")
  run_als(als, training, testing, 'rank', i)

# COMMAND ----------

# MAGIC %md
# MAGIC Optimized model has parameters: rank=50, maxIter=20, regParam=0.4<br>
# MAGIC The performance (based on MAE) of various models are:
# MAGIC - Global average: 0.943
# MAGIC - Based on category average: 0.830
# MAGIC - Based on ALS: 0.779
# MAGIC 
# MAGIC ALS provides a significant improvement over a model that would recommend the top categories.

# COMMAND ----------

# MAGIC %md
# MAGIC #Use the final model to predict products for a user
# MAGIC - The recommendForAllUsers(10) function will give, for each user, the top 10 category recommended based on October activity
# MAGIC - We save the results in table **ecommerce_demo.recommendations_categories**

# COMMAND ----------

als = ALS(rank=50, maxIter=20, regParam=0.4, userCol="user", itemCol="category", ratingCol="rating", coldStartStrategy="drop")
finalModel = als.fit(training)
finalModel.recommendForAllUsers(10).registerTempTable('userRecommendations')

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace Table ecommerce_demo.recommendations Using Delta AS
# MAGIC (Select user,
# MAGIC        recommendations.category,
# MAGIC        recommendations.rating,
# MAGIC        B.category_name
# MAGIC From (Select user, explode(recommendations) as recommendations
# MAGIC       From userRecommendations) A
# MAGIC Left Join categories B ON (recommendations.category = B.category));
# MAGIC 
# MAGIC OPTIMIZE ecommerce_demo.recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC #Productionalize model with MLFlow (not shown here)
# MAGIC - Track model results with [MLFlow tracking](https://www.mlflow.org/docs/latest/tracking.html#tracking)
# MAGIC - Package code with [MLFlow Projects](https://www.mlflow.org/docs/latest/projects.html#projects)
# MAGIC - Manage models, deploy and model serving with [MLFlow models](https://www.mlflow.org/docs/latest/models.html#models)
# MAGIC 
# MAGIC ##For a real company
# MAGIC - More complex models that  take more business constraints into account (price, churn, time spent on website...)
# MAGIC - Real-time recommendations during a user session
# MAGIC - Take time into account with sequence models (deep learning)
# MAGIC - Add marketing campaigns
# MAGIC - A/B testing

# COMMAND ----------

