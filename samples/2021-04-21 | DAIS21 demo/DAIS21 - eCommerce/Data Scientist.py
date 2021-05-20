# Databricks notebook source
# MAGIC %md
# MAGIC # Data science
# MAGIC 1. We start from user actions with the top 10 categories.
# MAGIC 2. We pick a category: electronics.audio.headphone.
# MAGIC 3. We want to predict whether a user is a potential headphone buyer based on his/her actions with the other categories.

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Data prep

# COMMAND ----------

# MAGIC %md
# MAGIC Define broad pool of features, and one feature to predict

# COMMAND ----------

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score

## Select features (10 categories * 3 actions = 30 features)
categories = ['electronics.smartphone',
             'electronics.audio.headphone',
             'electronics.video.tv',
             'electronics.clocks',
             'appliances.kitchen.washer',
             'computers.notebook',
             'appliances.environment.vacuum',
             'appliances.kitchen.refrigerators',
             'apparel.shoes',
             'electronics.tablet']
category_event = []
for cat in categories:
  category_event += [f'{cat}_purchase', f'{cat}_cart', f'{cat}_view']

## Feature to predict
target_category = 'electronics.audio.headphone'
target = f'{target_category}_purchase'
features = [x for x in category_event if target_category not in x]

# COMMAND ----------

# MAGIC %md
# MAGIC Select desired columns and convert to Pandas dataframe

# COMMAND ----------

cat_str = '`, `'.join(category_event)
df = spark.sql(f"Select user_id, `{cat_str}` From ecommerce_demo.user_features").toPandas()
print(len(df))

# COMMAND ----------

# MAGIC %md
# MAGIC Select balanced sample of buyers and non-buyers of the target_category

# COMMAND ----------

df_select = df[df[target] == 1].append(df[df[target] == 0].sample(np.sum(df[target] == 1)))
print(len(df_select))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. First model
# MAGIC We use Logistic regression. 

# COMMAND ----------

X, Y = df_select[features], df_select[target]
model = LogisticRegression(random_state=0)
print(f'Random guess score = {round(np.mean(cross_val_score(model, 0*X, Y, cv=5)), 3)}')
print(f'Model score = {round(np.mean(cross_val_score(model, X, Y, cv=5)), 3)}')
print(f'Number of features = {len(features)}')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Feature selection with MLFlow tracking
# MAGIC We use recursive feature elimination. We use MLFlow to save the model and track its performance atmosphere  each iteration.

# COMMAND ----------

experiment_id = mlflow.create_experiment("/Users/francois.callewaert@databricks.com/Headphone purchase prediction - Feature selection")

# COMMAND ----------

import mlflow
import mlflow.sklearn
from operator import itemgetter

def run(features_select, target, run_name, experiment_id):
  """1. Run cross-validation based on features_select
     2. Log model characteristics
     3. Return list of features sorted by importance"""
  ## Log run name
  with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    ## Finding score
    X, Y = df_select[features_select], df_select[target]
    score = np.mean(cross_val_score(model, X, Y, cv=5))
    
    ## feature importance
    clf = model.fit(X, Y)
    feature_importance = [[features_select[i], 
                           clf.coef_[0][i],
                           abs(np.sum(df_select[features_select[i]]) * clf.coef_[0][i])] for i in range(len(features_select))]
    feature_importance_sorted = sorted(feature_importance, key = itemgetter(2), reverse=True)
    
    ## Logging score, model and importance
    mlflow.log_metric("CV score", score)
    mlflow.sklearn.log_model(model, "Logistic Regression")
    mlflow.log_dict([[x[0], 
                      f"coefficient = {str(x[1])}",
                      f"importance = {str(x[2])}"] for x in feature_importance_sorted], "feature_importance.yml")
    mlflow.end_run()
  return feature_importance_sorted
  
  
model = LogisticRegression(random_state=0)
features_select = features
feature_importance_sorted = run(features_select, target, "All features", experiment_id)
while len(features_select) > 1:
  features_select = [x[0] for x in feature_importance_sorted[:-1]]
  feature_removed = feature_importance_sorted[-1][0]
  feature_importance_sorted = run(features_select, target, f"Removing {feature_removed}", experiment_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Final model
# MAGIC Features from .yml file are: [electronics.clocks_view, electronics.smartphone_purchase]

# COMMAND ----------

model_uri = 'runs:/37752b08ff754f53b149581537aad160/Logistic Regression'
final_model = mlflow.sklearn.load_model(model_uri)
print(f"A user who viewed a Smart Watch (electronics.clocks) and purchased a smartphone has a {round(final_model.predict_proba([[1, 1]])[0][1] * 100)}% probablility to be a headphone buyer.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Other MLFlow features
# MAGIC 1. MLFlow Projects
# MAGIC 2. MLFlow Model
# MAGIC 3. MLFlow Model Registry
# MAGIC 4. More to come...

# COMMAND ----------


