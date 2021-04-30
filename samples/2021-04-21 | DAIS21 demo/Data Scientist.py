# Databricks notebook source
df = table("ecommerce_demo.user_features").toPandas()

# COMMAND ----------

print(len(df))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data prep for modelling 

# COMMAND ----------

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score

## Select features
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

## Select balanced sample of buyers and non-buyers of the target_category
df_select = df[df[target] == 1].append(df[df[target] == 0].sample(np.sum(df[target] == 1)))
print(len(df_select))

# COMMAND ----------

# MAGIC %md
# MAGIC # First model

# COMMAND ----------

X, Y = df_select[features], df_select[target]
model = LogisticRegression(random_state=0)
print(np.mean(cross_val_score(model, 0*X, Y, cv=5)))
print(np.mean(cross_val_score(model, X, Y, cv=5)))

# COMMAND ----------

clf = model.fit(X,Y)
for i in range(len(features)):
  print(features[i], np.sum(df_select[features[i]]), clf.coef_[0][i])

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature selection with MLFlow

# COMMAND ----------

import mlflow
import mlflow.sklearn
from operator import itemgetter

def run(features_select, target, run_name, experiment_id):
  with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    X, Y = df_select[features_select], df_select[target]
    score = np.mean(cross_val_score(model, X, Y, cv=5))
    mlflow.log_metric("CV score", score)
    clf = model.fit(X, Y)
    feature_importance = sorted([[features_select[i], abs(np.sum(df_select[features_select[i]]) * clf.coef_[0][i])] for i in range(len(features_select))], key = itemgetter(1), reverse=True)
    mlflow.sklearn.log_model(model, "Logistic Regression")
    mlflow.log_dict([[x[0], f"importance = {str(x[1])}"] for x in feature_importance], "feature_importance.yml")
    mlflow.end_run()
  return feature_importance
  
# experiment_id = mlflow.create_experiment("/Users/francois.callewaert@databricks.com/Headphone purchase prediction - Feature selection")
model = LogisticRegression(random_state=0)
features_select = features
feature_importance = run(features_select, target, "All features", experiment_id)
while len(features_select) > 1:
  features_select = [x[0] for x in feature_importance[:-1]]
  feature_importance = run(features_select, target, f"Removing {feature_importance[-1][0]}", experiment_id)

# COMMAND ----------

mlflow.delete_experiment(experiment_id)

# COMMAND ----------

print("Name: {}".format(experiment.name))
print("Experiment_id: {}".format(experiment.experiment_id))
print("Artifact Location: {}".format(experiment.artifact_location))
print("Tags: {}".format(experiment.tags))
print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))

# COMMAND ----------


