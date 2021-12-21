# Databricks notebook source
# MAGIC %md
# MAGIC # NOTE - YES! Everything Below is Auto-Generated!!

# COMMAND ----------

# MAGIC %md
# MAGIC # Random Forest Regressor training
# MAGIC This is an auto-generated notebook. To reproduce these results, attach this notebook to the **digancluster** cluster and rerun it.
# MAGIC - Compare trials in the [MLflow experiment](#mlflow/experiments/886408696114747/s?orderByKey=metrics.%60val_rmse%60&orderByAsc=true)
# MAGIC - Navigate to the parent notebook [here](#notebook/886408696114737) (If you launched the AutoML experiment using the Experiments UI, this link isn't very useful.)
# MAGIC - Clone this notebook into your project folder by selecting **File > Clone** in the notebook toolbar.
# MAGIC 
# MAGIC Runtime Version: _10.1.x-cpu-ml-scala2.12_

# COMMAND ----------

import mlflow
import databricks.automl_runtime

# Use MLflow to track experiments
#mlflow.set_experiment("/Users/digan.parikh@databricks.com/databricks_automl/price_airbnb_features-2021_11_07")

target_col = "price"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# from mlflow.tracking import MlflowClient
# import os
# import uuid
# import shutil
# import pandas as pd

# # Create temp directory to download input data from MLflow
# input_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
# os.makedirs(input_temp_dir)


# # Download the artifact and read it into a pandas DataFrame
# input_client = MlflowClient()
# input_data_path = input_client.download_artifacts("ef61f60b65144ddb94deaaf169a19dc9", "data", input_temp_dir)

# df_loaded = pd.read_parquet(os.path.join(input_data_path, "training_data"))
# # Delete the temp data
# shutil.rmtree(input_temp_dir)

# # Preview data
# df_loaded.head(5)

# COMMAND ----------

df_loaded = spark.table("airbnb.features").drop("id").toPandas()

# Preview data
df_loaded.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select supported columns
# MAGIC Select only the columns that are supported. This allows us to train a model that can predict on a dataset that has extra columns that are not used in training.
# MAGIC `[]` are dropped in the pipelines.

# COMMAND ----------

from databricks.automl_runtime.sklearn.column_selector import ColumnSelector

supported_cols = ["availability_365", "calculated_host_listings_count", "minimum_nights", "neighbourhood_idx", "number_of_reviews", "reviews_per_month", "room_type_idx"]
col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

transformers = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical columns
# MAGIC 
# MAGIC Missing values for numerical columns are imputed with mean for consistency

# COMMAND ----------

from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputer", SimpleImputer(strategy="mean"))
])

transformers.append(("numerical", numerical_pipeline, ["availability_365", "calculated_host_listings_count", "minimum_nights", "neighbourhood_idx", "number_of_reviews", "reviews_per_month"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Low-cardinality categoricals
# MAGIC Convert each low-cardinality categorical column into multiple binary columns through one-hot encoding.
# MAGIC For each input categorical column (string or numeric), the number of output columns is equal to the number of unique values in the input column.

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

one_hot_encoder = OneHotEncoder(handle_unknown="ignore")

transformers.append(("onehot", one_hot_encoder, ["room_type_idx"]))

# COMMAND ----------

from sklearn.compose import ColumnTransformer

preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature standardization
# MAGIC Scale all feature columns to be centered around zero with unit variance.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

standardizer = StandardScaler()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC Split the input data into 3 sets:
# MAGIC - Train (60% of the dataset used to train the model)
# MAGIC - Validation (20% of the dataset used to tune the hyperparameters of the model)
# MAGIC - Test (20% of the dataset used to report the true performance of the model on an unseen dataset)

# COMMAND ----------

from sklearn.model_selection import train_test_split

split_X = df_loaded.drop([target_col], axis=1)
split_y = df_loaded[target_col]

# Split out train data
X_train, split_X_rem, y_train, split_y_rem = train_test_split(split_X, split_y, train_size=0.6, random_state=600418151)

# Split remaining data equally for validation and test
X_val, X_test, y_val, y_test = train_test_split(split_X_rem, split_y_rem, test_size=0.5, random_state=600418151)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train regression model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/886408696114747/s?orderByKey=metrics.%60val_rmse%60&orderByAsc=true)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor

help(RandomForestRegressor)

# COMMAND ----------

import mlflow
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline

set_config(display='diagram')

skrf_regressor = RandomForestRegressor(
  bootstrap=True,
  criterion="friedman_mse",
  max_depth=9,
  max_features=0.7451800710468635,
  min_samples_leaf=0.09789282591233368,
  min_samples_split=0.2060880699247218,
  n_estimators=10,
  random_state=600418151,
)

model = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
    ("standardizer", standardizer),
    ("regressor", skrf_regressor),
])

model

# COMMAND ----------

import pandas as pd

# Enable automatic logging of input samples, metrics, parameters, and models
mlflow.sklearn.autolog(log_input_examples=True, silent=True)

with mlflow.start_run(run_name="random_forest_regressor") as mlflow_run:
    model.fit(X_train, y_train)
    
    # Training metrics are logged by MLflow autologging
    # Log metrics for the validation set
    skrf_val_metrics = mlflow.sklearn.eval_and_log_metrics(model, X_val, y_val, prefix="val_")

    # Log metrics for the test set
    skrf_test_metrics = mlflow.sklearn.eval_and_log_metrics(model, X_test, y_test, prefix="test_")

    # Display the logged metrics
    skrf_val_metrics = {k.replace("val_", ""): v for k, v in skrf_val_metrics.items()}
    skrf_test_metrics = {k.replace("test_", ""): v for k, v in skrf_test_metrics.items()}
    display(pd.DataFrame([skrf_val_metrics, skrf_test_metrics], index=["validation", "test"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance
# MAGIC 
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot
# MAGIC of the relationship between features and model output. Features are ranked in descending order of
# MAGIC importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC - Generating SHAP feature importance is a very memory intensive operation, so to ensure that AutoML can run trials without
# MAGIC   running out of memory, we disable SHAP by default.<br />
# MAGIC   You can set the flag defined below to `shap_enabled = True` and re-run this notebook to see the SHAP plots.
# MAGIC - To reduce the computational overhead of each trial, a single example is sampled from the validation set to explain.<br />
# MAGIC   For more thorough results, increase the sample size of explanations, or provide your own examples to explain.
# MAGIC - SHAP cannot explain models using data with nulls; if your dataset has any, both the background data and
# MAGIC   examples to explain will be imputed using the mode (most frequent values). This affects the computed
# MAGIC   SHAP values, as the imputed samples may not match the actual data distribution.
# MAGIC 
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).

# COMMAND ----------

# Set this flag to True and re-run the notebook to see the SHAP plots
shap_enabled = True

# COMMAND ----------

if shap_enabled:
    from shap import KernelExplainer, summary_plot
    # Sample background data for SHAP Explainer. Increase the sample size to reduce variance.
    train_sample = X_train.sample(n=min(100, len(X_train.index)))

    # Sample a single example from the validation set to explain. Increase the sample size and rerun for more thorough results.
    example = X_val.sample(n=1)

    # Use Kernel SHAP to explain feature importance on the example from the validation set.
    predict = lambda x: model.predict(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="identity")
    shap_values = explainer.shap_values(example, l1_reg=False)
    summary_plot(shap_values, example)

# COMMAND ----------

# MAGIC %md
# MAGIC # Hyperopt
# MAGIC 
# MAGIC Hyperopt is a Python library for "serial and parallel optimization over awkward search spaces, which may include real-valued, discrete, and conditional dimensions"... In simpler terms, its a python library for hyperparameter tuning. 
# MAGIC 
# MAGIC There are two main ways to scale hyperopt with Apache Spark:
# MAGIC * Use single-machine hyperopt with a distributed training algorithm (e.g. MLlib)
# MAGIC * Use distributed hyperopt with single-machine training algorithms with the SparkTrials class. 
# MAGIC 
# MAGIC Resources:
# MAGIC 0. [Documentation](http://hyperopt.github.io/hyperopt/scaleout/spark/)
# MAGIC 0. [Hyperopt on Databricks](https://docs.databricks.com/applications/machine-learning/automl/hyperopt/index.html)
# MAGIC 0. [Hyperparameter Tuning with MLflow, Apache Spark MLlib and Hyperopt](https://databricks.com/blog/2019/06/07/hyperparameter-tuning-with-mlflow-apache-spark-mllib-and-hyperopt.html)

# COMMAND ----------

# MAGIC %md
# MAGIC The basic steps when using Hyperopt are:
# MAGIC 
# MAGIC * Define an objective function to minimize. Typically this is the training or validation loss.
# MAGIC *  Define the hyperparameter search space. Hyperopt provides a conditional search space, which lets you compare different ML algorithms in the same run.
# MAGIC * Specify the search algorithm. Hyperopt uses stochastic tuning algorithms that perform a more efficient search of hyperparameter space than a deterministic grid search.
# MAGIC * Run the Hyperopt function fmin(). fmin() takes the items you defined in the previous steps and identifies the set of hyperparameters that minimizes the objective function.

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

model_name = 'Airbnb_Model'
def objective_function(params):
  with mlflow.start_run(nested=True):
    mlflow.autolog()
    
    est = int(params['n_estimators'])
    md = int(params['max_depth'])
    msl = int(params['min_samples_leaf'])
    mss = int(params['min_samples_split'])
    
    model=RandomForestRegressor(n_estimators=est,max_depth=md,min_samples_leaf=msl,min_samples_split=mss)
    
    model.fit(X_train, y_train)
    
    pred = model.predict(split_X_rem)
    
    rmse = mean_squared_error(split_y_rem, pred)
    
  return {"loss": rmse, "status": STATUS_OK}


from hyperopt import hp

params={
  'n_estimators': hp.uniform('n_estimators',100,500),
  'max_depth': hp.uniform('max_depth',5,20),
  'min_samples_leaf': hp.uniform('min_samples_leaf',1,5),
  'min_samples_split': hp.uniform('min_samples_split',2,6)
}

# COMMAND ----------

from math import factorial
from hyperopt import fmin, tpe, STATUS_OK, SparkTrials
import numpy as np

# set the parallelism of the search
cluster_nodes = 3
node_cores = 4
num_parallelism = min( (cluster_nodes * node_cores),
                       factorial(len(params)) )

# Creating a parent run
with mlflow.start_run():
  num_evals = 100 #max models to evaluate
  trials = SparkTrials(num_parallelism)
  best_hyperparam = fmin(fn=objective_function, 
                         space=params,
                         algo=tpe.suggest, 
                         max_evals=num_evals,
                         trials=trials)
  
  # Log param and metric for the best model
  for name, value in best_hyperparam.items():
    mlflow.log_param(name, value)
    
  mlflow.log_metric("loss", trials.best_trial["result"]["loss"])

# COMMAND ----------


