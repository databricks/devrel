# Databricks notebook source
# MAGIC %md
# MAGIC ## Load parameters form MLFlow Registry

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

# set model & registry params
model_name = 'Airbnb_Model'
cmr_uri = 'databricks://mlops_webinar:CMR'
dev_uri = 'databricks://mlops_webinar:dev'

# fetch model from registry
client = MlflowClient(tracking_uri=dev_uri, registry_uri=cmr_uri)
reg_mdl = client.get_registered_model(model_name)

# COMMAND ----------

# find latest production model
prod_mdl = None
for mdl in reg_mdl.latest_versions:
  print(f"{mdl.name} (v. {mdl.version}) in {mdl.current_stage} is {mdl.status}")  
  if mdl.current_stage == "Production":
    prod_mdl = mdl
    
if prod_mdl is None:
  raise RuntimeError("No Production Model Found")

# COMMAND ----------

# get parameters for last production model
run = client.get_run(prod_mdl.run_id)

# extract logged parameters
current_params = run.data.params

print("Current Parameters:")
for param in current_params:
  print(f"{param} = {current_params[param]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

from sklearn.model_selection import train_test_split

target_col = "price"
all_data_df = spark.table("airbnb.features").drop("id").toPandas()
split_X = all_data_df.drop([target_col], axis=1)
split_y = all_data_df[target_col]

# Split out train data
X_train, split_X_rem, y_train, split_y_rem = train_test_split(split_X, split_y, train_size=0.6, random_state=600418151)

# Split remaining data equally for validation and test
X_val, X_test, y_val, y_test = train_test_split(split_X_rem, split_y_rem, test_size=0.5, random_state=600418151)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Model

# COMMAND ----------

import json
from databricks.automl_runtime.sklearn.column_selector import ColumnSelector

select_cols = json.loads(current_params['column_selector__cols'].replace("'", '"'))
col_selector = ColumnSelector(select_cols)

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputer", SimpleImputer(strategy=current_params['preprocessor__numerical__imputer__strategy']))
])
one_hot_encoder = OneHotEncoder(handle_unknown=current_params['preprocessor__onehot__handle_unknown'])

transformers = []
transformers.append(("numerical", numerical_pipeline, ["availability_365", "calculated_host_listings_count", "minimum_nights", "neighbourhood_idx", "number_of_reviews", "reviews_per_month"]))
transformers.append(("onehot", one_hot_encoder, ["room_type_idx"]))

preprocessor = ColumnTransformer(transformers, 
                                 remainder=current_params['preprocessor__remainder'], 
                                 sparse_threshold=int(current_params['preprocessor__sparse_threshold']))

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

standardizer = StandardScaler()

# COMMAND ----------

import mlflow
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor

set_config(display='diagram')

skrf_regressor = RandomForestRegressor(
  bootstrap=bool(current_params['regressor__bootstrap']),
  criterion=current_params['regressor__criterion'],
  max_depth=int(current_params['regressor__max_depth']),
  max_features=float(current_params['regressor__max_features']),
  min_samples_leaf=float(current_params['regressor__min_samples_leaf']),
  min_samples_split=float(current_params['regressor__min_samples_split']),
  n_estimators=int(current_params['regressor__n_estimators']),
  random_state=int(current_params['regressor__random_state']),
)

model = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
    ("standardizer", standardizer),
    ("regressor", skrf_regressor),
])

model

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Train with discovered parameters

# COMMAND ----------

import pandas as pd
import mlflow.sklearn

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
# MAGIC ## Automatically push model to registry

# COMMAND ----------

import time
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

artifact_name = "model"
artifact_uri = f"runs:/{mlflow_run.info.run_id}/{artifact_name}"
mlflow.set_registry_uri(cmr_uri)
registered_mdl = mlflow.register_model(artifact_uri, model_name)

# Wait until the model is ready
def wait_until_ready(model_name, model_version):
  client = MlflowClient(registry_uri=cmr_uri)
  for _ in range(20):
    model_version_details = client.get_model_version(
      name=model_name,
      version=model_version,
    )
    status = ModelVersionStatus.from_string(model_version_details.status)
    print("Model status: %s" % ModelVersionStatus.to_string(status))
    if status == ModelVersionStatus.READY:
      break
    time.sleep(5)

wait_until_ready(registered_mdl.name, registered_mdl.version)

# COMMAND ----------


