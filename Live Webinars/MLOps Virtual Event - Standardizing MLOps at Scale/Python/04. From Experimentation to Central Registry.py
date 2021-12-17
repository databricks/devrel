# Databricks notebook source
from mlflow.tracking import MlflowClient
import mlflow 

expirement_location = "/Repos/tristan.nixon@databricks.com/mlops_webinar/Python/03. Auto_ML Baseline"

mlflow.set_experiment(expirement_location)
mlflow_experiments = mlflow.get_experiment_by_name(expirement_location)


# COMMAND ----------

client = MlflowClient()
experiment_id = mlflow_experiments.experiment_id
experiment_df = spark.read.format("mlflow-experiment").load(experiment_id)
experiment_df.createOrReplaceTempView("airbnb_model_experiment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the latest HyperOpt Run

# COMMAND ----------

latest_hyperopt_run = spark.sql("""
select
  run_id,
  end_time
from
  airbnb_model_experiment
where
  tags['mlflow.parentRunId'] is null
  and
  status = 'FINISHED'
  and
  end_time = 
    ( select
        max(end_time) as last_run_time
      from 
        airbnb_model_experiment
      where
        tags['mlflow.parentRunId'] is null
        and
        status = 'FINISHED' )
""")
hyperopt_run_id = latest_hyperopt_run.collect()[0]['run_id']

# COMMAND ----------

hyperopt_run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the best model from the experiment

# COMMAND ----------

best_run = spark.sql(f"""
select
  experiment_id,
  run_id,
  metrics['loss'] as loss,
  artifact_uri
from
  airbnb_model_experiment
where
  tags['mlflow.parentRunId'] = '{hyperopt_run_id}'
  and
  status = 'FINISHED'
  and
  metrics['loss'] = 
    ( select
        min(metrics['loss']) as min_loss
      from
        airbnb_model_experiment
      where
        where tags['mlflow.parentRunId'] = '{hyperopt_run_id}'
        and
        status = 'FINISHED' )
""")
display(best_run)

# COMMAND ----------

selected_experiment_row = best_run.first()
selected_experiment_id = selected_experiment_row['experiment_id']
selected_run_id = selected_experiment_row['run_id']
selected_model_loss = selected_experiment_row['loss']
selected_artifact_uri = selected_experiment_row['artifact_uri']

print(f"Selected experiment ID: {selected_experiment_id}")
print(f"Selected run ID: {selected_run_id}")
print(f"Selected model loss: {selected_model_loss}")
print(f"Selected artifact URI: {selected_artifact_uri}")


# COMMAND ----------

# MAGIC %md
# MAGIC   <img src="https://docs.microsoft.com/en-us/azure/databricks/_static/images/mlflow/multiworkspace.png" width="45%"/>
# MAGIC   
# MAGIC   [Central Model Registry](https://docs.databricks.com/applications/machine-learning/manage-model-lifecycle/multiple-workspaces.html)

# COMMAND ----------

registry_uri = 'databricks://mlops_webinar:CMR'
model_name = 'Airbnb_Model'
selected_artifact_path = 'model'
mlflow.set_registry_uri(registry_uri)
model_uri = f"runs:/{selected_run_id}/{selected_artifact_path}"
mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



client = MlflowClient(tracking_uri=None, registry_uri=registry_uri)

registered_models = client.list_registered_models()
for model in registered_models:
  print ("Model Name: {}".format(model.name))

# COMMAND ----------

client = MlflowClient(tracking_uri=None, registry_uri=registry_uri)
model_names = [m.name for m in client.list_registered_models() if m.name.startswith('Air')]
print(model_names)

# COMMAND ----------

client.update_registered_model(model_name, description='For ranking')


# COMMAND ----------

client.transition_model_version_stage(model_name, 1, 'Staging')
client.get_model_version(model_name, 1)

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
experiment_id = '3181692072700047'
runs_df = mlflow.search_runs(experiment_id)

display(runs_df)

# COMMAND ----------

df_client = spark.read.format("mlflow-experiment").load("3181692072700047")
df_client.createOrReplaceTempView("airbnb_model")

df_model_selector = (spark.sql("""
select experiment_id, run_id, end_time, metrics.training_r2_score as R2, metrics.training_rmse as RMSE, metrics.training_mae as MAE, CONCAT(artifact_uri,'/log-model') as artifact_uri
FROM 
airbnb_model
WHERE status = 'FINISHED' and metrics.training_rmse IS NOT NULL
ORDER BY RMSE 
limit 1
""")
)
display(df_model_selector)

# COMMAND ----------


