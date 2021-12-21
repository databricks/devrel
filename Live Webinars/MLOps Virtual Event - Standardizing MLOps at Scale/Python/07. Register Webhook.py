# Databricks notebook source
model_name = 'Airbnb_Model'
cmr_host = "https://e2-demo-west.cloud.databricks.com"
cmr_token = dbutils.secrets.get(scope='mlops_webinar',key='CMR-token')
auth_header = {"Authorization": f"Bearer {cmr_token}"}

# COMMAND ----------

# MAGIC %md ## List Existing WebHooks

# COMMAND ----------

import json
import requests

list_endpoint = f"{cmr_host}/api/2.0/mlflow/registry-webhooks/list"
list_webhook_params = {
  'model_name': model_name
}
response = requests.get( list_endpoint, headers=auth_header, data=json.dumps(list_webhook_params) )
response.content

# COMMAND ----------

# MAGIC %md ## Create a new WebHook

# COMMAND ----------

create_webhook_endpoint = f"{cmr_host}/api/2.0/mlflow/registry-webhooks/create"

job_id = 109624
releases_job_spec = {
  'job_id': job_id,
  'access_token': cmr_token
}
create_webhook_doc = {
  'model_name': model_name,
  'events': 'MODEL_VERSION_TRANSITIONED_STAGE',
  'description': f"{model_name} CI-CD WebHook",
  'job_spec': releases_job_spec
}
response = requests.post( create_webhook_endpoint, headers=auth_header, data=json.dumps(create_webhook_doc) )
response.content

# COMMAND ----------

# MAGIC %md ## Test WebHook

# COMMAND ----------

test_webhook_endpoint = f"{cmr_host}/api/2.0/mlflow/registry-webhooks/test"

test_webhook_doc = {
  'id': 'b1658054e6a845349da9b71d3777c033',
  'event': 'MODEL_VERSION_TRANSITIONED_STAGE'
}
response = requests.post( test_webhook_endpoint, headers=auth_header, data=json.dumps(test_webhook_doc) )
response.content

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete a WebHook

# COMMAND ----------

delete_webhook_endpoint = f"{cmr_host}/api/2.0/mlflow/registry-webhooks/delete"

del_webhook_doc = { 'id': '39492f30cb744c5d87e2f303ad460bb5' }
response = requests.delete( delete_webhook_endpoint, headers=auth_header, data=json.dumps(del_webhook_doc) )
response.content

# COMMAND ----------


