# Databricks notebook source
# MAGIC %md ## Receive and parse event message

# COMMAND ----------

import json
 
dbutils.widgets.text("event_message",'{}')
event_message = dbutils.widgets.get("event_message")
event_message_dict = json.loads(event_message)

# COMMAND ----------

event_message_dict

# COMMAND ----------

# MAGIC %md ## Functions to trigger pipelines

# COMMAND ----------

import requests
import base64

org_id = 'tristannixon-databricks'
project_id = 'MLOps%20Webinar'
devops_token = dbutils.secrets.get(scope='mlops_webinar',key='Devops_Token')

releases_uri = f"https://vsrm.dev.azure.com/{org_id}/{project_id}/_apis/release/releases?api-version=6.0"
encoded_token = base64.b64encode(bytes(f":{devops_token}", 'utf-8')).decode("utf-8")
devops_auth = {'Authorization': f"Basic {encoded_token}",
               'Content-Type': 'application/json'}

def trigger_release_pipeline( pipeline_def_id ):
  create_release_doc = { 'definitionId': pipeline_def_id }
  response = requests.post(releases_uri, headers=devops_auth, data = json.dumps(create_release_doc) )
  return response.content

# COMMAND ----------

# MAGIC %md ## Trigger Test pipeline

# COMMAND ----------

test_pipeline_id = 2

if ( event_message_dict['event'] == 'MODEL_VERSION_TRANSITIONED_STAGE' ) \
   and \
   ( event_message_dict['to_stage'] == 'Staging' ):
  print("running test pipeline")
  results = trigger_release_pipeline( test_pipeline_id )
  print(results)

# COMMAND ----------

# MAGIC %md ## Trigger Prod pipeline

# COMMAND ----------

prod_pipeline_id = 3

if ( event_message_dict['event'] == 'MODEL_VERSION_TRANSITIONED_STAGE' ) \
   and \
   ( event_message_dict['to_stage'] == 'Production' ):
  print("running prod pipeline")
  results = trigger_release_pipeline( prod_pipeline_id )
  print(results)
