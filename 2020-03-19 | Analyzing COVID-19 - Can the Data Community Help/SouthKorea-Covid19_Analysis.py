# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ##Covid-19 South Korea Analysis
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 3px;">
# MAGIC   <img src="https://miro.medium.com/max/3840/1*Mf9K7Nj-wMlZHqt4cSOWNA.jpeg"></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Analyzing South Korea COVID-19 Pandemic Data
# MAGIC In this notebook we will try to understand the patterns underlying the Coronavirus pandemic in South Korea. We will use the freely available outbreak data availableon Kaggle to answer questions like:
# MAGIC 
# MAGIC - South Korea Regions affected by the virus 
# MAGIC - What are the causes of Covid-19 infection
# MAGIC - Timeline trend of the Accumulated Cases
# MAGIC - Recovered Patients in each region
# MAGIC - Fatalities by Region
# MAGIC - % of Recovered Patient cases by type of Infection
# MAGIC - % of Fatality cases by type Infection

# COMMAND ----------

# MAGIC %md
# MAGIC ###The Data
# MAGIC Data is taken from a public repo : A kaggle data source

# COMMAND ----------

# DBTITLE 1,Load the Data for Analysis
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS patient_csv;
# MAGIC 
# MAGIC CREATE TABLE patient_csv
# MAGIC USING csv
# MAGIC OPTIONS (path "/databricks-datasets/COVID/SouthKorea/patient.csv", header "true")

# COMMAND ----------

# DBTITLE 1,Patient Data Exploration
# MAGIC %sql
# MAGIC --Lets look at the patient data to get ourselves familiarize with the schema and data 
# MAGIC select * from patient_csv limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ###No. of patients in each region
# MAGIC It tells us which regions experienced most number of patients

# COMMAND ----------

# MAGIC %sql
# MAGIC select region, count(patient_id) from patient_csv where region is not null group by region order by count(patient_id) desc

# COMMAND ----------

# MAGIC %md
# MAGIC ###Infection reasons for the patients
# MAGIC There are many reason for infection but most cases have similar reasons that they have visited to wuhan. 
# MAGIC You can see a trend each month separated by different pie charts below. Since January when the cases actually started from the church gathering, then the contact with other people and eventually more cases in the following months by various reasons of infection - mainly during from differnt countries and coming in contact with regions outside the original source.
# MAGIC This insight also highlights how the cases spread globally.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT infection_reason, month(cast(confirmed_date as date)) AS month, count(patient_id) FROM patient_csv where infection_reason is not null
# MAGIC group by infection_reason, month order by month

# COMMAND ----------

# MAGIC %md
# MAGIC ####Timeline trend of the Accumulated Cases
# MAGIC The graph below shows the timeline on the increase in number of patients from January 2020 with highest spikes of no. of patients ~1062 around end of February - beginnning of March 2020.

# COMMAND ----------

# MAGIC %sql
# MAGIC Use covid;
# MAGIC select confirmed_date, count(patient_id) from covid.patient_csv where confirmed_date is not null group by confirmed_date order by confirmed_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Recovered Patients in each region
# MAGIC Looks like "capital area" has the highest number of recovered patients.

# COMMAND ----------

# MAGIC %sql
# MAGIC select region, datediff(released_date, confirmed_date) as recovery_time, count(patient_id) from patient_csv  where released_date is not null group by region, recovery_time order by count(patient_id) desc

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fatalities by Region
# MAGIC There are 3 regions which got impacted significantly in terms of fatalities. This correlates to the actual situations we heard in the news where on 19 February 2020, cases in South Korea had a sudden jump from a gathering at a Shincheonji Church.

# COMMAND ----------

# MAGIC %sql
# MAGIC select region, datediff(deceased_date, confirmed_date) as fatality_time, count(patient_id) from patient_csv  where deceased_date is not null group by region, fatality_time order by count(patient_id) desc

# COMMAND ----------

# MAGIC %md
# MAGIC ###% of Recovered Patient cases by type of Infection 
# MAGIC This graph shows the recovery percentage of patient from different types of infections. 40% of total recovered were infected by patient contact and second most recovery was from the patients who visited wuhan.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select infection_reason, datediff(released_date, confirmed_date) as recovery_time, count(patient_id) from patient_csv where released_date is not null group by infection_reason, recovery_time order by recovery_time desc

# COMMAND ----------

# MAGIC %md
# MAGIC ###% of Fatality cases by type Infection
# MAGIC This graph shows the recovery percentage of deaths from different types of infections. It mainly shows the fatalities with "unknown reason". We can assmue a lot of factors for fatalities but we may need more data or insights.

# COMMAND ----------

# MAGIC %sql
# MAGIC select infection_reason, datediff(deceased_date, confirmed_date) as fatality, count(patient_id) from patient_csv where deceased_date is not null group by infection_reason, fatality order by fatality desc

# COMMAND ----------

# MAGIC %md
# MAGIC Lets find more insights if time plays a factor in the recovery of patients

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Below shows the number of patients with duration from the confirmed date to the deceased date. On an average, they spent 3 days since confirmation date.
# MAGIC select infection_reason, state, datediff(deceased_date, confirmed_date) as fatality, count(patient_id) from patient_csv where deceased_date is not null group by infection_reason, state, fatality order by fatality 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Below shows the number of patients with duration from the confirmed date to the recovery date. On an average, the patients who spent more days since confirmation, were recovered.
# MAGIC 
# MAGIC select infection_reason, state, datediff(released_date, confirmed_date) as recovery, count(patient_id) from patient_csv where released_date is not null group by infection_reason, state, recovery order by recovery 
