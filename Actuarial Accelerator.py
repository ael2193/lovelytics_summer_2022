# Databricks notebook source
import pandas as pd
import requests

# COMMAND ----------

# Reading in workers compensation data from csv file uploaded in local file store.

# df = spark.read.csv("/FileStore/tables/NY_workers_compensation.csv", header=True, inferSchema=True)

# COMMAND ----------

# Connecting to API using requests
# r = requests.get('https://data.ny.gov/resource/jshw-gkgu.json')
# workers_compensation = r.json()

# COMMAND ----------

# Reading csv directly from API using pandas
df = pd.read_csv('https://data.ny.gov/resource/jshw-gkgu.csv')

# COMMAND ----------

# Converting pandas df to spark df
spark_df = spark.createDataFrame(df)

# COMMAND ----------

spark_df.collect()

# COMMAND ----------

selected_cols = ["Accident", "Accident Date", "Age at Injury", "Assembly date", "Attorney/Representative", "Average Weekly Wage", "Birth Year", "Carrier Name", "Carrier Type", "Claim Identifier", "Claim Injury Type", "Claim Type", "Closed Count", "County of Injury", "District Name", "Gender", "Medical Fee Region", "Occupational Disease", "WCIO Cause of Injury Description", "WCIO Nature of Injury Description", "WCIO Part Of Body Description", "Zip Code"]
df_selected_cols = df.select(selected_cols)

# COMMAND ----------

from pyspark.sql.functions import *

df_selected_cols = (df_selected_cols
                    .withColumn("Accident Date",to_date("Accident Date", "MM/dd/yyyy"))
                    .withColumn("Assembly date",to_date("Assembly Date", "MM/dd/yyyy"))
                    )



# COMMAND ----------

display(df_selected_cols)

# COMMAND ----------


