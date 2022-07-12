# Databricks notebook source
import pandas as pd
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Reading in workers compensation data from csv file uploaded in local file store.
spark_df_local_csv = spark.read.csv("/FileStore/tables/NY_workers_compensation.csv", header=True, inferSchema=True)
display(spark_df_local_csv)

# COMMAND ----------

# Checking number of records in spark df read from local csv
spark_df_local_csv.count()

# COMMAND ----------

# Connecting to API using requests
r = requests.get('https://data.ny.gov/resource/jshw-gkgu.json')
r.status_code

# COMMAND ----------

# Reading csv directly from API using pandas
df = pd.DataFrame(workers_compensation)
df

# COMMAND ----------

len(df)

# COMMAND ----------

# Using sodapy to call API. Returns json - adjust timeout and limit to control number of records returned. Requires App Token from Socrata.
from sodapy import Socrata

client = Socrata("data.ny.gov",
                  <insert App Token>, timeout=5000000)

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("jshw-gkgu", limit=5000000)

# Convert to pandas DataFrame
spark_df = spark.createDataFrame(results)

# COMMAND ----------

display(spark_df)

# COMMAND ----------

spark_df.count()

# COMMAND ----------

selected_cols = ["Accident", "Accident Date", "Age at Injury", "Assembly date", "Attorney/Representative", "Average Weekly Wage", "Birth Year", "Carrier Name", "Carrier Type", "Claim Identifier", "Claim Injury Type", "Claim Type", "Closed Count", "County of Injury", "District Name", "Gender", "Medical Fee Region", "Occupational Disease", "WCIO Cause of Injury Description", "WCIO Nature of Injury Description", "WCIO Part Of Body Description", "Zip Code"]
df_selected_cols = df.select(selected_cols)

# COMMAND ----------

from pyspark.sql.functions import *

df_selected_cols = (df_selected_cols
                    .withColumn("Accident Date",to_date("Accident Date", "MM/dd/yyyy"))
                    .withColumn("Assembly date",to_date("Assembly Date", "MM/dd/yyyy"))
                    )


