# Databricks notebook source
df = spark.read.csv("/FileStore/tables/NY_workers_compensation.csv", header=True, inferSchema=True)

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


