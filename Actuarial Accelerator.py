# Databricks notebook source
from sodapy import Socrata
from pyspark.sql.functions import *

# COMMAND ----------

# Reading in workers compensation data from csv file uploaded in local file store.
# spark_df_local_csv = spark.read.csv("/FileStore/tables/NY_workers_compensation.csv", header=True, inferSchema=True)

# Checking number of records in spark df read from local csv
# spark_df_local_csv.count()

# COMMAND ----------

# Writing spark_df created from local csv file to Delta table
# spark_df_local_csv.write.mode('overwrite').format('delta').save('dbfs/user/jade.qiu@lovelytics.com/')

# COMMAND ----------

# Connecting to API using requests
# r = requests.get('https://data.ny.gov/resource/jshw-gkgu.json')
# r.status_code

# COMMAND ----------

# Using sodapy to call API. Returns json - adjust timeout and limit to control number of records returned.

api_token = ''
client = Socrata("data.ny.gov", api_token, timeout=5000000)

# Selected columns
columns = 'accident_date, ancr_date, age_at_injury, average_weekly_wage, birth_year, carrier_name, claim_identifier, claim_injury_type, claim_type, closed_count, current_claim_status, gender, injured_in_county_name, wcio_cause_of_injury_code, wcio_cause_of_injury_desc, wcio_nature_of_injury_code, wcio_nature_of_injury_desc, wcio_pob_code, wcio_pob_desc, industry_code, industry_desc'

# First 5,000,000 results (i.e. all results), returned as JSON from API / converted to Python list of dictionaries by sodapy.
results = client.get("jshw-gkgu", limit = 5000000, select = columns)

# Convert to spark DataFrame
# spark_df = spark.createDataFrame(results)

# COMMAND ----------

display(spark_df)

# COMMAND ----------

# Changing column schemas

spark_df = (spark_df
            .withColumn("accident_date", regexp_replace(spark_df['accident_date'],"[A-Za-z]", " "))
            .withColumn("accident_date",to_date("accident_date"))
            .withColumn("ancr_date", regexp_replace(spark_df['ancr_date'],"[A-Za-z]", " "))
            .withColumn("ancr_date",to_date("ancr_date"))
            )

# COMMAND ----------

display(spark_df)

# COMMAND ----------

spark_df.na.drop().filter(spark_df.accident_date>=2000)

# COMMAND ----------

display(spark_df)

# COMMAND ----------

spark_df.write.mode('overwrite').format('delta').save('dbfs/user/jade.qiu@lovelytics.com/')

# COMMAND ----------

table_name = 'workers_compensation_bronze_table'
save_path = '/dbfs/dbfs/user/jade.qiu@lovelytics.com'
spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

client.close()
