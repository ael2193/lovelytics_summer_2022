# Databricks notebook source
from sodapy import Socrata
from pyspark.sql.functions import *

# COMMAND ----------

# Reading in workers compensation data from csv file uploaded in local file store.
spark_df = spark.read.csv("/FileStore/tables/NY_workers_compensation.csv", header=True, inferSchema=True)

# COMMAND ----------

# Checking number of records in spark df read from local csv
spark_df.count()

# COMMAND ----------

for col in spark_df.columns:
  spark_df = spark_df.withColumnRenamed(col, col.replace(" ", "_"))
for col in spark_df.columns: 
  spark_df = spark_df.withColumnRenamed(col, col.replace("Description", "desc"))
for col in spark_df.columns:
  spark_df = spark_df.withColumnRenamed(col, col.replace("Part_Of_Body", "pob"))
for col in spark_df.columns:
  spark_df = spark_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

spark_df = (spark_df.withColumnRenamed('accident', 'accident_ind')
            .withColumnRenamed('attorney/representative', 'atty_rep_ind')
            .withColumnRenamed('c-2_date', 'c2_date')
            .withColumnRenamed('c-3_date', 'c3_date')
            .withColumnRenamed('covid-19_indicator', 'covid_19_indicator')
            .withColumnRenamed('county_of_injury', 'injured_in_county_name')
            .withColumnRenamed('ime-4_count', 'ime4_count')
            .withColumnRenamed('industry_code_desc', 'industry_desc')
            .withColumnRenamed('oiics_nature_of_injury_code', 'oiics_nature_injury_code')
            .withColumnRenamed('oiics_nature_of_injury_desc', 'oiics_nature_injury_desc')
            .withColumnRenamed('occupational_disease', 'occupational_disease_ind')
            .withColumnRenamed('ppd_non-scheduled_loss_date', 'ppd_non_scheduled_loss_date')
           )

# COMMAND ----------

# Writing spark_df created from local csv file to Delta table
spark_df.write.format('delta').mode('overwrite').option('overwriteSchema', True).option("path", '/user/jade.qiu@lovelytics.com/').saveAsTable("bronze_table")

# COMMAND ----------

query = """
select *

where
    accident_date > '2000-01-01T00:00:00'
    
limit 5000
    
"""

# COMMAND ----------

# Using sodapy to call API. Returns json - adjust timeout and limit to control number of records returned.

api_token = None
client = Socrata("data.ny.gov", api_token, timeout=5000000)

# Results returned as JSON from API / converted to Python list of dictionaries by sodapy.
results = client.get("jshw-gkgu", query = query)

client.close()

# COMMAND ----------

# Convert to spark DataFrame
spark_df_update = spark.createDataFrame(results)

# COMMAND ----------

spark_df_update.count()

# COMMAND ----------

display(spark_df_update)

# COMMAND ----------

spark_df_update.write.format('delta').mode('overwrite').option('overwriteSchema', True).option("path", '/user/jade.qiu@lovelytics.com/bronze_table_update').saveAsTable("bronze_table_update")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze_table
# MAGIC USING bronze_table_update
# MAGIC ON bronze_table.claim_identifier = bronze_table_update.claim_identifier
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_table

# COMMAND ----------

spark_df_silver = spark.read.format("delta").load('/user/jade.qiu@lovelytics.com/')

# COMMAND ----------

spark_df_silver.count()

# COMMAND ----------

display(spark_df_silver)

# COMMAND ----------

# Changing column schemas

spark_df_silver = (spark_df_silver
            .withColumn("accident_date", regexp_replace(spark_df['accident_date'],"[T]", " "))
            .withColumn("accident_date",to_date("accident_date"))
            .withColumn("ancr_date", regexp_replace(spark_df['ancr_date'],"[T]", " "))
            .withColumn("ancr_date",to_date("ancr_date"))
            )

# COMMAND ----------

display(spark_df_silver)

# COMMAND ----------

display(spark.sql('SELECT * FROM bronze_table'))
