# Databricks notebook source
from sodapy import Socrata
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS actuarial_accelerator;
# MAGIC USE actuarial_accelerator

# COMMAND ----------

# Reading in workers compensation data from csv file uploaded in local file store.
spark_df_bronze = spark.read.csv("/FileStore/tables/NY_workers_compensation.csv", header=True, inferSchema=True)

# COMMAND ----------

# Checking number of records in spark df read from local csv
spark_df_bronze.count()

# COMMAND ----------

def rename_col(col):
    return col.lower().replace(" ", "_").replace("description", "desc").replace("part_of_body", "pob")

# COMMAND ----------

for col in spark_df_bronze.columns:
  spark_df_bronze = spark_df_bronze.withColumnRenamed(col, rename_col(col))

# COMMAND ----------

spark_df_bronze = (spark_df_bronze.withColumnRenamed('accident', 'accident_ind')
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
spark_df_bronze.write.format('delta').mode('overwrite').option("path", '/actuarial_accelerator/bronze_table').saveAsTable("bronze_table")

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

spark_df_update.write.format('delta').mode('overwrite').option("path", '/actuarial_accelerator/bronze_table_update').saveAsTable("bronze_table_update")

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

spark_df_silver = spark.read.format("delta").load('/actuarial_accelerator/silver_table')

# COMMAND ----------

display(spark_df_silver)

# COMMAND ----------

# Helper function to change date column schemas from string to date
def string_to_date(date_string_column):
  r1 = spark_functions.regexp_replace(date_string_column, "[T]", " ")
  r2 = spark_functions.regexp_replace(r1, "[-]", "/")
  r3 = spark_functions.to_date(r2, "MM/dd/yyyy")
  return r3

# COMMAND ----------

# Change date column schemas from string to date
date_columns = [col for col in spark_df_silver.columns if 'date' in col]
for col in date_columns:
  spark_df_silver = spark_df_silver.withColumn(col, string_to_date(col))

# COMMAND ----------

# Checking to make sure no records were turned to null during the string to date conversation
assert spark_df_bronze.filter(spark_df_bronze.ancr_date.isNull()).count()  == spark_df_bronze.filter(spark_df_bronze.ancr_date.isNull()).count()

# COMMAND ----------

# Calculating report lag
spark_df_silver = spark_df_silver.withColumn('report_lag', spark_functions.datediff('ancr_date', 'accident_date'))

# COMMAND ----------

display(spark_df_silver.filter(spark_df_silver.ancr_date.isNotNull()))

# COMMAND ----------

# Helper function to recode body parts
def body_part_recode(string):
  hand = ['FINGER', 'SHOULDER', 'HAND', 'ARM', 'THUMB', 'ELBOW', 'WRIST']
  face = ['EYE', 'HEAD', 'SKULL', 'FACE', 'FACIAL', 'EAR', 'NOSE', 'MOUTH', 'BRAIN', 'TEETH']
  leg = ['KNEE', 'ANKLE', 'FOOT', 'LEG', 'TOE', 'BUTTOCKS', 'LOWER']
  respiratory = ['LUNGS', 'LARYNX', 'SACRUM AND COCCYX', 'TRACHEA']
  spinal = ['BACK', 'NECK', 'DISC', 'VERTEBRAE', 'SPINAL']
  torso = ['CHEST', 'ABDOMEN', 'HIP', 'TRUNK', 'PELVIS', 'HEART']
  multiple = ['MULTIPLE', 'WHOLE']
  if any(x in string for x in hand):
    return 'hand'
  elif any(x in string for x in face):
    return 'face'
  elif any(x in string for x in leg):
    return 'leg'
  elif any(x in string for x in respiratory):
    return 'respiratory'
  elif any(x in string for x in spinal):
    return 'spinal'
  elif any(x in string for x in torso):
    return 'torso'
  elif any(x in string for x in multiple):
    return 'multiple'
  else:
    return 'others'

# COMMAND ----------

body_part_recode_udf = udf(lambda x: body_part_recode(x) if not x is None else None , spark_type.StringType())

# COMMAND ----------

spark_df_silver = spark_df_silver.withColumn('injured_body_part', body_part_recode_udf(spark_functions.col('wcio_pob_desc')))

# COMMAND ----------

display(spark_df_silver.filter(spark_df_silver.wcio_pob_desc.isNotNull()))

# COMMAND ----------

# Removing all claims that were labelled as cancelled - based on data dictionary, claims are marked as cancelled if assembled in error or determined to be a duplicate
spark_df_silver = spark_df_silver.filter(spark_df_silver.claim_injury_type != '1. CANCELLED')

# COMMAND ----------

spark_df_silver.count()

# COMMAND ----------

spark_df_silver.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path", '/actuarial_accelerator/silver_table').saveAsTable("silver_table")

# COMMAND ----------

selected_cols = ['accident_date', 'ancr_date', 'age_at_injury', 'average_weekly_wage', 'carrier_name', 'carrier_type', 'claim_injury_type', 'claim_type', 'closed_count', 'injured_in_county_name', 'covid_19_indicator', 'current_claim_status', 'gender', 'industry_desc', 'report_lag', 'injured_body_part']
spark_df_gold = spark_df_silver.select(selected_cols)

# COMMAND ----------

spark_df_gold = spark_df_gold.filter(spark_df_gold.accident_date > '2000-01-01')

# COMMAND ----------

spark_df_gold.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path", '/actuarial_accelerator/gold_table').saveAsTable("gold_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (*) FROM gold_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (*) as Claim_Count_YTD FROM gold_table WHERE ancr_date >= string(year(current_date))

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ytd_calc(count)
# MAGIC   AS (SELECT COUNT (*) as Claim_Count_YTD FROM gold_table WHERE ancr_date >= '2022-01-01'),
# MAGIC ytd_lastyear_calc(count)
# MAGIC   AS (SELECT COUNT(*) as Claim_Count_Last_Year_YTD FROM gold_table WHERE ancr_date >= string(year(current_date) - 1)  AND ancr_date <= date_sub(current_date, 365))
# MAGIC SELECT ytd_calc.count, ytd_lastyear_calc.count,  (ytd_calc.count) / ytd_lastyear_calc.count, (ytd_calc.count - ytd_lastyear_calc.count) / ytd_lastyear_calc.count FROM ytd_calc, ytd_lastyear_calc

# COMMAND ----------


