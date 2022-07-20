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

# spark_df_silver = spark.read.format("delta").load('/actuarial_accelerator/silver_table')
spark_df_silver = spark_df_bronze

# COMMAND ----------

spark_df_silver.count()

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

assert spark_df_bronze.filter(spark_df_bronze.ancr_date.isNull()).count()  == spark_df_bronze.filter(spark_df_bronze.ancr_date.isNull()).count()

# COMMAND ----------

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

from pyspark.mllib.random import RandomRDDs

sc = ... # SparkContext

# Generate a random double RDD that contains 1 million i.i.d. values drawn from the
# standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
u = RandomRDDs.uniformRDD(sc, 1000000L, 10)
# Apply a transform to get a random double RDD following `N(1, 4)`.
v = u.map(lambda x: 1.0 + 2.0 * x)

# COMMAND ----------

spark_df_silver.write.format('delta').mode('overwrite').option("overwriteSchema", "true").option("path", '/actuarial_accelerator/silver_table').saveAsTable("silver_table")

# COMMAND ----------

display(spark.sql("SELECT * FROM silver_table"))

# COMMAND ----------

'''
Selecting more relevant columns for analysis
selected_cols = ['accident_ind', 'accident_date', 'age_at_injury', 'ancr_date', 'average_weekly_wage', 'carrier_name', 'carrier_type'
claim_identifier:integer
claim_injury_type:string
claim_type:string
closed_count:integer
controverted_date:date
injured_in_county_name:string
covid_19_indicator:string
current_claim_status:string
district_name:string
first_appeal_date:date
first_hearing_date:date
gender:string
hearing_count:integer
highest_process:string
ime4_count:integer
industry_code:integer
industry_desc:string
interval_assembled_to_ancr:integer
medical_fee_region:string
occupational_disease_ind:string
oiics_event_exposure_code:string
oiics_event_exposure_desc:string
oiics_injury_source_code:string
oiics_injury_source_desc:string
oiics_nature_injury_code:string
oiics_nature_injury_desc:string
oiics_pob_code:string
oiics_pob_desc:string
oiics_secondary_source_code:string
oiics_secondary_source_desc:string
ppd_non_scheduled_loss_date:date
ppd_scheduled_loss_date:date
ptd_date:date
section_32_date:date
wcio_cause_of_injury_code:integer
wcio_cause_of_injury_desc:string
wcio_nature_of_injury_code:integer
wcio_nature_of_injury_desc:string
wcio_pob_code:integer
wcio_pob_desc:string
zip_code:string
spark_df_silver = spark_df_silver.select()
'''
