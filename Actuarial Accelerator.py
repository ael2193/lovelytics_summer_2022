# Databricks notebook source
from sodapy import Socrata
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_type

# COMMAND ----------

# Reading in workers compensation data from csv file uploaded in local file store.
spark_df = spark.read.csv("/FileStore/tables/NY_workers_compensation.csv", header=True, inferSchema=True)

# COMMAND ----------

# Checking number of records in spark df read from local csv
spark_df.count()

# COMMAND ----------

def rename_col(col):
    return col.lower().replace(" ", "_").replace("description", "desc").replace("part_of_body", "pob")

# COMMAND ----------

for col in spark_df.columns:
  spark_df = spark_df.withColumnRenamed(col, rename_col(col))

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
spark_df.write.format('delta').mode('overwrite').option("path", '/user/jade.qiu@lovelytics.com/').saveAsTable("bronze_table")

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

spark_df_bronze = spark.read.format("delta").load('/user/jade.qiu@lovelytics.com/')

# COMMAND ----------

spark_df_silver.count()

# COMMAND ----------

display(spark_df_silver)

# COMMAND ----------

# Helper function to change date column schemas from string to date
def string_to_date(date_string):
  r1 = spark_functions.regexp_replace(date_string, "[T]", " ")
  r2 = spark_functions.regexp_replace(r1, "[-]", "/")
  r3 = spark_functions.to_date(r2, "MM/dd/yyyy")
  return r3

# COMMAND ----------

# Change date column schemas from string to date

date_columns = [col for col in spark_df_bronze.columns if 'date' in col]
for col in date_columns:
  spark_df_silver = spark_df_bronze.withColumn(col, string_to_date(col))
  
assert spark_df_silver.filter(spark_df_bronze.ancr_date.isNull()).count() == spark_df_silver.filter(spark_df_silver.ancr_date.isNull()).count()

# COMMAND ----------

display(spark_df_silver)

# COMMAND ----------

spark_df_silver = spark_df_silver.withColumn('days_btw_accident_and_claim_estd', 'ancr_date' - 'accident_date')

# COMMAND ----------

# Selecting more relevant columns for analysis
selected_cols = ['accident_ind', 'accident_date', 'age_at_injury', 'ancr_date'
assembly_date:date
atty_rep_ind:string
average_weekly_wage:double
birth_year:integer
c2_date:date
c3_date:date
carrier_name:string
carrier_type:string
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
