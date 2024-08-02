# Databricks notebook source
input="dbfs:/mnt/hexawaredatabricks/raw/input_files/"
output="dbfs:/mnt/hexawaredatabricks/raw/output_files/"
 

# COMMAND ----------

input
output

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

def add_ingestion(a):
    b=a.withColumn("ingestion_date",current_timestamp())
    return b

# COMMAND ----------


