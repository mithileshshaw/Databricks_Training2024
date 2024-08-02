# Databricks notebook source
Autoloader: https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/
Autoloader schema : https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/schema

# COMMAND ----------

(spark
 .readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/mith/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/mith/autoloader")
.trigger(once=True)
.table("bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader

# COMMAND ----------

Schema Evolution:
    1. I should get notified (decide -- get a new column or drop the source)
    2.

# COMMAND ----------

# Merge schema with new column It may fails first in case of new column incoming
(spark
 .readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/mith/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/mith/autoloader")
.option("mergeSchema",True)
.table("bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader

# COMMAND ----------

# Merge schema with new column It will process in the second run
(spark
 .readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/mith/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/mith/autoloader")
.option("mergeSchema",True)
.table("bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader

# COMMAND ----------

https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/schema

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/mith/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/mith/autoloader")
.option("mergeSchema",True)
.table("bronze.autoloader")
) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.autoloader

# COMMAND ----------

print("push code from Notebook to Github Repo1")

# COMMAND ----------
print("push code from github to notebook")

