# Databricks notebook source
Batch
Read
df=spark.read.csv("path")
write
df.write.saveAsTable("path")
df.write.mode("overwirte").saveAsTable("path")
df.write.mode("append").saveAsTable("tblname")

Real-time (Data which grow)
Spark Structured Streaming
1. Exactly gurnateed that it will read only once

df=spark.readStream.csv("path")
df.writeStream.option("checkpointLocation","path").table("tblname")

Limitation:
     Define your Schema

# COMMAND ----------

# MAGIC %run "/Workspace/Hexaware_Training2024/Day5/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/hexawaredatabricks/raw/stream_in/

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

df=spark.readStream.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/Jan.CSV") # Will not work

# COMMAND ----------

#Not required sch="Id int, Name, Gender, Salary,Country,Date"   

# COMMAND ----------

users_schema=StructType([StructField("Id", IntegerType()),
                        StructField("Name", StringType()),
                        StructField("Gender", StringType()),
                        StructField("Salary", IntegerType()),
                        StructField("Country", StringType()),
                        StructField("Date", StringType())
                        ])
df=spark.readStream.schema(users_schema).csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)

# COMMAND ----------

## df.display() We should never use seperate df.dispaly as it this job will never stop and cluster will also not automatically terminate and cost huge computation

# COMMAND ----------

# Always create checkpoint outside stream directory
df.writeStream.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/mith/stream").table("bronze.stream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.stream

# COMMAND ----------

(spark.readStream.schema(users_schema).csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
 .writeStream.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/mith/stream").trigger(once=True).table("bronze.stream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.stream
