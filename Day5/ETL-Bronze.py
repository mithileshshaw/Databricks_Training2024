# Databricks notebook source
# MAGIC %run "/Workspace/Hexaware_Training2024/Day5/includes"

# COMMAND ----------

input

# COMMAND ----------

df=spark.read.csv(f"{input}")

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True) # one spark job

# COMMAND ----------

# DBTITLE 1,Reading
df=spark.read.csv(f"{input}",header=True,inferSchema=True) # Two spark job

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn("ingestion_date",current_timestamp()).display()

# COMMAND ----------

df1=add_ingestion(df) # timestamp using user defined function
df1.display()

# COMMAND ----------

df1.write.saveAsTable("richest") # Error it will give

# COMMAND ----------

df1.createOrReplaceTempView("richest_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select Name as name, Country as country from richest_view
# MAGIC where `Net Worth (in billions)` > 100

# COMMAND ----------

df1.createOrReplaceGloblalTempView("richest_globalview")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.richest_globalview

# COMMAND ----------

df1.columns

# COMMAND ----------

new_col=['name', 'country', 'industry', 'net_worth_in_billions', 'company', 'ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

df2.write.mode("overwrite").save(f"{output}mith/richest")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w=dbutils.widgets.get("environment")

# COMMAND ----------

w

# COMMAND ----------

df3=df2.withColumn("environment",lit(w))
df3.display()

# COMMAND ----------

df3.write.mode("overwrite").save(f"{output}mith/richest") # will give error

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").save(f"{output}mith/richest")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/hexawaredatabricks/raw/output_files/mith/richest`

# COMMAND ----------

## After break

# COMMAND ----------

# MAGIC %sql
# MAGIC -- use catalog datamaster;
# MAGIC create schema if not exists bronze;
# MAGIC use bronze

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").saveAsTable("richest_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from richest_bronze

# COMMAND ----------


