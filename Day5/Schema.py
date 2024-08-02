# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

# df_airlines=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True,infersSchema=True)
df_airlines=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True)

# COMMAND ----------

df_airlines.count()

# COMMAND ----------

User defined Schema(Table schema)
str,
list,
pyspark

# COMMAND ----------

# MAGIC %run "/Workspace/Hexaware_Training2024/Day5/includes"

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

str_schema="name string, country string, industry string, net_worth_in_billion double, company string"

# COMMAND ----------

df_new=spark.read.schema(str_schema).csv(f"{input}",header=True)

# COMMAND ----------

df_new.display()

# COMMAND ----------

# Not work
data={"id":1, "name":"mith","mobile":[9749,150]}
schema="id int, name string, mobile array"

# COMMAND ----------

# Not work
data={"id":1, "name":"mith","mobile":{"home":9749,"office":150}
schema="id int, name string, mobile array"

# COMMAND ----------

pyspark DataType
1. Struct
2. Array
3. Map

# COMMAND ----------

str_schema="name string, country string, industry string net_worth_in_billion double, company string"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pyspark_schema = StructType([StructField("name",StringType()),
                             StructField("country",StringType()),
                             StructField("industry",StringType()),
                             StructField("net_worth",DoubleType()),
                             StructField("company",StringType())                             
                             ])

# COMMAND ----------

df_new2=spark.read.schema(pyspark_schema).csv(f"{input}",header=True)

# COMMAND ----------


