# Databricks notebook source
# MAGIC %sql
# MAGIC -- use catalog datamaster;
# MAGIC -- select * from datamaster.bronze.richest_bronze
# MAGIC
# MAGIC select * from bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create schema if not exists datamaster.silver
# MAGIC create schema if not exists silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select name, country, industry, net_worth_in_billions, company, ingestion_date from bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table silver.richest_silver as 
# MAGIC select name, country, industry, net_worth_in_billions, company, ingestion_date from bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, count(country) as count from silver.richest_silver group by country order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists gold;
# MAGIC use gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.country_count as
# MAGIC select country, count(country) as count from silver.richest_silver group by country order by count desc -- group by counrty

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.country_count

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.richest_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.name_count as
# MAGIC select name, count(country) as count from silver.richest_silver group by name order by count desc -- groupby name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.name_count

# COMMAND ----------


