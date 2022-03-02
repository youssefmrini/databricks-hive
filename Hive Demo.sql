-- Databricks notebook source
show databases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.ls("dbfs:/databricks-datasets/amazon/data20K")

-- COMMAND ----------

create database if not exists hivedemoyoussef

-- COMMAND ----------

use hivedemoyoussef

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #dbutils.fs.mkdirs("dbfs:/youssefmrini")
-- MAGIC #dbutils.fs.rm("dbfs:/youssefmrini", recurse=True)
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/amazon/data20K","dbfs:/youssefmrini/amazon", recurse=True)
-- MAGIC #dbutils.fs.cp("dbfs:/databricks-datasets/amazon/data20K","dbfs:/youssefmrini/amazondelta", recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.ls("dbfs:/youssefmrini")

-- COMMAND ----------

create table if not exists hivedemoyoussef.amazon USING parquet location 'dbfs:/youssefmrini/amazon'

-- COMMAND ----------

select * from amazon

-- COMMAND ----------

describe detail hivedemoyoussef.amazon


-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df=spark.sql("show tables from hivedemoyoussef")
-- MAGIC display(df)
