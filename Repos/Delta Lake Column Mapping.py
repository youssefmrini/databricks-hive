# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Column Mapping Demonstration
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook demonstrates **Delta Lake Column Mapping**, a critical feature for schema evolution in modern data lakes. Column mapping enables safe schema changes like renaming and dropping columns without breaking existing queries or data pipelines.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC * **Column Mapping Fundamentals**: Understanding the difference between positional and name-based column mapping
# MAGIC * **Schema Evolution**: How to safely rename and drop columns in Delta tables
# MAGIC * **Best Practices**: When and why to enable column mapping for production workloads
# MAGIC * **Common Pitfalls**: What happens when you try schema evolution without column mapping
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Basic understanding of Delta Lake and SQL
# MAGIC * Familiarity with data lake concepts
# MAGIC * Understanding of schema evolution challenges
# MAGIC
# MAGIC ## Notebook Structure
# MAGIC 1. **Setup**: Create demo catalog, schema, and sample data
# MAGIC 2. **Comparison**: Create tables with and without column mapping
# MAGIC 3. **Schema Evolution**: Demonstrate rename and drop operations
# MAGIC 4. **Results Analysis**: Compare outcomes and understand limitations
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC  ![Delta Lake Logo](https://delta.io/_astro/delta-lake-logo.Bqi7mgVq_Kp5oj.webp)
# MAGIC   <br><br> Delta Lake OSS (Open Source Software) is an open-source storage framework designed to bring reliable ACID transactions, scalable metadata handling, and unified batch and streaming data processing to data lakes, enabling the construction of modern "lakehouse" architectures.​
# MAGIC
# MAGIC ##   Key Features
# MAGIC   **ACID Transactions**: Delta Lake ensures data reliability and consistency by providing serializability, the strongest level of isolation for transactions.​
# MAGIC
# MAGIC   **Scalable Metadata**: It efficiently manages petabyte-scale tables and billions of partitions, making large-scale analytics practical.​
# MAGIC
# MAGIC   **Time Travel**: Users can access and revert to earlier versions of datasets, supporting auditing and rollbacks.​
# MAGIC
# MAGIC   **Schema Enforcement & Evolution**: Delta Lake prevents "bad" data from corrupting datasets and supports gradual schema updates.​
# MAGIC
# MAGIC   **Unified Batch/Streaming**: The same table can serve both streaming and batch processing seamlessly.​
# MAGIC
# MAGIC   **Openness**: Delta Lake OSS is governed by the Linux Foundation and is community-driven without control by any single company.​
# MAGIC
# MAGIC   **Multi-Engine Support**: Works natively with engines like Apache Spark, Flink, Hive, Trino, and Presto, and provides APIs in multiple programming languages (Scala, Java, Python, Rust, Ruby).​

# COMMAND ----------

# DBTITLE 1,Create Catalog and Schema in SQL for Demo
# MAGIC %sql
# MAGIC
# MAGIC drop catalog demo_youssefM cascade;
# MAGIC create catalog demo_youssefM;
# MAGIC use catalog demo_youssefM;
# MAGIC create schema delta;
# MAGIC use schema delta;

# COMMAND ----------

# DBTITLE 1,Create View with Fake Data
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW fake_data_view AS
# MAGIC SELECT
# MAGIC   1 AS id,
# MAGIC   'Alice' AS `first name`,
# MAGIC   'Engineering' AS department
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   2 AS id,
# MAGIC   'Bob' AS `first name`,
# MAGIC   'Sales' AS department
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   3 AS id,
# MAGIC   'Carol' AS `first name`,
# MAGIC   'Marketing' AS department;
# MAGIC
# MAGIC select * from fake_data_view

# COMMAND ----------

# DBTITLE 1,Create Customer Table
# MAGIC %sql
# MAGIC create table customer as select * from fake_data_view;

# COMMAND ----------

# MAGIC %md
# MAGIC - FAILED ATTEMPT: Creating a table without column mapping enabled
# MAGIC - This cell demonstrates the default Delta Lake behavior
# MAGIC - Without explicit column mapping configuration, tables use positional column mapping
# MAGIC - This will fail or have limitations when we try to rename/drop columns later
# MAGIC - Column mapping must be enabled at table creation time - it cannot be added later

# COMMAND ----------

# DBTITLE 1,Create Customer Table with Column Mapping Enabled
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE customer_with_CM
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )
# MAGIC AS
# MAGIC SELECT * FROM fake_data_view;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <br>SUCCESS: Creating a Delta table with column mapping enabled
# MAGIC <br>KEY FEATURE: 'delta.columnMapping.mode' = 'name' enables column mapping by name
# MAGIC <br>This allows future schema evolution operations like:
# MAGIC - Renaming columns without breaking existing queries
# MAGIC - Dropping columns safely
# MAGIC - Handling column order changes in INSERT operations
# MAGIC - Column mapping MUST be enabled at table creation - cannot be added later!

# COMMAND ----------

# DBTITLE 1,Retrieve Data from Customer Table
# MAGIC %sql
# MAGIC
# MAGIC select * from customer_with_CM

# COMMAND ----------

# DBTITLE 1,Create Fake Data View 2
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW fake_data_view_2 AS
# MAGIC SELECT
# MAGIC   1 AS id,
# MAGIC   'Alice' AS `firstname`,
# MAGIC   'Engineering' AS department
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   2 AS id,
# MAGIC   'Bob' AS `firstname`,
# MAGIC   'Sales' AS department
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   3 AS id,
# MAGIC   'Carol' AS `firstname`,
# MAGIC   'Marketing' AS department;

# COMMAND ----------

# DBTITLE 1,Create Customer Table from Fake Data View 2
# MAGIC %sql
# MAGIC
# MAGIC create table customer_without_CM as select * from fake_data_view_2;

# COMMAND ----------

# DBTITLE 1,Rename Customer Table Column to full_name
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC ALTER TABLE customer_with_CM RENAME COLUMN `first name` TO full_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ SUCCESS: Renaming a column in a table WITH column mapping enabled
# MAGIC -  This operation succeeds because column mapping allows safe schema evolution
# MAGIC -  The 'first name' column (with space) is renamed to 'full_name' (no space)
# MAGIC -  Column mapping maintains the relationship between logical and physical column names
# MAGIC -  Existing queries and applications continue to work during the transition

# COMMAND ----------

# DBTITLE 1,Rename Customer Table Column to Full Name
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE customer_without_CM RENAME COLUMN `firstname` TO full_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ❌ FAILED: Attempting to rename a column in a table WITHOUT column mapping
# MAGIC -  This operation FAILS because standard Delta tables use positional column mapping
# MAGIC -  Without column mapping, Delta Lake cannot safely rename columns
# MAGIC -  This demonstrates why column mapping is crucial for schema evolution
# MAGIC -  The error shows the limitation of traditional Delta Lake tables

# COMMAND ----------

# DBTITLE 1,Drop Column with Column Mapping
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE customer_with_CM drop COLUMN full_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ SUCCESS: Dropping a column in a table WITH column mapping enabled
# MAGIC - This operation succeeds because column mapping allows safe schema evolution
# MAGIC - The 'department' column is safely removed from the table structure
# MAGIC - Column mapping ensures that the physical data files remain intact
# MAGIC - This is a non-breaking change that doesn't affect existing data files

# COMMAND ----------

# DBTITLE 1,Drop Column without Column Mapping
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE customer_without_CM drop COLUMN firstname;

# COMMAND ----------

# MAGIC %md
# MAGIC ❌ FAILED: Attempting to drop a column in a table WITHOUT column mapping
# MAGIC - This operation FAILS because standard Delta tables cannot safely drop columns
# MAGIC - Without column mapping, dropping columns would break the physical data structure
# MAGIC - This demonstrates another critical limitation of tables without column mapping
# MAGIC - Schema evolution operations require column mapping to work properly

# COMMAND ----------

# DBTITLE 1,Customer Data Selection from Table
# MAGIC %sql
# MAGIC
# MAGIC select * from customer_with_CM

# COMMAND ----------

# DBTITLE 1,Customer Data Extraction Without Column Mapping
# MAGIC %sql
# MAGIC
# MAGIC select * from customer_without_CM
