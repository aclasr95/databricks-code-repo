# Databricks notebook source
# MAGIC %md
# MAGIC ###  OPTIMIZE COMMAND IN DATABRICKS (DELTA LAKE)
# MAGIC
# MAGIC #### PURPOSE:
# MAGIC - The OPTIMIZE command in Databricks compacts small files into larger ones within a Delta table.
# MAGIC - This improves query performance by reducing the number of files that Spark needs to read.
# MAGIC
# MAGIC #### WHY NEEDED:
# MAGIC - Delta tables can accumulate many small files due to frequent updates, merges, and streaming writes.
# MAGIC - OPTIMIZE combines these small files into fewer large files, which helps improve read performance.
# MAGIC
# MAGIC #### HOW IT WORKS:
# MAGIC - It rewrites data files within each partition(if any) into optimized files.
# MAGIC - Uses a bin-packing algorithm to combine smaller files into target-sized files (~1GB each).
# MAGIC - Only affects physical layout of data — does NOT change data content.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create a Delta table
# MAGIC CREATE OR REPLACE TABLE inceptez_catalog.inputdb.tblsales
# MAGIC (
# MAGIC   sales_id INT,
# MAGIC   product_id INT,
# MAGIC   region STRING,
# MAGIC   sales_amount DOUBLE,
# MAGIC   sales_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Insert sample data
# MAGIC
# MAGIC -- Let’s add multiple small batches to simulate many small files:
# MAGIC
# MAGIC INSERT INTO inceptez_catalog.inputdb.tblsales VALUES
# MAGIC   (1, 101, 'North', 1000.50, '2025-10-16'),
# MAGIC   (2, 102, 'South', 500.75, '2025-10-16'),
# MAGIC   (3, 103, 'East', 700.20, '2025-10-16'),
# MAGIC   (4, 104, 'West', 1200.00, '2025-10-16');
# MAGIC
# MAGIC INSERT INTO inceptez_catalog.inputdb.tblsales VALUES
# MAGIC   (5, 101, 'North', 800.00, '2025-10-17'),
# MAGIC   (6, 102, 'South', 450.00, '2025-10-17'),
# MAGIC   (7, 103, 'East', 600.00, '2025-10-17'),
# MAGIC   (8, 104, 'West', 1100.00, '2025-10-17');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inceptez_catalog.inputdb.tblsales;
# MAGIC --spark.sql("select * from inceptez_catalog.inputdb.tblsales");
# MAGIC --spark.Table("inceptez_catalog.inputdb.tblsales")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Check fragmentation
# MAGIC
# MAGIC DESCRIBE DETAIL inceptez_catalog.inputdb.tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Optimize the table
# MAGIC -- This performs file compaction:
# MAGIC -- Combines many small Parquet files into fewer large files (around 1 GB default).
# MAGIC -- Improves read performance and reduces metadata overhead.
# MAGIC
# MAGIC
# MAGIC OPTIMIZE inceptez_catalog.inputdb.tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 5: Verify compaction
# MAGIC
# MAGIC -- After optimization, run:
# MAGIC
# MAGIC DESCRIBE DETAIL inceptez_catalog.inputdb.tblsales;
