# Databricks notebook source
# MAGIC %md
# MAGIC ###  VACUUM COMMAND IN DATABRICKS (DELTA LAKE)
# MAGIC
# MAGIC #### PURPOSE:
# MAGIC - The VACUUM command in Databricks deletes obsolete files from a Delta table to free up storage.
# MAGIC - Obsolete files are created during operations like UPDATE, DELETE, MERGE, or OPTIMIZE.
# MAGIC
# MAGIC #### WHY NEEDED:
# MAGIC - Delta tables maintain a transaction log (.delta_log) that keeps old versions of files
# MAGIC - for time-travel and ACID compliance. Over time, these files accumulate and occupy storage.
# MAGIC - VACUUM safely removes files older than the retention period.
# MAGIC
# MAGIC #### COMMAND SYNTAX:
# MAGIC      VACUUM table_name [RETAIN num HOURS]
# MAGIC
# MAGIC #### DEFAULT RETENTION:
# MAGIC - By default, Delta enforces a 7-day retention period to prevent accidental data loss.
# MAGIC - To override, you can specify a shorter period using RETAIN num HOURS.
# MAGIC
# MAGIC **NOTES AND BEST PRACTICES:**
# MAGIC - Do not reduce retention below 1 hour without understanding consequences.
# MAGIC - Always use VACUUM after OPTIMIZE to clean obsolete files if needed.
# MAGIC - VACUUM only affects physical files, not the transaction log or metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create a Delta table
# MAGIC use lakehousecat.deltadb;
# MAGIC CREATE TABLE IF NOT EXISTS product_inventory (
# MAGIC     product_id INT,
# MAGIC     product_name STRING,
# MAGIC     category STRING,
# MAGIC     price DOUBLE,
# MAGIC     quantity INT,
# MAGIC     updated_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Insert initial data
# MAGIC
# MAGIC INSERT INTO product_inventory VALUES
# MAGIC  (1, 'Laptop', 'Electronics', 65000, 10, '2025-10-01'),
# MAGIC  (2, 'Headphones', 'Electronics', 2500, 50, '2025-10-01'),
# MAGIC  (3, 'Desk Chair', 'Furniture', 4500, 20, '2025-10-01');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Update some records (creates new Parquet file versions)
# MAGIC
# MAGIC UPDATE product_inventory
# MAGIC SET price = price * 1.1,
# MAGIC     updated_date = '2025-10-05'
# MAGIC WHERE category = 'Electronics';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: View Delta table history (shows create, insert, update operations)
# MAGIC
# MAGIC DESCRIBE HISTORY product_inventory;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 5: View current table details (file count, size, path)
# MAGIC
# MAGIC DESCRIBE DETAIL product_inventory;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable retention duration check (for demo only)
# MAGIC -- Default retention is 7 days for safety
# MAGIC -- Run VACUUM to permanently delete obsolete files
# MAGIC -- WARNING: Irreversible! Use only for demo or test
# MAGIC
# MAGIC --SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM product_inventory RETAIN 0 HOURS;
