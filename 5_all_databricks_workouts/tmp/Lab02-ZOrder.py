# Databricks notebook source
# MAGIC %md
# MAGIC ### ZORDER IN DATABRICKS (DELTA LAKE)
# MAGIC
# MAGIC #### ZORDER:
# MAGIC - ZORDER is an optional feature used with OPTIMIZE to colocate related data physically in the same set of files.
# MAGIC - It improves query performance for range or equality filters on the specified columns.
# MAGIC
# MAGIC #### USAGE:
# MAGIC - OPTIMIZE table_name [WHERE predicate] ZORDER BY (col1, col2, ...)
# MAGIC - Reduces file scan for queries filtering on ZORDER columns.
# MAGIC - Works best for columns used frequently in WHERE clauses.
# MAGIC - Only reorganizes existing data; does not add or remove rows.
# MAGIC
# MAGIC #### EXAMPLE USE CASE:
# MAGIC - Periodically optimize large Delta tables with frequent writes/updates.
# MAGIC - Use ZORDER on high-selectivity columns to improve read performance.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Step 1 – Create the Delta table
# MAGIC
# MAGIC CREATE TABLE inceptez_catalog.inputdb.customer_txn (
# MAGIC     txn_id INT,
# MAGIC     customer_id INT,
# MAGIC     region STRING,
# MAGIC     txn_amount DOUBLE,
# MAGIC     txn_type STRING,
# MAGIC     transaction_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Step 2 – Insert multiple small batches
# MAGIC
# MAGIC --Each insert writes a few small Parquet files.
# MAGIC
# MAGIC -- Batch 1
# MAGIC INSERT INTO inceptez_catalog.inputdb.customer_txn VALUES
# MAGIC  (1, 1001, 'North', 250.00, 'Online', '2025-10-01'),
# MAGIC  (2, 1002, 'South', 400.00, 'Offline', '2025-10-02'),
# MAGIC  (3, 1003, 'West', 600.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC -- Batch 2
# MAGIC INSERT INTO inceptez_catalog.inputdb.customer_txn VALUES
# MAGIC  (4, 1001, 'North', 300.00, 'Offline', '2025-10-01'),
# MAGIC  (5, 1004, 'East', 750.00, 'Online', '2025-10-02'),
# MAGIC  (6, 1005, 'South', 180.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC -- Batch 3
# MAGIC INSERT INTO inceptez_catalog.inputdb.customer_txn VALUES
# MAGIC  (7, 1001, 'North', 270.00, 'Online', '2025-10-01'),
# MAGIC  (8, 1003, 'West', 500.00, 'Offline', '2025-10-02'),
# MAGIC  (9, 1002, 'South', 900.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC /*
# MAGIC region=North
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC region=South
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC region=West
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC region=East
# MAGIC     - part-0
# MAGIC
# MAGIC select * from inceptez_catalog.inputdb.customer_txn where region='North';
# MAGIC
# MAGIC optimize inceptez_catalog.inputdb.customer_txn;
# MAGIC region=North
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3 - after optimize
# MAGIC region=South
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3 - after optimize
# MAGIC region=West
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2 - after optimize
# MAGIC region=East
# MAGIC     - part-0
# MAGIC     - part-1 - after optimize
# MAGIC
# MAGIC
# MAGIC optimize inceptez_catalog.inputdb.customer_txn zorder by transaction_date;
# MAGIC
# MAGIC optimize inceptez_catalog.inputdb.customer_txn;
# MAGIC region=North
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3 - optimize & sort the data rows in transaction_date
# MAGIC region=South
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3 - optimize & sort the data rows in transaction_date
# MAGIC region=West
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2 - optimize & sort the data rows in transaction_date
# MAGIC region=East
# MAGIC     - part-0
# MAGIC     - part-1 - optimize & sort the data rows in transaction_date
# MAGIC */
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3 – Inspect fragmentation
# MAGIC
# MAGIC DESCRIBE DETAIL inceptez_catalog.inputdb.customer_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4 – Run OPTIMIZE ZORDER
# MAGIC
# MAGIC -- Now compact and physically order data.
# MAGIC
# MAGIC OPTIMIZE inceptez_catalog.inputdb.customer_txn ZORDER BY (transaction_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3 – Inspect fragmentation
# MAGIC
# MAGIC DESCRIBE DETAIL inceptez_catalog.inputdb.customer_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM inceptez_catalog.inputdb.customer_txn
# MAGIC WHERE region = 'North'
# MAGIC AND transaction_date BETWEEN '2025-10-01' AND '2025-10-03';
