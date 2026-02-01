# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview
# MAGIC **Liquid Clustering**: Databricks' next-generation data clustering feature that automatically manages physical data organization for Delta tables.
# MAGIC
# MAGIC ### What is Liquid Clustering?
# MAGIC - Liquid Clustering is a **data layout optimization technique** for Delta tables.
# MAGIC - It automatically manages clustering without requiring manual Z-order or partitioning.
# MAGIC - Data is **physically organized** on disk to minimize scan cost for frequently queried columns.
# MAGIC - It provides **better performance** for selective queries and incremental updates.
# MAGIC
# MAGIC ### Key Points
# MAGIC - Introduced in Databricks Runtime 13.3+
# MAGIC - Works with Delta tables only
# MAGIC - Replaces Manual partitioning and Z-Ordering
# MAGIC - Physically reorders data? Yes, in background compaction and OPTIMIZE runs
# MAGIC - Maintenance Databricks handles clustering maintenance automatically
# MAGIC
# MAGIC ### Typical Use Cases
# MAGIC  - Large tables with frequent inserts, updates, and deletes.
# MAGIC  - Query filtering on specific columns like `customer_id`, `region`, `order_date`.
# MAGIC  - Scenarios where manual ZORDER tuning is difficult or costly.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create a Delta table using Liquid Clustering
# MAGIC -- The CLUSTER BY clause enables liquid clustering automatically.
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS inceptez_catalog.inputdb.sales_orders_liquid
# MAGIC (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   region STRING,
# MAGIC   product STRING,
# MAGIC   quantity INT,
# MAGIC   price DOUBLE,
# MAGIC   order_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (customer_id, region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Insert sample data (multiple small batches)
# MAGIC -- Each insert simulates separate data ingestion.
# MAGIC
# MAGIC INSERT INTO inceptez_catalog.inputdb.sales_orders_liquid VALUES
# MAGIC  (1, 101, 'North', 'Laptop', 2, 65000, '2025-10-01'),
# MAGIC  (2, 102, 'South', 'Headphones', 5, 2500, '2025-10-01'),
# MAGIC  (3, 103, 'West', 'Desk Chair', 3, 4500, '2025-10-02');
# MAGIC
# MAGIC INSERT INTO inceptez_catalog.inputdb.sales_orders_liquid VALUES
# MAGIC  (4, 101, 'North', 'Keyboard', 1, 1200, '2025-10-03'),
# MAGIC  (5, 104, 'East', 'Monitor', 2, 9500, '2025-10-03'),
# MAGIC  (6, 105, 'South', 'Mouse', 4, 700, '2025-10-03');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Query data
# MAGIC SELECT * FROM inceptez_catalog.inputdb.sales_orders_liquid;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: View table details
# MAGIC DESCRIBE DETAIL inceptez_catalog.inputdb.sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY  inceptez_catalog.inputdb.sales_orders_liquid;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 5: Simulate updates (which trigger reclustering under the hood)
# MAGIC UPDATE inceptez_catalog.inputdb.sales_orders_liquid
# MAGIC SET price = price * 1.05
# MAGIC WHERE region = 'North';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 6: Verify table history (you will see multiple operations)
# MAGIC DESCRIBE HISTORY inceptez_catalog.inputdb.sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE inceptez_catalog.inputdb.sales_orders_liquid
# MAGIC SET price = price * 1.05
# MAGIC WHERE region = 'South';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM inceptez_catalog.inputdb.sales_orders_liquid
# MAGIC WHERE region = 'East';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY inceptez_catalog.inputdb.sales_orders_liquid;
