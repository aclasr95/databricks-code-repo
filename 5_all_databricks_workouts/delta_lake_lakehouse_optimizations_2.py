# Databricks notebook source
# MAGIC %md
# MAGIC ###Delta Lake (Lakehouse Performance Optimization, Cost Saving & Best Practices)

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/5_all_databricks_workouts/DELTA OPTIMIZATIONS.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. OPTIMIZE COMMAND
# MAGIC - The OPTIMIZE command in Databricks compacts small files (due to frequent updates, merges, and streaming writes) into larger ones (~1GB) within a Delta table.
# MAGIC - This improves query performance by reducing the number of files that Spark needs to read and reduces metadata overhead.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat.deltadb

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tblsales
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
# MAGIC select * from tblsales

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tblsales VALUES
# MAGIC   (1, 101, 'North', 1000.50, '2025-10-16'),
# MAGIC   (2, 102, 'South', 500.75, '2025-10-16'),
# MAGIC   (3, 103, 'East', 700.20, '2025-10-16'),
# MAGIC   (4, 104, 'West', 1200.00, '2025-10-16');
# MAGIC
# MAGIC INSERT INTO tblsales VALUES
# MAGIC   (5, 101, 'North', 800.00, '2025-10-17'),
# MAGIC   (6, 102, 'South', 450.00, '2025-10-17'),
# MAGIC   (7, 103, 'East', 600.00, '2025-10-17'),
# MAGIC   (8, 104, 'West', 1100.00, '2025-10-17');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check fragmentation (numFiles & sizeInBytes)
# MAGIC DESCRIBE DETAIL tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Optimize the table
# MAGIC --This performs file compaction:
# MAGIC --Combines many small Parquet files into fewer large files (around 1 GB default).
# MAGIC --Improves read performance and reduces metadata overhead.
# MAGIC OPTIMIZE tblsales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify compaction
# MAGIC -- After optimization, run:
# MAGIC DESCRIBE DETAIL tblsales;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. ZORDER
# MAGIC - ZORDER is an optional feature used with OPTIMIZE to colocate related data physically in the same set of files by sorting.
# MAGIC - Reduces file scan for queries filtering on ZORDER columns.
# MAGIC - Works best for columns used frequently in WHERE clauses.
# MAGIC
# MAGIC #### EXAMPLE USE CASE:
# MAGIC - Periodically optimize large Delta tables with frequent writes/updates.
# MAGIC - Use ZORDER on high-selectivity/filtering columns to improve read performance.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Step 1 – Create the Delta table
# MAGIC use lakehousecat.deltadb;
# MAGIC CREATE OR REPLACE TABLE customer_txn (
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
# MAGIC describe history customer_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC --Step 2 – Insert multiple small batches
# MAGIC --Each insert writes a few small Parquet files.
# MAGIC -- Batch 1
# MAGIC INSERT INTO customer_txn VALUES
# MAGIC  (1, 1001, 'North', 250.00, 'Online', '2025-10-01'),
# MAGIC  (2, 1002, 'South', 400.00, 'Offline', '2025-10-02'),
# MAGIC  (3, 1003, 'West', 600.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC -- Batch 2
# MAGIC INSERT INTO customer_txn VALUES
# MAGIC  (4, 1001, 'North', 300.00, 'Offline', '2025-10-01'),
# MAGIC  (5, 1004, 'East', 750.00, 'Online', '2025-10-02'),
# MAGIC  (6, 1005, 'South', 180.00, 'Online', '2025-10-03');
# MAGIC
# MAGIC -- Batch 3
# MAGIC INSERT INTO customer_txn VALUES
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
# MAGIC select * from customer_txn where region='North';
# MAGIC
# MAGIC optimize customer_txn;
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
# MAGIC optimize customer_txn zorder by transaction_date;
# MAGIC
# MAGIC optimize customer_txn;
# MAGIC region=North
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3 - optimize & sort + colocate the data rows in transaction_date
# MAGIC region=South
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2
# MAGIC     - part-3 - optimize & sort + colocate the data rows in transaction_date
# MAGIC region=West
# MAGIC     - part-0
# MAGIC     - part-1
# MAGIC     - part-2 - optimize & sort + colocate the data rows in transaction_date
# MAGIC region=East
# MAGIC     - part-0
# MAGIC     - part-1 - optimize & sort + colocate the data rows in transaction_date
# MAGIC */
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history customer_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3 – Inspect fragmentation (numFiles & sizeInBytes)
# MAGIC DESCRIBE DETAIL customer_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4 – Run OPTIMIZE ZORDER - watch out the metrics - zOrderStats
# MAGIC -- Now compact and physically order data.
# MAGIC OPTIMIZE customer_txn ZORDER BY (transaction_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customer_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3 – Inspect fragmentation
# MAGIC DESCRIBE DETAIL customer_txn;

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Partitioning
# MAGIC Partitioning is the practice of physically splitting a table's data into separate **folders** based on a column.<br>
# MAGIC Good partition columns:<br>
# MAGIC - Low cardinality (low difference columns such as date, age, city, region, gender)
# MAGIC - Columns used Frequently used in filters
# MAGIC - Stable (we can't change the partition columns very frequently)

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat.deltadb;
# MAGIC CREATE OR REPLACE TABLE customer_txn_part1 (
# MAGIC     txn_id INT,
# MAGIC     customer_id INT,
# MAGIC     region STRING,
# MAGIC     txn_amount DOUBLE,
# MAGIC     txn_type STRING,
# MAGIC     transaction_date DATE
# MAGIC ) 
# MAGIC using delta
# MAGIC partitioned by (transaction_date);
# MAGIC insert into customer_txn_part1 select * from customer_txn;
# MAGIC --or
# MAGIC create or replace table customer_txn_part partitioned by (transaction_date) as select * from customer_txn;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select * from customer_txn_part1 where transaction_date='2025-10-01';

# COMMAND ----------

#Just to show you how the data is partitioned in the filesystem (behind the scene)
spark.sql("select * from customer_txn").write.partitionBy("region").format("delta").save("/Volumes/catalog3_we47/schema3_we47/datalake/cust_txns_partdelta")


#equivalent CTAS in Pyspark python programming
spark.sql("select * from customer_txn").write.partitionBy("region").saveAsTable("customer_txn_part2")

# COMMAND ----------

display(spark.sql('SHOW PARTITIONS customer_txn_part'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM customer_txn_part
# MAGIC WHERE transaction_date BETWEEN '2025-10-01' AND '2025-10-01';--picks the data from the 2025-10-01 folder directly and show the result quickly.

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Vaccum
# MAGIC *VACUUM* in Delta Lake removes old, unused files to free up storage, default retention hours is 168. These files come from operations like DELETE, UPDATE, or MERGE and are kept temporarily so time-travel queries can work.<br>
# MAGIC
# MAGIC Before VACUUM<br>
# MAGIC Active + deleted parquet files exist<br>
# MAGIC
# MAGIC After VACUUM<br>
# MAGIC Only ACTIVE parquet files remains and delete Old parquet files (from UPDATE/MERGE/DELETE)<br>
# MAGIC Logs remain maintained (will not delete logs, only old data deleted)<br>
# MAGIC Time travel beyond retention becomes impossible<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM drugstbl_merge RETAIN 168 HOURS;
# MAGIC --SET spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Liquid Clustering
# MAGIC *Liquid Clustering is the* Next-generation data clustering feature that automatically manages physical data organization on disk to minimize scan cost for frequently queried columns only on Delta tables by performing automatic Z-Ordering, Partitioning and Optimize.<br>
# MAGIC while clustering in databricks delta does partition happens literally?
# MAGIC No, liquid clustering does not create literal physical partitions (subdirectories). 
# MAGIC still we get the benifits of partitioning while doing clustering?
# MAGIC Yes, you absolutely still get the benefits of partitioning while doing clustering.
# MAGIC
# MAGIC **Partition vs Liquid Clustering**
# MAGIC | Use case                       | Recommendation         |
# MAGIC | ------------------------------ | ---------------------- |
# MAGIC | High-cardinality columns       | Liquid clustering    |
# MAGIC | Frequently changing filters    | Liquid clustering    |
# MAGIC | Streaming / incremental loads  | Liquid clustering    |
# MAGIC | Static, low-cardinality (date) | Partition OR Liquid |
# MAGIC | Legacy Hive-style tables       | Partition           |
# MAGIC
# MAGIC
# MAGIC **Typical Use Cases**
# MAGIC - Large tables with frequent inserts, updates, and deletes.
# MAGIC - Query filtering on specific columns like customer_id, region, order_date.

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat.deltadb

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The CLUSTER BY clause enables liquid clustering automatically.
# MAGIC CREATE TABLE IF NOT EXISTS sales_orders_liquid
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
# MAGIC CLUSTER BY (customer_id, region);--clustering column can be high or low cardinal, unlike partition which requires only low cardinal columns.
# MAGIC --column order used in cluster by is based on the primary filter, ie. whether you first filter based on customer_id or region, accordingly keep the coloumns order.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Each insert simulates separate data ingestion.
# MAGIC
# MAGIC INSERT INTO sales_orders_liquid VALUES
# MAGIC  (1, 101, 'North', 'Laptop', 2, 65000, '2025-10-01'),
# MAGIC  (2, 102, 'South', 'Headphones', 5, 2500, '2025-10-01'),
# MAGIC  (3, 103, 'West', 'Desk Chair', 3, 4500, '2025-10-02');
# MAGIC
# MAGIC INSERT INTO sales_orders_liquid VALUES
# MAGIC  (4, 101, 'North', 'Keyboard', 1, 1200, '2025-10-03'),
# MAGIC  (5, 104, 'East', 'Monitor', 2, 9500, '2025-10-03'),
# MAGIC  (6, 105, 'South', 'Mouse', 4, 700, '2025-10-03');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_orders_liquid where customer_id=102;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sales_orders_liquid
# MAGIC SET price = price * 1.05
# MAGIC WHERE region = 'North';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_orders_liquid;--It proves the optimize and zordering is done naturally (look at the operation column)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM sales_orders_liquid
# MAGIC WHERE region = 'East';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_orders_liquid;

# COMMAND ----------

spark.sql("select * from lakehousecat.deltadb.sales_orders_liquid order by region").write.clusterBy("region").format("csv").save("/Volumes/catalog3_we47/schema3_we47/datalake/cust_txns_clustercsv",mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Delta Table – CLONE
# MAGIC
# MAGIC Delta Cloning allows to create a **copy of a Delta table** efficiently:
# MAGIC - **Full clone**: independent copy of data and metadata  
# MAGIC - **Shallow clone**: metadata-only copy referencing the same underlying data files  
# MAGIC
# MAGIC **Clone vs CTAS**
# MAGIC | Aspect                  | CLONE (Delta Lake)                     | CTAS (Create Table As Select)                |
# MAGIC | ----------------------- | -------------------------------------- | -------------------------------------------- |
# MAGIC | Type                    | Delta Lake feature                     | Standard SQL feature                         |
# MAGIC | Data copy               | Metadata-only (Shallow) or full (Deep) | Full physical data copy                      |
# MAGIC | Speed                   | Very fast (especially Shallow Clone)   | Slower for large tables                      |
# MAGIC | Storage usage           | Minimal for Shallow Clone              | High (duplicates data)                       |
# MAGIC | Time travel & history   | Preserved                              | Not preserved                                |
# MAGIC | Schema                  | Exact copy                             | Can be modified                              |
# MAGIC | Dependency on source    | Shallow clone depends on source files  | Fully independent                            |
# MAGIC | Use case                | Dev/Test copies, backups, experiments  | Aggregations, filtered or transformed tables |
# MAGIC | Source table type       | Delta tables only                      | Delta or non-Delta tables                    |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CTAS (Create Table as Select)
# MAGIC
# MAGIC **Full copy** creates an **independent copy**:
# MAGIC - Data files are **copied**
# MAGIC - No metadata copy

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales_orders_ctas AS SELECT * FROM 
# MAGIC sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_orders_ctas;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail sales_orders_ctas;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Full Clone
# MAGIC
# MAGIC **Full clone** creates an **independent copy**:
# MAGIC - Data files are **copied**
# MAGIC - Medata copied
# MAGIC - Uses more storage
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales_orders_sclone 
# MAGIC CLONE sales_orders_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_orders_liquid

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales_orders_clone

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Shallow Clone
# MAGIC
# MAGIC **Shallow clone** creates a **metadata-only copy**:
# MAGIC - Shares the same underlying data files
# MAGIC - Very fast, uses minimal extra storage
# MAGIC - A shallow clone shares data files, but it does NOT share the transaction log
# MAGIC - Even if two tables point to the same data files, they are logically independent because they have separate logs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sales_orders_l_sclone
# MAGIC SHALLOW CLONE sales_orders_l;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify shallow clone
# MAGIC SELECT * FROM sales_orders_l_sclone;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_orders_l_sclone;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sales_orders_l VALUES
# MAGIC  (7, 101, 'North', 'Keyboard', 1, 1200, '2025-10-04');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE sales_orders_l
# MAGIC SET price = 200.1
# MAGIC WHERE region = 'South';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify shallow clone
# MAGIC SELECT * FROM sales_orders_l;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Still points the old data files
# MAGIC SELECT * FROM sales_orders_l_sclone;

# COMMAND ----------

# MAGIC %md
# MAGIC ####7. Deletion Vector
# MAGIC A Deletion Vector is a metadata structure that marks specific rows as deleted inside a Parquet file, without rewriting the file.<br>
# MAGIC Eg. Instead of rewriting whole files, Delta just says: “row 3, row 15, row 102 are deleted”
# MAGIC DV Benifits:
# MAGIC - Parquet file count is unchanged
# MAGIC - New DV files exist internally
# MAGIC
# MAGIC If you disable DV:
# MAGIC - File rewrite happens
# MAGIC - New parquet files created
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_dv AS
# MAGIC SELECT
# MAGIC   id AS order_id,
# MAGIC   CASE WHEN id % 2 = 0 THEN 'APAC' ELSE 'EMEA' END AS region
# MAGIC FROM range(0, 20);
# MAGIC select * from orders_dv;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_dv
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_dv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_dv WHERE region = 'APAC';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_dv

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_dv;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_dv
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_dv WHERE order_id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_dv;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check the OperationMetrics
# MAGIC DESCRIBE HISTORY orders_dv;
