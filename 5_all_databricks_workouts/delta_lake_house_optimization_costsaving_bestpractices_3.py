# Databricks notebook source
# MAGIC %md
# MAGIC ###Deltalake & Lakehouse Optimization Usecases

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/5_all_databricks_workouts/DELTA OPTIMIZATIONS.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Handling Data Skew & Query Performance (Optimize & Z-Order)
# MAGIC Scenario: The analytics team reports that queries filtering silver_shipments by source_city and shipment_date are becoming slow as data volume grows.
# MAGIC
# MAGIC Task: Run the OPTIMIZE command with ZORDER on the silver_shipments table to co-locate related data in the same files.
# MAGIC
# MAGIC Outcome:
# MAGIC Why did we choose source_city and shipment_date for Z-Ordering instead of shipment_id? Think about high cardinality vs. query filtering

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from logistics_job_project.landing_schema.silver_shipments
# MAGIC
# MAGIC OPTIMIZE logistics_job_project.landing_schema.silver_shipments ZORDER BY (source_city,shipment_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why ZOrdering
# MAGIC
# MAGIC **Z-Ordering in Databricks/Delta Lake reorganizes data files so that related values of one or more columns are physically stored together on disk. This helps skip irrelevant files during queries, reducing read time.**
# MAGIC
# MAGIC So ,
# MAGIC - High-cardinality columns (like shipment_id) don’t benefit Z-Ordering, because almost every file will have unique values — no skipping happens.
# MAGIC - Low-to-medium cardinality columns used in filters let Delta skip entire files efficiently.
# MAGIC
# MAGIC So in the above usecase the columns like source_city and shipment_date are kind of low cardinal columns since it can be same for many rows so we can either use it for filtering or ordering that means if we use high cardinal columns like shipment_id would not help, since almost every file has a unique shipment_id, giving no real file-skipping advantage.
# MAGIC
# MAGIC
# MAGIC So queries like _SELECT * FROM silver_shipments  WHERE source_city = 'Chicago' AND shipment_date BETWEEN '2026-01-01' AND '2026-01-31';_ will improve performance
# MAGIC
# MAGIC nb : If this table is very large, you can also add a WHERE clause to optimize only recent data:
# MAGIC
# MAGIC _OPTIMIZE silver_shipments
# MAGIC WHERE shipment_date >= '2026-01-01'
# MAGIC ZORDER BY (source_city, shipment_date);_

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Speeding up Regional Queries (Partition Pruning)
# MAGIC Scenario: The dashboard team reports that queries filtering for orgin_hub_city with "New York" shipments from the gold_core_curated_tbl table are scanning the entire dataset (Terabytes of data), even though New York is only 5% of the data. This is racking up compute costs.
# MAGIC
# MAGIC Task: Re-create the gold_core_curated_tbl table partitioned by orgin_hub_city. Run a query filtering for one city to demonstrate "Partition Pruning" (where Spark skips files that don't match the filter).
# MAGIC
# MAGIC Outcome: Verify the partition filtering is applied or not, by performing explain plan, check for the PartitionFilters in the output.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE logistics_job_project.landing_schema.gold_core_curated_tbl_backup
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM logistics_job_project.landing_schema.gold_core_curated_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logistics_job_project.landing_schema.gold_core_curated_tbl_backup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS logistics_job_project.landing_schema.gold_core_curated_tbl;
# MAGIC
# MAGIC CREATE TABLE logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (origin_hub_city)
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM logistics_job_project.landing_schema.gold_core_curated_tbl_backup;

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT *
# MAGIC FROM logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC WHERE origin_hub_city = 'London';

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT *
# MAGIC FROM logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC WHERE origin_hub_city = 'Mumbai';

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT *
# MAGIC FROM logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC where role='driver'

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Storage Cost Savings (Vacuum)
# MAGIC Scenario: Your Project pipeline runs every hour, creating many small files and obsolete versions of data. Your storage costs are rising. You need to clean up files that are no longer needed for time travel.
# MAGIC
# MAGIC Task: Execute a Vacuum command to remove data files older than the retention threshold.
# MAGIC
# MAGIC Outcome: Perform the describe history and find whether vacuum is completed.

# COMMAND ----------

# MAGIC %sql
# MAGIC --this wont work since we are on serverless cluster
# MAGIC --SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC --VACUUM logistics_job_project.landing_schema.gold_core_curated_tbl RETAIN 24 HOURS;
# MAGIC
# MAGIC --this will remove the history before 7 days
# MAGIC VACUUM logistics_job_project.landing_schema.gold_core_curated_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE History logistics_job_project.landing_schema.gold_core_curated_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Modern Data Layout (Liquid Clustering)
# MAGIC Scenario: You are redesigning the silver_shipments table. You want to avoid the "small files" problem and need a flexible layout that adapts to changing query patterns automatically without rewriting the table.
# MAGIC
# MAGIC Task: Re-create the silver_shipments table using Liquid Clustering on the shipment_id column.
# MAGIC
# MAGIC Outcome: Liquid Clustering over traditional partitioning when the cardinality of shipment_id is very high.

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists logistics_job_project.landing_schema.silver_shipments_backup
# MAGIC USING DELTA
# MAGIC AS
# MAGIC select * from logistics_job_project.landing_schema.silver_shipments

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists logistics_job_project.landing_schema.silver_shipments;
# MAGIC
# MAGIC create table logistics_job_project.landing_schema.silver_shipments
# MAGIC using delta
# MAGIC cluster by (shipment_id)
# MAGIC AS
# MAGIC select * from logistics_job_project.landing_schema.silver_shipments_backup

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE History logistics_job_project.landing_schema.silver_shipments

# COMMAND ----------

# MAGIC %md
# MAGIC ### Liquid Clustering:
# MAGIC
# MAGIC - Continuously reorganizes data as new files arrive
# MAGIC  
# MAGIC - Prevents small-file explosion
# MAGIC  
# MAGIC - Adapts automatically when:
# MAGIC  
# MAGIC   - Data volume grows
# MAGIC  
# MAGIC   - Query patterns change
# MAGIC  
# MAGIC   - OPTIMIZE is run later
# MAGIC
# MAGIC You don’t need to redesign partitions again and again.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Cost Efficient Environment Cloning (Shallow Clone)
# MAGIC Scenario: The QA team needs to test an update on the gold_core_curated_tbl table. The table is 5TB in size. You cannot afford to duplicate the storage cost just for a test and the update should not affect the copied table.
# MAGIC
# MAGIC Task: Create a Shallow Clone of the gold table for the QA team.
# MAGIC
# MAGIC Outcome: If we delete records from the source table (gold_core_curated_tbl), will the QA table (gold_core_curated_tbl_qa) be affected? Why or why not?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE logistics_job_project.landing_schema.gold_core_curated_tbl_qa
# MAGIC SHALLOW CLONE logistics_job_project.landing_schema.gold_core_curated_tbl;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shallow Copy
# MAGIC - No 5TB data copy
# MAGIC - Both tables initially reference the same underlying data files
# MAGIC - QA gets an independent table to test updates
# MAGIC
# MAGIC **If we delete data from source nothing will happen to the QA table**
# MAGIC Becasuse,
# MAGIC Even though both tables start by pointing to the same data files:
# MAGIC - Delta Lake uses copy-on-write
# MAGIC - Delta:
# MAGIC   - Creates new files for the modified data
# MAGIC   - Updates only the source table’s transaction log
# MAGIC   - Leaves the original files intact
# MAGIC - The QA table:
# MAGIC   - Still references the original data files
# MAGIC   - Has its own independent Delta log
# MAGIC   - Sees no changes from deletes or updates in the source table
# MAGIC
# MAGIC
# MAGIC ###### When could QA be affected? 
# MAGIC
# MAGIC Only if:
# MAGIC **_VACUUM gold_core_curated_tbl;_**
# MAGIC the vacuum removes files that the QA clone still references.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Disaster Recovery (Time Travel & Restore)
# MAGIC Scenario: A junior data engineer accidentally ran a logic error that corrupted the gold_core_curated_tbl table 15 minutes ago. You need to revert the table to its previous state immediately.
# MAGIC
# MAGIC Task: Use Delta Lake's Restore feature to roll back the table.
# MAGIC
# MAGIC Outcome:What is the difference between querying with VERSION AS OF (Time Travel) and running RESTORE?

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC --update logistics_job_project.landing_schema.gold_core_curated_tbl set latitude=10.111 where shipment_id=6000039

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history logistics_job_project.landing_schema.gold_core_curated_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp() - INTERVAL 15 MINUTES;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC TO TIMESTAMP AS OF '2026-02-01T13:22:00.374+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- not sure why this is not working
# MAGIC RESTORE TABLE logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC TO TIMESTAMP AS OF current_timestamp() - INTERVAL 15 MINUTES;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE logistics_job_project.landing_schema.gold_core_curated_tbl
# MAGIC TO VERSION AS OF 2;
