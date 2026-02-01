# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Deletion Vectors
# MAGIC
# MAGIC ### **What are Deletion Vectors?**
# MAGIC A **Deletion Vector (DV)** in Delta Lake is a mechanism that enables **faster and more efficient row-level deletes and updates** without rewriting entire data files.
# MAGIC
# MAGIC Traditionally, when a record was deleted or updated in a Delta table, the affected Parquet file had to be rewritten.  
# MAGIC With **Deletion Vectors**, Delta Lake now marks deleted rows **logically**, storing their positions in a separate **deletion vector file (.dv)** — rather than physically removing them immediately.
# MAGIC
# MAGIC This allows:
# MAGIC - **Faster DELETE, UPDATE, and MERGE operations**
# MAGIC - **Reduced data rewriting**
# MAGIC - **Better concurrency and scalability**
# MAGIC
# MAGIC Deletion vectors were introduced with **Delta Lake 2.3+** and are **enabled by default** in **Databricks Runtime 13.0+**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **How Deletion Vectors Work**
# MAGIC 1. When a DELETE or UPDATE happens, Delta records the row positions that were removed or changed in a **deletion vector bitmap**.  
# MAGIC 2. The underlying data file remains unchanged, but during reads, those rows are **logically filtered out**.  
# MAGIC 3. Periodic **OPTIMIZE** or **VACUUM** operations can later rewrite files to physically remove the deleted rows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Key Features**
# MAGIC | **Feature** | **Description** |
# MAGIC |--------------|----------------|
# MAGIC | **Logical Deletes** | Marks deleted rows using a bitmap instead of rewriting files. |
# MAGIC | **Faster Updates** | Reduces I/O during MERGE and UPDATE operations. |
# MAGIC | **Automatic Management** | Databricks manages DV creation and cleanup automatically. |
# MAGIC | **Compatibility** | Works with Delta tables using column mapping mode `name` or `id`. |
# MAGIC | **Compaction** | `OPTIMIZE` operation can compact and physically remove DVs. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **When Deletion Vectors Are Created**
# MAGIC - During `DELETE`, `UPDATE`, or `MERGE` operations.
# MAGIC - When **Liquid Clustering** or **OPTIMIZE** is used, DVs may also appear for data reorganization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Important Configuration**
# MAGIC You can control deletion vectors using the following properties:
# MAGIC ```sql
# MAGIC -- Enable or disable deletion vectors
# MAGIC SET spark.databricks.delta.properties.defaults.enableDeletionVectors = true;
# MAGIC

# COMMAND ----------


# Step 1: Create a Delta table with sample data

data = [
    (1, "Kamath", 5000),
    (2, "Raghu", 6000),
    (3, "Avantika", 7000),
    (4, "Bhavana", 8000)
]

columns = ["emp_id", "name", "salary"]

df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("lakehousecat.deltadb.employee_dv_demo1")

display(spark.table("lakehousecat.deltadb.employee_dv_demo1"))


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakehousecat.deltadb.employee_dv_demo1
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat.deltadb;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Delete record from the Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employee_dv_demo1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employee_dv_demo1 WHERE emp_id = 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employee_dv_demo1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY employee_dv_demo1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Disable Deletion Vector Feature for the Table
# MAGIC

# COMMAND ----------

spark.sql("""
ALTER TABLE employee_dv_demo1
SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)
""")

# Confirm the table property
display(spark.sql("DESCRIBE EXTENDED employee_dv_demo1"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Perform a DELETE Operation
# MAGIC

# COMMAND ----------

# Delete a record (will create deletion vector instead of rewriting files)
spark.sql("DELETE FROM employee_dv_demo1 WHERE emp_id = 4")



# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY employee_dv_demo1;

# COMMAND ----------

# MAGIC %sql
# MAGIC update employee_dv_demo set name = 'Vishal' where emp_id=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY employee_dv_demo1;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE employee_dv_demo1
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC update employee_dv_demo1 set name = 'Vimal' where emp_id=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY employee_dv_demo1;
