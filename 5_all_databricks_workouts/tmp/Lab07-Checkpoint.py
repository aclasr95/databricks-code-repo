# Databricks notebook source
# MAGIC %md
# MAGIC # Delta File – Checkpoint Example
# MAGIC
# MAGIC **Checkpoint in Delta Lake**
# MAGIC
# MAGIC ### Overview
# MAGIC - A checkpoint is a compacted snapshot of the Delta log
# MAGIC - **Purpose**: Reduce query latency for Delta tables with large number of `_delta_log` directories.  
# MAGIC - **How it works**:
# MAGIC   1. Creates a **Parquet checkpoint** summarizing table state.
# MAGIC   2. Each checkpoint corresponds to a **specific version**.
# MAGIC   3. Speeds up future queries by reading fewer JSON logs.
# MAGIC - **Applicable for** both streaming and batch tables.
# MAGIC - **Best practice**: Periodically run checkpoint for frequently updated Delta datasets.
# MAGIC

# COMMAND ----------



# Step 1: Prepare sample transaction data
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.sql import Row

data = [
    Row(emp_id=1, emp_name="Venkat", dept="HR", salary=50000),
    Row(emp_id=2, emp_name="Sathish", dept="Finance", salary=60000),
    Row(emp_id=3, emp_name="Jay", dept="IT", salary=70000)
]
df_txn = spark.createDataFrame(data)
display(df_txn)


# COMMAND ----------

# Step 2: Write Data as Delta Files (not table)

# Save as Delta format in the given path
df_txn.write.format("delta").mode("overwrite").save("/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint")

# Verify the data written
df_verify = spark.read.format("delta").load("/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint")
display(df_verify)


# COMMAND ----------

delta_path = "/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint"

# Overwrite existing data
df.write.format("delta").mode("overwrite").save(delta_path)

print(" Data written to Delta path:", delta_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Update record directly using Delta path (no table)
# MAGIC UPDATE delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`
# MAGIC SET salary = 55000
# MAGIC WHERE emp_name = 'Sathish';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Update record directly using Delta path (no table)
# MAGIC UPDATE delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`
# MAGIC SET salary = 82000
# MAGIC WHERE emp_name = 'Venkat';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 5: Delete record directly using Delta path (no table)
# MAGIC DELETE FROM delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`
# MAGIC WHERE emp_name = 'Jay';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 6: Verify Data After Update/Delete
# MAGIC SELECT * FROM delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`
# MAGIC SET TBLPROPERTIES ('delta.checkpointInterval' = '3');

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Update record directly using Delta path (no table)
# MAGIC UPDATE delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`
# MAGIC SET salary = 905000
# MAGIC WHERE emp_name = 'Venkat';

# COMMAND ----------

DESC HISTORY delta.`/Volumes/inceptez_catalog/inputdb/employee/dept_checkpoint`;
