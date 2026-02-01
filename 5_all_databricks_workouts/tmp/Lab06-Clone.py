# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table – CLONE
# MAGIC
# MAGIC Delta Cloning allows to create a **copy of a Delta table** efficiently:
# MAGIC - **Full clone**: independent copy of data and metadata  
# MAGIC - **Shallow clone**: metadata-only copy referencing the same underlying data files  

# COMMAND ----------

# Step 1: Prepare sample transaction data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.getOrCreate()

data = [
  ("00000000","06-26-2011","4007024","040.33","Exercise & Fitness","Cardio Machine Accessories","Clarksville","Tennessee","credit"),
  ("00000001","05-26-2011","4006742","198.44","Exercise & Fitness","Weightlifting Gloves","Long Beach","California","credit"),
  ("00000002","06-01-2011","4009775","005.58","Exercise & Fitness","Weightlifting Machine Accessories","Anaheim","California","credit"),
  ("00000003","06-05-2011","4002199","198.19","Gymnastics","Gymnastics Rings","Milwaukee","Wisconsin","credit"),
  ("00000004","12-17-2011","4002613","098.81","Team Sports","Field Hockey","Nashville  ","Tennessee","credit"),
  ("00000005","02-14-2011","4007591","193.63","Outdoor Recreation","Camping & Backpacking & Hiking","Chicago","Illinois","credit"),
  ("00000006","10-28-2011","4002190","027.89","Puzzles","Jigsaw Puzzles","Charleston","South Carolina","credit"),
  ("00000007","07-14-2011","4002964","096.01","Outdoor Play Equipment","Sandboxes","Columbus","Ohio","credit"),
  ("00000008","01-17-2011","4007361","010.44","Winter Sports","Snowmobiling","Des Moines","Iowa","credit")
]

schema = StructType([
    StructField("txnid", StringType(), True),
    StructField("txndate", StringType(), True),
    StructField("custid", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("paytype", StringType(), True)
])

df_txn = spark.createDataFrame(data, schema)
display(df_txn)


# COMMAND ----------

df_txn.write.format("delta").mode("overwrite").saveAsTable("inceptez_catalog.inputdb.txn_base")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify base table
# MAGIC SELECT * FROM inceptez_catalog.inputdb.txn_base;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Full Clone
# MAGIC
# MAGIC **Full clone** creates an **independent copy**:
# MAGIC - Data files are **copied**
# MAGIC - Changes in the clone **do not affect the original table**
# MAGIC - Uses more storage
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inceptez_catalog.inputdb.txn_full_clone
# MAGIC CLONE inceptez_catalog.inputdb.txn_base;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inceptez_catalog.inputdb.txn_full_clone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Shallow Clone
# MAGIC
# MAGIC **Shallow clone** creates a **metadata-only copy**:
# MAGIC - Shares the same underlying data files
# MAGIC - Very fast, uses minimal extra storage
# MAGIC - Changes to the data files in the original table will reflect in shallow clone (unless new files are added)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inceptez_catalog.inputdb.txn_shallow_clone
# MAGIC SHALLOW CLONE inceptez_catalog.inputdb.txn_base;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify shallow clone
# MAGIC SELECT * FROM inceptez_catalog.inputdb.txn_shallow_clone;

# COMMAND ----------

df_txn.write.format("delta").mode("append").saveAsTable("inceptez_catalog.inputdb.txn_base")
print("Record inserted");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inceptez_catalog.inputdb.txn_base;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inceptez_catalog.inputdb.txn_full_clone

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM inceptez_catalog.inputdb.txn_shallow_clone;
# MAGIC
# MAGIC DESCRIBE HISTORY inceptez_catalog.inputdb.txn_base;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE inceptez_catalog.inputdb.txn_shallow_clone
# MAGIC SHALLOW CLONE inceptez_catalog.inputdb.txn_base;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY inceptez_catalog.inputdb.txn_shallow_clone;
