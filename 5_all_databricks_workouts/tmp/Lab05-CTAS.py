# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table – CTAS (Create Table As Select) Example
# MAGIC
# MAGIC ## Overview
# MAGIC CTAS (Create Table As Select) is a **Databricks SQL command** used to:
# MAGIC 1. Create a new Delta table
# MAGIC 2. Populate it immediately with data from a **SELECT query**
# MAGIC
# MAGIC **Advantages of CTAS:**
# MAGIC - Create & populate table in **one step**
# MAGIC - Avoids separate `CREATE TABLE` + `INSERT INTO`
# MAGIC - Supports **transformations during table creation**
# MAGIC - Can specify **table properties, partitioning, clustering, and storage location**
# MAGIC
# MAGIC Syntax:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE <table_name>
# MAGIC USING <format>
# MAGIC [LOCATION <path>]
# MAGIC AS
# MAGIC SELECT ...;
# MAGIC ```
# MAGIC - **Advantages**:
# MAGIC   1. Creates & populates table in **one step**.
# MAGIC   2. Supports **transformations** (casting, filtering, computed columns) during creation.
# MAGIC   3. Supports **Delta table properties**, partitioning, and clustering.
# MAGIC   4. Reduces need for separate `CREATE TABLE` + `INSERT` operations.
# MAGIC

# COMMAND ----------

# Step 1: Prepare sample transaction data as DataFrame
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


## Step 2: Write the DataFrame to a staging Delta table
# We will create a **temporary table** to use for the CTAS example.

df_txn.write.format("delta").mode("overwrite").saveAsTable("inceptez_catalog.inputdb.txn_staging")
print("Staging Table Created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a new table using CTAS
# MAGIC
# MAGIC - The new table `txn_ctas` is **created and populated** in a single command.
# MAGIC - We can also apply transformations during creation (e.g., cast amount to double, convert date).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inceptez_catalog.inputdb.txn_ctas
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   txnid,
# MAGIC   to_date(txndate,'MM-dd-yyyy') as txndate,
# MAGIC   custid,
# MAGIC   CAST(amount AS DOUBLE) as amount,
# MAGIC   product,
# MAGIC   category,
# MAGIC   city,
# MAGIC   state,
# MAGIC   paytype
# MAGIC FROM inceptez_catalog.inputdb.txn_staging;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Query the new CTAS table
# MAGIC select * from inceptez_catalog.inputdb.txn_ctas;
# MAGIC describe formatted inceptez_catalog.inputdb.txn_ctas;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inceptez_catalog.inputdb.txn_ctas_exercise
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM inceptez_catalog.inputdb.txn_ctas
# MAGIC WHERE category = 'Exercise & Fitness';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inceptez_catalog.inputdb.txn_ctas_opt
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (state)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataCapture' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   txnid,
# MAGIC   to_date(txndate,'MM-dd-yyyy') as txndate,
# MAGIC   custid,
# MAGIC   CAST(amount AS DOUBLE) as amount,
# MAGIC   product,
# MAGIC   category,
# MAGIC   city,
# MAGIC   state,
# MAGIC   paytype
# MAGIC FROM inceptez_catalog.inputdb.txn_staging;
