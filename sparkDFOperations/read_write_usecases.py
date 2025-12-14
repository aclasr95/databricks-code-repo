# Databricks notebook source
# MAGIC %md
# MAGIC #Telecom Domain ReadOps Assignment
# MAGIC This notebook contains assignments to practice Spark read options and Databricks volumes. <br>
# MAGIC Sections: Sample data creation, Catalog & Volume creation, Copying data into Volumes, Path glob/recursive reads, toDF() column renaming variants, inferSchema/header/separator experiments, and exercises.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png)
# MAGIC ![](https://theciotimes.com/wp-content/uploads/2021/03/TELECOM1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ##First Import all required libraries & Create spark session object

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Write SQL statements to create:
# MAGIC 1. A catalog named telecom_catalog_assign
# MAGIC 2. A schema landing_zone
# MAGIC 3. A volume landing_vol
# MAGIC 4. Using dbutils.fs.mkdirs, create folders:<br>
# MAGIC /Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/
# MAGIC /Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/
# MAGIC /Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/
# MAGIC 5. Explain the difference between (Just google and understand why we are going for volume concept for prod ready systems):<br>
# MAGIC a. Volume vs DBFS/FileStore<br>
# MAGIC b. Why production teams prefer Volumes for regulated data<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS telecom_catalog_assign

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS telecom_catalog_assign.landing_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS telecom_catalog_assign.landing_zone.landing_vol

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/")
dbutils.fs.mkdirs("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/")
dbutils.fs.mkdirs("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Volume vs DBFS/FileStore
# MAGIC Volumes are governed storage, while DBFS/FileStore is unmanaged workspace storage. </br>
# MAGIC **1.Volume?**
# MAGIC _A Volume is a Unity Catalog–governed storage abstraction used to store files securely in Databricks.
# MAGIC It is designed for production-grade, regulated, enterprise workloads._</br>
# MAGIC **2. DBFS/FileStore**
# MAGIC _DBFS (Databricks File System) and FileStore are legacy, workspace-scoped storage locations mainly used for:_
# MAGIC - Temporary files
# MAGIC - Demos
# MAGIC - Notebook experiments
# MAGIC
# MAGIC ### Why production teams prefer Volumes for regulated data
# MAGIC
# MAGIC DBFS/FileStore = personal notebook scratchpad </br>
# MAGIC Volumes = governed, secure, auditable production storage
# MAGIC
# MAGIC Thats why production teams always choose Volumes for regulated and business-critical data.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data files to use in this usecase:
# MAGIC customer_csv = '''
# MAGIC 101,Arun,31,Chennai,PREPAID
# MAGIC 102,Meera,45,Bangalore,POSTPAID
# MAGIC 103,Irfan,29,Hyderabad,PREPAID
# MAGIC 104,Raj,52,Mumbai,POSTPAID
# MAGIC 105,,27,Delhi,PREPAID
# MAGIC 106,Sneha,abc,Pune,PREPAID
# MAGIC '''
# MAGIC
# MAGIC usage_tsv = '''customer_id\tvoice_mins\tdata_mb\tsms_count
# MAGIC 101\t320\t1500\t20
# MAGIC 102\t120\t4000\t5
# MAGIC 103\t540\t600\t52
# MAGIC 104\t45\t200\t2
# MAGIC 105\t0\t0\t0
# MAGIC '''
# MAGIC
# MAGIC tower_logs_region1 = '''event_id|customer_id|tower_id|signal_strength|timestamp
# MAGIC 5001|101|TWR01|-80|2025-01-10 10:21:54
# MAGIC 5004|104|TWR05|-75|2025-01-10 11:01:12
# MAGIC '''

# COMMAND ----------

customer_csv = (
    "customer_id,name,age,city,plan_type\n"
    "101,Arun,31,Chennai,PREPAID\n"
    "102,Meera,45,Bangalore,POSTPAID\n"
    "103,Irfan,29,Hyderabad,PREPAID\n"
    "104,Raj,52,Mumbai,POSTPAID\n"
    "105,,27,Delhi,PREPAID\n"
    "106,Sneha,abc,Pune,PREPAID\n"
)

usage_tsv = (
    "customer_id\tvoice_mins\tdata_mb\tsms_count\n"
    "101\t320\t1500\t20\n"
    "102\t120\t4000\t5\n"
    "103\t540\t600\t52\n"
    "104\t45\t200\t2\n"
    "105\t0\t0\t0\n"
)

tower_logs_region1 = (
    "event_id|customer_id|tower_id|signal_strength|timestamp\n"
    "5001|101|TWR01|-80|2025-01-10 10:21:54\n"
    "5004|104|TWR05|-75|2025-01-10 11:01:12\n"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Filesystem operations
# MAGIC 1. Write code to copy the above datasets into your created Volume folders:
# MAGIC Customer → /Volumes/.../customer/
# MAGIC Usage → /Volumes/.../usage/
# MAGIC Tower (region-based) → /Volumes/.../tower/region1/ and /Volumes/.../tower/region2/
# MAGIC
# MAGIC 2. Write a command to validate whether files were successfully copied

# COMMAND ----------

base_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol"

customer_path = f"{base_path}/customer"
usage_path = f"{base_path}/usage"
tower_region1_path = f"{base_path}/tower/region1"
tower_region2_path = f"{base_path}/tower/region2"

# COMMAND ----------

dbutils.fs.mkdirs(tower_region1_path)
dbutils.fs.mkdirs(tower_region2_path)

# COMMAND ----------

dbutils.fs.put(f"{customer_path}/customer.csv", customer_csv, True)
dbutils.fs.put(f"{usage_path}/usage.csv", usage_tsv, True)
dbutils.fs.put(f"{tower_region1_path}/region1.csv", tower_logs_region1, True)
dbutils.fs.put(
    f"{tower_region2_path}/region2.csv",
    "event_id|customer_id|tower_id|signal_strength|timestamp\n",
    True
)

# COMMAND ----------

#Validation
print("Customer Folder:")
print(customer_path)
dbutils.fs.ls(customer_path)

# COMMAND ----------


print("\nUsage Folder:")
dbutils.fs.ls(usage_path)

# COMMAND ----------


print("\nTower Region 1 Folder:")
dbutils.fs.ls(tower_region1_path)

# COMMAND ----------

print("\nTower Region 1 Folder:")
dbutils.fs.ls(tower_region1_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Directory Read Use Cases
# MAGIC 1. Read all tower logs using:
# MAGIC Path glob filter (example: *.csv)
# MAGIC Multiple paths input
# MAGIC Recursive lookup
# MAGIC
# MAGIC 2. Demonstrate these 3 reads separately:
# MAGIC Using pathGlobFilter
# MAGIC Using list of paths in spark.read.csv([path1, path2])
# MAGIC Using .option("recursiveFileLookup","true")
# MAGIC
# MAGIC 3. Compare the outputs and understand when each should be used.

# COMMAND ----------

# simply reading
df_tower = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1",header=True, sep="|")
display(df_tower)

# COMMAND ----------

# Read all tower logs using: Path glob filter (example: *.csv) Multiple paths input Recursive lookup
df_tower_glob = spark.read.option("delimiter","|").option("pathGlobFilter","*.csv").csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1",header=True)
display(df_tower_glob)

# COMMAND ----------

#  Using pathGlobFilter Using list of paths in spark.read.csv([path1, path2]) 
region1_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1"
region2_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region2"
df_reg_pattern = spark.read.option("delimiter","|").csv([region1_path,region2_path],header=True)
display(df_reg_pattern)

# COMMAND ----------

#Using .option("recursiveFileLookup","true")
df_recursive = spark.read.option("delimiter","|").option("recursiveFileLookup", "true").csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower",header=True)
display(df_recursive)

# COMMAND ----------

# MAGIC %md
# MAGIC ### When to Use What 
# MAGIC
# MAGIC **Scenario 1 -> Known directory, filter file type** -> 	_pathGlobFilter_ <br>
# MAGIC **Scenario 2 -> Fixed list of regions**	-> _Multiple paths_ <br>
# MAGIC **Scenario 3 -> Dynamic / growing folders**	-> _recursiveFileLookup_ <br>
# MAGIC **Scenario 4 - >Production ingestion**	-> _Recursive (with filters)_ <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Schema Inference, Header, and Separator
# MAGIC 1. Try the Customer, Usage files with the option and options using read.csv and format function:<br>
# MAGIC header=false, inferSchema=false<br>
# MAGIC or<br>
# MAGIC header=true, inferSchema=true<br>
# MAGIC 2. Write a note on What changed when we use header or inferSchema  with true/false?<br>
# MAGIC 3. How schema inference handled “abc” in age?<br>

# COMMAND ----------

customer_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv"
usage_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv"

# COMMAND ----------

# Try the Customer, Usage files with the option and options using read.csv and format function:
# header=false, inferSchema=false
cust_false_df = spark.read.options(header=False,inferSchema=False).csv(customer_path)
usage_false_df = spark.read.options(header=False,inferSchema=False,delimiter='\t').csv(usage_path)
display(cust_false_df)
display(usage_false_df)

# COMMAND ----------

# header=True, inferSchema=True 
cust_true_df = spark.read.options(header=True,inferSchema=True).csv(customer_path)
usage_True_df = spark.read.options(header=True,inferSchema=True,delimiter='\t').csv(usage_path)
display(cust_true_df)
display(usage_True_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What changed when we use header or inferSchema with true/false?
# MAGIC
# MAGIC When the header is false the First row treated as data and the header of each column will be like c1, c2....<br>
# MAGIC In case of inferschema, by default the column will be considered as string. If we make inferschema as true, from the data it will infer the schema and find the appropriate datatype for each column

# COMMAND ----------

# MAGIC %md
# MAGIC **How schema inference handled “abc” in age?**<br>
# MAGIC _Column with mostly integers but one value as abc → Spark cannot cast abc to int, which results in: Column is typed as string. <br>
# MAGIC To get rid of this either Use enforced schema with .schema(custom_schema) or check the data before storing_

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Column Renaming Usecases
# MAGIC 1. Apply column names using string using toDF function for customer data
# MAGIC 2. Apply column names and datatype using the schema function for usage data
# MAGIC 3. Apply column names and datatype using the StructType with IntegerType, StringType, TimestampType and other classes for towers data 

# COMMAND ----------

customer_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv"
usage_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv"

# COMMAND ----------

#Apply column names using string using toDF function for customer data
customer_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv"
cust_df = spark.read.options(header=True,inferSchema=True).csv(customer_path).toDF(
    "Customer Id", "Name", "Age", "City", "Plan Type"
)
display(cust_df)

# COMMAND ----------

#Apply column names and datatype using the schema function for usage data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

usage_path = "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv"
usage_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("voice_mins", IntegerType(), True),
    StructField("data_mb", IntegerType(), True),
    StructField("sms_count", IntegerType(), True)
])
usage_df = spark.read.options(header=True,inferSchema=True,delimiter='\t').schema(usage_schema).csv(usage_path)
display(usage_df)
usage_df.printSchema()

# COMMAND ----------

#Apply column names and datatype using the StructType with IntegerType, StringType, TimestampType and other classes for towers data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
tower_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("tower_id", StringType(), True),
    StructField("signal_strength", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])
tower_df=spark.read.option("delimiter","|").option("recursiveFileLookup", "true").schema(tower_schema).csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower",header=True)
display(tower_df)
tower_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. More to come (stay motivated)....
