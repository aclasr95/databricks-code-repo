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

# DBTITLE 1,Write DataFrames to Delta format
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
# MAGIC ## Spark Write Operations using 
# MAGIC - csv, json, orc, parquet, delta, saveAsTable, insertInto, xml with different write mode, header and sep options

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Write Operations (Data Conversion/Schema migration) – CSV Format Usecases
# MAGIC 1. Write customer data into CSV format using overwrite mode
# MAGIC 2. Write usage data into CSV format using append mode
# MAGIC 3. Write tower data into CSV format with header enabled and custom separator (|)
# MAGIC 4. Read the tower data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating a volumn to store converted files
# MAGIC create volume if not exists telecom_catalog_assign.landing_zone.conversion_vol

# COMMAND ----------

#customer df in csv in overwrite mode
cust_df.write.format('csv').mode('overwrite').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/custdata')
display(spark.read.format('csv').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/custdata'))

# COMMAND ----------

#usage data in csv with append mode
usage_df.write.format('csv').mode('append').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/usageData')


# COMMAND ----------

display(spark.read.format('csv').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/usageData'))

# COMMAND ----------

#tower df with |seperator
tower_df.write.format('csv').mode('overwrite').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/towerdata_csv',sep='|')

# COMMAND ----------

display(spark.read.format('csv').option('delimiter','|').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/towerdata_csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##7. Write Operations (Data Conversion/Schema migration)– JSON Format Usecases
# MAGIC 1. Write customer data into JSON format using overwrite mode
# MAGIC 2. Write usage data into JSON format using append mode and snappy compression format
# MAGIC 3. Write tower data into JSON format using ignore mode and observe the behavior of this mode
# MAGIC 4. Read the tower data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

#costomer in json -> in overwrite mode
cust_df.write.format('json').mode('overwrite').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/json/cutomer_json')


# COMMAND ----------

#uasgedf in json with snappy compresson and append mode
usage_df.write.mode('append').format('json').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/json/usage_json',compression='snappy')

# COMMAND ----------

display(spark.read.format('json').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/json/usage_json'))

# COMMAND ----------

#tower_df write with ignore mode
tower_df.write.mode('ignore').format('json').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/json/tower_json')
display(spark.read.format('json').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/json/tower_json/').limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Write Operations (Data Conversion/Schema migration) – Parquet Format Usecases
# MAGIC 1. Write customer data into Parquet format using overwrite mode and in a gzip format
# MAGIC 2. Write usage data into Parquet format using error mode
# MAGIC 3. Write tower data into Parquet format with gzip compression option
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

cust_df.write.mode('overwrite').format('parquet').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/parquet/customer')

# COMMAND ----------

usage_df.write.mode('append').format('parquet').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/parquet/usage')

# COMMAND ----------

tower_df.write.mode('ignore').format('parquet').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/parquet/tower',compression='gzip')

# COMMAND ----------

display(spark.read.format('parquet').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/parquet/tower'))

# COMMAND ----------

usage_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##9. Write Operations (Data Conversion/Schema migration) – Orc Format Usecases
# MAGIC 1. Write customer data into ORC format using overwrite mode
# MAGIC 2. Write usage data into ORC format using append mode
# MAGIC 3. Write tower data into ORC format and see the output file structure
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

cust_df.write.mode('overwrite').format('orc').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/orc/customer')
usage_df.write.mode('append').format('orc').option('compression','snappy').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/orc/usage')
tower_df.write.mode('append').format('orc').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/orc/tower')
spark.read.format('orc').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/orc/usage').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##10. Write Operations (Data Conversion/Schema migration) – Delta Format Usecases
# MAGIC 1. Write customer data into Delta format using overwrite mode
# MAGIC 2. Write usage data into Delta format using append mode
# MAGIC 3. Write tower data into Delta format and see the output file structure
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.
# MAGIC 6. Compare the parquet location and delta location and try to understand what is the differentiating factor, as both are parquet files only.

# COMMAND ----------

#doing this since in delta format it wont allow spaces or special charectors in the table header
cust_df=cust_df.withColumnRenamed("Customer Id","CustomerId")\
                .withColumnRenamed("Plan Type","PlanType")
cust_df.display()

# COMMAND ----------

cust_df.write.mode('overwrite').format('delta').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/delta/customer')
usage_df.write.mode('append').format('delta').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/delta/usage')
tower_df.write.mode('ignore').format('delta').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/delta/tower')
spark.read.format('delta').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/delta/usage')

# COMMAND ----------

spark.read.format('delta').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/delta/usage').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##11. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data using saveAsTable() as a managed table
# MAGIC 2. Write usage data using saveAsTable() with overwrite mode
# MAGIC 3. Drop the managed table and verify data removal
# MAGIC 4. Go and check the table overview and realize it is in delta format in the Catalog.
# MAGIC 5. Use spark.read.sql to write some simple queries on the above tables created.
# MAGIC

# COMMAND ----------

cust_df.write.mode('overwrite').format('delta').saveAsTable('telecom_catalog_assign.landing_zone.customer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from telecom_catalog_assign.landing_zone.customer

# COMMAND ----------

usage_df.write.mode('append').format('delta').saveAsTable('telecom_catalog_assign.landing_zone.usage')

# COMMAND ----------

usage_df.printSchema()

# COMMAND ----------

 spark.sql('select * from telecom_catalog_assign.landing_zone.usage where customer_id=101').show(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from telecom_catalog_assign.landing_zone.usage

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table telecom_catalog_assign.landing_zone.usage

# COMMAND ----------

# MAGIC %md
# MAGIC ##12. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data using insertInto() in a new table and find the behavior
# MAGIC 2. Write usage data using insertTable() with overwrite mode

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE telecom_catalog_assign.landing_zone.customer_new (
# MAGIC   CustomerId INT,
# MAGIC   Name STRING,
# MAGIC   Age STRING,
# MAGIC   CityPlanType STRING   
# MAGIC );
# MAGIC CREATE OR REPLACE TABLE telecom_catalog_assign.landing_zone.usage_new (
# MAGIC   customer_id INT,
# MAGIC   voice_mins INT,
# MAGIC   data_mb INT,
# MAGIC   sms_count INT   
# MAGIC )

# COMMAND ----------

cust_df.write.option("mergeSchema", "true").insertInto("telecom_catalog_assign.landing_zone.customer_new")

# COMMAND ----------

spark.sql('select * from telecom_catalog_assign.landing_zone.customer_new').printSchema()

# COMMAND ----------

usage_df.write.mode("overwrite").insertInto("telecom_catalog_assign.landing_zone.usage_new")

# COMMAND ----------

spark.sql('select * from telecom_catalog_assign.landing_zone.usage_new').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##13. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data into XML format using rowTag as cust
# MAGIC 2. Write usage data into XML format using overwrite mode with the rowTag as usage
# MAGIC 3. Download the xml data and open the file in notepad++ and see how the xml file looks like.

# COMMAND ----------

cust_df.write.mode('append').format('xml').option('rowTag','cust').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/xmlfile/custxml')

# COMMAND ----------

usage_df.write.mode('overwrite').format('xml').option('rowTag','usage').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/xmlfile/usage')

# COMMAND ----------

spark.read.format('xml').option('rowTag','usage').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/xmlfile/usage').show(10)
spark.read.format('xml').option('rowTag','cust').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/xmlfile/custxml').show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##14. Compare all the downloaded files (csv, json, orc, parquet, delta and xml) 
# MAGIC 1. Capture the size occupied between all of these file formats and list the formats below based on the order of size from small to big.

# COMMAND ----------

# MAGIC %md
# MAGIC ###15. Try to do permutation and combination of performing Schema Migration & Data Conversion operations like...
# MAGIC 1. Read any one of the above orc data in a dataframe and write it to dbfs in a parquet format
# MAGIC 2. Read any one of the above parquet data in a dataframe and write it to dbfs in a delta format
# MAGIC 3. Read any one of the above delta data in a dataframe and write it to dbfs in a xml format
# MAGIC 4. Read any one of the above delta table in a dataframe and write it to dbfs in a json format
# MAGIC 5. Read any one of the above delta table in a dataframe and write it to another table

# COMMAND ----------

spark.read.format('orc').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/orc/customer/').\
    write.mode('overwrite').format('parquet').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/perStorage/parkdata')

# COMMAND ----------

spark.read.format('parquet').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/parquet/customer/').\
    withColumnRenamed('Customer Id','CustomerId').withColumnRenamed('Plan Type','PlanType').write.mode('overwrite').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/perStorage/deltadata')

# COMMAND ----------

spark.read.format('delta').load('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/perStorage/deltadata').write.mode('overwrite').format('xml').option('rowTag','customer').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/perStorage/xmldatfromdelta')

# COMMAND ----------

spark.sql('select * from telecom_catalog_assign.landing_zone.customer_new').write.mode('overwrite').format('json').save('/Volumes/telecom_catalog_assign/landing_zone/conversion_vol/perStorage/jsonfromdelta')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from telecom_catalog_assign.landing_zone.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from telecom_catalog_assign.landing_zone.customer_new

# COMMAND ----------

spark.sql('select * from telecom_catalog_assign.landing_zone.customer_new').write.mode('append').option("mergeSchema",True).saveAsTable("telecom_catalog_assign.landing_zone.customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ##16. Do a final exercise of defining one/two liner of... 
# MAGIC 1. When to use/benifits csv
# MAGIC 2. When to use/benifits json
# MAGIC 3. When to use/benifit orc
# MAGIC 4. When to use/benifit parquet
# MAGIC 5. When to use/benifit delta
# MAGIC 6. When to use/benifit xml
# MAGIC 7. When to use/benifit delta tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **CSV**
# MAGIC Use for simple, flat data exchange.
# MAGIC Human-readable but large, slow, and no schema support.
# MAGIC
# MAGIC **JSON**
# MAGIC
# MAGIC Use for semi-structured or nested data (APIs, logs).
# MAGIC Flexible schema but slower and larger than columnar formats.
# MAGIC
# MAGIC **ORC**
# MAGIC
# MAGIC Use for Hive-based analytical workloads.
# MAGIC Highly compressed, columnar, and efficient for aggregations.
# MAGIC
# MAGIC **Parquet**
# MAGIC
# MAGIC Use for analytics and big data processing.
# MAGIC Columnar, compressed, fast reads, but no ACID guarantees.
# MAGIC
# MAGIC **Delta (files)**
# MAGIC
# MAGIC Use for reliable data lakes with updates.
# MAGIC Adds ACID, schema enforcement, and time travel on Parquet.
# MAGIC
# MAGIC **XML**
# MAGIC
# MAGIC Use for legacy or enterprise systems.
# MAGIC Supports hierarchy but verbose and slow to process.
# MAGIC
# MAGIC **Delta Tables**
# MAGIC
# MAGIC Use for production, governed analytics in Databricks.
# MAGIC Delta files + catalog metadata, permissions, and lineage.
