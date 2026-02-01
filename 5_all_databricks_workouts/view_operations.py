# Databricks notebook source
# MAGIC %md
# MAGIC Local temp views exist only within the current Spark session.

# COMMAND ----------

df1=spark.sql("select * from orders_dv")
df1.createOrReplaceTempView("localview1")
spark.sql("select * from localview1").show(2)

# COMMAND ----------

#Open a new notebook (or detach & reattach)


# COMMAND ----------

# MAGIC %md
# MAGIC Global temp views are shared across notebooks in the same cluster.

# COMMAND ----------

df1.createOrReplaceTempView("localview1")
df1.createOrReplaceGlobalTempView("globalview1")

# COMMAND ----------

spark.sql(select * from localview1).show(2)

# COMMAND ----------

spark.sql(select * from globalview1).show(2)

# COMMAND ----------

df1=spark.sql("""create or replace temp view view1 as 
SELECT
  id AS order_id, 'retail' as system,
  CASE WHEN id % 2 = 0 THEN 'APAC' ELSE 'EMEA' END AS region
FROM range(0, 20);""")

# COMMAND ----------

# DBTITLE 1,Cell 9
# Create the temp view
spark.sql("""
create or replace temp view view1 as 
SELECT
  id AS order_id, 'retail' as system,
  CASE WHEN id % 2 = 0 THEN 'APAC' ELSE 'EMEA' END AS region
FROM range(0, 20)
""")

# Load data from the view into df1
df1 = spark.sql("SELECT * FROM view1")


# COMMAND ----------

df1.write.clusterBy("region").csv("/Volumes/lakehousecat/deltadb/datalake/tgt4/")
