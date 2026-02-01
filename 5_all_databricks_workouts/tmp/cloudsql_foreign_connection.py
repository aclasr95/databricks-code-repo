# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CONNECTION mysql_connectionworking1
# MAGIC TYPE mysql
# MAGIC OPTIONS (
# MAGIC   host '152.58.71.34.bc.googleusercontent.com',
# MAGIC   port '3306',
# MAGIC   user 'root',
# MAGIC   password 'Inceptez@123',
# MAGIC   trustServerCertificate 'true'
# MAGIC );
# MAGIC
# MAGIC CREATE FOREIGN CATALOG cloudsql_foreign_catalog1
# MAGIC USING CONNECTION mysql_connectionworking1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from foreigncat.testdb.tbl1;
