# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://groupe8@esgidatas.blob.core.windows.net",
mount_point = "/mnt/groupe8",
extra_configs = {"fs.azure.account.key.esgidatas.blob.core.windows.net":dbutils.secrets.get(scope = "ScopeESGI", key = "testH")})


# COMMAND ----------

rawdata = spark.read.csv("/mnt/groupe8/Characters.csv",sep=';',header=True)


# COMMAND ----------


