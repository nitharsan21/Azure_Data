# Databricks notebook source
if not any(mount.mountPoint == '/mnt/groupe8' for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = "wasbs://groupe8@esgidatas.blob.core.windows.net",
    mount_point = "/mnt/groupe8",
    extra_configs = {"fs.azure.account.key.esgidatas.blob.core.windows.net":dbutils.secrets.get(scope = "ScopeESGI", key = "testH")})


# COMMAND ----------

for mount in dbutils.fs.mounts():
    print(mount)

# COMMAND ----------

df = spark.read.csv("/mnt/groupe8/Characters.csv",sep=';',header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#rawdata.show()
#display c'est plus joli
display(df)

# COMMAND ----------

# rawdata1 = spark.read.csv("/mnt/groupe8/Harry Potter 1.csv",sep=';',header=True)

# COMMAND ----------

# display(rawdata1)
