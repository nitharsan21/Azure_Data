# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS groupe8;

# COMMAND ----------

df = spark.read.csv("/mnt/groupe8/Characters.csv",sep=';',header=True)
listOfHouse = ['Gryffindor', 'Slytherin', 'Ravenclaw', 'Hufflepuff']
formatDate = "yyyy-mm"

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

data = df.filter(col("House").isin(listOfHouse))
data = data.select("Gender", "Wand", "Patronus", "Species", "Blood status", "Loyalty", "Skills", "Birth", "House", "Eye colour").withColumnRenamed("Blood status", "Bloodstatus").withColumnRenamed("Eye colour", "Eyecolour")
data = data.withColumn("Patronus", when(data.Patronus == "Unknown", "null") .when(data.Patronus == "None", "null").otherwise(data.Patronus))

# COMMAND ----------

data = data.withColumn("Birth", regexp_replace('Birth', 'Late', '28'))
data = data.withColumn("Birth", regexp_replace('Birth', 'or earlier', ''))
data = data.withColumn("Birth", regexp_replace('Birth', 'prior to', ''))
data = data.withColumn("Birth", expr("CASE WHEN Birth LIKE '%–%' THEN 'null' " + 
               "WHEN Birth LIKE '%Pre%' THEN 'null' "+
               "WHEN Birth LIKE '%pre%' THEN 'null' "+
               "WHEN Birth LIKE '%c.%' THEN 'null' " +
               "WHEN Birth LIKE '%Post%' THEN 'null' "+
               "WHEN Birth LIKE '%century%' THEN 'null' "+
               "WHEN Birth LIKE '%In or before%' THEN 'null' "+
               "ELSE Birth END"))

# COMMAND ----------

data.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable("groupe8.pottertable3")

# COMMAND ----------

display(data)

# COMMAND ----------

display(df)

# COMMAND ----------


