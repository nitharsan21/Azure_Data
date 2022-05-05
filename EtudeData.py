# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS groupe8;

# COMMAND ----------

df = spark.read.csv("/mnt/groupe8/Characters.csv",sep=';',header=True)
listOfHouse = ['Gryffindor', 'Slytherin', 'Ravenclaw', 'Hufflepuff']

# COMMAND ----------

from pyspark.sql.functions import col

filered_data = df.filter(col("House").isin(listOfHouse))
simpletestdata = filered_data.select("Gender", "Wand", "Patronus", "Species", "Blood status", "Loyalty", "Skills", "Birth", "House").withColumnRenamed("Blood status", "Bloodstatus")

# COMMAND ----------

simpletestdata.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable("groupe8.pottertest")

# COMMAND ----------

display(df)

# COMMAND ----------


