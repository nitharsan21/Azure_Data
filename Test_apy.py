# Databricks notebook source
if not any(mount.mountPoint == '/mnt/groupe8' for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = "wasbs://groupe8@esgidatas.blob.core.windows.net",
    mount_point = "/mnt/groupe8",
    extra_configs = {"fs.azure.account.key.esgidatas.blob.core.windows.net":dbutils.secrets.get(scope = "ScopeESGI", key = "testH")})


# COMMAND ----------

df = spark.read.csv("/mnt/groupe8/Characters_val.csv",sep=';',header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json 

def score_model(url,token,data_json):
    
    headers = {'Authorization': f'Bearer '+token}
    
    response = requests.request(method='POST', headers=headers, url=url, json=data_json)
    
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()

#mettre le contenu de json que vous voulez tester
t = """[
  {
    "Gender": "Female",
    "Wand": "Unknown",
    "Patronus": "Hare",
    "Species": "Human",
    "Bloodstatus": "Pure-blood or half-blood",
    "Loyalty": "Dumbledore's Army |Hogwarts School of Witchcraft and Wizardry",
    "Skills": "Spotting Nargles",
    "Birth": "13 February, 1981"
  }
] """

json_test = json.loads(t)

#Mettre l'url de l'api que vous avez mis en service 
url_api = "dbfs:/databricks/mlflow-tracking/4439892708736586/bcc4d9f2d075483aadb2a47fb422169d/artifacts/model"
#Mettre le token que vous venez de créer dans ce String 
token = "dapifdaa7eb49339a555117f54cd49d09b5d"

score_model(url_api,token,json_test)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *

# cleanData(df)
def cleanData(df):
    data = df.select("Name", "Gender", "Wand", "Patronus", "Species", "Blood status", "Loyalty", "Skills", "Birth").withColumnRenamed("Blood status", "Bloodstatus")
#     .withColumnRenamed("Eye colour", "Eyecolour")
    # .withColumn("FirstName", split(col("Name"), " ").getItem(0))

    # trim data
    data = data.withColumn("Name", trim(col('Name')))
    data = data.withColumn("Gender", trim(col('Gender')))
    data = data.withColumn("Wand", trim(col('Wand')))
    data = data.withColumn("Patronus", trim(col('Patronus')))
    data = data.withColumn("Species", trim(col('Species')))
    data = data.withColumn("Bloodstatus", trim(col('Bloodstatus')))
    data = data.withColumn("Loyalty", trim(col('Loyalty')))
    data = data.withColumn("Skills", trim(col('Skills')))
    data = data.withColumn("Birth", trim(col('Birth')))
#     data = data.withColumn("House", trim(col('House')))
#     data = data.withColumn("Eyecolour", trim(col('Eyecolour')))


    # clean Gender
    data = data.withColumn("Gender", when(data.Name == "Horace Eugene Flaccus Slughorn", "Male").otherwise(data.Gender))

    # clean Wand
    data = data.withColumn("Wand", when(data.Wand == "Unknown", None) .when(data.Wand == "None", None).otherwise(data.Wand))

    # clean Patronus
    data = data.withColumn("Patronus", when(data.Patronus == "Unknown", None) .when(data.Patronus == "None", None).otherwise(data.Patronus))

    # clean Blood status
    data = data.withColumn("Bloodstatus", when(data.Bloodstatus == "Unknown", None) .when(data.Bloodstatus == "None", None).otherwise(data.Bloodstatus))

    # clean Loyalty
    data = data.withColumn("Loyalty", when(data.Loyalty == "Unknown", None) .when(data.Loyalty == "None", None).otherwise(data.Loyalty))

    #clean Skills
    data = data.withColumn("Skills", when(data.Skills == "Unknown", None) .when(data.Skills == "None", None).otherwise(data.Skills))

#     #clean House
#     data = data.withColumn("House", when(data.House == "Unknown", None) .when(data.Skills == "None", None).otherwise(data.House))

#     #clean Eyecolour
#     data = data.withColumn("Eyecolour", when(data.Eyecolour == "Unknown", None) .when(data.Eyecolour == "None", None).otherwise(data.Eyecolour))


    display(data)
    return data


def cleanDate(data):
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
    return data

def savedata(data, table):
    data.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable(f"groupe8.{table}")

# COMMAND ----------

df = cleanData(df)

df_collect = df.collect()
import json

for row in df_collect:  
    x = [ {
    "Gender": row["Gender"],
    "Wand": row["Wand"],
    "Patronus": row["Patronus"],
    "Species": row["Species"],
    "Bloodstatus": row["Bloodstatus"],
    "Loyalty": row["Loyalty"],
    "Skills": row["Skills"],
    "Birth": row["Birth"]
    } ]
    
    t = json.dumps(x)



    json_test = json.loads(t)
    print(row["Name"] , " : ", score_model(url_api,token,json_test) )
#     print(json)
#     print(t)
    
   

# COMMAND ----------





# COMMAND ----------

json_test

# COMMAND ----------


