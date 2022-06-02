# Databricks notebook source
if not any(mount.mountPoint == '/mnt/groupe8' for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = "wasbs://groupe8@esgidatas.blob.core.windows.net",
    mount_point = "/mnt/groupe8",
    extra_configs = {"fs.azure.account.key.esgidatas.blob.core.windows.net":dbutils.secrets.get(scope = "ScopeESGI", key = "testH")})


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS groupe8;

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.csv("/mnt/groupe8/Characters.csv",sep=';',header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.groupBy("house", "Gender").count())

# COMMAND ----------

display(df.groupBy("house", "Gender").count())

# COMMAND ----------

## Clearning

# COMMAND ----------

from datetime import datetime

# cleanData(df)
def cleanData(df):
    # clean House
    listOfHouse = ['Gryffindor', 'Slytherin', 'Ravenclaw', 'Hufflepuff']
    data = df.filter(col("House").isin(listOfHouse))

    # Select Important data

    data = data.select("Name", "Gender", "Wand", "Patronus", "Species", "Blood status", "Loyalty", "Skills", "Birth", "House", "Eye colour").withColumnRenamed("Blood status", "Bloodstatus").withColumnRenamed("Eye colour", "Eyecolour")
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
    data = data.withColumn("House", trim(col('House')))
    data = data.withColumn("Eyecolour", trim(col('Eyecolour')))


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

    #clean House
    data = data.withColumn("House", when(data.House == "Unknown", None) .when(data.Skills == "None", None).otherwise(data.House))

    #clean Eyecolour
    data = data.withColumn("Eyecolour", when(data.Eyecolour == "Unknown", None) .when(data.Eyecolour == "None", None).otherwise(data.Eyecolour))


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

data = cleanData(df)

# COMMAND ----------

data1 = cleanDate(data)
display(data1)

# COMMAND ----------



# COMMAND ----------

import re

def getdayAndMonth(date):
    print(date)

    day = None
    month = None

    m = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'] 

    if date is not None:
        if re.findall(r"^\d{2} ",date) != []:
            day = int(re.findall(r"^\d{2} ",date)[0])
        elif re.findall(r"^\d{1} ",date) != []:
            day = int(re.findall(r"^\d{1} ",date)[0])
        elif re.findall(r" \d{2} ",date) != []:
            day = int(re.findall(r" \d{2} ",date)[0])
        elif re.findall(r" \d{1} ",date) != []:
            day = int(re.findall(r" \d{1} ",date)[0])

        print(date.lower().replace(',', ' ').split(" "))
        for i in date.lower().replace(',', ' ').split(" "):
            for j in m:
                if i.find(j) != -1 :
                    month = j
                    break;
    print(day, month)
    return day, month

def signAstrological(date):
    day, month = getdayAndMonth(date)
    astro_sign = None
    if day is not None and month is not None:
        if month == 'december':
            astro_sign = 'Sagittarius' if (day < 22) else 'capricorn'
        elif month == 'january':
            astro_sign = 'Capricorn' if (day < 20) else 'aquarius'
        elif month == 'february':
            astro_sign = 'Aquarius' if (day < 19) else 'pisces'
        elif month == 'march':
            astro_sign = 'Pisces' if (day < 21) else 'aries'
        elif month == 'april':
            astro_sign = 'Aries' if (day < 20) else 'taurus'
        elif month == 'may':
            astro_sign = 'Taurus' if (day < 21) else 'gemini'
        elif month == 'june':
            astro_sign = 'Gemini' if (day < 21) else 'cancer'
        elif month == 'july':
            astro_sign = 'Cancer' if (day < 23) else 'leo'
        elif month == 'august':
            astro_sign = 'Leo' if (day < 23) else 'virgo'
        elif month == 'september':
            astro_sign = 'Virgo' if (day < 23) else 'libra'
        elif month == 'october':
            astro_sign = 'Libra' if (day < 23) else 'scorpio'
        elif month == 'november':
            astro_sign = 'scorpio' if (day < 22) else 'sagittarius'
       
    return astro_sign



# COMMAND ----------

from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone

# COMMAND ----------

def pickup_features_Astrological(df, ts_column):
    df = df.select("Name", "Birth")
    
    convertUDF = udf(lambda z: signAstrological(z) if z != None else None ,StringType())

    pickupzip_features = (
        df.select(
            col("Name"),
            convertUDF(trim(col("Birth"))).alias(f"{ts_column}"),
        )
    )
    return pickupzip_features


# COMMAND ----------

features_Astrological = pickup_features_Astrological(data, "Astro")
featues_Character = (data)

fs = feature_store.FeatureStoreClient()


# COMMAND ----------

display(features_Astrological.groupBy("Astro").count())

# COMMAND ----------

display(features_Astrological.groupBy("Astro").count())

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")

fs.create_table(
    name="groupe8.AstroSign",
    primary_keys=["Name"],
    df=features_Astrological,
)

fs.write_table(
   name="groupe8.AstroSign",
  df=features_Astrological,
  mode="overwrite",
)

fs.create_table(
    name="groupe8.character",
    primary_keys=["Name"],
    df=featues_Character,
)

fs.write_table(
   name="groupe8.character",
  df=featues_Character,
  mode="overwrite",
)

# COMMAND ----------

from databricks.feature_store import FeatureLookup
import mlflow

AstroSign_features_table = "groupe8.AstroSign"

AstroSign_feature_lookups = [
    FeatureLookup( 
      table_name = AstroSign_features_table,
      feature_name = "Astro",
      lookup_key = ["Name"],
    ),
]

# COMMAND ----------

training_set = fs.create_training_set(
  featues_Character,
  feature_lookups = AstroSign_feature_lookups,
  label = "Name"
)

# COMMAND ----------

training_df = training_set.load_df()

# COMMAND ----------

display(training_df)

# COMMAND ----------

training_df.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable(f"groupe8.AstroTable")

# COMMAND ----------

savedata(training_df, "AstroTable"):

# COMMAND ----------

display(top)

# COMMAND ----------



# COMMAND ----------

display(df)

# COMMAND ----------

rawdata1 = spark.read.csv("/mnt/groupe8/Harry Potter 1.csv",sep=';',header=True)
rawdata2 = spark.read.csv("/mnt/groupe8/Harry Potter 2.csv",sep=';',header=True)
rawdata3 = spark.read.csv("/mnt/groupe8/Harry Potter 3.csv",sep=';',header=True)

# COMMAND ----------

display(rawdata1)
display(rawdata1)
display(rawdata1)

# COMMAND ----------


t1 = rawdata1.groupBy(lower(trim(col('Character'))).alias('Character')).agg(
    count(when(lower(col("Sentence")).like("%gryffindor%"),True)).alias('cnt_G'),
    count(when(lower(col("Sentence")).like("%slytherin%"),True)).alias('cnt_S'),
    count(when(lower(col("Sentence")).like("%ravenclaw%"),True)).alias('cnt_R'),
    count(when(lower(col("Sentence")).like("%hufflepuff%"),True)).alias('cnt_H')
    
)

# COMMAND ----------


t2 = rawdata2.groupBy(lower(trim(col('Character'))).alias('Character')).agg(
     count(when(lower(col("Sentence")).like("%gryffindor%"),True)).alias('cnt_G'),
    count(when(lower(col("Sentence")).like("%slytherin%"),True)).alias('cnt_S'),
    count(when(lower(col("Sentence")).like("%ravenclaw%"),True)).alias('cnt_R'),
    count(when(lower(col("Sentence")).like("%hufflepuff%"),True)).alias('cnt_H')
    
    
)

# COMMAND ----------

t3 = rawdata3.groupBy(lower(trim(col('Character'))).alias('Character')).agg(
    count(when(lower(col("Sentence")).like("%gryffindor%"),True)).alias('cnt_G'),
    count(when(lower(col("Sentence")).like("%slytherin%"),True)).alias('cnt_S'),
    count(when(lower(col("Sentence")).like("%ravenclaw%"),True)).alias('cnt_R'),
    count(when(lower(col("Sentence")).like("%hufflepuff%"),True)).alias('cnt_H')
    
)

# COMMAND ----------

unionT = t1.union(t2).union(t3)

unionT.show(100)

# COMMAND ----------

t = unionT.groupBy(col('Character')).agg(
    sum(unionT.cnt_G).alias("cnt_G"),
    sum(unionT.cnt_S).alias("cnt_S"),
    sum(unionT.cnt_R).alias("cnt_R"),
    sum(unionT.cnt_H).alias("cnt_H"),
)

# COMMAND ----------

t.show()

# COMMAND ----------

t = t.filter((t.cnt_G > 0) | (t.cnt_S > 0) | (t.cnt_R > 0) | (t.cnt_H > 0) )
display(t)

# COMMAND ----------


