# Databricks notebook source
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
url_api = "https://adb-8992331337369088.8.azuredatabricks.net/model/test_potterG8%20-%201/1/invocations"
#Mettre le token que vous venez de créer dans ce String 
token = "dapifdaa7eb49339a555117f54cd49d09b5d"

score_model(url_api,token,json_test)

# COMMAND ----------





# COMMAND ----------

json_test

# COMMAND ----------


