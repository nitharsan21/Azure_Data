# Databricks notebook source
def getAstrologicalSign(day,month):
    signs = ["Capricorn","Aquarius","Pisces","Aries","Taurus","Gemini","Cancer","Leo","Virgo","Libra","Scorpio","Sagittarius"]
    dayTransition = [20,18,20,19,20,21,22,22,22,23,22,21]
    index = (month-1)%12 if day <= dayTransition[month-1] else month%12
    return signs[index]

# COMMAND ----------


