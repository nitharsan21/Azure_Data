# Databricks notebook source
def getAstrologicalSign(day,month):
    signs = ["Capricorn","Aquarius","Pisces","Aries","Taurus","Gemini","Cancer","Leo","Virgo","Libra","Scorpio","Sagittarius"]
    dayTransition = [20,18,20,19,20,21,22,22,22,23,22,21]
    index = (month-1)%12 if day <= dayTransition[month-1] else month%12
    return signs[index]

# COMMAND ----------

day = int(input("Input birthday: "))
month = input("Input month of birth (e.g. march, july etc): ")
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
print("Your Astrological sign is :",astro_sign)
