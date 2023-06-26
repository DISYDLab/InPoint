# https://amiradata.com/pyspark-filter-single-or-multiple-condition/
from pyspark.sql.functions import *
from pyspark.sql.functions import to_timestamp, to_date
from pyspark.sql import functions as F
import pyspark.sql.types as Types
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import DateType, DoubleType, IntegerType, MapType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, udf, pandas_udf, lit,PandasUDFType
import requests
import configparser
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import tweepy


from pyspark.sql import SparkSession, Row

import os

spark = SparkSession.builder.appName('pyspark - example join').getOrCreate()
sc = spark.sparkContext
  
dataset1 = [
  {
  'id' : '1',
  'name' : 'Bulbasaur',
  'primary_type' : 'Grass',
  'secondary_type' : 'Poison',
  'evolve':'Ivysaur'
  },
  {
  'id' : '2',
  'name' : 'Ivysaur',
  'primary_type' : 'Grass',
  'secondary_type' : 'Poison',
  'evolve':'Venusaur'
  },
  {
  'id' : '3',
  'name' : 'Venusaur',
  'primary_type' : 'Grass',
  'secondary_type' : 'Poison',
  'evolve':''
  },
  {
  'id' : '4',
  'name' : 'Charmander',
  'primary_type' : 'Fire',
  'secondary_type' : 'Fire',
  'evolve':'Charmeleon'
  },
  {
  'id' : '5',
  'name' : 'Charmeleon',
  'primary_type' : 'Fire',
  'secondary_type' : 'Fire',
  'evolve':'Charizard'
  },
  {
  'id' : '6',
  'name' : 'Charizard',
  'primary_type' : 'Fire',
  'secondary_type' : 'Flying',
  'evolve':''
  }
      
]



 
# rdd1 = sc.parallelize(dataset1)
# df1 = spark.createDataFrame(rdd1)
df1 = spark.createDataFrame([Row(**i) for i in dataset1])
print('df1')
print(type(df1))
df1.show()


###############################################
# Pyspark Filter data with single condition   #
#                                             #
############################################### 
df1.filter(df1.primary_type == "Fire").show()
print(f'{"*"*60}')
df1.filter(df1.id < 4).show()

print(f'{"*"*60}')

# print(f'{"*"*50}')

print(f'{"*"*60}')

###############################################
# Pyspark Filter data with multiple conditions#
#                                             #
############################################### 

# Multiple conditon using OR operator         #
df1.filter("primary_type == 'Grass' or secondary_type == 'Flying'").show()

print(f'{"*"*60}')

# Multiple conditon using AND operator        #
df1.filter("primary_type == 'Fire' and secondary_type == 'Flying'").show()

###############################################
# Pyspark Filter data with multiple conditions#
# using Spark SQL                             #
############################################### 

## filter with multiple condition using sql.functions
  
from pyspark.sql import functions as f

 
df1.filter((f.col('primary_type') == 'Fire') & (f.col('secondary_type') == 'Fire')).show()
df1.filter(f.expr("primary_type == 'Fire' and secondary_type == 'Fire'")).show()

# Summary

# As you can see, the filter() function is very easy to use and allows you to quickly filter your spark dataframe. In particular, it allows you to filter :

#     By using one or more conditions
#     Using the AND and OR operators
#     With regular expressions
#     By using other combination functions such as lower(),isin() etc…

# 1. PySpark expr() Syntax

# Following is syntax of the expr() function.


# expr(str)

# expr() function takes SQL expression as a string argument, executes the expression, and returns a PySpark
# Column type. Expressions provided with this function are not a compile-time safety like DataFrame operations.

# 2.1 Concatenate Columns using || (similar to SQL)

# If you have SQL background, you pretty much familiar using || to concatenate values from two 
# string columns, you can use expr() expression to do exactly same.


#Concatenate columns using || (sql like)
data=[("James","Bond"),("Scott","Varsa")] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.withColumn("Name",expr(" col1 ||','|| col2")).show()
# +-----+-----+-----------+
# | col1| col2|       Name|
# +-----+-----+-----------+
# |James| Bond| James,Bond|
# |Scott|Varsa|Scott,Varsa|
# +-----+-----+-----------+
from pyspark.sql.functions import expr
data = [("James","M"),("Michael","F"),("Jen","")]
columns = ["name","gender"]
df = spark.createDataFrame(data = data, schema = columns)

#Using CASE WHEN similar to SQL.
from pyspark.sql.functions import expr
df2=df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))
df2.show()
# +-------+-------+
# |   name| gender|
# +-------+-------+
# |  James|   Male|
# |Michael| Female|
# |    Jen|unknown|
# +-------+-------+

# Using an Existing Column Value for Expression

from pyspark.sql.functions import expr
data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",4)] 
df=spark.createDataFrame(data).toDF("date","increment") 

#Add Month value from another column
df.select(df.date,df.increment,
     expr("add_months(date,increment)")
  .alias("inc_date")).show()

# +----------+---------+----------+
# |      date|increment|  inc_date|
# +----------+---------+----------+
# |2019-01-23|        1|2019-02-23|
# |2019-06-24|        2|2019-08-24|
# |2019-09-20|        3|2019-12-20|
# +----------+---------+----------+

# +----------+---------+----------+
# |      date|increment|  inc_date|
# +----------+---------+----------+
# |2019-01-23|        1|2019-02-23|
# |2019-06-24|        2|2019-08-24|
# |2019-09-20|        4|2020-01-20|
# +----------+---------+----------+

# Giving Column Alias along with expr()
