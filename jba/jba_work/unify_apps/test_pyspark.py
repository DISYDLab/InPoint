# 
import os
# -----------------------------------------------------------------------------------------------------
import configparser
# -----------------------------------------------------------------------------------------------------
import datetime  # (datetime.datetime, datetime.timezone)
# -----------------------------------------------------------------------------------------------------
import logging
# Python logging levels
# Levels are used for identifying the severity of an event. There are six logging levels:
# 	• CRITICAL || 50
# 	• ERROR    || 40
# 	• WARNING  || 30
# 	• INFO     || 20
# 	• DEBUG    || 10
# 	• NOTSET   || 0
# If the logging level is set to WARNING, all WARNING, ERROR, and CRITICAL messages are written to the log file or console.
# If it is set to ERROR, only ERROR and CRITICAL messages are logged.
# The default level is WARNING, which means that only events of this level and above will be tracked, unless the logging
# package is configured to do otherwise.
# -----------------------------------------------------------------------------------------------------
import sys
# -----------------------------------------------------------------------------------------------------
import threading
# -----------------------------------------------------------------------------------------------------
import time
# -----------------------------------------------------------------------------------------------------
import pandas as pd
# -----------------------------------------------------------------------------------------------------
import pyspark.sql.types as Types
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
# from pyspark.sql.functions import *
from pyspark.sql.functions import (PandasUDFType, col, lit, pandas_udf,
                                   to_date, to_timestamp, udf)
# ArrayType, ByteType, DateType, DoubleType, FloatType,IntegerType, LongType, Row, ShortType, StructField, StructType
from pyspark.sql.types import (DateType, DoubleType, IntegerType, MapType,
                               StringType, StructField, StructType)

# -----------------------------------------------------------------------------------------------------
import tweepy
# -----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# ----------------------------------------------------------------------------------------------------
# pytz brings the Olson tz database into Python. This library allows accurate and cross platform timezone calculations
# using Python 2.4 or higher. It also solves the issue of ambiguous times at the end of daylight saving time, which you
# can read more about in the Python Library Reference (datetime.tzinfo).
from pytz import timezone
# ----------------------------------------------------------------------------------------------------


# -----------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------

logger = logging.getLogger('tweets_search')

print(
    f"logger.root.level = {logger.root.level}, logger.root.name = {logger.root.name}")
print(f"logger.name = {logger.name}")

format = "%(asctime)s - %(levelname)s - %(message)s"
# logging.basicConfig(format=format, stream=sys.stdout, level = logging.DEBUG)
logging.basicConfig(format=format, stream=sys.stdout, level=logging.INFO)

print(logger.root.level)
# logger.root.level = 10
# ---------------------------------------------------------------------------------------------------

# When starting the pyspark shell, you can specify:
# the --packages option to download the MongoDB Spark Connector package.
# os.environ['PYSPARK_SUBMIT_ARGS'] = \
# '--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0  pyspark-shell'

# Provides findspark.init() to make pyspark importable as a regular library.
# Find spark home, and initialize by adding pyspark to sys.path.
# import findspark
# findspark.init()

# from pyspark.sql.types import toJSON


# Tweepy, a package that provides a very convenient way to use the Twitter API

# The Natural Language Toolkit (NLTK) is a Python package for natural language processing.
nltk.download('vader_lexicon')

# Koalas is an open source project that provides a drop-in replacement for pandas.
#import databricks.koalas as ks

# This module provides the ConfigParser class which implements a basic configuration language
#  which provides a structure similar to what’s found in Microsoft Windows INI files. You can use
# this to write Python programs which can be customized by end users easily.

# The requests module allows you to send HTTP requests using Python.
# The HTTP request returns a Response Object with all the response data (content, encoding,
# status, etc).


os.environ["PYSPARK_PIN_THREAD"] = "true"

#sc = SparkContext()
#sqlContx = SQLContext(sc)

# nltk.download('vader_lexicon')
# sid = SentimentIntensityAnalyzer()
# sid = SentimentIntensityAnalyzer('file:///home/hadoopuser/nltk_data/sentiment/vader_lexicon.zip/vader_lexicon/vader_lexicon.txt')

# *********************************************************************************************************
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# ********************************************************************************************************
# ---------------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---------------------------------------------------------------------------------------------------------
def start_spark():
    global spark
    # *** WITH spark-submit ***
    # =========================
    # spark  = SparkSession \
    # 	.builder \
    #     .config("spark.driver.allowMultipleContexts","true") \
    #     .config("spark.suffle.service.enabled","true") \
    #     .config("spark.default.parallelism","8") \
    #     .config("spark.sql.shuffle.partitions","8") \
    #     .config("spark.scheduler.mode","FAIR") \
    #     .config("spark.scheduler.allocation.file", "/home/hadoopuser/spark/conf/fairscheduler.xml") \
    #     .getOrCreate()

    # WITH DEBUGGER
    # ==============
    spark = SparkSession \
        .builder \
        .config("spark.driver.allowMultipleContexts", "true") \
        .config("spark.suffle.service.enabled", "true") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.scheduler.allocation.file", "/home/hadoopuser/spark/conf/fairscheduler.xml") \
        .appName('spark_threads') \
        .config("spark.mongodb.input.uri",
                "mongodb://ubuntu:27017/test.myCollection?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://ubuntu:27017/test.myCollection") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()
    # ------------------------------------------------------------------------------------------------------------
    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set(
        "spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")
    spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled")
    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------

    print(f"{'*'* 22}\nsparkSession started\n")


start_spark()
# **********************
# sparkSession started

data = [
    {'employeeId': 1, 'employeeName': "Anand"},
    {'employeeId': 2, 'employeeName': "Wayne"},
    {'employeeId': 3, 'employeeName': "Clark"},
    {'employeeId': 2, 'employeeName': "Juan"},
    {'employeeId': 3, 'employeeName': "Naval"}

]

# ---------------------------------------------------------------------------------------------------------
# ### create sdf from list
# ---------------------------------------------------------------------------------------------------------
# Finally, we convert the `output` list to a `spark DataFrame` and we store results.
print('create sdf from list')
# create sdf from list


sdf = spark.createDataFrame([Row(**i) for i in data])
sdf.show(truncate=30)
# +----------+------------+                                                       
# |employeeId|employeeName|
# +----------+------------+
# |         1|       Anand|
# |         2|       Wayne|
# |         3|       Clark|
# |         2|        Juan|
# |         3|       Naval|
# +----------+------------+

sdf.printSchema()
# root
#  |-- employeeId: long (nullable = true)
#  |-- employeeName: string (nullable = true)

# ==================
# Write to mongoDB
# ==================
# replaceDocument
# Replace the whole document when saving Datasets that contain an _id field. If false it will only update the fields in the document that match the fields in the Dataset.
# Default: true
sdf.write.format("mongo").option("database", "test_dublicateDB").option(
    "collection", "dublicate_documents_collection").mode("overwrite").save()  # .option("replaceDocument", "false")

sdf = spark.read.format("mongo").option("database", "test_dublicateDB").option(
    "collection", "dublicate_documents_collection").load()
sdf.select('*').show(truncate=30)
# +--------------------------+----------+------------+                            
# |                       _id|employeeId|employeeName|
# +--------------------------+----------+------------+
# |{60f320b38610c15265f81644}|         2|       Wayne|
# |{60f320b2992bd43b34b7be7d}|         1|       Anand|
# |{60f320b38610c15265f81645}|         3|       Clark|
# |{60f320b38610c15265f81646}|         2|        Juan|
# |{60f320b38610c15265f81647}|         3|       Naval|
# +--------------------------+----------+------------+

print(spark)
# <pyspark.sql.session.SparkSession object at 0x7f39ebaf3310>

# #######################################
# AGGREGATION PIPELINE
# #######################################    
# Place the $match as early in the aggregation pipeline as possible. Because $match limits
# the total number of documents in the aggregation pipeline, earlier $match operations
# minimize the amount of processing down the pipe.
# $match with equality match
# "," comma between key:value pairs is implicit and operator i.e: {k1:v1, k2:v2, k3:v3}
# $match: {<query>} => match specific documents using query
pipeline = str([
    {
        '$group': {
            '_id': '$featureName', 
            'dups': {
                '$push': '$_id'
            }, 
            'count': {
                '$sum': 1
            }
        }
    }, {
        '$match': {
            'count': {
                '$gt': 1
            }
        }
    }, {
        '$sort': {
            'count': -1
        }
    }
])

sdf = spark.read.format("mongo").option("database", "tweets_DB").option(
    "collection", "tweets_sentiment_scores").option("pipeline", pipeline).load()

spark.stop()