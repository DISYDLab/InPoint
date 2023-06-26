# The OS module in Python provides functions for interacting with the operating system. OS comes
# under Python’s standard utility modules. This module provides a portable way of using operating
#  system-dependent functionality. The *os* and *os.path* modules include many functions to
#  interact with the file system.
import os

# -----------------------------------------------------------------------------------------------------

import configparser

# -----------------------------------------------------------------------------------------------------
# The datetime module of Python helps us handle time-related information on any precision level.
# 4 Must-Know Objects: date, timedelta, time, datetime (is kind of a combination of date and time).
# The datetime object provides the flexibility of using date only, or date and time combined.
import datetime  # (datetime.datetime, datetime.timezone)

# -----------------------------------------------------------------------------------------------------
# The logging module provides a way for applications to configure different log handlers and a way of routing log messages
# to these handlers. This allows for a highly flexible configuration that can deal with a lot of different use cases.
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
# The sys module provides functions and variables used to manipulate different parts of the
#  Python runtime environment.
import sys

# -----------------------------------------------------------------------------------------------------
# Thread module emulating a subset of Java's threading mode.
# Python threading allows you to have different parts of your program run concurrently and can simplify your design.
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
# Tweepy is an open source Python package that gives you a very convenient way to access the Twitter API with Python.
# Tweepy includes a set of classes and methods that represent Twitter's models and API endpoints, and it transparently
# handles various implementation details, such as: Data encoding and decoding.
import tweepy

# -----------------------------------------------------------------------------------------------------
# Flask is a lightweight WSGI web application framework. It is designed to make getting started
#  quick and easy, with the ability to scale up to complex applications. It began as a simple
#  wrapper around Werkzeug and Jinja and has become one of the most popular Python web
#  application frameworks.
from flask import Flask, jsonify, request

# ----------------------------------------------------------------------------------------------------
# NLTK is a leading platform for building Python programs to work with human language data. It provides
# easy-to-use interfaces to over 50 corpora and lexical resources such as WordNet, along with a suite of
# text processing libraries for classification, tokenization, stemming, tagging, parsing, and semantic
# reasoning, wrappers for industrial-strength NLP libraries.
# 	• A SentimentAnalyzer is a tool to implement and facilitate Sentiment Analysis tasks using NLTK features and
#     classifiers, especially for teaching and demonstrative purposes.
# 	• VADER ( Valence Aware Dictionary for Sentiment Reasoning) is a model used for text sentiment analysis that is
#     sensitive to both polarity (positive/negative) and intensity (strength) of emotion. It is available in the
#     NLTK package and can be applied directly to unlabeled text data.
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
# *********************************************************************************************************

app = Flask(__name__)

print("^"*50)
print(__name__)
print("^"*50)

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

# ---------------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---------------------------------------------------------------------------------------------------------
def check_tweets_list(potential_full: list) -> bool:
    if not potential_full:
        # Try to convert argument into a float
        print("list is  empty")
        return False
    else:
        print("list is  not empty")
        return True

# ---------------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---------------------------------------------------------------------------------------------------------
def set_api(twitter_tokens):

    # ConfigParser is a Python class which implements a basic configuration language for Python
    # programs. It provides a structure similar to Microsoft Windows INI files. ConfigParser
    # allows to write Python programs which can be customized by end users easily.

    # The configuration file consists of sections followed by key/value pairs of options. The
    # section names are delimited with [] characters. The pairs are separated either with : or =.
    #  Comments start either with # or with ;.
    config = configparser.RawConfigParser()
    config.read(filenames=twitter_tokens)
    print(f'config-sections: {config.sections()}')

    # creating 4 variables and assigning them basically saying read these 4 keys from file and assign
    consumer_key = config.get('twitter', 'consumer_key')
    consumer_secret = config.get('twitter', 'consumer_secret')
    access_token = config.get('twitter', 'access_token')
    access_token_secret = config.get('twitter', 'access_token_secret')

    # Creating the authentication object to Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    # Setting your access token and secret
    auth.set_access_token(access_token, access_token_secret)

    # Create API object - . You can use the API to read and write information related to
    # Twitter entities such as tweets, users, and trends.
    # The Twitter API uses OAuth, a widely used open authorization protocol, to authenticate
    # all the requests.
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api


# ---------------------------------------------------------------------------------------------------------
twitter_tokens = 'twitter.properties'
api = set_api(twitter_tokens)
# ---------------------------------------------------------------------------------------------------------

# ---VADER-------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---VADER-------------------------------------------------------------------------------------------------
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,StructField, StructType, Row
print('VADER')
def sentiment_scores(sentance: str) -> dict:
    # Create a SentimentIntensityAnalyzer object.
    sid = SentimentIntensityAnalyzer(
        'file:///home/hadoopuser/nltk_data/sentiment/vader_lexicon.zip/vader_lexicon/vader_lexicon.txt')
    # polarity_scores method of SentimentIntensityAnalyzer
    # oject gives a sentiment dictionary.
    # which contains pos, neg, neu, and compound scores.
    r = sid.polarity_scores(sentance)
    return r
    # You can optionally set the return type of your UDF. The default return type␣,→is StringType.
    # udffactorial_p = udf(factorial_p, LongType())

sentiment_scoresUDF = udf(sentiment_scores, Types.MapType(
    Types.StringType(), Types.DoubleType()))

# ---------------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---------------------------------------------------------------------------------------------------------
def tweets_list_output(tweets_list):
    start = time.time()
    output = []
    for tweet in tweets_list:
        if tweet._json['full_text'].startswith("RT @"):
            text = tweet.retweeted_status.full_text
        else:
            text = tweet._json["full_text"]
        # print(text)
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets

        logger.debug(f"full_text: '{text}'")
        favourite_count = tweet.favorite_count
        retweet_count = tweet.retweet_count
        created_at = tweet.created_at

        line = {'text': text, 'favourite_count': favourite_count,
                'retweet_count': retweet_count, 'created_at': created_at}
        output.append(line)
        logger.info(f"Append list length : { len(output)}")
    end = time.time()
    logger.info(f"elapsed_time: '{end - start}'")
    print(output[:1])
    print(f'output_length = {len(output)}')
    return output

# ======================================================================================================
# ### Evaluate sentiment with Spark UDF and Spark pandasUDF functions:
# ======================================================================================================
# - positive -> 1,
# - negative -> -1,
# - neutral -> 0

# ======================================================================================================
# 1. Spark - UDF
# ======================================================================================================
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,StructField, StructType, Row

# def sentiment_eval(comp_score: float) -> int:

#     # if compound_score > 0.05 => 1 i.e positive
#     if comp_score > 0.05:
#         return 1
#     elif comp_score < 0.05:
#         return -1
#     else:
#         return 0

# sentiment_evalUDF = udf(sentiment_eval, IntegerType())

# ======================================================================================================
# 2. Spark - PandasUDF
# ======================================================================================================
# create a wrapper_function for Spark pandasUDF function 
def sentiment_eval_pUDF_wrapper(pdf_series):
    @pandas_udf('integer')
    def sentiment_eval_pUDF(comp_score: pd.Series) -> pd.Series:
        s = []
        # if compound_score > 0.05 => 1 i.e positive
        for elmnt in comp_score:
            if elmnt > 0.05:
                s.append(1)
            elif elmnt < 0.05:
                s.append(-1)
            else:
                s.append(0)
        return pd.Series(s)

    return sentiment_eval_pUDF(pdf_series)

# ---------------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---------------------------------------------------------------------------------------------------------
# Registers a function to be run before the first request to this instance of the application.
# The function will be called without any arguments and its return value is ignored.
app.before_first_request(start_spark)

#spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)

# ---------------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---------------------------------------------------------------------------------------------------------
def scrapetweets(t_user, t_search_words, t_date_since, t_date_until, t_lang):
    # ------------------------------------------------------------------------------------------------------------
    # CURSOR function
    # ------------------------------------------------------------------------------------------------------------
    # We search for tweets on Twitter by using the `Cursor()` function.
    # We pass the `api.search` parameter to the cursor, as well as the query string, which is specified through the `q`
    # parameter of the cursor. The query string can receive many parameters, such as the following (not mandatory) ones:
    # * `from:` - to specify a specific Twitter user profile
    # * `since:` - to specify the beginning date of search
    # * `until:` - to specify the ending date of search
    # The cursor can also receive other parameters, such as the language and the `tweet_mode`. If `tweet_mode='extended'`,
    # all the text of the tweet is returned, otherwise only the first 140 characters.
    # -----------------------
    # Greek Language = 'el'
    # -----------------------
    # English Language = 'en'
    # -----------------------
    # tweets_list = tweepy.Cursor(api.search, q=['coffee island'],since= str(since), until=str(until),tweet_mode='extended', lang='el').items()

    tweets_list = (tweepy
                   .Cursor(api.search,
                           q=t_search_words,
                           since=str(t_date_since), until=str(t_date_until),
                           tweet_mode='extended',
                           lang=t_lang).items()
                   )

    # Now we loop across the `tweets_list`, and, for each tweet, we extract the text, the creation date, the number of
    # retweets and the favourite count. We store every tweet into a list, called `output`.

    # ---------------------------------------------------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------------------
    import time
    start = time.time()
    output = []
    for tweet in tweets_list:
        if tweet._json['full_text'].startswith("RT @"):
            text = tweet.retweeted_status.full_text
        else:
            text = tweet._json["full_text"]
        # print(text)
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
        #

        logger.debug(f"full_text: '{text}'")
        id_str = tweet._json["id_str"]
        favourite_count = tweet.favorite_count
        retweet_count = tweet.retweet_count
        # ****Datetime naive object****
        created_at = tweet.created_at
        # Conversion from a "naive' to an 'aware' datetime object-Replace tzinfo with the desired timezone
        # created_at1 = tweet.created_at.replace(tzinfo = datetime.timezone.utc)
        # print(created_at1.tzname()) # UTC: Coordinated Universal Time
        # Conversion from a "naive' to an 'aware' datetime object
        # dt_eest = timezone('EET').localize(created_at)
        # print(dt_eest.tzname()) # EEST : Eastern European Summer Time (UTC + 3 time zone) / EET: Easter European Time (UTC+2 time zone)
        # print(dt_eest) # 2021-07-10 23:36:27+03:00
        # dt_athens = timezone('Europe/Athens').localize(created_at)
        # print(dt_athens.tzname()) # EEST
        # print(dt_athens) # 2021-07-10 23:36:27+03:00
        # created_at2 = tweet.created_at.replace(tzinfo = datetime.timezone.utcoffset())
        hashtags = [hashtag['text']
                    for hashtag in tweet._json['entities']['hashtags']]
        user_id_str = tweet._json['user']['id_str']
        screen_name = tweet._json['user']['screen_name']

        line = {'query_user': t_user,
                'id_str': id_str,
                'search_words': t_search_words,
                'text': text,
                'favourite_count': favourite_count,
                'retweet_count': retweet_count,
                'created_at': created_at,
                'hashtags': hashtags,
                'user_id_str': user_id_str,
                'screen_name': screen_name
                }
        output.append(line)
        logger.info(f"Append list length : { len(output)}")
    end = time.time()
    logger.info(f"elapsed_time: '{end - start}'")
    print(output[:3], "\n")
    logger.info(f"output_length: {len(output)}\n")

    # ---------------------------------------------------------------------------------------------------------
    # ### create sdf from list
    # ---------------------------------------------------------------------------------------------------------
    # Finally, we convert the `output` list to a `spark DataFrame` and we store results.
    print('create sdf from list')
    sdf = spark.createDataFrame([Row(**i) for i in output])
    # sdf.show(2, truncate=30)

    # |query_user|             id_str|                  search_words|                          text|favourite_count|retweet_count|         created_at|hashtags|        user_id_str|    screen_name|
    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+
    # |       jba|1414005629384105993|[coffeeIsland OR (coffee is...|Learn all about the intrigu...|              0|            3|2021-07-10 23:36:27|[Coffee]|         1584528546|QueenBeanCoffee|
    # |       jba|1414004639557513219|[coffeeIsland OR (coffee is...|I'm at the movie theatre, c...|              2|            0|2021-07-10 23:32:31|      []|1136790522335383552|   libratyranny|
    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+

    return sdf

# *********************************************************************************************************
# =========================================================================================================
# *********************************************************************************************************
# PRODUCER-1
#############
def detachedProcessReadSentimentResultsFromMongDB(spark, request_data):
    print("#"*40)
    print(request_data)
    # {'user': 'jba', 'search_words': ['coffeeIsland OR (coffee island)'], 'date_since': '2021-07-10', 'date_until': '2021-07-11', 'lang': 'en'}
    # print(type(request_data))
    # <class 'dict'>

    search_words = request_data["search_words"]
    # 'search_words': "['coffeeIsland OR (coffee island)']"  
    #   

    # date_since = datetime.datetime.strptime(request_data["date_since"], '%y-%m-%d %H:%M:%S')
    #"Z time" or "Zulu Time"
    ########################################
    # print(f'time.tzname: {time.tzname}')
    # print(f'datetime.tzinfo: {datetime.tzinfo}')
    # time.tzname: ('EET', 'EEST')
    # datetime.tzinfo: <class 'datetime.tzinfo'

    # date_since = parser.parse(request_data["date_since"])
    # date_since = datetime.datetime.strptime(str(date_since), '%Y-%m-%dT%H:%M:%S%Z')
    date_since = request_data["date_since"]
    date_until = request_data["date_until"]
    lang = request_data["lang"]
    print(f"search_words={search_words}")
    # search_words=['coffeeIsland OR (coffee island)]
    print(type(search_words))  # <class 'list'>
    print(f"date_since={date_since}")
    # date_since=2021-07-10
    print(f"date_until={date_until}")
    # date_until=2021-07-11
    date_since_obj = datetime.datetime.strptime(date_since, '%Y-%m-%d')
    # A datetime object d is aware if both of the following hold:
    # d.tzinfo is not None
    # d.tzinfo.utcoffset(d) does not return None

    print(date_since_obj.tzinfo)
    # print(datetime.tzinfo.tcoffset(date_since_obj))
    # date_since_obj = date_since_obj.isoformat()
    # date_since_obj = date_since_obj+"Z"
    # date_since_obj = date_since_obj+"+03:00"
    dt_athens_since = timezone('Europe/Athens').localize(date_since_obj)
    print(dt_athens_since.tzname())  # EEST
    print(dt_athens_since)  # 2021-07-10 23:36:27+03:0
    date_since_str = dt_athens_since.isoformat()
    # date_since_obj = datetime.datetime.combine(date_since_obj, datetime.datetime.min.time())
    date_until_obj = datetime.datetime.strptime(date_until, '%Y-%m-%d')
    date_until_obj = datetime.datetime.combine(
        date_until_obj, datetime.datetime.max.time())
    dt_athens_until = timezone('Europe/Athens').localize(date_until_obj)
    date_until_str = dt_athens_until.isoformat()
    print(type(date_until_str))

    # date_until_str = date_until_str+"Z"
    # date_until_str = date_until_str+"+03:00"
    # date_since = datetime.datetime.strptime(dt_since, '%Y-%m-%dT%H:%M:%S')
    print(f"date_since={date_since_str}")
    # date_since=2021-07-10 00:00:00
    print(f"date_until={date_until_str}")
    # The ISO format for timestamps has a 'T' separating the date from the time part.
    # date_since=2021-07-07T00:00:00Z
    # date_until=2021-07-14T23:59:59.999999Z

    print(f"lang={lang}")
    # lang=en
    #  from_date = datetime.datetime(2018, 1, 1)
    # print(type(from_date ))
    # >> <type 'datetime.datetime'>
    # from_date = from_date.isoformat()
    # from_date = from_date+"Z"
    # print(from_date)
    # >> 2018-01-01T00:00:00Z

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")
    # #######################################
    # AGGREGATION PIPELINE
    # #######################################    
    # Place the $match as early in the aggregation pipeline as possible. Because $match limits
    # the total number of documents in the aggregation pipeline, earlier $match operations
    # minimize the amount of processing down the pipe.
    # $match with equality match
    # "," comma between key:value pairs is implicit and operator i.e: {k1:v1, k2:v2, k3:v3}
    # $match: {<query>} => match specific documents using query
    pipeline = str({'$match': {'$and': [{'created_at': {'$gte': {'$date': date_since_str}}}, {
                   'created_at': {'$lte': {'$date': date_until_str}}}], 'search_words': search_words}})

    print("\npipeline : ", pipeline, "\n")

    # mongodf = spark.read.format("mongo").option("database", "tweets_DB").option(
    #     "collection", "tweets_sentiment_scores").load()
    # print(mongodf.count())
    # print(mongodf.schema)
    # print(type(date_since_str))
    # print(str(search_words))
    # <class 'str'>
    # print(datetime.datetime.timestamp(date_since_obj))
    # ---------------------------------------------------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------------------
    # mongodf = mongodf.withColumn(
    #     'user_search_words', F.array(*[F.lit(i) for i in search_words]))
    # mongodf = mongodf.withColumn('size', F.size(F.col('search_words')))
    # mongodf.show(2, truncate=25)
    # mongodf = mongodf.withColumn('is_equal', F.col(
    #     'search_words') == F.col('user_search_words'))
    # print(mongodf.columns)
    # print(mongodf.schema)
    # print(mongodf.count())
    # mongodf = mongodf.filter(mongodf.is_equal == 'true')
    # print(mongodf.count())
    # ---------------------------------------------------------------------------------------------------------
    # --+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+-----------------+----+--------+
    # |                      _id|compound_nltk|         created_at|favourite_count|                 hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|screen_name|             search_words|sentiment_nltk|                     text|user_id_str|user_search_words|size|is_equal|
    # +-------------------------+-------------+-------------------+---------------+-------------------------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+-----------------+----+--------+
    # |{60eeb7282a84be10cd0cd...|       0.6249|2021-07-10 11:48:37|              0|[Bahamas, GreatAbaco, ...|1413827497653846017|          0.0|       0.854|        0.146|       jba|           63|    slitoff|[coffeeIsland OR (coff...|             1|#Bahamas Fishing Pier,...|   84190574|   [nike, adidas]|   1|   false|
    # |{60eeb7282a84be10cd0cd...|          0.0|2021-07-10 11:20:36|             67|                       []|1413820444021608454|          0.0|         1.0|          0.0|       jba|            1|HaggisAdele|[coffeeIsland OR (coff...|            -1|Went down to Sanda Isl...|   27868226|   [nike, adidas]|   1|   false|
    # +-------------------------+-------------+-------------------+---------------+-------------------------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+-----------------+----+--------+
    # +-------------------------+-------------+-------------------+---------------+-------------------------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+-----------------+----+
    # |                      _id|compound_nltk|         created_at|favourite_count|                 hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|screen_name|             search_words|sentiment_nltk|                     text|user_id_str|user_search_words|size|
    # +-------------------------+-------------+-------------------+---------------+-------------------------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+-----------------+----+
    # |{60eeb7282a84be10cd0cd...|       0.6249|2021-07-10 11:48:37|              0|[Bahamas, GreatAbaco, ...|1413827497653846017|          0.0|       0.854|        0.146|       jba|           63|    slitoff|[coffeeIsland OR (coff...|             1|#Bahamas Fishing Pier,...|   84190574|   [nike, adidas]|   1|
    # |{60eeb7282a84be10cd0cd...|          0.0|2021-07-10 11:20:36|             67|                       []|1413820444021608454|          0.0|         1.0|          0.0|       jba|            1|HaggisAdele|[coffeeIsland OR (coff...|            -1|Went down to Sanda Isl...|   27868226|   [nike, adidas]|   1|
    # +-------------------------+-------------+-------------------+---------------+-------------------------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+-----------------+----+
    #     +--------------------+-------------+-------------------+---------------+--------------------+-------------------+-------------+------------+-------------+----------+-------------+---------------+--------------------+--------------+--------------------+-------------------+----+
    # |                 _id|compound_nltk|         created_at|favourite_count|            hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|    screen_name|        search_words|sentiment_nltk|                text|        user_id_str|size|
    # +--------------------+-------------+-------------------+---------------+--------------------+-------------------+-------------+------------+-------------+----------+-------------+---------------+--------------------+--------------+--------------------+-------------------+----+
    # |{60eeb7282a84be10...|       0.6249|2021-07-10 11:48:37|              0|[Bahamas, GreatAb...|1413827497653846017|          0.0|       0.854|        0.146|       jba|           63|        slitoff|[coffeeIsland OR ...|             1|#Bahamas Fishing ...|           84190574|   1|
    # |{60eeb7282a84be10...|          0.0|2021-07-10 11:20:36|             67|                  []|1413820444021608454|          0.0|         1.0|          0.0|       jba|            1|    HaggisAdele|[coffeeIsland OR ...|            -1|Went down to Sand...|           27868226|   1|
    # |{60eeb7282a84be10...|          0.0|2021-07-10 11:01:23|              1|[specialitycoffee...|1413815611994542084|          0.0|         1.0|
    # mongodf.filter(F.expr("primary_type == 'Fire' and secondary_type == 'Fire'")).show()
    # print(mongodf.count())
    # mongodf.show(100, truncate=25)
    # +-------------------------+-------------+-------------------+---------------+-------------------------+-------------------+-------------+------------+-------------+----------+-------------+---------------+-------------------------+--------------+----------------------------+-------------------+
    # |                      _id|compound_nltk|         created_at|favourite_count|                 hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|    screen_name|             search_words|sentiment_nltk|                        text|        user_id_str|
    # +-------------------------+-------------+-------------------+---------------+-------------------------+-------------------+-------------+------------+-------------+----------+-------------+---------------+-------------------------+--------------+----------------------------+-------------------+
    # |{60eeb7282a84be10cd0cd...|       0.6249|2021-07-10 11:48:37|              0|[Bahamas, GreatAbaco, ...|1413827497653846017|          0.0|       0.854|        0.146|       jba|           63|        slitoff|[coffeeIsland OR (coff...|             1|   #Bahamas Fishing Pier,...|           84190574|
    # |{60eeb7282a84be10cd0cd...|          0.0|2021-07-10 11:20:36|             67|                       []|1413820444021608454|          0.0|         1.0|          0.0|       jba|            1|    HaggisAdele|[coffeeIsland OR (coff...|            -1|   Went down to Sanda Isl...|           27868226|
    # |{60eeb7282a84be10cd0cd...|          0.0|2021-07-10 11:01:23|              1|[specialitycoffee, cup...|1413815611994542084|          0.0|         1.0|          0.0|       jba|            0| island_roasted|[coffeeIsland OR (coff...|            -1|   When you don't mind br...|         2436085045|
    # |{60eeb7282a84be10cd0cd...|       0.4404|2021-07-10 11:00:40|              0|                       []|1413815429357768709|          0.0|       0.734|        0.266|       jba|            0|island_surf_duc|[coffeeIsland OR (coff...|             1|   Good Morning, I've got...|          155903774|
    # |{60eeb7282a84be10cd0cd...|        0.296|2021-07-10 10:14:46|              0|                       []|1413803879473500162|          0.0|       0.845|        0.155|       jba|            0|  abook_and_bev|[coffeeIsland OR (coff...|             1|   Read Treasure Island a...|1368800100705734659|
    # |{60eeb7282a84be10cd0cd...|          0.0|2021-07-10 09:47:59|              2|[timeout, whataday, id...|1413797138249789440|          0.0|         1.0|          0.0|       jba|            0|   CastawayWild|[coffeeIsland OR (coff...|            -1|   Treating myself to a l...|1379838295249727491|
    # |{60eeb7282a84be10cd0cd...|          0.0|2021-07-10 09:02:13|              0|                   [LUCY]|1413785621575831555|          0.0|         1.0|          0.0|       jba|          536|  coffee_chodai|[coffeeIsland OR (coff...|            -1|   [#LUCY] I Got U 🤘 Con...|1403978203241148416|

    # mongodf.sort(mongodf.created_at.desc()).show(1000, truncate=25)

    # mongodf.filter()
    # +-------------------------+-------------+-------------------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+
    # |                      _id|compound_nltk|         created_at|favourite_count|hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|screen_name|             search_words|sentiment_nltk|                     text|user_id_str|
    # +-------------------------+-------------+-------------------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+
    # |{60edf8d9c2115956fc3f3...|      -0.6486|2021-07-10 23:28:10|              1|      []|1414003546203201537|        0.261|       0.739|          0.0|       jba|            0|hiyaimdanae|[coffeeIsland OR (coff...|            -1|I miss Coronado island...| 4923368664|
    # |{60edf8d9c2115956fc3f3...|       0.4019|2021-07-10 23:19:33|              2|  [acnh]|1414001376015888385|        0.056|        0.85|        0.094|       jba|            0| meredactyl|[coffeeIsland OR (coff...|             1|my greatest #acnh desi...|  159501860|
    # +-------------------------+-------------+-------------------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+-------------------------+--------------+-------------------------+-----------+
    # print(f'mongodf_rows = {mongodf.count()}')

    mongodf = spark.read.format("mongo").option("database", "tweets_DB").option(
        "collection", "tweets_sentiment_scores").option("pipeline", pipeline).load()
    mongodf.orderBy('created_at').show(1000, truncate = 30)
    logger.info(f'mongodf_rows = {mongodf.count()}')
    logger.info(f"mongodf.columns = {mongodf.columns}")
    # mongodf.columns = ['_id', 'compound_nltk', 'created_at', 'favourite_count', 'hashtags', 'id_str', 'negative_nltk', 'neutral_nltk', 'positive_nltk', 'query_user', 'retweet_count', 'screen_name', 'search_words', 'sentiment_nltk', 'text', 'user_id_str']

    # mongoAggregationdf.select(columns).show(100, truncate=False)
    logger.info(f'mongodf Schema = {mongodf.schema}')

    # mongodf Schema=
    # root
    #  |-- _id: struct (nullable = true)
    #  |    |-- oid: string (nullable = true)
    #  |-- compound_nltk: double (nullable = true)
    #  |-- created_at: timestamp (nullable = true)
    #  |-- favourite_count: long (nullable = true)
    #  |-- hashtags: array (nullable = true)
    #  |    |-- element: string (containsNull = true)
    #  |-- id_str: string (nullable = true)
    #  |-- negative_nltk: double (nullable = true)
    #  |-- neutral_nltk: double (nullable = true)
    #  |-- positive_nltk: double (nullable = true)
    #  |-- query_user: string (nullable = true)
    #  |-- retweet_count: long (nullable = true)
    #  |-- screen_name: string (nullable = true)
    #  |-- search_words: array (nullable = true)
    #  |    |-- element: string (containsNull = true)
    #  |-- sentiment_nltk: integer (nullable = true)
    #  |-- text: string (nullable = true)
    #  |-- user_id_str: string (nullable = true)


    # mongodf.columns = ['_id', 'compound_nltk', 'created_at', 'favourite_count', 'negative_nltk', 'neutral_nltk', 'positive_nltk', 'retweet_count', 'search_words', 'text', 'user']
    # --------------------------------------------------------------
    # ### Convert created_at from timestamp to date
    # --------------------------------------------------------------
    mongodf = mongodf.withColumn('created_at', F.to_date(F.col('created_at')))
    # mongodf.show(2, truncate=20)

    # +--------------------+-------------+----------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+--------------------+--------------+--------------------+-----------+
    # |                 _id|compound_nltk|created_at|favourite_count|hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|screen_name|        search_words|sentiment_nltk|                text|user_id_str|
    # +--------------------+-------------+----------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+--------------------+--------------+--------------------+-----------+
    # |{60edf8d9c2115956...|      -0.6486|2021-07-10|              1|      []|1414003546203201537|        0.261|       0.739|          0.0|       jba|            0|hiyaimdanae|[coffeeIsland OR ...|            -1|I miss Coronado i...| 4923368664|
    # |{60edf8d9c2115956...|       0.4019|2021-07-10|              2|  [acnh]|1414001376015888385|        0.056|        0.85|        0.094|       jba|            0| meredactyl|[coffeeIsland OR ...|             1|my greatest #acnh...|  159501860|
    # +--------------------+-------------+----------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+--------------------+--------------+--------------------+-----------+

    # --------------------------------------------------------------
    # ### groupBy and aggregate on multiple columns
    # --------------------------------------------------------------
    exprs = {}
    cols = ['created_at',
            'negative_nltk',
            'positive_nltk',
            'neutral_nltk',
            'compound_nltk'
            ]
    exprs = {x: "sum" for x in cols}
    exprs['created_at'] = 'count'
    # {'created_at': 'count', 'negative_nltk': 'sum', 'positive_nltk': 'sum', 'neutral_nltk': 'sum', 'compound_nltk': 'sum'}
    print(exprs)

    aggregated_mongodf = mongodf.groupBy('created_at').agg(
        exprs).withColumnRenamed('count(created_at)', 'tweets')
    # aggregated_mongodf.show(2, truncate=30)
    # +----------+------------------+------------------+------+------------------+------------------+
    # |created_at|sum(compound_nltk)|sum(positive_nltk)|tweets|sum(negative_nltk)| sum(neutral_nltk)|
    # +----------+------------------+------------------+------+------------------+------------------+
    # |2021-07-10|26.011400000000005| 8.827999999999998|    58|             1.367|47.803000000000004|
    # |2021-07-11|10.456599999999998|             5.827|    50|2.1609999999999996|            42.014|
    # +----------+------------------+------------------+------+------------------+------------------+


    # How to delete columns in pyspark dataframe
    columns_to_drop = ['sum(compound_nltk)', 'sum(positive_nltk)',
                       'sum(negative_nltk)', 'sum(neutral_nltk)']

    aggregated_mongodf = (aggregated_mongodf
                          .withColumn('compound_nltk', F.col('sum(compound_nltk)')/F.col('tweets'))
                          .withColumn('positive_nltk', F.col('sum(positive_nltk)')/F.col('tweets'))
                          .withColumn('negative_nltk', F.col('sum(negative_nltk)')/F.col('tweets'))
                          .withColumn('neutral_nltk', F.col('sum(neutral_nltk)')/F.col('tweets'))
                          .drop(*columns_to_drop)
                          )
    # +--------------------+-------------+----------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+--------------+--------------+--------------------+-----------+
    # |                 _id|compound_nltk|created_at|favourite_count|hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|screen_name|  search_words|sentiment_nltk|                text|user_id_str|
    # +--------------------+-------------+----------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+--------------+--------------+--------------------+-----------+
    # |{60f12ee9cfa6595a...|       0.3612|2021-07-10|              0|      []|1414011376721403905|          0.0|       0.737|        0.263|       jba|            0| iamkaelaaa|[nike, adidas]|             1|@producedbylex Li...| 2510095346|
    # |{60f12ee9cfa6595a...|       0.7506|2021-07-10|              0|      []|1414010773249085442|          0.0|       0.789|        0.211|       jba|           16| hennyramen|[nike, adidas]|             1|“nike vs. adidas”...|  102880590|
    # +--------------------+-------------+----------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-----------+--------------+--------------+--------------------+-----------+

    # --------------------------------------------------------------
    # ### Evaluate sentiment:
    # --------------------------------------------------------------
    # - positive -> 1,
    # - negative -> -1,
    # - neutral -> 0

    start = time.time()
    # aggregated_mongodf = aggregated_mongodf.withColumn(
    #     'sentiment', sentiment_evalUDF(col('compound_nltk')))
    aggregated_mongodf = aggregated_mongodf.withColumn(
        'sentiment', sentiment_eval_pUDF_wrapper(F.col('compound_nltk')))
    # print(aggregated_mongodf.toPandas())
    end = time.time()
    print(f'Spark pandasUDF - elapsed: {end-start}')
    #    created_at  tweets  compound_nltk  positive_nltk  negative_nltk  neutral_nltk  sentiment
    # 0  2021-07-10      58       0.448472       0.152207       0.023569       0.82419          1
    # 1  2021-07-11      50       0.209132       0.116540       0.043220       0.84028          1
    # Spark UDF - elapsed: 1.7359755039215088

    aggregated_mongodf.show(truncate=30)
    # +----------+------+-------------------+------------------+--------------------+------------------+---------+
    # |created_at|tweets|      compound_nltk|     positive_nltk|       negative_nltk|      neutral_nltk|sentiment|
    # +----------+------+-------------------+------------------+--------------------+------------------+---------+
    # |2021-07-10|    58|0.44847241379310354|0.1522068965517241| 0.02356896551724138|0.8241896551724138|        1|
    # |2021-07-11|    50|0.20913199999999996|           0.11654|0.043219999999999995|           0.84028|        1|
    # +----------+------+-------------------+------------------+--------------------+------------------+---------+

# +----------+------+-------------------+-------------------+--------------------+------------------+---------+
# |created_at|tweets|      compound_nltk|      positive_nltk|       negative_nltk|      neutral_nltk|sentiment|
# +----------+------+-------------------+-------------------+--------------------+------------------+---------+
# |2021-07-10|    65| 0.4364523076923076|0.15336923076923073| 0.02847692307692308|0.8181230769230768|        1|
# |2021-07-09|    74| 0.3649783783783783|0.12793243243243244| 0.01710810810810811|0.8549729729729724|        1|
# |2021-07-08|   115|0.29746347826086944|0.10959999999999996|0.022139130434782613|0.8682521739130437|        1|
# |2021-07-07|    84|0.15158690476190473|0.05659523809523809|0.009619047619047619|0.9337738095238096|        1|
# |2021-07-12|    65| 0.3699553846153845|0.13213846153846154|0.016984615384615386| 0.850876923076923|        1|
# |2021-07-11|    50| 0.2091320000000001|0.11654000000000002|             0.04322|           0.84028|        1|
# |2021-07-13|   155|0.35817225806451597|0.09319354838709676| 0.01238064516129032|0.8944193548387095|        1|
# +----------+------+-------------------+-------------------+--------------------+------------------+---------+
    rows = aggregated_mongodf.count()
    print(f"rows = {rows}")
    # rows = 2
    mydata = threading.local()
    # global output
    mydata.output = []
    mydata.dict_output = {}
    # dict_output = {}
    for row in range(rows):
        mydata.dict_output = {'created_at': aggregated_mongodf.select("created_at").collect()[row][0],
                              'tweets': aggregated_mongodf.select("tweets").collect()[row][0],
                              'positive_nltk': aggregated_mongodf.select("positive_nltk").collect()[row][0],
                              'negative_nltk': aggregated_mongodf.select("negative_nltk").collect()[row][0],
                              'neutral_nltk': aggregated_mongodf.select("neutral_nltk").collect()[row][0],
                              'compound_nltk': aggregated_mongodf.select("compound_nltk").collect()[row][0],
                              'sentiment': aggregated_mongodf.select("sentiment").collect()[row][0]
                              }
    # for row in range(rows):
    #     mydata.dict_output = {'_id': mongodf.select("_id").collect()[row][0]['oid'],
    #                           'query_user': str(mongodf.select("query_user").collect()[row][0]),
    #                           'search_words': mongodf.select("search_words").collect()[row][0],
    #                           'created_at': mongodf.select("created_at").collect()[row][0],
    #                           'text': mongodf.select("text").collect()[row][0],
    #                           'favourite_count': mongodf.select('favourite_count').collect()[row][0],
    #                           'retweet_count': mongodf.select('retweet_count').collect()[row][0],
    #                           'positive_nltk': mongodf.select("positive_nltk").collect()[row][0],
    #                           'negative_nltk': mongodf.select("negative_nltk").collect()[row][0],
    #                           'neutral_nltk': mongodf.select("neutral_nltk").collect()[row][0],
    #                           'compound_nltk': mongodf.select("compound_nltk").collect()[row][0]}

    # 'positive_avg':mongodf.select("positive_avg").collect()[row][0],
    # 'negative_avg':mongodf.select("negative_avg").collect()[row][0],
    # 'neutral_avg':mongodf.select("neutral_avg").collect()[row][0]}

        dictionary_copy = mydata.dict_output.copy()
        # WRONG !!!!!
        # output = output.append(dictionary_copy)
        # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        mydata.output.append(dictionary_copy)
        print(mydata.output)
        # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

    #mongodf_json = mongoAggregationdf.select(to_json(struct(mongoAggregationdf.columns)))
    # mongodf_json.show()
    success_msg = 'success - tweets results'
    print(f' {"="*(len(success_msg)+20)}')
    print(f'\n{" " *10 + success_msg}\n')
    print(f' {"="*(len(success_msg)+20)}')
    print(mydata.output)
    # spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
    return mydata.output

# ************************************************************************************************
def detachedProcessSearchTweets(spark, tweets_data):
    #request_data = request.get_json()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")
    #from pyspark.sql.functions import col, factorial, log, reverse, sqrt
    print("&"*40)
    print(tweets_data)
    # {'user': 'jba',
    # 'search_words': ['coffeeIsland OR (coffee island)'],
    # 'date_since': '2021-07-10',
    # 'date_until': '2021-07-11',
    # 'lang': 'en'
    # }
    # print(type(tweets_data))
    # <class 'dict'>
    query_user = tweets_data["user"]
    search_words = tweets_data["search_words"]
    date_since = tweets_data["date_since"]
    date_until = tweets_data["date_until"]
    lang = tweets_data["lang"]

    print(f"query_user={query_user}")
    print(f"search_words={search_words}")
    print(f"date_since={date_since}")
    print(f"date_until={date_until}")
    print(f"lang={lang}")

    mongo_tweetsDF = scrapetweets(
        query_user, search_words, date_since, date_until, lang)
    # print(f'sdf_schema = {mongo_tweetsDF.schema}')
    # mongo_tweetsDF.printSchema()

    # --------------------------------------------------------------
    # ### sdf
    # --------------------------------------------------------------
    # create a new column ('rating') with sentiment_scores
    mongo_tweetsDF = mongo_tweetsDF.withColumn(
        "rating", sentiment_scoresUDF(mongo_tweetsDF.text))
    # mongo_tweetsDF.show(2, truncate=30)

    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+------------------------------+
    # |query_user|             id_str|                  search_words|                          text|favourite_count|retweet_count|         created_at|hashtags|        user_id_str|    screen_name|                        rating|
    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+------------------------------+
    # |       jba|1414005629384105993|[coffeeIsland OR (coffee is...|Learn all about the intrigu...|              0|            3|2021-07-10 23:36:27|[Coffee]|         1584528546|QueenBeanCoffee|{neg -> 0.0, pos -> 0.222, ...|
    # |       jba|1414004639557513219|[coffeeIsland OR (coffee is...|I'm at the movie theatre, c...|              2|            0|2021-07-10 23:32:31|      []|1136790522335383552|   libratyranny|{neg -> 0.058, pos -> 0.094...|
    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+------------------------------+
    # from pyspark.sql.functions import col
    # 
    mongo_tweetsDF = mongo_tweetsDF.withColumn('negative_nltk', F.col('rating')['neg']) .withColumn('positive_nltk', F.col('rating')[
        'pos']) .withColumn('neutral_nltk', F.col('rating')['neu']) .withColumn('compound_nltk', F.col('rating')['compound']) .drop('rating')
    # mongo_tweetsDF.show(2, truncate=15)
    # +----------+---------------+---------------+---------------+---------------+-------------+---------------+--------+---------------+---------------+-------------+-------------+------------+-------------+
    # |query_user|         id_str|   search_words|           text|favourite_count|retweet_count|     created_at|hashtags|    user_id_str|    screen_name|negative_nltk|positive_nltk|neutral_nltk|compound_nltk|
    # +----------+---------------+---------------+---------------+---------------+-------------+---------------+--------+---------------+---------------+-------------+-------------+------------+-------------+
    # |       jba|141400562938...|[coffeeIslan...|Learn all ab...|              0|            3|2021-07-10 2...|[Coffee]|     1584528546|QueenBeanCoffee|          0.0|        0.222|       0.778|       0.6467|
    # |       jba|141400463955...|[coffeeIslan...|I'm at the m...|              2|            0|2021-07-10 2...|      []|113679052233...|   libratyranny|        0.058|        0.094|       0.848|        0.296|
    # +----------+---------------+---------------+---------------+---------------+-------------+---------------+--------+---------------+---------------+-------------+-------------+------------+-------------+

    # Spark UDF
    # start = time.time()
    # print(mongo_tweetsDF.withColumn('sentiment',
    #       sentiment_evalUDF(F.col('compound_nltk'))).toPandas())
    # end = time.time()
    # print(f'Spark UDF - elapsed: {end-start}')

    # Spark Pandas UDF
    start = time.time()
    mongo_tweetsDF = mongo_tweetsDF.withColumn(
        'sentiment_nltk', sentiment_eval_pUDF_wrapper(F.col('compound_nltk')))
    print(mongo_tweetsDF.toPandas())
    end = time.time()
    print(f'Spark pandas UDF elapsed: {end-start}')
    print(mongo_tweetsDF.schema)
    # schema = StructType([
    #                   StructField(query_user,StringType,true),
    #                   StructField(id_str,StringType,true),
    #                   StructField(search_words,ArrayType(StringType,true),true),
    #                   StructField(text,StringType,true),StructField(favourite_count,LongType,true),
    #                   StructField(retweet_count,LongType,true),
    #                   StructField(created_at,TimestampType,true),
    #                   StructField(hashtags,ArrayType(StringType,true),true),
    #                   StructField(user_id_str,StringType,true),
    #                   StructField(screen_name,StringType,true),
    #                   StructField(negative_nltk,DoubleType,true),
    #                   StructField(positive_nltk,DoubleType,true),
    #                   StructField(neutral_nltk,DoubleType,true),
    #                   StructField(compound_nltk,DoubleType,true)
    #                   StructField(sentiment_nltk,IntegerType,true)
    #                 ]
    #             )

    # ==================
    # Write to mongoDB
    # ==================
    # replaceDocument
    # Replace the whole document when saving Datasets that contain an _id field. If false it will only update the fields in the document that match the fields in the Dataset.
    # Default: true
    mongo_tweetsDF.write.format("mongo").option("database", "tweets_DB").option(
        "collection", "tweets_sentiment_scores").mode("append").save()  # .option("replaceDocument", "false")

    # ==================
    # Read from mongoDB
    # ==================
    from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
                                   TimestampType)
    schema = StructType([
        StructField('query_user', StringType(), True),
        StructField('id_str', StringType(), True),
        StructField('search_words', ArrayType(StringType(), True), True),
        StructField('text', StringType(), True),
        StructField('favourite_count', LongType(), True),
        StructField('retweet_count', LongType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('hashtags', ArrayType(StringType(), True), True),
        StructField('user_id_str', StringType(), True),
        StructField('screen_name', StringType(), True),
        StructField('negative_nltk', DoubleType(), True),
        StructField('positive_nltk', DoubleType(), True),
        StructField('neutral_nltk', DoubleType(), True),
        StructField('compound_nltk', DoubleType(), True),
        StructField('sentiment_nltk', IntegerType(), True)
    ]
    )
    mongo_tweetsDF = spark.read.format("mongo").option("database", "tweets_DB").option(
        "collection", "tweets_sentiment_scores").option('schema', schema).load()
    mongo_tweetsDF.select('*').show(10, truncate=20)
    print('\n', '='*60)
    print(f'mongo_tweetsDF Schema: ')
    print(mongo_tweetsDF.printSchema())

    mongo_tweetsDF = spark.read.schema(schema).format("mongo").option("database", "tweets_DB").option(
        "collection", "tweets_sentiment_scores").option('schema', schema).load()
    # columns = ['_id', 'date_since', 'search_words', 'numTweets', 'numRuns', 'currentRun', 'positive_avg', 'negative_avg', 'neutral_avg']
    mongo_tweetsDF.select('*').show(10, truncate=20)

    #  +----------+-------------------+--------------------+--------------------+---------------+-------------+-------------------+--------------------+------------------+--------------+-------------+-------------+------------+-------------+--------------+
    # |query_user|             id_str|        search_words|                text|favourite_count|retweet_count|         created_at|            hashtags|       user_id_str|   screen_name|negative_nltk|positive_nltk|neutral_nltk|compound_nltk|sentiment_nltk|
    # +----------+-------------------+--------------------+--------------------+---------------+-------------+-------------------+--------------------+------------------+--------------+-------------+-------------+------------+-------------+--------------+
    # |       jba|1414003546203201537|[coffeeIsland OR ...|I miss Coronado i...|              1|            0|2021-07-10 23:28:10|                  []|        4923368664|   hiyaimdanae|        0.261|          0.0|       0.739|      -0.6486|            -1|
    # |       jba|1414001376015888385|[coffeeIsland OR ...|my greatest #acnh...|              2|            0|2021-07-10 23:19:33|              [acnh]|         159501860|    meredactyl|        0.056|        0.094|        0.85|       0.4019|             1|
    # |       jba|1413960668966006784|[coffeeIsland OR ...|PEI Twitter!  My ...|              0|            7|2021-07-10 20:37:48|                  []|         499084012|      SmackPEI|          0.0|        0.255|       0.745|       0.9183|             1|
    # |       jba|1413954485949435907|[coffeeIsland OR ...|@AngelaZito A bit...|              1|            0|2021-07-10 20:13:14|                  []|        1532683519|HarshfieldGreg|        0.025|        0.448|       0.526|       0.9694|             1|
    # |       jba|1413949256763854857|[coffeeIsland OR ...|Looking for gift ...|              0|            0|2021-07-10 19:52:27|[english, island,...|865970381143904256|   ScoutsSouth|          0.0|        0.184|       0.816|        0.743|             1|
    # |       jba|1413948580600111105|[coffeeIsland OR ...|PEI Twitter!  My ...|              0|            7|2021-07-10 19:49:46|                  []|         312017493|      mprshane|          0.0|        0.255|       0.745|       0.9183|             1|
    # |       jba|1413942352318799875|[coffeeIsland OR ...|PEI Twitter!  My ...|              0|            7|2021-07-10 19:25:01|                  []|          14713115|     rosieshaw|          0.0|        0.255|       0.745|       0.9183|             1|
    # |       jba|1413940708915007488|[coffeeIsland OR ...|PEI Twitter!  My ...|              0|            7|2021-07-10 19:18:29|                  []|         149164559|  Mortgagespei|          0.0|        0.255|       0.745|       0.9183|             1|
    # |       jba|1413935357834502148|[coffeeIsland OR ...|PEI Twitter!  My ...|              0|            7|2021-07-10 18:57:13|                  []|          20915691|     gablegirl|          0.0|        0.255|       0.745|       0.9183|             1|
    # |       jba|1413932036658581505|[coffeeIsland OR ...|PEI Twitter!  My ...|              0|            7|2021-07-10 18:44:01|                  []|         323472359|    ToqueCanoe|          0.0|        0.255|       0.745|       0.9183|             1|
    # +----------+-------------------+--------------------+--------------------+---------------+-------------+-------------------+--------------------+------------------+--------------+-------------+-------------+------------+-------------+--------------+

    # print(mongo_tweetsDF.schema)
    # StructType(List(StructField(_id,StructType(List(StructField(oid,StringType,true))),true),
    #                   StructField(compound_nltk,DoubleType,true),
    #                   StructField(created_at,TimestampType,true),
    #                   StructField(favourite_count,LongType,true),
    #                   StructField(negative_nltk,DoubleType,true),
    #                   StructField(neutral_nltk,DoubleType,true),
    #                   StructField(positive_nltk,DoubleType,true),
    #                   StructField(retweet_count,LongType,true),
    #                   StructField(search_words,ArrayType(StringType,true),true),
    #                   StructField(text,StringType,true),
    #                   StructField(user,StringType,true)))

    print('\n', '='*60)
    print(f'mongo_tweetsDF Schema: ')
    print(mongo_tweetsDF.printSchema())
    #  ============================================================
    # mongo_tweetsDF Schema:
    # root
    #  |-- query_user: string (nullable = true)
    #  |-- id_str: string (nullable = true)
    #  |-- search_words: array (nullable = true)
    #  |    |-- element: string (containsNull = true)
    #  |-- text: string (nullable = true)
    #  |-- favourite_count: long (nullable = true)
    #  |-- retweet_count: long (nullable = true)
    #  |-- created_at: timestamp (nullable = true)
    #  |-- hashtags: array (nullable = true)
    #  |    |-- element: string (containsNull = true)
    #  |-- user_id_str: string (nullable = true)
    #  |-- screen_name: string (nullable = true)
    #  |-- negative_nltk: double (nullable = true)
    #  |-- positive_nltk: double (nullable = true)
    #  |-- neutral_nltk: double (nullable = true)
    #  |-- compound_nltk: double (nullable = true)
    #  |-- sentiment_nltk: integer (nullable = true)

    print(' ==============================================================')
    print('success - scrape tweets')
    print(' ==============================================================')
    # spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
    # return jsonify({'result': 'success'})


# ************************************************************************************************
#   *** Request 1 - getSentimentResults from MongoDB***
# ************************************************************************************************
# Using Query Argument
@app.route('/tweets_sentiment_results', methods=['GET', 'POST'])
def tweets_sentiment_results():
    # Accept data from the front end
    request_sentiment_data = request.get_json()
    print(
        f'in tweets_sentiment_results func request_data = {request_sentiment_data}')
    # in tweets_sentiment_results func request_data = {'user': 'jba', 'search_words': ['coffeeIsland OR (coffee island)'], 'date_since': '2021-07-10', 'date_until': '2021-07-11', 'lang': 'en'}
    if request.method == 'POST':
        ret_results = detachedProcessReadSentimentResultsFromMongDB(
            spark, request_sentiment_data)
        print(f'\n\n\ntweets_sentiment_results - ThreadName ')

        ret = jsonify({'result': ret_results})
        print(f"{'*'*60}")
        print(f"{'*'*60}")

        return ret
        # ==============================================================

        # success - tweets results

        # ==============================================================
        # [{'_id': '60b80dde34573b382e41a1dc', 'date_since': '2021-01-03 00:00:00', 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 2, 'positive_avg': 0.03975, 'negative_avg': 0.01745, 'neutral_avg': 0.9427999999999999}, {'_id': '60b8122bf919ab69a1449d90', 'date_since': '2021-01-03 00:00:00', 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 1, 'positive_avg': 0.0534, 'negative_avg': 0.027399999999999997, 'neutral_avg': 0.9192}, {'_id': '60b8126ff919ab69a1449d95', 'date_since': '2021-01-03 00:00:00', 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 2, 'positive_avg': 0.06445, 'negative_avg': 0.027249999999999996, 'neutral_avg': 0.9083}]
        # **********************************************
        # <Response 707 bytes [200 OK]>
        # **********************************************
    # else:
        # return q1.get() #render_template('process1.html')

# *************************************************************************************************
#   *** Request 2 - tweetsSentimentAnalysis***
# *************************************************************************************************
@app.route('/tweets', methods=['POST'])
def twitter_sentiment_results_func():
    tweets_data = request.get_json()
    print(tweets_data)
    # {'user': 'jba', 'search_words': ['astrazeneca', 'pfizer'], 'date_since': '2021-07-10', 'date_until': '2021-07-11', 'lang': 'en'}
    print(type(tweets_data))
    # <class 'dict'>

    search_words = tweets_data["search_words"]
    date_since = tweets_data["date_since"]
    date_until = tweets_data["date_until"]
    lang = tweets_data["lang"]

    tweets_list = (tweepy
                   .Cursor(api.search,
                           q=search_words,
                           since=str(date_since), until=str(date_until),
                           tweet_mode='extended',
                           lang=lang).items(1)
                   )
    output = tweets_list_output(tweets_list)

    # tweets_entities = {'text' : text, 'favourite_count' : favourite_count, 'retweet_count' : retweet_count, 'created_at' : created_at}

    output_list_not_empty = check_tweets_list(output)
    print(output_list_not_empty)
    print(output)
    if output_list_not_empty:
        # if request.method == 'POST':
        global t2
        t2 = threading.Thread(
            target=detachedProcessSearchTweets, args=(spark, tweets_data))
        t2.start()
        return {"message": "Accepted"}, 202
    else:
        return {"message": "Not Accepted - empty result_list - change your search_query"}, 200


if __name__ == "__main__":

    #import multiprocessing
    from pyspark.sql.functions import pandas_udf

    # multiprocessing.set_start_method('spawn')
    app.run(host='0.0.0.0', port=5001, threaded=True)
