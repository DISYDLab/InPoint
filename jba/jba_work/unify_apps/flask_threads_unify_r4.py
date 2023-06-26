# The OS module in Python provides functions for interacting with the operating system. OS comes
# under Python’s standard utility modules. This module provides a portable way of using operating
#  system-dependent functionality. The *os* and *os.path* modules include many functions to
#  interact with the file system.
import os

# -----------------------------------------------------------------------------------------------------
# This module provides the ConfigParser class which implements a basic configuration language
# which provides a structure similar to what’s found in Microsoft Windows INI files. You can use
# this to write Python programs which can be customized by end users easily.
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

# from pyspark.sql.DataFrame import toJSON


# The Natural Language Toolkit (NLTK) is a Python package for natural language processing.
nltk.download('vader_lexicon')

# Koalas is an open source project that provides a drop-in replacement for pandas.
#import databricks.koalas as ks

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

    print(f"{'*'* 22}\n>> sparkSession started\n")

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
    # --------------------------------
    # Testing authentication failure
    # access_token_secret = 1
    # --------------------------------
    # Creating the authentication object to Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    # Setting your access token and secret
    auth.set_access_token(access_token, access_token_secret)

    # Create API object - . You can use the API to read and write information related to
    # Twitter entities such as tweets, users, and trends.
    # The Twitter API uses OAuth, a widely used open authorization protocol, to authenticate
    # all the requests.
    api = tweepy.API(auth, wait_on_rate_limit=True)
    try:
        api.verify_credentials()
    except:
        logger.error(f"Error during authentication")
        sys.exit(1)
    else:
        return api


# ---------------------------------------------------------------------------------------------------------
# assign twitter_tokens configuration file to twitter_tokens variable
twitter_tokens = 'twitter_user.properties'
api = set_api(twitter_tokens)
# ---------------------------------------------------------------------------------------------------------

# ---VADER-------------------------------------------------------------------------------------------------
# =========================================================================================================
# ---VADER-------------------------------------------------------------------------------------------------
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,MapType, StructField, StructType, Row
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

# Registering the UDF function
# You can optionally set the return type of your UDF. The default return type␣,→is StringType.
# i.e udffactorial_p = udf(factorial_p, LongType())
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
# ### Clean text string:
# ======================================================================================================
def clean_text(c):
    # c = F.lower(c)
    c = F.regexp_replace(c, "^RT ", "")
    # Remove mentions i.e @bowandyou
    c = F.regexp_replace(c, "(@)\S+", "")
    # Remove URLs
    c = F.regexp_replace(c, "(https?\://)\S+", "")
    # Remove Hashtag symbol from hashtags i.e #chargernation -> chargernation -> charger nation
    # c = F.regexp_replace(c, "(#)\S+", "")
    # c = F.regexp_replace(c, "[^a-zA-Z0-9\\s]", "")
    #c = split(c, "\\s+") tokenization...
    return c

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
        # logger.info(f"Append list length : { len(output)}")
    end = time.time()
    logger.info(f"elapsed_time: '{end - start}'")
    # print(output[:3], "\n")
    logger.info(f"output_length: {len(output)}\n")

    # ---------------------------------------------------------------------------------------------------------
    # ### create sdf from list
    # ---------------------------------------------------------------------------------------------------------
    # Finally, we convert the `output` list to a `spark DataFrame` and we store results.
    print(' >> Create sdf from list')
    sdf = spark.createDataFrame([Row(**i) for i in output])
    # sdf.show(2, truncate=30)

    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+
    # |query_user|             id_str|                  search_words|                          text|favourite_count|retweet_count|         created_at|hashtags|        user_id_str|    screen_name|
    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+
    # |       jba|1414005629384105993|[coffeeIsland OR (coffee is...|Learn all about the intrigu...|              0|            3|2021-07-10 23:36:27|[Coffee]|         1584528546|QueenBeanCoffee|
    # |       jba|1414004639557513219|[coffeeIsland OR (coffee is...|I'm at the movie theatre, c...|              2|            0|2021-07-10 23:32:31|      []|1136790522335383552|   libratyranny|
    # +----------+-------------------+------------------------------+------------------------------+---------------+-------------+-------------------+--------+-------------------+---------------+
    clean_text_sdf = sdf.withColumn('clean_text', clean_text(F.col("text")))
    # clean_text_sdf = sdf.select(clean_text(F.col("text")).alias("text"))
    return clean_text_sdf

# *********************************************************************************************************
# =========================================================================================================
# *********************************************************************************************************
# PRODUCER-1
#############
def detachedProcessReadSentimentResultsFromMongDB(spark, request_data):
    print("#"*40)
    # print(request_data)
    # {'user': 'jba', 'search_words': ['coffeeIsland OR (coffee island)'], 'date_since': '2021-07-10', 'date_until': '2021-07-11', 'lang': 'en'}
    # print(type(request_data))
    # <class 'dict'>
    user = request_data["search_words"]
    print(f"user = {user}")
    search_words = request_data["search_words"]
    print(f"search_words = {search_words}")
    # search_words=['coffeeIsland OR (coffee island)]
    # print(type(search_words))  # <class 'list'>
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

    print(f"date_since = {date_since}")
    # date_since=2021-07-10
    print(f"date_until = {date_until}")
    # date_until=2021-07-11
    date_since_obj = datetime.datetime.strptime(date_since, '%Y-%m-%d')
    # A datetime object d is aware if both of the following hold:
    # d.tzinfo is not None
    # d.tzinfo.utcoffset(d) does not return None

    # print(date_since_obj.tzinfo) # None
    # print(datetime.tzinfo.tcoffset(date_since_obj))
    # date_since_obj = date_since_obj.isoformat()
    # date_since_obj = date_since_obj+"Z"
    # date_since_obj = date_since_obj+"+03:00"
    dt_athens_since = timezone('Europe/Athens').localize(date_since_obj)
    print(f'Time zone = {dt_athens_since.tzname()}')  # EEST
    # print(dt_athens_since)  # 2021-07-10 23:36:27+03:0
    date_since_str = dt_athens_since.isoformat()
    # date_since_obj = datetime.datetime.combine(date_since_obj, datetime.datetime.min.time())
    date_until_obj = datetime.datetime.strptime(date_until, '%Y-%m-%d')
    date_until_obj = datetime.datetime.combine(
        date_until_obj, datetime.datetime.max.time())
    dt_athens_until = timezone('Europe/Athens').localize(date_until_obj)
    date_until_str = dt_athens_until.isoformat()
    # print(type(date_until_str)) # <class 'str'>
    print(f"date_since = {date_since_str}")
    # date_since=2021-07-10 00:00:00
    
    print(f"date_until = {date_until_str}")
    # The ISO format for timestamps has a 'T' separating the date from the time part.
    # date_since=2021-07-07T00:00:00Z
    # date_until=2021-07-14T23:59:59.999999Z

    print(f"lang = {lang}")
    # lang=en

    # Spark fair scheduler - Scheduling Within an Application
    # =========================================================================================================
    # Spark has several facilities for scheduling resources between computations. First, each Spark application 
    # (instance of SparkContext) runs an independent set of executor processes. The cluster managers that Spark 
    # runs on provide facilities for scheduling across applications. Second, within each Spark application, multiple 
    # “jobs” (Spark actions) may be running concurrently if they were submitted by different threads. This is common 
    # if your application is serving requests over the network. Spark includes a fair scheduler to schedule resources 
    # within each SparkContext.

    # Spark’s scheduler is fully thread-safe and supports this use case to enable applications that serve multiple 
    # requests (e.g. queries for multiple users).

    # By default, Spark’s scheduler runs jobs in FIFO fashion. Each job is divided into “stages” (e.g. map and reduce 
    # phases), and the first job gets priority on all available resources while its stages have tasks to launch, then 
    # the second job gets priority, etc. If the jobs at the head of the queue don’t need to use the whole cluster, 
    # later jobs can start to run right away, but if the jobs at the head of the queue are large, then later jobs may 
    # be delayed significantly.

    # Starting in Spark 0.8, it is also possible to configure fair sharing between jobs. Under fair sharing, Spark 
    # assigns tasks between jobs in a “round robin” fashion, so that all jobs get a roughly equal share of cluster 
    # resources. This means that short jobs submitted while a long job is running can start receiving resources right 
    # away and still get good response times, without waiting for the long job to finish. This mode is best for 
    # multi-user settings.

    # To enable the fair scheduler, simply set the spark.scheduler.mode property to FAIR when configuring SparkSession:
    # spark  = SparkSession \
    # 	  .builder \
    #     .config("spark.driver.allowMultipleContexts","true") \
    #     .config("spark.suffle.service.enabled","true") \
    #     .config("spark.default.parallelism","8") \
    #     .config("spark.sql.shuffle.partitions","8") \
    #     .config("spark.scheduler.mode","FAIR") \
    #     .config("spark.scheduler.allocation.file", "/home/hadoopuser/spark/conf/fairscheduler.xml") \
    #     .getOrCreate()
    # Fair Scheduler Pools
    # ======================
    # The fair scheduler also supports grouping jobs into pools, and setting different scheduling options (e.g. weight) 
    # for each pool. This can be useful to create a “high-priority” pool for more important jobs, for example, or to 
    # group the jobs of each user together and give users equal shares regardless of how many concurrent jobs they have 
    # instead of giving jobs equal shares. This approach is modeled after the Hadoop Fair Scheduler.

    # Without any intervention, newly submitted jobs go into a default pool, but jobs’ pools can be set by adding the 
    # spark.scheduler.pool “local property” to the SparkContext in the thread that’s submitting them. This is done as 
    # follows:
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
    # ****** + user 
    print('\n >> Create a MongoDB aggregation pipeline')
    pipeline = str({'$match': {'$and': [{'created_at': {'$gte': {'$date': date_since_str}}}, {
                   'created_at': {'$lte': {'$date': date_until_str}}}], 'search_words': search_words}})

    print("\npipeline : ", pipeline, "\n")
    # pipeline :  {'$match': {'$and': [{'created_at': {'$gte': {'$date': '2021-07-10T00:00:00+03:00'}}}, {'created_at': {'$lte': {'$date': '2021-07-11T23:59:59.999999+03:00'}}}], 'search_words': ['coffeeIsland OR (coffee island)']}} 
    print(' >> Read from mongoDB db according to aggegation pipeline\n')
    mongodf = spark.read.format("mongo").option("database", "tweets_DB").option(
        "collection", "tweets_sentiment_scores").option("pipeline", pipeline).load()
    # mongodf.orderBy('created_at').show(2, truncate = 30)
    # +--------------------------+-------------+-------------------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-------------+------------------------------+--------------+------------------------------+------------------+
    # |                       _id|compound_nltk|         created_at|favourite_count|hashtags|             id_str|negative_nltk|neutral_nltk|positive_nltk|query_user|retweet_count|  screen_name|                  search_words|sentiment_nltk|                          text|       user_id_str|
    # +--------------------------+-------------+-------------------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-------------+------------------------------+--------------+------------------------------+------------------+
    # |{60f2ab05317c3108eda4f0c0}|       0.2732|2021-07-10 00:34:23|              7|      []|1413657820394893312|        0.133|       0.687|         0.18|       jba|            0|_badgalmariri|[coffeeIsland OR (coffee is...|             1|I think the love island vil...|839356681142296576|
    # |{60eeb7282a84be10cd0cd016}|       0.2732|2021-07-10 00:34:23|              7|      []|1413657820394893312|        0.133|       0.687|         0.18|       jba|            0|_badgalmariri|[coffeeIsland OR (coffee is...|             1|I think the love island vil...|839356681142296576|
    # +--------------------------+-------------+-------------------+---------------+--------+-------------------+-------------+------------+-------------+----------+-------------+-------------+------------------------------+--------------+------------------------------+------------------+

    logger.info(f'mongodf_rows = {mongodf.count()}')
    logger.info(f"mongodf.columns = {mongodf.columns}")
    # mongodf.columns = ['_id', 'compound_nltk', 'created_at', 'favourite_count', 'hashtags', 'id_str', 'negative_nltk', 'neutral_nltk', 'positive_nltk', 'query_user', 'retweet_count', 'screen_name', 'search_words', 'sentiment_nltk', 'text', 'user_id_str']
   
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

    # --------------------------------------------------------------
    # ### Convert created_at from timestamp to date
    # --------------------------------------------------------------
    print("\n >> Convert data type of'created_at' column from timestamp to date")
    mongodf = mongodf.withColumn('created_at', F.to_date(F.col('created_at')))

    # --------------------------------------------------------------
    # ### groupBy and aggregate on multiple columns
    # --------------------------------------------------------------
    print(' >> groupBy and aggregate on multiple columns')
    exprs = {}
    cols = ['created_at',
            'negative_nltk',
            'positive_nltk',
            'neutral_nltk',
            'compound_nltk'
            ]
    exprs = {x: "sum" for x in cols}
    exprs['created_at'] = 'count'

    print(exprs)
    # {'created_at': 'count', 'negative_nltk': 'sum', 'positive_nltk': 'sum', 'neutral_nltk': 'sum', 'compound_nltk': 'sum'}

    aggregated_mongodf = mongodf.groupBy('created_at').agg(
        exprs).withColumnRenamed('count(created_at)', 'tweets')
    # aggregated_mongodf.show(2, truncate=30)
    # +----------+------------------+------------------+------+------------------+------------------+
    # |created_at|sum(compound_nltk)|sum(positive_nltk)|tweets|sum(negative_nltk)| sum(neutral_nltk)|
    # +----------+------------------+------------------+------+------------------+------------------+
    # |2021-07-10|26.011400000000005| 8.827999999999998|    58|             1.367|47.803000000000004|
    # |2021-07-11|10.456599999999998|             5.827|    50|2.1609999999999996|            42.014|
    # +----------+------------------+------------------+------+------------------+------------------+
    
    # --------------------------------------------------------------
    # ### Drop columns in pyspark dataframe
    # --------------------------------------------------------------
    print('\n >> Drop columns in pyspark dataframe')
    columns_to_drop = ['sum(compound_nltk)', 'sum(positive_nltk)',
                       'sum(negative_nltk)', 'sum(neutral_nltk)']
    print(columns_to_drop, '\n')
    aggregated_mongodf = (aggregated_mongodf
                          .withColumn('compound_nltk', F.col('sum(compound_nltk)')/F.col('tweets'))
                          .withColumn('positive_nltk', F.col('sum(positive_nltk)')/F.col('tweets'))
                          .withColumn('negative_nltk', F.col('sum(negative_nltk)')/F.col('tweets'))
                          .withColumn('neutral_nltk', F.col('sum(neutral_nltk)')/F.col('tweets'))
                          .drop(*columns_to_drop)
                          )
    
    aggregated_mongodf.show(truncate=30)

    # +----------+------+------------------+-------------------+--------------------+------------------+
    # |created_at|tweets|     compound_nltk|      positive_nltk|       negative_nltk|      neutral_nltk|
    # +----------+------+------------------+-------------------+--------------------+------------------+
    # |2021-07-10|   936| 0.444471153846154| 0.1549401709401705|0.027816239316239298|0.8172115384615406|
    # |2021-07-11|    98|0.2054000000000001|0.11687755102040817|0.043581632653061225|0.8395816326530613|
    # +----------+------+------------------+-------------------+--------------------+------------------+

    # --------------------------------------------------------------
    # ### Evaluate sentiment:
    # --------------------------------------------------------------
    # - positive -> 1,
    # - negative -> -1,
    # - neutral -> 0
    print(' >> Evaluate sentiments in sdf')
    start = time.time()
    aggregated_mongodf = aggregated_mongodf.withColumn(
        'sentiment', sentiment_eval_pUDF_wrapper(F.col('compound_nltk')))
    end = time.time()

    print(f'Spark pandasUDF - elapsed: {end-start}')
    # Spark UDF - elapsed: 1.7359755039215088

    aggregated_mongodf.show(truncate=30)
    # +----------+------+-------------------+------------------+--------------------+------------------+---------+
    # |created_at|tweets|      compound_nltk|     positive_nltk|       negative_nltk|      neutral_nltk|sentiment|
    # +----------+------+-------------------+------------------+--------------------+------------------+---------+
    # |2021-07-10|    58|0.44847241379310354|0.1522068965517241| 0.02356896551724138|0.8241896551724138|        1|
    # |2021-07-11|    50|0.20913199999999996|           0.11654|0.043219999999999995|           0.84028|        1|
    # +----------+------+-------------------+------------------+--------------------+------------------+---------+

    rows = aggregated_mongodf.count()
    print(f"rows = {rows}")
    # rows = 2

    # Thread-local data is data whose values are thread specific.
    # To manage yhread-local data, just create an instance ol local(or a subclass)
    # and store attributes on it:
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

        dictionary_copy = mydata.dict_output.copy()
        mydata.output.append(dictionary_copy)

    success_msg = 'success - tweets results'
    print(f' {"="*(len(success_msg)+20)}')
    print(f'\n{" " *10 + success_msg}\n')
    print(f' {"="*(len(success_msg)+20)}')
    print(mydata.output)
    # [
    #  {'created_at': datetime.date(2021, 7, 10), 'tweets': 192, 'positive_nltk': 0.15395833333333342, 
    #   'negative_nltk': 0.02822916666666667, 'neutral_nltk': 0.8177812500000002, 'compound_nltk': 0.439459375, 
    #   'sentiment': 1
    #  }, 
    #  {'created_at': datetime.date(2021, 7, 11), 'tweets': 98, 'positive_nltk': 0.11687755102040817, 
    #   'negative_nltk': 0.043581632653061225, 'neutral_nltk': 0.8395816326530613, 'compound_nltk': 0.2054000000000001, 
    #   'sentiment': 1
    #  }
    # ]
    # spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
    return mydata.output

# ************************************************************************************************
# ************************************************************************************************
def detachedProcessSearchTweets(spark, tweets_data):
    #request_data = request.get_json()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")
    #from pyspark.sql.functions import col, factorial, log, reverse, sqrt
    print("&"*60)
    # print(tweets_data)
    # {'user': 'jba',
    # 'search_words': ['coffeeIsland OR (coffee island)'],
    # 'date_since': '2021-07-10',
    # 'date_until': '2021-07-11',
    # 'lang': 'en'
    # }
    # print(type(tweets_data))
    # <class 'dict'>
    print(' >> tweets_query_data')
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

    print()
    print(' >> Scrape tweets')
    mongo_tweetsDF = scrapetweets(
        query_user, search_words, date_since, date_until, lang)
    mongo_tweetsDF.show(2, truncate = 30)
    mongo_tweetsDF.show(mongo_tweetsDF.count(), truncate = 30)               
    mongo_tweetsDF.select('text', 'clean_text').show(mongo_tweetsDF.count(), truncate = False)
    # print(f'sdf_schema = {mongo_tweetsDF.schema}')
    # mongo_tweetsDF.printSchema()

    # --------------------------------------------------------------
    # ### Apply Vader Sentiment analysis to 'text' column of sdf
    # --------------------------------------------------------------
    print(" >> Create in sdf a new column ('rating') with VADER sentiment_scores")
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
    print(" >> Extract ratings ['negative_nltk', 'positive_nltk', 'neutral_nltk', 'compound_nltk'] from rating column  to columns")
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
    print(" >> Evaluate sentiments from 'compount_nltk' column")
    start = time.time()
    mongo_tweetsDF = mongo_tweetsDF.withColumn(
        'sentiment_nltk', sentiment_eval_pUDF_wrapper(F.col('compound_nltk')))
    # print(mongo_tweetsDF.toPandas())
    end = time.time()
    # print(f'Spark pandas UDF elapsed: {end-start}')
    # print(mongo_tweetsDF.schema)
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
    print(' >> Write sdf to mongoDB')
    # replaceDocument
    # Replace the whole document when saving Datasets that contain an _id field. If false it will only update the fields in the document that match the fields in the Dataset.
    # Default: true
    mongo_tweetsDF.write.format("mongo").option("database", "tweets_DB").option(
        "collection", "tweets_sentiment_scores").mode("append").save()  # .option("replaceDocument", "false")

    # # ==================
    # # Read from mongoDB
    # # ==================
    # from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
    #                                TimestampType)
    # schema = StructType([
    #     StructField('query_user', StringType(), True),
    #     StructField('id_str', StringType(), True),
    #     StructField('search_words', ArrayType(StringType(), True), True),
    #     StructField('text', StringType(), True),
    #     StructField('favourite_count', LongType(), True),
    #     StructField('retweet_count', LongType(), True),
    #     StructField('created_at', TimestampType(), True),
    #     StructField('hashtags', ArrayType(StringType(), True), True),
    #     StructField('user_id_str', StringType(), True),
    #     StructField('screen_name', StringType(), True),
    #     StructField('negative_nltk', DoubleType(), True),
    #     StructField('positive_nltk', DoubleType(), True),
    #     StructField('neutral_nltk', DoubleType(), True),
    #     StructField('compound_nltk', DoubleType(), True),
    #     StructField('sentiment_nltk', IntegerType(), True)
    # ]
    # )
    # mongo_tweetsDF = spark.read.format("mongo").option("database", "tweets_DB").option(
    #     "collection", "tweets_sentiment_scores").option('schema', schema).load()
    # mongo_tweetsDF.select('*').show(10, truncate=20)
    # print('\n', '='*60)
    # print(f'mongo_tweetsDF Schema: ')
    # print(mongo_tweetsDF.printSchema())

    # mongo_tweetsDF = spark.read.schema(schema).format("mongo").option("database", "tweets_DB").option(
    #     "collection", "tweets_sentiment_scores").load()
    # # columns = ['_id', 'date_since', 'search_words', 'numTweets', 'numRuns', 'currentRun', 'positive_avg', 'negative_avg', 'neutral_avg']
    # mongo_tweetsDF.select('*').show(10, truncate=20)

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

    print(' ==============================================================')
    print('success - scrape tweets')
    print(' ==============================================================')
    # spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)


# ************************************************************************************************
#   *** Request 1 - getSentimentResults from MongoDB***
# ************************************************************************************************
# Using Query Argument
@app.route('/tweets_sentiment_results', methods=['POST'])
def tweets_sentiment_results():
    # Accept data from the front end
    request_sentiment_data = request.get_json()
    # print(f'in tweets_sentiment_results func request_data = {request_sentiment_data}')
    # in tweets_sentiment_results func request_data = {'user': 'jba', 'search_words': ['coffeeIsland OR (coffee island)'], 'date_since': '2021-07-10', 'date_until': '2021-07-11', 'lang': 'en'}
    if request.method == 'POST':
        ret_results = detachedProcessReadSentimentResultsFromMongDB(
            spark, request_sentiment_data)
        # print(f'\n\n\ntweets_sentiment_results - ThreadName ')

        ret = jsonify({'result': ret_results})
        print(f"\n{'*'*60}")
        print(f"{'*'*60}")

        return ret
        # 2021-07-18 11:04:15,186 - INFO - 0.0.0.0 - - [18/Jul/2021 11:04:15] "POST /tweets_sentiment_results HTTP/1.1" 200 -
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
    app.run(host='0.0.0.0', port=5000, threaded=True)
