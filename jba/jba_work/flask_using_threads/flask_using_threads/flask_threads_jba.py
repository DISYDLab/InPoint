# The OS module in Python provides functions for interacting with the operating system. OS comes 
# under Python’s standard utility modules. This module provides a portable way of using operating
#  system-dependent functionality. The *os* and *os.path* modules include many functions to
#  interact with the file system.
import os

# The sys module provides functions and variables used to manipulate different parts of the
#  Python runtime environment.
import sys
import threading

import requests_oauthlib
lock = threading.Lock()
# Flask is a lightweight WSGI web application framework. It is designed to make getting started
#  quick and easy, with the ability to scale up to complex applications. It began as a simple
#  wrapper around Werkzeug and Jinja and has become one of the most popular Python web 
#  application frameworks.
#  application frameworks.
from flask import Flask,render_template,  jsonify, request, make_response

import json

# In Python, the multiprocessing module includes a very simple and intuitive API for dividing
#  work between multiple processes. 
# A multiprocessing system can have:
#   - multiprocessor, i.e. a computer with more than one central processor.
#   - multi-core processor, i.e. a single computing component with two or more independent actual
#     processing units (called “cores”).
from multiprocessing import Process, Queue
from multiprocessing.pool import ThreadPool
import time, datetime
# Thread module emulating a subset of Java's threading mode
import threading
from threading import Timer
import queue

import joblib





# When starting the pyspark shell, you can specify:
# the --packages option to download the MongoDB Spark Connector package.
os.environ['PYSPARK_SUBMIT_ARGS'] = \
'--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0  pyspark-shell'

# Provides findspark.init() to make pyspark importable as a regular library.
# Find spark home, and initialize by adding pyspark to sys.path.
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import pandas_udf, PandasUDFType
# from pyspark.sql.types import toJSON

# import pyspark.sql.types as Types
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType, StructField, StructType, Row
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.types import DateType, DoubleType, IntegerType, MapType
from pyspark.sql.functions import col , to_timestamp, to_date

from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Tweepy, a package that provides a very convenient way to use the Twitter API
import tweepy


# import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import databricks.koalas as ks
import pandas as pd

# This module provides the ConfigParser class which implements a basic configuration language
#  which provides a structure similar to what’s found in Microsoft Windows INI files. You can use 
# this to write Python programs which can be customized by end users easily.
import configparser

# The requests module allows you to send HTTP requests using Python.
# The HTTP request returns a Response Object with all the response data (content, encoding, 
# status, etc).
import requests


os.environ["PYSPARK_PIN_THREAD"] = "true"

#sc = SparkContext()
#sqlContx = SQLContext(sc)

#nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()

#*************************************************************************************************
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#*************************************************************************************************

app = Flask(__name__)

print("^"*50)
print(__name__)
print("^"*50)

#------------------
def start_spark():
    global spark
    spark  = SparkSession \
        .builder \
        .appName('spark_threads') \
        .config("spark.mongodb.input.uri", 
        "mongodb://ubuntu/test.myCollection?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://ubuntu/test.myCollection") \
        .config('spark.jars.packages', 
        'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0') \
        .config("spark.scheduler.mode","FAIR") \
        .config("spark.scheduler.allocation.file", "/opt/spark/conf/fairscheduler.xml") \
        .getOrCreate()

    print(f"{'*'* 22}\nsparkSession started\n")

#------------------
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

    consumer_key = config.get('twitter', 'consumer_key') # creating 4 variables and assigning them basically saying read these 4 keys from file and assign
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

twitter_tokens = 'twitter.properties'
api = set_api(twitter_tokens)

# Registers a function to be run before the first request to this instance of the application.
# The function will be called without any arguments and its return value is ignored.
app.before_first_request(start_spark)

#############################
#spark = start_spark()
#sc = spark.sparkContext
##############################

#spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)

#************************************************************************************************
def scraptweets(search_words, date_since, numTweets):
    positive_avg = 0;
    negative_avg = 0;
    neutral_avg = 0;
    cnt = 0;

    program_start = time.time()

        
    # We will time how long it takes to scrape tweets for each run:
    start_run = time.time()


    # function Args: 
    #    search_words, 
    #    date_since, 
    #    numTweets, 
    #    numRuns

    # You will use the .Cursor method to get an object containing tweets containing the hashtag 
    # search_words.
    # You can restrict the number of tweets returned by specifying a number in the .items() method.
    # https://developer.twitter.com/en/docs/pagination#:~:text=The%20Twitter%20standard%20REST%20APIs%20utilize%20a%20technique,to%20move%20backwards%20and%20forwards%20through%20these%20pages.
    # The Twitter standard REST APIs utilize a technique called ‘cursoring’ to paginate large result sets.
    # Cursoring separates results into pages (the size of which are defined by the count request 
    # parameter) and provides a means to move backwards and forwards through the pages.          
    tweets = tweepy.Cursor(api.search, 
                           q=search_words, 
                           since=date_since, 
                           tweet_mode='extended', lang='en').items(numTweets)

    tweet_list = [tweet for tweet in tweets]

    # Cursor() returns an object that you can iterate or loop over to access the data collected.
    noTweets = 0
    for tweet in tweet_list:
        # Each item in the iterator has various attributes that you can access to get information about each tweet including:
        # Pull the values

        cnt = cnt + 1
        try:
            text = tweet.retweeted_status.full_text
        except AttributeError:  # Not a Retweet
            text = tweet.full_text
        #print(text)

        r = sid.polarity_scores(text)
        positive_avg = (positive_avg + r['pos'])
        negative_avg = (negative_avg + r['neg'])
        neutral_avg = (neutral_avg + r['neu'])

#    print('*'*80)
#    print(f'cnt = {cnt}')
#    print(f'numRun = {i}')

#     print(db_tweets)
    try:
        tweets_sentiments = {'positive_avg' : positive_avg/cnt, 'negative_avg' : negative_avg/cnt, 'neutral_avg' : neutral_avg/cnt}
        return tweets_sentiments
    except:
        cnt = 1
        tweets_sentiments = {'positive_avg' : positive_avg/cnt, 'negative_avg' : negative_avg/cnt, 'neutral_avg' : neutral_avg/cnt}
        return tweets_sentiments

# scraptweetsUDF = udf(scraptweets, Types.MapType(Types.StringType(), Types.DoubleType()))
scraptweetsUDF = udf(scraptweets, MapType(StringType(), DoubleType()))

#*************************************************************************************************
# search_words = ["astrazeneca", "pfizer"]
# date_since = "2020-11-03"
# numTweets = 20
# numRuns = 2
# sentiments_scores = {'pos':float(1), 'neg': float(-1), 'neu':float(0.8)}
#q1 = queue.Queue() # queue module for threading. queue.Queue() => FIFO queue | 
                   # queue. => LIFO, 
                   # queue. => 
                   # q1.put(5), q1.get(), q1.empty()
#q1 = Queue() # from multiprocessing

#i = 0
from pyspark.sql.functions import *

#************************************************************************************************
# PRODUCER-1
#############
def detachedProcessFunction(spark, test_data):
    #test_data = request.get_json()
    print("#"*40)
    #print (request.is_json)
    print(test_data)
    # {'search_words': ['astrazeneca', 'pfizer'], 'date_since': '2021-01-02T22:00:00Z'}
    print(type(test_data))
    # <class 'dict'>

    search_words = test_data["search_words"]
    from dateutil import parser

    # date_since = datetime.datetime.strptime(test_data["date_since"], '%y-%m-%d %H:%M:%S')
    #"Z time" or "Zulu Time"
    ########################################
    print(f'time.tzname: {time.tzname}')
    print(f'datetime.tzinfo: {datetime.tzinfo}')
    # time.tzname: ('EET', 'EEST')
    # datetime.tzinfo: <class 'datetime.tzinfo'>

    # pipeline :  {'$match': {'search_words': ['astrazeneca', 'pfizer'], 'date_since': {'$date': '2021-01-02T22:00:00Z'}}} 

    # date_since = parser.parse(test_data["date_since"])
    # date_since = datetime.datetime.strptime(str(date_since), '%Y-%m-%dT%H:%M:%S%Z')

    date_since = test_data["date_since"]
 
    print(f"search_words={search_words}")
    # search_words=['astrazeneca', 'pfizer']
    print(f"date_since={date_since}") 
    # date_since=2021-01-02T22:00:00Z
    
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")
    #from pyspark.sql.functions import col, factorial, log, reverse, sqrt
    
    # Place the $match as early in the aggregation pipeline as possible. Because $match limits 
    # the total number of documents in the aggregation pipeline, earlier $match operations
    # minimize the amount of processing down the pipe.
    # $match with equality match 
    # "," comma between key:value pairs is implicit and operator i.e: {k1:v1, k2:v2, k3:v3}
    # $match: {<query>} => match specific documents using query
    # pipeline = str({'$match': {'search_words':search_words, 'date_since':date_since, 'numRuns':'1'}})
    # pipeline = str({'$match': {'search_words':search_words, 'currentRun':2}})
    # pipeline = str({'$match': {'search_words':search_words}})
    # pipeline = str({'$match': {'search_words':search_words, 'date_since':date_since}})

    # pipeline = str([{'$match': {'search_words':search_words, 'date_since':{'$dateFromString': {'dateString': date_since} }}}])
    # date_since = "2021-01-02T22:00:00.00Z"
    pipeline = str({'$match': {'search_words':search_words, 'date_since':{'$date':date_since}}})

    print("\npipeline : ", pipeline,"\n")
    # pipeline :  {'$match': {'search_words': ['astrazeneca', 'pfizer'], 'date_since': {'$date': '2021-01-02T22:00:00Z'}}} 

    mongoAggregationdf = spark.read.format("mongo").option("database", "tweets_scoresDB").option("collection", "tweets_scores").option("pipeline", pipeline).load()
    
    print(f"mongoAggregationdf.columns = {mongoAggregationdf.columns}")

    columns = ['_id', 'date_since', 'search_words', 'numTweets', 'numRuns', 'currentRun', 'positive_avg', 'negative_avg', 'neutral_avg']
    mongoAggregationdf.select(columns).show(100, truncate=False)
    print(f'mongoAggregationdf Schema= ')
    print(mongoAggregationdf.printSchema())

    rows = mongoAggregationdf.count()
    print(f"rows = {rows}")
    mydata = threading.local()
    global output
    mydata.output = []
    mydata.dict_output ={}
    dict_output ={}
    for row in range(rows):
        mydata.dict_output = {'_id':mongoAggregationdf.select("_id").collect()[row][0]['oid'],
        'date_since':str(mongoAggregationdf.select("date_since").collect()[row][0]),
        'search_words':mongoAggregationdf.select("search_words").collect()[row][0],
        'numTweets':mongoAggregationdf.select("numTweets").collect()[row][0],
        'numRuns':mongoAggregationdf.select("numRuns").collect()[row][0],
        'currentRun':mongoAggregationdf.select("currentRun").collect()[row][0],
        'positive_avg':mongoAggregationdf.select("positive_avg").collect()[row][0],
        'negative_avg':mongoAggregationdf.select("negative_avg").collect()[row][0],
        'neutral_avg':mongoAggregationdf.select("neutral_avg").collect()[row][0]}

        dictionary_copy = mydata.dict_output.copy()
        # WRONG !!!!!
        # output = output.append(dictionary_copy)
        # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   
        mydata.output.append(dictionary_copy)
        print (mydata.output)
        # $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

    #mongodf_json = mongoAggregationdf.select(to_json(struct(mongoAggregationdf.columns)))
    #mongodf_json.show()
    
    print(' ==============================================================')
    print('\nsuccess - tweets results\n')
    print(' ==============================================================')
    print(mydata.output)
    #spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)

    def myconverter(o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

    # Interthread communication by using Queue:
    # global q1
    # producer thread
    #Queue.put(item, block=True, timeout=None)
    return mydata.output
    #q1.put(mongoAggregationJSON)
    #with open ('myjson.json', 'w') as json_file:
        #json.dump(output, json_file, default = myconverter)

#************************************************************************************************
def detachedProcessFunction3(spark, tweets_data):
    #test_data = request.get_json()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")    
    #from pyspark.sql.functions import col, factorial, log, reverse, sqrt
    print("&"*40)
    
    #print (request.is_json)
    print(tweets_data)
    print(type(tweets_data))
    # {"search_words": [],
    # "date_since":''},
    # "numTweets": 20,
    # "numRuns": 2
    # }
      
    search_words = tweets_data["search_words"]
    date_since = tweets_data["date_since"]
    numTweets = tweets_data["numTweets"]
    numRuns = tweets_data["numRuns"]
    currentRun = 0
      
    print(f"search_words={search_words}")
    print(f"date_since={date_since}")    
    print(f"numTweets={numTweets}")
    print(f"numRuns={numRuns}")

    # Create schema: social_twitter_end_service(search_words, date_since, numTweets, numRuns)
    schema = StructType([
        StructField('search_words', StringType(), True), \
        StructField('date_since', StringType(), True), \
        StructField('numTweets', IntegerType(), True), \
        StructField('numRuns', IntegerType(), True), \
        StructField('currentRun', IntegerType(), True) \
    
      ])    
    
    mongo_tweetsDF = spark.createDataFrame([(search_words, date_since, numTweets,  numRuns, currentRun)]) \
        .toDF("search_words", "date_since", "numTweets", "numRuns", "currentRun")
    # mongo_tweetsDF.withColumn('date_since', toDate('date_since'))
    mongo_tweetsDF.show(truncate =False)
    
    
    for i in range(1,numRuns+1):
    

	    mongo_tweetsDF = mongo_tweetsDF.withColumn('currentRun', 1 + col('currentRun')) \
            .withColumn('sentiments_scores', scraptweetsUDF('search_words', 'date_since', 'numTweets'))

	    # Cast string column 'date_since' to date column in pyspark
	    mongo_tweetsDF = mongo_tweetsDF.withColumn('date_since', to_date(mongo_tweetsDF.date_since,'yyyy-MM-dd')) \
		.withColumn("positive_avg", mongo_tweetsDF['sentiments_scores'].getItem("positive_avg").cast(DoubleType())) \
		.withColumn("negative_avg", mongo_tweetsDF['sentiments_scores'].getItem("negative_avg").cast(DoubleType())) \
		.withColumn("neutral_avg", mongo_tweetsDF['sentiments_scores'].getItem("neutral_avg").cast(DoubleType())) \
		.drop('sentiments_scores')


	    mongo_tweetsDF.show(truncate =False)
	    
#	    columns = ['_id', 'date_since', 'search_words', 'numTweets', 'numRuns', 'positive_avg', 'negative_avg', 'neutral_avg']

	    #print(f'mongo_tweetsDF Schema=' )
	    #print(mongo_tweetsDF.printSchema())
	    # write modes: overwrite | append | ignore | error or eror 
	    lock.acquire()
	    mongo_tweetsDF.write.format("mongo").option("database", "tweets_scoresDB").option("collection", "tweets_scores").mode("append").save()
	    lock.release()
	    mongoAggregationdf = spark.read.format("mongo").option("database", "tweets_scoresDB").option("collection", "tweets_scores").load()
	    
	    columns = ['_id', 'date_since', 'search_words', 'numTweets', 'numRuns', 'currentRun', 'positive_avg', 'negative_avg', 'neutral_avg']
	    mongoAggregationdf.select(columns).show(100, truncate=False)
	    #print(f'mongoAggregationdf Schema= ')
	    #print(mongoAggregationdf.printSchema())
	    time.sleep(20)
    
    print(' ==============================================================')
    print('success - scrape tweets')
    print(' ==============================================================')
    #spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)	
    #return jsonify({'result': 'success'})


#************************************************************************************************
#   *** Request 1 - getSentimentResults from MongoDB***
#************************************************************************************************
# Using Query Arguments
@app.route('/tweets_sentiment_results',methods=['GET','POST'])
def tweets_sentiment_results():
    # #Accept data from the front end
    test_data = request.get_json()
    print (f'in tweets_sentiment_results func test_data = {test_data}')
    if request.method == 'POST':
        #global p1;
        #global t1; 
        #global q1
        
        #global i
        #i += 1

        # If multiple queues are required, then clients need the ability to create a queue dynamically. 

        # A process can be created by providing a target function and its input arguments to the
        # Process constructor. The process can then be started with the start method and ended
        # using the join method. Below is a very simple example that prints the square of a number.

        # Threading enables more than one work to be done almost simultaneously in the same
        #  process environment. It is one of the parallel programming methods. Thread provides us
        #  convenience by shortening the loading time.
        #p = threading.Thread(target=detachedProcessFunction, args=(spark,test_data))
        #q1 = Queue()
        # p1 = Process(target=detachedProcessFunction, args=(spark,test_data, q1))
        # p1.start()
        # t1 = threading.Thread(target=detachedProcessFunction, args=(spark,test_data), daemon = True, name=f'thread{i}')
        ret = detachedProcessFunction(spark,test_data)
        
        print(f'\n\n\ntweets_sentiment_results - ThreadName ')

        #p1.join()
        #t1.join()
        # Consumer thread 
        ret = jsonify({'result':ret})
        print("**********************************************")
        print(ret)
        print("retretretretret")
        print("**********************************************")

        # return render_template('process1.html')

        # return jsonify({'result':[{'_id': '60b8122bf919ab69a1449d90', 'date_since': '2021-01-01', 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 1, 'positive_avg': 0.0534, 'negative_avg': 0.027399999999999997, 'neutral_avg': 0.9192}]})

        # return jsonify({'result':[{'_id': Row(oid='60b8122bf919ab69a1449d90'), 'date_since': datetime.datetime(2021, 1, 3, 0, 0), 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 1, 'positive_avg': 0.0534, 'negative_avg': 0.027399999999999997, 'neutral_avg': 0.9192}]})

        return ret
        # ==============================================================

        # success - tweets results

        # ==============================================================
        # [{'_id': '60b80dde34573b382e41a1dc', 'date_since': '2021-01-03 00:00:00', 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 2, 'positive_avg': 0.03975, 'negative_avg': 0.01745, 'neutral_avg': 0.9427999999999999}, {'_id': '60b8122bf919ab69a1449d90', 'date_since': '2021-01-03 00:00:00', 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 1, 'positive_avg': 0.0534, 'negative_avg': 0.027399999999999997, 'neutral_avg': 0.9192}, {'_id': '60b8126ff919ab69a1449d95', 'date_since': '2021-01-03 00:00:00', 'search_words': ['astrazeneca', 'pfizer'], 'numTweets': 20, 'numRuns': 2, 'currentRun': 2, 'positive_avg': 0.06445, 'negative_avg': 0.027249999999999996, 'neutral_avg': 0.9083}]
        # **********************************************
        # <Response 707 bytes [200 OK]>
        # **********************************************


    #else:
        #return q1.get() #render_template('process1.html')




#*************************************************************************************************
#   *** Request 2 ***
#*************************************************************************************************

#twitter_sentiment_results
@app.route('/tweets',methods=['POST'])
def twitter_sentiment_results_func():
    tweets_data = request.get_json()
    
    #if request.method == 'POST':
    #global p2
    global t2
    # A process can be created by providing a target function and its input arguments to the
    # Process constructor. The process can then be started with the start method and ended
    # using the join method. Below is a very simple example that prints the square of a number.
    #p2= threading.Thread(target=detachedProcessFunction3, args=(spark,tweets_data))
    # p2= Process(target=detachedProcessFunction3, args=(spark,tweets_data))
    # p2.start()
    # p2.join()

    # t2= threading.Thread(target=detachedProcessFunction3, args=(spark,tweets_data), daemon = True)
    t2= threading.Thread(target=detachedProcessFunction3, args=(spark,tweets_data))
    t2.start()
    # try:
    #     t2.join()
    # except Exception:
    #     pass
    # return render_template('process2.html')
    return {"message": "Accepted"}, 202


if __name__ == "__main__":

    #import multiprocessing
    
    #multiprocessing.set_start_method('spawn')
    app.run(host = '0.0.0.0', port = 5001, threaded = True)


