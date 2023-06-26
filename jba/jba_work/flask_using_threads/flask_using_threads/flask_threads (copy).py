import os
import sys


from flask import Flask,render_template,  jsonify, request
from multiprocessing import Process
from multiprocessing.pool import ThreadPool
import time
import threading

import joblib

os.environ['PYSPARK_SUBMIT_ARGS'] = \
'--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0  pyspark-shell'

import findspark

findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf 
from pyspark.sql.functions import pandas_udf, PandasUDFType

# import pyspark.sql.types as Types
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType, StructField, StructType, Row
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.types import DateType, DoubleType, IntegerType, MapType
from pyspark.sql.functions import col , to_timestamp, to_date

from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Tweepy, a package that provides a very convenient way to use the Twitter API
import tweepy
# import json
# import csv

# from datetime import date
# from datetime import datetime
import time

# import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import databricks.koalas as ks
import pandas as pd


import configparser


import requests
from threading import Timer
from flask import request
import time

os.environ["PYSPARK_PIN_THREAD"] = "true"

#sc = SparkContext()
#sqlContx = SQLContext(sc)



# https://www.cloudera.com/documentation/enterprise/5-8-x/topics/spark_develop_run.html
def wordCount(spark, file_a, pool_a):
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_a)
    file = spark.sparkContext.textFile("inputfiles/" + file_a).flatMap(lambda line: line.split(" ")).cache()
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).toDF(["Word","Count"])
    wordCounts.write.mode("overwrite").parquet("outFiles/wordcount")
    #f= [n for n in globals() if not n.startswith('__')]
    #print(f)
    
    #print(locals())
    #print(spark.sparkContext.getConf().getAll())
    
    #// must return "FAIR"
    print("\n", f"spark.scheduler.mode: {spark.conf.get('spark.scheduler.mode')}", "\n")
    
    print("\n", f"spark.scheduler.pool: {spark.sparkContext.getLocalProperty('spark.scheduler.pool')}", "\n")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
    
def charCount(spark, file_b, pool_b):
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_b)
    file = spark.sparkContext.textFile("inputfiles/" + file_b).flatMap(lambda line: line.split(" ")).cache()
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
    charCounts = wordCounts.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2).toDF()
    charCounts.write.mode("overwrite").parquet("outFiles/charcount")
        #// must return "FAIR"
    print("\n", f"spark.scheduler.mode: {spark.conf.get('spark.scheduler.mode')}", "\n")
    
    print("\n", f"spark.scheduler.pool: {spark.sparkContext.getLocalProperty('spark.scheduler.pool')}", "\n")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)



    
#twitter_tokens = 'twitter.properties'
#api = set_api(twitter_tokens)

#nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()


#*************************************************************************************************
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#*************************************************************************************************

app = Flask(__name__)

print("^"*50)
print(f"app: {__name__}")
print("^"*50)

def start_spark():
    global spark
    spark  = SparkSession \
        .builder \
        .appName('spark_threads') \
        .master("local[2]") \
        .config("spark.mongodb.input.uri", 
        "mongodb://ubuntu/test.myCollection?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://ubuntu/test.myCollection") \
        .config('spark.jars.packages', 
        'org.mongodb.spark:mongo-spark-connector_2.11:3.0.1') \
        .config("spark.scheduler.mode","FAIR") \
        .config("spark.scheduler.allocation.file", "/opt/spark/conf/fairscheduler.xml") \
        .getOrCreate()

    print("sparkSession started")



def set_api(twitter_tokens):
    # twitter_tokens = 'twitter.properties'
    # authorization tokens

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

# CALL set_api()
twitter_tokens = 'twitter.properties'
api = set_api(twitter_tokens)
    #return spark


# Registers a function to be run before the first request to this instance of the application.
# The function will be called **without any arguments and its return value is ignored**.
app.before_first_request(start_spark)


# Loading input file and breaking into words separated by space.
#file = sc.textFile("inputfiles/shakespeare.txt").flatMap(lambda line: line.split(" ")).cache()

#spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)

#*************************************************************************************************
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

    # You will use the .Cursor method to get an object containing tweets containing the hashtag #search_words.
    # You can restrict the number of tweets returned by specifying a number in the .items() method.
    # https://developer.twitter.com/en/docs/pagination#:~:text=The%20Twitter%20standard%20REST%20APIs%20utilize%20a%20technique,to%20move%20backwards%20and%20forwards%20through%20these%20pages.
    # The Twitter standard REST APIs utilize a technique called ‘cursoring’ to paginate large result sets. 
    # Cursoring separates results into pages (the size of which are defined by the count request parameter) 
    # and provides a means to move backwards and forwards through these pages.            
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

        cnt = cnt + 1;
        try:
            text = tweet.retweeted_status.full_text
        except AttributeError:  # Not a Retweet
            text = tweet.full_text
        #print(text)

        r = sid.polarity_scores(text);
#       print(f"text = {text}, r = {r}")
#                 positive_avg = (positive_avg + r['pos'])/cnt;
#                 negative_avg = (negative_avg + r['neg'])/cnt;
#                 neutral_avg = (neutral_avg + r['neu'])/cnt;
        positive_avg = (positive_avg + r['pos'])
        negative_avg = (negative_avg + r['neg'])
        neutral_avg = (neutral_avg + r['neu'])

#    print('*'*80)
#    print(f'cnt = {cnt}')
#    print(f'numRun = {i}')

#     print(db_tweets)
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


def detachedProcessFunction(spark, test_data):
    #test_data = request.get_json()
    print("#"*40)
    print (request.is_json)
    print(test_data)
    print(type(test_data))
    # {"search_words": [astrazeneca, pfizer],
    # "date_since":''},
    # "numTweets": 20,
    # "numRuns": 2
    # }
    

    
    search_words = test_data["search_words"]
    date_since = test_data["date_since"]
    # numTweets = test_data["numTweets"]
    # numRuns = test_data["numRuns"]
    # currentRun = 0
    
    
    print(f"search_words={search_words}")
    print(f"date_since={date_since}")    
    # print(f"numTweets={numTweets}")
    # print(f"numRuns={numRuns}")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")
    from pyspark.sql.functions import col, factorial, log, reverse, sqrt

    #spark = start_spark()

    pipeline = str({'$match': {'search_words':search_words, 'date_since':date_since}})
    print("pipeline : ", pipeline)

    # mongoAggregationdf = spark.read.format("mongo").option("database", "factorials").option("collection", "numbers").option("pipeline", pipeline).load()

    # mongoAggregationdf = mongoAggregationdf.withColumn("factorial", factorial(col("number")))
    
    # mongoAggregationdf.show()

    mongoAggregationdf = spark.read.format("mongo").option("database", "tweets_scoresDB").option("collection", "tweets_scores").load()
    
    columns = ['_id', 'date_since', 'search_words', 'numTweets', 'numRuns', 'currentRun', 'positive_avg', 'negative_avg', 'neutral_avg']
    mongoAggregationdf.select(columns).show(truncate=False)
    print(f'mongoAggregationdf Schema= ')
    print(mongoAggregationdf.printSchema())


    output = {'_id':mongoAggregationdf.select("_id").collect()[0][0],
    'date_since':mongoAggregationdf.select("date_since").collect()[0][0],
    'search_words':mongoAggregationdf.select("search_words").collect()[0][0],
    'numTweets':mongoAggregationdf.select("numTweets").collect()[0][0],
    'numRuns':mongoAggregationdf.select("numRuns").collect()[0][0],
    'currentRun':mongoAggregationdf.select("currentRun").collect()[0][0],
    'positive_avg':mongoAggregationdf.select("positive_avg").collect()[0][0],
    'negative_avg':mongoAggregationdf.select("negative_avg").collect()[0][0],
    'neutral_avg':mongoAggregationdf.select("neutral_avg").collect()[0][0]}

    print('success')
    #spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)

    return jsonify(output)
    # return jsonify({'result': 'success'})

#*************************************************************************************************
def detachedProcessFunction3(spark, tweets_data):
    #test_data = request.get_json()
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")    
    from pyspark.sql.functions import col, factorial, log, reverse, sqrt
    print("&"*40)
    #spark = start_spark()
    
    
    print (request.is_json)
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

	    print(f'mongo_tweetsDF Schema=' )
	    print(mongo_tweetsDF.printSchema())
	    
	    mongo_tweetsDF.write.format("mongo").option("database", "tweets_scoresDB").option("collection", "tweets_scores").mode('overwrite').save()
	    
	    mongoAggregationdf = spark.read.format("mongo").option("database", "tweets_scoresDB").option("collection", "tweets_scores").load()
	    
	    columns = ['_id', 'date_since', 'search_words', 'numTweets', 'numRuns', 'currentRun', 'positive_avg', 'negative_avg', 'neutral_avg']
	    mongoAggregationdf.select(columns).show(truncate=False)
	    print(f'mongoAggregationdf Schema= ')
	    print(mongoAggregationdf.printSchema())
	    # time.sleep(20)
    
    print('success')
    #spark.stop()
    #spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)	
    return jsonify({'result': 'success'})

def square(x):
    time.sleep(1)
    print(x * x)
    i=0
    while i<x:
        i = i+1
        print (f" i = {i} , i^2 = {i*i}")
        time.sleep(1)

#*************************************************************************************************
#   *** Request 2 - getSentimentResults from MongoDB***
#*************************************************************************************************
# Using Query Arguments
@app.route('/tweets_sentiment_results',methods=['GET','POST'])
def tweets_sentiment_results():
    test_data = request.get_json()

#*************************************************************************************************

    if request.method == 'POST':
        global p
        # A process can be created by providing a target function and its input arguments to the
        # Process constructor. The process can then be started with the start method and ended
        # using the join method. Below is a very simple example that prints the square of a number.
        #p = threading.Thread(target=detachedProcessFunction, args=(spark,test_data))
        p = Process(target=detachedProcessFunction, args=(spark,test_data))
        p.start()
        p.join()
        return render_template('process2.html')
    else:
        return render_template('process2.html')




#*************************************************************************************************
#   *** Request 3 ***
#*************************************************************************************************

#twitter_sentiment_results
@app.route('/tweets',methods=['POST'])
def twitter_sentiment_results_func():
    tweets_data = request.get_json()
    
    #if request.method == 'POST':
    global p3
    # A process can be created by providing a target function and its input arguments to the
    # Process constructor. The process can then be started with the start method and ended
    # using the join method. Below is a very simple example that prints the square of a number.
    #p3= threading.Thread(target=detachedProcessFunction3, args=(spark,tweets_data))
    p3= Process(target=detachedProcessFunction3, args=(spark,tweets_data))
    p3.start()
    p3.join()
    return render_template('process1.html')

    


#*************************************************************************************************
#   *** Request 4 ***
#*************************************************************************************************

@app.route('/compute_square', methods=['GET', 'POST'])
def compute_square():
    if request.method == 'POST':
        global p
        # if key doesn't exist, returns None
        x = request.args.get('x')

        # if key doesn't exist, returns a 400, bad request error
        # factorial_of_n = request.args['factorial']

        print(x)
        # '20'
        print(type(x))

        p = Process(target=square, args=(int(x),))
        p.start()
        return render_template('process2.html')
    else:
        return render_template('process2.html')
    # p.join()

    
    


#sc = spark.sparkContext
    
#spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production1")
# Instanciando variáveis threads com funções na memória.
# Instantiating thread variables with functions in memory.
#file_a = "shakespeare.txt"
#file_b = "big.txt"
#pool_a = "production1"
#pool_b = "production1"
#T1 = threading.Thread(target=wordCount, args=(spark, file_a, pool_a))
#T1.start()

#spark.sparkContext.setLocalProperty("spark.scheduler.pool", "production2")
#We can also pass the SparkSession object to multiple threads so that each thread can create
# their own spark jobs.
#T2 = threading.Thread(target=charCount, args=(spark,file_b, pool_b))


# Starting execution of threads.
#T1.start()
#T2.start()

# Pausing thread execution to follow main stream. Without them, the thread will run in parallel.
#T1.join()
#T2.join()
