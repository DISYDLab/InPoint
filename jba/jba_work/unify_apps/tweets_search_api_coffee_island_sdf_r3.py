# # Get Tweets
# 
# This script extracts all the tweets with hashtag #covid-19 related to the day before today (yesterday) and saves them into a .csv file.
# We use the `tweepy` library, which can be installed with the command `pip install tweepy`.
# 
# Firstly, we import the configuration file, called `config.py`, which is located in the same directory of this script.

import typing
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

spark=(SparkSession
        .builder \
        .appName("tweets_search_api_coffee_island_sdf_Yarn_Client") \
        .getOrCreate()
        )

print(spark)
# ------------------------------------------------------------------------------------------------------------
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")
spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled")
# ------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------
from config import *
import tweepy
import datetime

import sys
import logging
logger = logging.getLogger('tweets_search')

import pandas as pd
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

print(f"logger.root.level = {logger.root.level}, logger.root.name = {logger.root.name}")
print(f"logger.name = {logger.name}")

format = "%(asctime)s - %(levelname)s - %(message)s"
# logging.basicConfig(format=format, stream=sys.stdout, level = logging.DEBUG)
logging.basicConfig(format=format, stream=sys.stdout, level = logging.INFO)

print(logger.root.level)

# ------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------
# We setup the connection to our Twitter App by using the `OAuthHandler()` class and its `access_token()` function.
# Then we call the Twitter API through the `API()` function.

auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify = True)

# api.me()
# api.rate_limit_status()

# ## setup dates (recent 7 days max)
# ```
# pdf['created_at'].dt.strftime('%Y-%m-%d')
# - provide since and until dates (date_format = 'Y-m-d', i.e '2021-01-30')
# since:'', until:'', or
# - provide timedelta (until= datetime.date.today(), since= datetime.date.today()-timedelta), i.e timedelta: '2', 
# or
# ------------------------------------------------------------------------------------------------------------
# - setup
# ------------------------------------------------------------------------------------------------------------
# Now we setup dates. We need to setup today
# ```
# ## setup dates (recent 7 days max)
# If today is 2021-06-26 then :
# 
# 1. `time_frame = {timedelta:'2'}` (we get tweets from 2021-06-24 up to 2021-06-25 (today - 1 day))
# 2. `time_frame = {since:'2021-06-23', timedelta:'2'}` 
# 3. `time_frame = {until:'2021-06-25', timedelta:'2'}` (2 & 3 & 4 expressions are equivalent)
# 4. `time_frame = {since:'2021-06-23', until:'2021-06-25'}` -> we get tweets from 2021-06-23 up to 2021-06-24
# 
# `note:` from today we can get a time_frame of 7 days max, i.e since 2021-06-19
# ------------------------------------------------------------------------------------------------------------
# https://www.educative.io/edpresso/how-to-convert-a-string-to-a-date-in-python
import datetime
# since = '2021-01-30'
# datetime.datetime.strptime(since, '%Y-%m-%d').date()

today = datetime.date.today()
since= today - datetime.timedelta(days=2)
until= today
until, since
# (datetime.date(2021, 6, 7), datetime.date(2021, 6, 6))

logger.debug(f"time_frame: '{until, since}'")

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
#-----------
# # example
#-----------
# code tweets = tweepy.Cursor(api.search, tweet_mode=’extended’) 
# for tweet in tweets:
#     content = tweet.full_text

# tweets_list = tweepy.Cursor(api.search, q="#Covid-19 since:" + str(yesterday)+ " until:" + str(today),tweet_mode='extended', lang='en').items()
# tweets_list = tweepy.Cursor(api.search, q=f"#Covid-19 since:{str(yesterday)} until:{str(today)}",tweet_mode='extended', lang='en').items()
# tweets_list = tweepy.Cursor(api.search, q=['astrazeneca', 'pfizer'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()
#--------------------
# Greek Language = el
#--------------------
# tweets_list = tweepy.Cursor(api.search, q=['coffee island'],since= str(since), until=str(until),tweet_mode='extended', lang='el').items()
#--------------------
# English Language = en
#--------------------
tweets_list = (tweepy
                .Cursor(api.search, 
                        q=['coffee island OR CoffeeIsland'],
                        since= str(since), until=str(until),
                        tweet_mode='extended',
                        lang='en').items()
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
    #print(text) 
    # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets           

    logger.debug(f"full_text: '{text}'")
    favourite_count = tweet.favorite_count
    retweet_count = tweet.retweet_count
    created_at = tweet.created_at
    
    line = {'text' : text, 'favourite_count' : favourite_count, 'retweet_count' : retweet_count, 'created_at' : created_at}
    output.append(line)
    logger.info(f"Append list length : { len(output)}")
end = time.time()
logger.info(f"elapsed_time: '{end - start}'")
print(output[:3])
print(len(output))
# ---------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------
def check_tweets_list(potential_full: list) -> bool:
    if not potential_full:
        #Try to convert argument into a float
        print("list is  empty")
        return False
    else:
        print("list is  not empty")
        return True

output_list_not_empty = check_tweets_list(output)
print (output_list_not_empty)
# ---------------------------------------------------------------------------------------------------------
# sc = spark.sparkContext
# print(sc.getConf().getAll())
# ---------------------------------------------------------------------------------------------------------
# ### create sdf from list
# ---------------------------------------------------------------------------------------------------------
# Finally, we convert the `output` list to a `spark DataFrame` and we store results.
print('create sdf from list')
sdf = spark.createDataFrame([Row(**i) for i in output])
sdf.show(2, truncate = 30)
# {"fields":[{"metadata":{},"name":"created_at","nullable":true,"type":"timestamp"},{"metadata":{},"name":"favourite_count","nullable":true,"type":"long"},{"metadata":{},"name":"retweet_count","nullable":true,"type":"long"},{"metadata":{},"name":"text","nullable":true,"type":"string"}],"type":"struct"}

print(f'({sdf.count(), sdf.columns})')
# ((189, ['text', 'favourite_count', 'retweet_count', 'created_at']))  

# Selecting specific columns in a spark dataframe
# sdf = sdf.select('text', 'favourite_count', 'retweet_count', 'created_at')
# print(sdf.limit(3).toPandas())

# Extract First N rows in pyspark – Top N rows in pyspark using show() function
sdf.show(3, truncate=30)
# +------------------------------+---------------+-------------+-------------------+
# |                          text|favourite_count|retweet_count|         created_at|
# +------------------------------+---------------+-------------+-------------------+
# |Afternoon at the Eighteenth...|              0|           67|2021-07-09 23:57:55|
# |currently sitting at Philz ...|              2|            1|2021-07-09 23:52:21|
# |@StopandShop can you please...|              0|            0|2021-07-09 23:11:40|
# +------------------------------+---------------+-------------+-------------------+

from pyspark.sql.functions import current_date, current_timestamp
# data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]
# df2=spark.createDataFrame(data,["id","input"])
# df2.show(truncate=False)
# df2.withColumn("current_date",current_date()).withColumn("current_timestamp",current_timestamp()).show(truncate=False)
# df2.select(current_timestamp().alias("current_timestamp")
#   ).show(truncate=False)
# -----------------------------------------------------------------------------------------------------
# ### save and read sdf without header
# -----------------------------------------------------------------------------------------------------
#  Spark DataFrameWriter class provides a method csv() to save or write a DataFrame at a specified path on disk, this method takes a file path where you wanted to write a file and by default, it doesn’t write a header or column names.
# ----------------------
#  #### overwrite mode
# ----------------------
print('write to csv file in overwrite mode')
sdf.write.format('csv').mode('overwrite') \
    .save('/user/hadoopuser/export_datasets/output_cof_island_sdf.csv', escape = '"')

# ----------------------
# ### append mode
# ----------------------
# sdf.write.csv('output_cof_island_sdf.csv', mode = 'append')

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, TimestampType
# ['text','favourite_count','retweet_count','created_at']
# ------------------------------------------------------------------
# DateType default format is yyyy-MM-dd 
# TimestampType default format is yyyy-MM-dd HH:mm:ss.SSSS
# ------------------------------------------------------------------
schema = StructType([
    StructField("text", StringType(), True),
    StructField("favourite_count", IntegerType(), True),
    StructField("retweet_count", IntegerType(), True),
    StructField("created_at", TimestampType(), True)
    ])

print('read csv file')
sdf_2 = spark.read.csv('/user/hadoopuser/export_datasets/output_cof_island_sdf.csv',schema=schema, header=False,escape = '"', multiLine=True) #, escape = '"',

sdf.show(2, truncate =30)
# +------------------------------+---------------+-------------+-------------------+
# |                          text|favourite_count|retweet_count|         created_at|
# +------------------------------+---------------+-------------+-------------------+
# |Afternoon at the Eighteenth...|              0|           67|2021-07-09 23:57:55|
# |currently sitting at Philz ...|              2|            1|2021-07-09 23:52:21|
# +------------------------------+---------------+-------------+-------------------+

# print(sdf_2.limit(2).toPandas())

# sdf_2 = sdf_2.sort(F.col('created_at'), ascending=True)
sdf_2 = sdf_2.sort(F.col('created_at').asc())

sdf_2.show(2,truncate=30)
# +------------------------------+---------------+-------------+-------------------+
# |                          text|favourite_count|retweet_count|         created_at|
# +------------------------------+---------------+-------------+-------------------+
# |If a car leaves Boston goin...|              0|            1|2021-07-08 00:47:29|
# |Black Coffee               ...|              0|           36|2021-07-08 01:00:20|
# +------------------------------+---------------+-------------+-------------------+

# print(sdf_2.limit(2).toPandas())
# sdf_2 = sdf_2.sort(F.col('created_at').desc())
# sdf_2 = sdf_2.sort(F.col('created_at'), ascending=False)
# sdf_2.show(2,truncate=30)

# Get First N rows in pyspark – Top N rows in pyspark using head()
# function – (First 10 rows)
# sdf_2.head(2)

# Extract First row of dataframe in pyspark – using first() function
# sdf_2.first()

# from pyspark.sql.functions import col
# sdf_2.select(col("created_at")).first()[0]
# sdf_2.limit(2).toPandas()

# sdf_2.sort(F.col('created_at'), ascending=True).limit(2).toPandas()
# sdf_2.toPandas().tail(2)

print(f'({sdf_2.count(), len(sdf_2.columns)})') # ((189, 4))
# ----------------------------------------------------------------------------------------------------
# ### save a sdf to a csv file with header
# ----------------------------------------------------------------------------------------------------
# #### The core syntax for writing data in Apache Spark
# 
# DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy( ...).save()

# get_ipython().run_line_magic('ls', 'output_cof_island_sdf.csv')

#df = pd.DataFrame(output)
# header: str or bool, optional writes the names of columns as the first line. If None
#  is set, it uses the default value, false.

# DataFrameWriter.csv(path, mode=None, compression=None, sep=None, quote=None, escape=None, header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None, timestampFormat=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None, charToEscapeQuoteEscaping=None, encoding=None, emptyValue=None, lineSep=None)
# .option("timestampFormat", "MM-dd-yyyy hh mm ss")
# sdf_2.write.csv('output_cof_island_sdf.csv', header = 'true', mode='overwrite')

# You can also use below
sdf_2.write.format("csv").mode("overwrite").options(header="true", escape = '"') \
    .save("/user/hadoopuser/export_datasets/output_cof_island_sdf_3.csv")
# ----------------------------------------------------------------------------------------------------
# #### Create a sdf from a csv file with header
# ----------------------------------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType, TimestampType
schema = StructType([
    StructField("text", StringType()),
    StructField("favourite_count", IntegerType()),
    StructField("retweet_count", IntegerType()),
    StructField("created_at", TimestampType())
])
# https://datascience.stackexchange.com/questions/12727/reading-csvs-with-new-lines-in-fields-with-spark
sdf_2 = spark.read.csv('/user/hadoopuser/export_datasets/output_cof_island_sdf_3.csv',schema=schema,header=True,  escape = '"',multiLine=True) #escape = '"',

# type(sdf_2)
# pyspark.sql.dataframe.DataFrame
print(sdf_2.printSchema())
# root                                                                            
#  |-- text: string (nullable = true)
#  |-- favourite_count: integer (nullable = true)
#  |-- retweet_count: integer (nullable = true)
#  |-- created_at: timestamp (nullable

print(sdf_2.count() ) # 189
print(sdf_2.columns) # ['text', 'favourite_count', 'retweet_count', 'created_at']
print(len(sdf_2.columns))# 4

# --------------------------------------------------------------
# ---VADER -----------------------------------------------------
# ### def sentiment_scores & sentiment_scoresUDF
# ---VADER -----------------------------------------------------
import sys
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,StructField, StructType, Row
from pyspark.sql.functions import col, udf, pandas_udf, PandasUDFType
import pyspark.sql.types as Types

print('VADER')
def sentiment_scores(sentance: str) -> dict:
    # Create a SentimentIntensityAnalyzer object.
    sid = SentimentIntensityAnalyzer('file:///home/hadoopuser/nltk_data/sentiment/vader_lexicon.zip/vader_lexicon/vader_lexicon.txt')
    # polarity_scores method of SentimentIntensityAnalyzer
    # oject gives a sentiment dictionary.
    # which contains pos, neg, neu, and compound scores.
    r = sid.polarity_scores(sentance)
    return r
    # You can optionally set the return type of your UDF. The default return type␣,→is StringType.
    # udffactorial_p = udf(factorial_p, LongType())

sentiment_scoresUDF = udf(sentiment_scores, Types.MapType(Types.StringType(),Types.DoubleType()))

# --------------------------------------------------------------
# ### sdf
# --------------------------------------------------------------
# create a new column with sentiment_scores

sdf_2 = sdf_2.withColumn("rating", sentiment_scoresUDF(sdf_2.text))
sdf_2.show(2, truncate=30)
# +------------------------------+---------------+-------------+-------------------+------------------------------+
# |                          text|favourite_count|retweet_count|         created_at|                        rating|
# +------------------------------+---------------+-------------+-------------------+------------------------------+
# |@top10traveler @Trinifoodto...|              6|            0|2021-07-08 23:09:28|{neg -> 0.0, pos -> 0.051, ...|
# |@FlufffyMonkey The only isl...|              1|            0|2021-07-08 23:44:17|{neg -> 0.0, pos -> 0.0, co...|
# +------------------------------+---------------+-------------+-------------------+------------------------------+
sdf_2 = sdf_2.withColumn('negative_nltk', col('rating')['neg']) .withColumn('positive_nltk', col('rating')['pos']) .withColumn('neutral_nltk', col('rating')['neu']) .withColumn('compound_nltk',col('rating')['compound']) .drop('rating')
sdf_2.show(2, truncate=30)
# +------------------------------+---------------+-------------+-------------------+-------------+-------------+------------+-------------+
# |                          text|favourite_count|retweet_count|         created_at|negative_nltk|positive_nltk|neutral_nltk|compound_nltk|
# +------------------------------+---------------+-------------+-------------------+-------------+-------------+------------+-------------+
# |@top10traveler @Trinifoodto...|              6|            0|2021-07-08 23:09:28|          0.0|        0.051|       0.949|       0.6249|
# |@FlufffyMonkey The only isl...|              1|            0|2021-07-08 23:44:17|          0.0|          0.0|         1.0|          0.0|
# +------------------------------+---------------+-------------+-------------------+-------------+-------------+------------+-------------+

# PYSPARK TOKENIZER 
from pyspark.ml.feature import Tokenizer, StopWordsRemover

# use PySparks build in tokenizer to tokenize tweets
tokenizer = Tokenizer(inputCol  = "text",
                      outputCol = "token")

sdf_2_token = tokenizer.transform(sdf_2)
sdf_2_token.select('text', 'token').show(2, truncate = 50)





# https://stackoverflow.com/questions/61608057/output-vader-sentiment-scores-in-columns-based-on-dataframe-rows-of-tweets

# --------------------------------------------------------------
# ### Spark GroupBy and Aggregate Functions
# --------------------------------------------------------------
# `GroupBy` allows you to group rows together based off some column value, for example, you could group together sales
# data by the day the sale occured, or group repeast customer data based off the name of the customer.
# Once you've performed the GroupBy operation you can use an aggregate function off that data.An `aggregate function`
# aggregates multiple rows of data into a single output, such as taking the sum of inputs, or counting the number of inputs.

 # --------------------------------------------------------------
 # ### **`Dataframe Aggregation`**
 # --------------------------------------------------------------
# A set of methods for aggregations on a DataFrame:
#     agg
#     avg
#     count
#     max
#     mean
#     min
#     pivot
#     sum
# 
# https://hendra-herviawan.github.io/pyspark-groupby-and-aggregate-functions.html
print(sdf_2.columns)
# ['text', 'favourite_count', 'retweet_count', 'created_at', 'negative_nltk', 'positive_nltk', 'neutral_nltk', 'compound_nltk']

# sdf_2.select('created_at')

# ```
# sdf_agg_byDate =  sdf.groupBy('created_at').agg({'negative_nltk':'sum'}, {'positive_nltk':'sum'}, {'neutral_nltk':'sum'},	{'compound_nltk':'sum'})
# import pyspark.sql.functions as f
# from pyspark.sql.functions import col
# 
# sdf_agg_byDate =  sdf.groupBy('created_at').agg({'negative_nltk':'sum','positive_nltk':'sum','neutral_nltk':'sum','compound_nltk':'sum','created_at':'count'})
# 
# sdf_agg_byDate.count()
# ```
# sdf.groupBy('created_at').count().select('created_at', f.col('count').alias('tweets')).show()

# ### Convert Timestamp to Date
# 
# > Syntax: to_date(timestamp_column)
# 
# > Syntax: to_date(timestamp_column,format)
# 
# PySpark timestamp (TimestampType) consists of value in the format yyyy-MM-dd HH:mm:ss.SSSS and Date (DateType) format would be yyyy-MM-dd. Use to_date() function to truncate time from Timestamp or to convert the timestamp to date on DataFrame column.
 # --------------------------------------------------------------
# ### timestamp to date
sdf_2 = sdf_2.withColumn('created_at', F.to_date(F.col('created_at')))
sdf_2.show(2, truncate=20)
# +--------------------+---------------+-------------+----------+-------------+-------------+------------+-------------+
# |                text|favourite_count|retweet_count|created_at|negative_nltk|positive_nltk|neutral_nltk|compound_nltk|
# +--------------------+---------------+-------------+----------+-------------+-------------+------------+-------------+
# |@top10traveler @T...|              6|            0|2021-07-08|          0.0|        0.051|       0.949|       0.6249|
# |@FlufffyMonkey Th...|              1|            0|2021-07-08|          0.0|          0.0|         1.0|          0.0|
# +--------------------+---------------+-------------+----------+-------------+-------------+------------+-------------+

 # --------------------------------------------------------------
print(sdf_2.dtypes)
# [('text', 'string'), ('favourite_count', 'int'), ('retweet_count', 'int'), ('created_at', 'date'), ('negative_nltk', 'double'), ('positive_nltk', 'double'), ('neutral_nltk', 'double'), ('compound_nltk', 'double')]

# sdf_agg_byDate.show()
# sdf_agg_byDate = sdf_agg_byDate.withColumn("sum", col("sum(negative_nltk)")+col("science_score"))
# 	df1.show()
# --------------------------------------------------------------
# ### groupBy and aggregate on multiple columns
# --------------------------------------------------------------
exprs={}
cols = ['created_at',
 'negative_nltk',
 'positive_nltk',
 'neutral_nltk',
 'compound_nltk']
exprs = {x: "sum" for x in cols}
exprs['created_at'] = 'count'
print(exprs) # {'created_at': 'count', 'negative_nltk': 'sum', 'positive_nltk': 'sum', 'neutral_nltk': 'sum', 'compound_nltk': 'sum'}

# sdf_agg_byDate = sdf.groupBy('created_at').agg({'negative_nltk':'sum'}, {'positive_nltk':'sum'}, {'neutral_nltk':'sum'}, {'compound_nltk':'sum'})
# sdf_agg_byDate = sdf_2.groupBy('created_at').agg({'negative_nltk':'sum','positive_nltk':'sum','neutral_nltk':'sum','compound_nltk':'sum','created_at':'count'}).withColumnRenamed('count(created_at)', 'tweets')
sdf_agg_byDate = sdf_2.groupBy('created_at').agg(exprs).withColumnRenamed('count(created_at)', 'tweets')
sdf_agg_byDate.show(2, truncate=30)


# sdf.groupBy("department","state") \
#     .sum("salary","bonus") \
#     .show(false)
# sdf_2.groupBy('created_at').agg(exprs).withColumnRenamed('count(created_at)', 'tweets').limit(1).toPandas()

# How to delete columns in pyspark dataframe
columns_to_drop = ['sum(compound_nltk)', 'sum(positive_nltk)', 'sum(negative_nltk)', 'sum(neutral_nltk)']

sdf_agg_byDate = (sdf_agg_byDate
    .withColumn('compound_nltk', F.col('sum(compound_nltk)')/F.col('tweets'))
    .withColumn( 'positive_nltk',F.col('sum(positive_nltk)')/F.col('tweets'))
    .withColumn( 'negative_nltk',F.col('sum(negative_nltk)')/F.col('tweets'))
    .withColumn( 'neutral_nltk',F.col('sum(neutral_nltk)')/F.col('tweets'))
    .drop(*columns_to_drop)
    )

# print(sdf_agg_byDate.limit(1).toPandas())
sdf_agg_byDate.show(2, truncate=30)
# +----------+------+-------------------+-------------------+--------------------+------------------+
# |created_at|tweets|      compound_nltk|      positive_nltk|       negativen_ltk|      neutral_nltk|
# +----------+------+-------------------+-------------------+--------------------+------------------+
# |2021-07-08|   115|0.29746347826086955|0.10959999999999999|0.022139130434782606|0.8682521739130434|
# |2021-07-09|    74| 0.3649783783783784| 0.1279324324324324|0.017108108108108106|0.8549729729729729|
# +----------+------+-------------------+-------------------+--------------------+------------------+

# .drop(*columns_to_drop)
# print(sdf_agg_byDate.created_at[0])
# Column<'created_at[0]'>
# The below statement changes column 'count(created_at)' to 'tweets' on PySpark DataFrame.
# sdf_agg_byDate = (sdf_agg_byDate
#     .withColumnRenamed('count(created_at)', 'tweets'))
# DataFrame.sort(*cols, **kwargs) - Returns a new DataFrame sorted by the specified column(s).
sdf_agg_byDate = sdf_agg_byDate.sort('created_at')

sdf_agg_byDate.show(2, truncate=30)
print(sdf_agg_byDate.dtypes)

# +----------+------+-------------------+------------------+--------------------+------------------+
# |created_at|tweets|      compound_nltk|     positive_nltk|       negativen_ltk|      neutral_nltk|
# +----------+------+-------------------+------------------+--------------------+------------------+
# |2021-07-08|   115|0.29746347826086955|            0.1096|0.022139130434782613|0.8682521739130435|
# |2021-07-09|    74| 0.3649783783783784|0.1279324324324324| 0.01710810810810811| 0.854972972972973|
# +----------+------+-------------------+------------------+--------------------+------------------+

# [('created_at', 'date'), ('tweets', 'bigint'), ('compound_nltk', 'double'), ('positive_nltk', 'double'), ('negativen_ltk', 'double'), ('neutral_nltk', 'double')]
# ====================================================================================================================================
# last, head, tail
# sdf_agg_byDate.first()

# since, until

# ### to_date() – Convert String to Date Format
# 
# Syntax: to_date(column,format)
# 
# Example: to_date(col("string_column"),"MM-dd-yyyy")

# from pyspark.sql.functions import lit
# dates = ("2021-06-26",  "2021-06-27")
# date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]
# (date_from, date_to)

# from pyspark.sql.functions import lit
# dates = ("2021-06-26",  "2021-06-27")
# date_from, date_to = [to_date(lit(s)) for s in dates]
# (date_from, date_to)

# import datetime, time
# dates = ("2021-06-26 00:00:00",  "2021-06-27 00:00:00")
# # date_from, date_to = ("2021-06-26",  "2021-06-27")

# timestamps = (
#     time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())
#     for s in dates)
days=1
today = datetime.date.today()
date_from= today - datetime.timedelta(days=2)
date_to= today-datetime.timedelta(days=days)

from pyspark.sql.functions import to_date
sdf_agg_byDate.filter(col('created_at') > date_from).show()
# +----------+------+-------------------+-------------------+-------------------+-----------------+
# |created_at|tweets|      compound_nltk|      positive_nltk|      negativen_ltk|     neutral_nltk|
# +----------+------+-------------------+-------------------+-------------------+-----------------+
# |2021-07-09|    74|0.36497837837837843|0.12793243243243238|0.01710810810810811|0.854972972972973|
# +----------+------+-------------------+-------------------+-------------------+-----------------+
sdf_agg_byDate.filter(col('created_at') >= date_from).show()

# +----------+------+------------------+-------------------+--------------------+------------------+
# |created_at|tweets|     compound_nltk|      positive_nltk|       negativen_ltk|      neutral_nltk|
# +----------+------+------------------+-------------------+--------------------+------------------+
# |2021-07-08|   115|0.2974634782608695|0.10959999999999999|0.022139130434782613|0.8682521739130434|
# |2021-07-09|    74|0.3649783783783783| 0.1279324324324324| 0.01710810810810811| 0.854972972972973|
# +----------+------+------------------+-------------------+--------------------+------------------+
# `sf = sf.filter(sf.my_col >= date_from).filter(sf.my_col <= date_to)
# sf.count()`
# 
# https://stackoverflow.com/questions/31407461/datetime-range-filter-in-pyspark-sql
# 
# .alias('sentiment_eval')
# https://sparkbyexamples.com/pyspark/pyspark-difference-between-two-dates-days-months-years/

print(sdf_agg_byDate.filter(sdf_agg_byDate['created_at'] >= date_from).filter(sdf_agg_byDate['created_at'] <= date_to).count()) #2
print(sdf_agg_byDate.select('*', sdf_agg_byDate['created_at'].between(date_from, date_to)).count()) #2
print(sdf_agg_byDate.select('*', sdf_agg_byDate['created_at'].between(date_from, date_to)).toPandas())

#    created_at  tweets  compound_nltk  positive_nltk  negativen_ltk  neutral_nltk  ((created_at >= DATE '2021-07-08') AND (created_at <= DATE '2021-07-09'))
# 0  2021-07-08     115       0.297463       0.109600       0.022139      0.868252                                               True                        
# 1  2021-07-09      74       0.364978       0.127932       0.017108      0.854973                                               True                        

print(sdf_agg_byDate.filter(sdf_agg_byDate['created_at'] >= date_from).filter(sdf_agg_byDate['created_at'] <= date_to).count()) #2

# --------------------------------------------------------------
# ### Evaluate sentiment: 
# --------------------------------------------------------------
# - positive -> 1, 
# - negative -> -1, 
# - neutral -> 0

# ======================================================================================================
# Spark - UDF
# ======================================================================================================
from pyspark.sql.functions import udf
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,StructField, StructType, Row
# from pyspark.sql.functions import pandas_udf, PandasUDFType


def sentiment_eval(comp_score: float) -> int :

    # if compound_score > 0.05 => 1 i.e positive
    if comp_score > 0.05:
        return 1
    elif comp_score < 0.05:
        return -1
    else:
        return 0

sentiment_evalUDF = udf(sentiment_eval, IntegerType())

# TypeError: 'Column' object is not callable
# sdf_agg_byDate['sentiment'] = (sdf_agg_byDate['compound_nltk']
#         .apply(lambda comp: 'positive' if comp > 0.05 else 'negative' if comp < -0.05 else 'neutral'))

start = time.time()
print(sdf_agg_byDate.withColumn('sentiment', sentiment_evalUDF(col('compound_nltk'))).toPandas())
end=time.time()
print(f'Spark UDF - elapsed: {end-start}')
#    created_at  tweets  compound_nltk  positive_nltk  negativen_ltk  neutral_nltk  sentiment
# 0  2021-07-08     115       0.297463       0.109600       0.022139      0.868252          1
# 1  2021-07-09      74       0.364978       0.127932       0.017108      0.854973          1
# Spark UDF - elapsed: 20.920655250549316

# ======================================================================================================
# Spark - PandasUDF
# ======================================================================================================
from pyspark.sql.functions import col, pandas_udf, PandasUDFType

@pandas_udf('float')
def sentiment_eval_pUDF(comp_score: pd.Series) -> pd.Series:
    s=[]
    # if compound_score > 0.05 => 1 i.e positive
    for elmnt in comp_score:
        if elmnt > 0.05:
            s.append(1)
        elif elmnt < 0.05:
            s.append(-1)
        else:
            s.append(0)
    return pd.Series(s)

# x = pd.Series([0, 1, 2])
# for el in x:
#     print((el > 1))
# print('*'*20)
# print(x.any())   # because one element is zero
# print(x.all())

start = time.time()
print(sdf_agg_byDate.withColumn('sentiment', sentiment_eval_pUDF(col('compound_nltk'))).toPandas())
end=time.time()
print(f'Spark pandas UDF elapsed: {end-start}')
#    created_at  tweets  compound_nltk  positive_nltk  negativen_ltk  neutral_nltk  sentiment
# 0  2021-07-08     115       0.297463       0.109600       0.022139      0.868252        1.0
# 1  2021-07-09      74       0.364978       0.127932       0.017108      0.854973        1.0
# Spark pandas UDF elapsed: 5.419882774353027

# File "<ipython-input-248-44cbe12222e7>", line 7, in sentiment_eval_pUDF
#   File "/opt/anaconda/envs/pyspark_env/lib/python3.7/site-packages/pandas/core/generic.py", line 1330, in __nonzero__
#     f"The truth value of a {type(self).__name__} is ambiguous. "
# ValueError: The truth value of a Series is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().

# https://stackoverflow.com/questions/36921951/truth-value-of-a-series-is-ambiguous-use-a-empty-a-bool-a-item-a-any-o
# ======================================================================================================
# Pandas Function in Pandas DataFrame
# ======================================================================================================
pdf_agg_byDate = sdf_agg_byDate.toPandas()
print(pdf_agg_byDate)
#    created_at  tweets  compound_nltk  positive_nltk  negativen_ltk  neutral_nltk
# 0  2021-07-08     115       0.297463       0.109600       0.022139      0.868252
# 1  2021-07-09      74       0.364978       0.127932       0.017108      0.854973

start = time.time()
pdf_agg_byDate['sentiment'] = (pdf_agg_byDate['compound_nltk']
        .apply(lambda comp: 'positive' if comp > 0.05 else 'negative' if comp < -0.05 else 'neutral'))
end=time.time()
print(f'pandas function - elapsed: {end-start}')

print(pdf_agg_byDate)
# pandas function - elapsed: 0.0023956298828125

#    created_at  tweets  compound_nltk  positive_nltk  negativen_ltk  neutral_nltk sentiment
# 0  2021-07-08     115       0.297463       0.109600       0.022139      0.868252  positive
# 1  2021-07-09      74       0.364978       0.127932       0.017108      0.854973  positive