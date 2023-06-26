# # Get Tweets
# 
# This script extracts all the tweets with hashtag #covid-19 related to the day before today (yesterday) and saves them into a .csv file.
# We use the `tweepy` library, which can be installed with the command `pip install tweepy`.
# 
# Firstly, we import the configuration file, called `config.py`, which is located in the same directory of this script.


#import sys, glob, os
import typing
#jv = os.environ.get('JAVA_HOME', None)

# import findspark
# findspark.init()

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.0 pyspark-shell'
# '--packages org.postgresql:postgresql:42.1.1 pyspark-shell'

# sys.path
#sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))
# sys.path.append('/home/hadoopuser/nltk_data/sentiment/')
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

spark=(SparkSession
        .builder \
        .appName("tweets_search_api_coffee_island_sdf_Yarn_Client") \
        .getOrCreate()
        )
# spark=(SparkSession
#         .builder
#         .appName("tweets_search_api_coffe_island_sdf")
#         .master("local[*]")
#         .getOrCreate()
#         )

# spark-submit -v \
#  --master yarn \
#  --deploy-mode client \
#  --name tweets_search_api_coffe_island_sdf_Yarn_Client \
#  --driver-memory 1024m \
#  --driver-cores 1 \
#  --executor-memory 4608m \
#  --executor-cores 1 \
#  --num-executors 24 \
#  --conf spark.suffle.service.enabled=true \
#  --conf spark.dynamicAllocation.enabled=false \
#  ~/Documents/jba/twitter_apps/tweets_search_api_coffee_island_sdf.py


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

import sys, os
# path = '/home/hadoopuser/nltk_data'
# os.environ['PATH'] += ':'+path
# print('sys.path')
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
# logger.root.level = 10
# print(logger.root.level)
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
# Selecting specific columns in a spark dataframe
# sdf = sdf.select('text', 'favourite_count', 'retweet_count', 'created_at')
# print(sdf.limit(3).toPandas())

# Extract First N rows in pyspark – Top N rows in pyspark using show() function
sdf.show(3, truncate=30)
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
# print(sdf_2.limit(2).toPandas())

# sdf_2 = sdf_2.sort(F.col('created_at'), ascending=True)
sdf_2 = sdf_2.sort(F.col('created_at').asc())

sdf_2.show(2,truncate=30)
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

print(f'({sdf_2.count(), len(sdf_2.columns)})')
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
print(sdf.printSchema())
print(sdf_2.count() )
print(sdf_2.columns)
print(len(sdf_2.columns))

# Add this to the your code:

# import pyspark
# def spark_shape(self):
#     return (self.count(), len(self.columns))
# pyspark.sql.dataframe.DataFrame.shape = spark_shape
# Then you can do

# >>> df.shape()
# (10000, 10)

# print(sdf_2.sort(F.col('created_at').desc()).limit(100).toPandas())
# print(sdf_2.toPandas().shape)
# --------------------------------------------------------------
# select columns as in a spark dataframe
# --------------------------------------------------------------
# sdf_2[['text','created_at']].show(2,truncate=30)
# sdf_2.select('text','created_at').describe().show()
# sdf_2[['text','created_at']].describe().show()
# sdf_2[sdf_2.columns].show(2, truncate=2)

# --------------------------------------------------------------
# ### def sentiment_scores & sentiment_scoresUDF
# --------------------------------------------------------------
# User-defined functions operate one-row-at-a-time, and thus suffer from high serialization and invocation overhead. As a result, many data pipelines define UDFs in Java and Scala and then invoke them from Python.
# 
# Pandas UDFs built on top of Apache Arrow bring you the best of both worlds—the ability to define low-overhead, high-performance UDFs entirely in Python.
# ------------------------
# #### Scalar Pandas UDFs
# ------------------------
# Scalar Pandas UDFs are used for vectorizing scalar operations. To define a scalar Pandas UDF, simply use @pandas_udf to annotate a Python function that takes in pandas.Series as arguments and returns another pandas.Series of the same size. Below we illustrate using two examples: Plus One and Cumulative Probability.
#
# #### PyArrow versions
# 
# PyArrow is installed in Databricks Runtime. For information on the version of PyArrow available in each Databricks Runtime version, see the Databricks runtime release notes.
# Supported SQL types
# 
# All `Spark SQL data types` are supported by Arrow-based conversion except `MapType`, `ArrayType` of `TimestampType`, and nested `StructType`. `StructType` is represented as a `pandas.DataFrame` instead of `pandas.Series`. `BinaryType` is supported only when PyArrow is equal to or higher than 0.10.0.
# https://sparkbyexamples.com/spark/spark-sql-dataframe-data-types/
# --------------------------------------------------------------

# ### Create DataFrame with schema
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
#
# simpleData = [["James ","","Smith","36636","M",3000],
#     ["Michael ","Rose","","40288","M",4000],
#     ["Robert ","","Williams","42114","M",4000],
#     ["Maria ","Anne","Jones","39192","F",4000],
#     ["Jen","Mary","Brown","","F",-1]]
#
# simpleSchema = StructType([
#     StructField("firstname",StringType()),
#     StructField("middlename",StringType()),
#     StructField("lastname",StringType()),
#     StructField("id", StringType()),
#     StructField("gender", StringType()),
#     StructField("salary", IntegerType())
# ])
#
# df = spark.createDataFrame(simpleData, schema=simpleSchema)
# df.printSchema()
# df.show()
# --------------------------------------------------------------
# --------------------------------------------------------------
import sys
from pyspark.sql.functions import udf
import json
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,StructField, StructType, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.types as Types
#from nltk.sentiment.vader import SentimentIntensityAnalyzer
# --------------------------------------------------------------
# from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType

# simpleData = [["James ","","Smith","36636","M",3000],
#     ["Michael ","Rose","","40288","M",4000],
#     ["Robert ","","Williams","42114","M",4000],
#     ["Maria ","Anne","Jones","39192","F",4000],
#     ["Jen","Mary","Brown","","F",-1]]

simpleSchema = StructType([
    StructField("neg",DoubleType()),
    StructField("pos",DoubleType()),
    StructField("compound",DoubleType()),
    StructField("neu", DoubleType())
])

# df = spark.createDataFrame(simpleData, schema=simpleSchema)
# df.printSchema()
# df.show()

# @pandas_udf('str',PandasUDFType.SCALAR)
#  For Types.MapType() - 2 required positional arguments: 'keyType' and 'valueType'
# @pandas_udf(simpleSchema, PandasUDFType.GROUPED_MAP)
# @pandas_udf(Types.MapType(Types.StringType(),Types.DoubleType()))
# @pandas_udf('struct<col1:string>')
# def sentiment_scores(sentance: str) -> dict :
# def sentiment_scores(pdf:pd.DataFrame) -> pd.DataFrame:
# def sentiment_scores(pdf):
    # Create a SentimentIntensityAnalyzer object.
#     sid = SentimentIntensityAnalyzer()
    # polarity_scores method of SentimentIntensityAnalyzer
    # oject gives a sentiment dictionary.
    # which contains pos, neg, neu, and compound scores.
    # ----
    # r = sid.polarity_scores(sentance)

    # pdf[r] = json.dumps(sid.polarity_scores(sentance))

    # json.dumps(sid.polarity_scores(s))
    # -----
    # simpleSchema.neg = r.neg
    # simpleSchema.pos = r.pos
    # simpleSchema.compound = r.compound
    # simpleSchema.neu = r.neu
    # s1 = pdf['text']
    # pdf.columns
    # pdf['s2'] = pdf.assign(json.dumps(sid.polarity_scores(s1)))
    # json.dumps(sid.polarity_scores(s)))
    
    # return r
    # return pd.DataFrame(simpleSchema)
    # 
#     return json.dumps(sid.polarity_scores(pdf['text']))#sid.polarity_scores(s)
    # return pdf.assign(rating = json.dumps(sid.polarity_scores(pdf)))
    # You can optionally set the return type of your UDF. The default return type␣,→is StringType.
    # udffactorial_p = udf(factorial_p, LongType())

# sentiment_scoresUDF = udf(sentiment_scores, Types.MapType(Types.StringType(),Types.DoubleType()))
# sentiment_scores_pUDF = pandas_udf(sentiment_scores, returnType=Types.MapType(Types.StringType(),Types.DoubleType()))
# sentiment_scores_pUDF = pandas_udf(sentiment_scores, returnType=simpleSchema)
#  udf_obj = UserDefinedFunction(
#      42         f, returnType=returnType, name=None, evalType=evalType, deterministic=True)
# sid = SentimentIntensityAnalyzer()
# ---
# mydata = {"text":['Hello wanderful world', 'Hello bad boy'], "rank":[0,1]}
# pdf_mstring = pd.DataFrame(mydata)
# pdf_mstring
# ------
# pdf_mstring['t'] = pdf_mstring.text.apply(lambda c1:json.dumps(sid.polarity_scores(c1)))

# pdf_mstring['text2'] =pdf_mstring.apply(lambda c1:sentiment_scores(c1['text']), axis=1)

# pdf_mstring['text2'] =pdf_mstring.apply(lambda c1:sentiment_scores(c1), axis=1)

# -----
# pdf_mstring['text2'] = pdf_mstring[['text', 'rank']].apply(lambda row: row['text'], axis=1)

# pdf_mstring['text2'] = pdf_mstring[['text', 'rank']].apply(lambda row: sentiment_scores(row), axis=1)
# pdf_mstring.assign(s2 = SentimentIntensityAnalyzer().polarity_scores(pdf_mstring.text))
# json.dumps(sid.polarity_scores('Hello wanderful world'))
# print(type(sid.polarity_scores('Hello wanderful world')))

# sdf_test = sdf_2
# add an index column
# sdf_tes = sdf_test.withColumn('index', F.monotonically_increasing_id())
# sdf_tes
# sdf_tes_min = sdf_tes.sort('index').limit(2)
# sdf_tes_min.show(truncate=20)
# pyspark.sql.functions.pandas_udf(f=None, returnType=None, functionType=None)
# --------------------------------------------------------------
# a Python native function that takes a pandas.DataFrame, and outputs a pandas.DataFrame.
# ss = sdf_tes_min.select('text', 'index').groupby("text").applyInPandas(sentiment_scores, schema="rating string")
# ss.show()
# 
# --------------------------------------------------------------
# pdf_mstring.drop('text2', axis=1, inplace = True)
# pdf_mstring
# sdf_tes_min.show()
# --------------------------------------------------------------
# --------------------------------------------------------------
# import pandas as pd
# from pyspark.sql.functions import pandas_udf
#
# df = spark.createDataFrame(
#     [[1, "a string", ("a nested string",)]],
#     "long_col long, string_col string, struct_col struct<col1:string>")
#
# @pandas_udf("col1 string, col2 long")
# def pandas_plus_len(
#         s1: pd.Series, s2: pd.Series, pdf: pd.DataFrame) -> pd.DataFrame:
#     # Regular columns are series and the struct column is a DataFrame.
#     pdf['col2'] = s1 + s2.str.len()
#     return pdf  # the struct column expects a DataFrame to return
#
# df.withColumn('new',pandas_plus_len("long_col", "string_col", "struct_col")).show()
#
#
#
# from pyspark.sql.functions import pandas_udf, PandasUDFType,col
# --------------------------------------------------------------
# --------------------------------------------------------------
# @pandas_udf('double', PandasUDFType.SCALAR)
# def pandas_plus_one(v):
#     # `v` is a pandas Series
#     return v.add(1)  # outputs a pandas Series
# # --------------------------------------------------------------
# --------------------------------------------------------------
# @pandas_udf('string') #return value
# def pandas_sum_two_columns(s1: pd.Series, s2: pd.Series) -> pd.Series:
#     # return s.add(1)
#     # pdf2 = pdf + s
#     # return (s1 + s2).astype(str)
#     return (s1 + s2).astype(str)
#
# sdf_pandas_udf_ex1 = spark.range(10).withColumn('pdf',col("id")*2).withColumn('sum', pandas_sum_two_columns("id", "pdf"))
# sdf_pandas_udf_ex1.show()
# sdf_pandas_udf_ex1.agg({'pdf':'max'}).show()
# print(sdf_pandas_udf_ex1.agg({'pdf':'max'}).collect()[0])
# print(sdf_pandas_udf_ex1.agg({'pdf':'max'}).collect()[0][0])
# --------------------------------------------------------------
# --------------------------------------------------------------
'''
/opt/spark/python/pyspark/sql/pandas/functions.py:386: UserWarning: In Python 3.6+ and Spark 3.0+, it is preferred to specify type hints for pandas UDF instead of specifying pandas UDF type which will be deprecated in the future releases. See SPARK-28264 for more details.
#   "in the future releases. See SPARK-28264 for more details.", UserWarning)
'''
# --------------------------------------------------------------
# --------------------------------------------------------------
# import pandas as pd
# from pyspark.sql.functions import col, pandas_udf
# from pyspark.sql.types import LongType

# Declare the function and create the UDF
# def multiply_func(a: pd.Series, b: pd.Series) -> pd.Series:
#     return a * b

# multiply = pandas_udf(multiply_func, returnType=LongType())

# The function for a pandas_udf should be able to execute with local pandas data
# x = pd.Series([1, 2, 3])
# ------------------------------------------------------
# print(multiply_func(x, x))
# 0    1
# 1    4
# 2    9
# dtype: int64
# ------------------------------------------------------
# Create a Spark DataFrame, 'spark' is an existing SparkSession
# df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

# Execute function as a Spark vectorized UDF
# df.select(multiply(col("x"), col("x"))).show()

# spark.range(10).withColumn('pdf',col("id").cast('string')).show()
# spark.range(10).withColumn('pdf',col("id")*2).show()
# print(spark.range(10).withColumn('pdf',col("id")*2).show())

# --------------------------------------------------------------
# --------------------------------------------------------------
#
# import pandas as pd
# from pyspark.sql.functions import pandas_udf, ceil
# df = spark.createDataFrame(
#     [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
#     ("id", "v"))
# def normalize(pdf):
#     v = pdf.v
#     # Assign new columns to a DataFrame
#     return pdf.assign(normalized=(v - v.mean()) / v.std())
# df.show()
# df.groupby("id").applyInPandas(
#     normalize, schema="id long, v double, normalized double").show()
#
# # --------------------------------------------------------------
# --------------------------------------------------------------
# ---VADER -----------------------------------------------------
# ### def sentiment_scores & sentiment_scoresUDF
# ---VADER -----------------------------------------------------
import sys
# sys.path.append('/home/hadoopuser/nltk_data')
# print(sys.path)
# import nltk
# nltk.download('vader_lexicon')
from pyspark.sql.functions import udf
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,StructField, StructType, Row
# from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.types as Types
#from nltk.sentiment.vader import SentimentIntensityAnalyzer
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

from pyspark.sql.functions import col,sqrt,log,reverse
sdf_2 = sdf_2.withColumn("rating", sentiment_scoresUDF(sdf_2.text))

# df.groupby("id").applyInPandas(
#     normalize, schema="id long, v double, normalized double").show()
# sdf_udf = sdf_2.groupby("text").applyInPandas(
    # sentiment_scores, schema="text string, rating string")

# sdf_pudf.show(2,truncate=20)
# t.show()
# sdf_2.toPandas().style.set_properties(subset=['text'], **{'width': '300px'})

print(sdf_2.show(2, truncate=30))

from pyspark.sql.functions import col,sqrt,log,reverse
sdf_2 = sdf_2.withColumn("rating", sentiment_scoresUDF(sdf_2.text))
# t.show()
print(sdf_2.limit(2).toPandas().style.set_properties(subset=['text'], **{'width': '300px'}))
# sdf.toPandas().head(2).style.set_properties(subset=['text'], **{'width': '300px'})
# sdf.toPandas().tail(2).style.set_properties(subset=['text'], **{'width': '300px'})
# sdf.show(2, truncate = 30)

# Returns the first num rows as a list of Row.
# This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.
# sdf.head(2)

# Returns the last num rows as a list of Row.
# sdf.tail(2)
# sdf.printSchema()

from pyspark.sql.functions import col
sdf_2 = sdf_2.withColumn('negative_nltk', col('rating')['neg']) .withColumn('positive_nltk', col('rating')['pos']) .withColumn('neutral_nltk', col('rating')['neu']) .withColumn('compound_nltk',col('rating')['compound']) .drop('rating')
print(sdf_2.limit(2).toPandas().style.set_properties(subset=['text'], **{'width': '300px'}))
print(sdf_2.toPandas().tail(2).style.set_properties(subset=['text'], **{'width': '300px'}))
# sdf_from_list_of_rows = spark.createDataFrame(sdf_2.tail(2))
# sdf_from_list_of_rows.show(truncate=15)

# sdf_from_pdf = spark.createDataFrame(pdf)
# sdf_from_list_of_rows.show(2)
# https://stackoverflow.com/questions/61608057/output-vader-sentiment-scores-in-columns-based-on-dataframe-rows-of-tweets

# Create the dataframe
# df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["letter", "name"])

# Function to get rows at `rownums`
# def getrows(df, rownums=None):
    # return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])

# Get rows at positions 0 and 2.
# getrows(df, rownums=[0, 2]).collect()

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
 # --------------------------------------------------------------

print(sdf_2.limit(1).toPandas().style.set_properties(subset=['text'], **{'width': '300px'}))
print(sdf_2.dtypes)


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
print(exprs)

# sdf_agg_byDate = sdf.groupBy('created_at').agg({'negative_nltk':'sum'}, {'positive_nltk':'sum'}, {'neutral_nltk':'sum'}, {'compound_nltk':'sum'})
import pyspark.sql.functions as f 
from pyspark.sql.functions import col

# sdf_agg_byDate = sdf_2.groupBy('created_at').agg({'negative_nltk':'sum','positive_nltk':'sum','neutral_nltk':'sum','compound_nltk':'sum','created_at':'count'}).withColumnRenamed('count(created_at)', 'tweets')
sdf_agg_byDate = sdf_2.groupBy('created_at').agg(exprs).withColumnRenamed('count(created_at)', 'tweets')
print(sdf_agg_byDate.limit(1).toPandas())

# sdf.groupBy("department","state") \
#     .sum("salary","bonus") \
#     .show(false)
# sdf_2.groupBy('created_at').agg(exprs).withColumnRenamed('count(created_at)', 'tweets').limit(1).toPandas()

# How to delete columns in pyspark dataframe
columns_to_drop = ['sum(compound_nltk)', 'sum(positive_nltk)', 'sum(negative_nltk)', 'sum(neutral_nltk)']

sdf_agg_byDate = (sdf_agg_byDate
    .withColumn('compound_nltk', F.col('sum(compound_nltk)')/F.col('tweets'))
    .withColumn( 'positive_nltk',F.col('sum(positive_nltk)')/F.col('tweets'))
    .withColumn( 'negativen_ltk',F.col('sum(negative_nltk)')/F.col('tweets'))
    .withColumn( 'neutral_nltk',F.col('sum(neutral_nltk)')/F.col('tweets'))
    .drop(*columns_to_drop)
    )

# print(sdf_agg_byDate.limit(1).toPandas())
print(sdf_agg_byDate.toPandas())
# .drop(*columns_to_drop)
print(sdf_agg_byDate.created_at[0])
# The below statement changes column 'count(created_at)' to 'tweets' on PySpark DataFrame.
# sdf_agg_byDate = (sdf_agg_byDate
#     .withColumnRenamed('count(created_at)', 'tweets'))
# DataFrame.sort(*cols, **kwargs) - Returns a new DataFrame sorted by the specified column(s).
sdf_agg_byDate = sdf_agg_byDate.sort('created_at')

print(sdf_agg_byDate.toPandas())
#
print(sdf_agg_byDate.dtypes)

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
sdf_agg_byDate.filter(col('created_at') >= date_from).show()


# +----------+------+------------------+-------------------+--------------------+-----------------+
# |created_at|tweets|     compound_nltk|      positive_nltk|       negativen_ltk|     neutral_nltk|
# +----------+------+------------------+-------------------+--------------------+-----------------+
# |2021-06-29|   159|0.5084113207547171|0.16976100628930818|0.006924528301886793|0.823308176100629|
# +----------+------+------------------+-------------------+--------------------+-----------------+

# `sf = sf.filter(sf.my_col >= date_from).filter(sf.my_col <= date_to)
# sf.count()`
# 
# https://stackoverflow.com/questions/31407461/datetime-range-filter-in-pyspark-sql
# 
# .alias('sentiment_eval')
# https://sparkbyexamples.com/pyspark/pyspark-difference-between-two-dates-days-months-years/

print(sdf_agg_byDate.filter(sdf_agg_byDate['created_at'] >= date_from).filter(sdf_agg_byDate['created_at'] <= date_to).count())
print(sdf_agg_byDate.select('*', sdf_agg_byDate['created_at'].between(date_from, date_to)).count())
print(sdf_agg_byDate.select('*', sdf_agg_byDate['created_at'].between(date_from, date_to)).toPandas())


# +----------+------+-------------------+-------------------+--------------------+------------------+
# |created_at|tweets|      compound_nltk|      positive_nltk|       negativen_ltk|      neutral_nltk|
# +----------+------+-------------------+-------------------+--------------------+------------------+
# |2021-06-28|   105|0.44016666666666665|0.13237142857142856|0.024638095238095237|0.8430095238095238|
# |2021-06-29|   159| 0.5084113207547171|0.16976100628930818|0.006924528301886793| 0.823308176100629|
# +----------+------+-------------------+-------------------+--------------------+------------------+

# --------------------------------------------------------------
# ### Evaluate sentiment: 
# --------------------------------------------------------------
# - positive -> 1, 
# - negative -> -1, 
# - neutral -> 0
import sys
from pyspark.sql.functions import udf
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType,StructField, StructType, Row
# from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.types as Types

def sentiment_eval(comp_score: float) -> int :

    # if compound_score > 0.05 => 1 i.e positive
    if comp_score > 0.05:
        return 1
    elif comp_score < 0.05:
        return -1
    else:
        return 0

print(sdf_agg_byDate.filter(sdf_agg_byDate['created_at'] >= date_from).filter(sdf_agg_byDate['created_at'] <= date_to).count())

sentiment_evalUDF = udf(sentiment_eval, IntegerType())

# TypeError: 'Column' object is not callable
# sdf_agg_byDate['sentiment'] = (sdf_agg_byDate['compound_nltk']
#         .apply(lambda comp: 'positive' if comp > 0.05 else 'negative' if comp < -0.05 else 'neutral'))

start = time.time()
print(sdf_agg_byDate.withColumn('sentiment', sentiment_evalUDF(col('compound_nltk'))).toPandas())
end=time.time()
print(f'UDF - elapsed: {end-start}')

from pyspark.sql.functions import pandas_udf, PandasUDFType

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

# File "<ipython-input-248-44cbe12222e7>", line 7, in sentiment_eval_pUDF
#   File "/opt/anaconda/envs/pyspark_env/lib/python3.7/site-packages/pandas/core/generic.py", line 1330, in __nonzero__
#     f"The truth value of a {type(self).__name__} is ambiguous. "
# ValueError: The truth value of a Series is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().

# https://stackoverflow.com/questions/36921951/truth-value-of-a-series-is-ambiguous-use-a-empty-a-bool-a-item-a-any-o

pdf_agg_byDate = sdf_agg_byDate.toPandas()
print(pdf_agg_byDate)

start = time.time()
pdf_agg_byDate['sentiment'] = (pdf_agg_byDate['compound_nltk']
        .apply(lambda comp: 'positive' if comp > 0.05 else 'negative' if comp < -0.05 else 'neutral'))
end=time.time()
print(f'pandas function - elapsed: {end-start}')

print(pdf_agg_byDate)

# import pandas as pd
# from pyspark.sql.functions import pandas_udf
#
# @pandas_udf("col1 string, col2 long")
# def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
#     s3['col2'] = s1 + s2.str.len()
#     return s3
# # Create a Spark DataFrame that has three columns including a sturct column.

# [[1, "a string", ("a nested string",)]],
# "long_col long, string_col string, struct_col struct<col1:string>")
# df.printSchema()
# df.show()
#
# df = df.withColumn('derived', func("long_col", "string_col", "struct_col"))
# # Create a Spark DataFrame that has three columns including a sturct column.
#
# df.show()

# from pyspark import SparkContext

# ### Best practices for successfully managing memory for Apache Spark applications on Amazon EMR
# 
# https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
# --------------------------------------------------------------
# ### Set sparkContext and sqlContext
# --------------------------------------------------------------
# from pyspark.sql import SQLContext
# sc = spark.sparkContext
# sqlContext = SQLContext(sc)
# --------------------------------------------------------------
# sqlContext.sql("get spark.sql.shuffle.partitions=10")

# sqlContext.sql("set spark.sql.shuffle.partitions=10")
# spark.default.parallelism is the default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set explicitly by the user. Note that spark.default.parallelism seems to only be working for raw RDD and is ignored when working with dataframes.

# If the task you are performing is not a join or aggregation and you are working with dataframes then setting these will not have any effect. You could, however, set the number of partitions yourself by calling df.repartition(numOfPartitions) (don't forget to assign it to a new val) in your code.
# sqlContext.sql("get spark.sql.shuffle.partitions")
# --------------------------------------------------------------
# spark.conf.get("spark.sql.shuffle.partitions")
# --------------------------------------------------------------
# spark.conf.get("spark.default.parallelism")
# sc.getConf().getAll()
# sc.getConf("spark.default.parallelism")
# sc.conf.get("spark.driver.memory")
# spark.sparkContext.get(spark.sql.shuffle.partitions)#spark.sql.functions.partitions
# --------------------------------------------------------------
# --------------------------------------------------------------
# configurations = spark.sparkContext.getConf().getAll()
# for conf in configurations:
#     print(conf[0],':',conf[1])
#
# configurations = spark.sparkContext.getConf().getAll()
# for conf in configurations:
#     if conf[0] == 'spark.sql.shuffle.partitions':
#         print(conf[0],':',conf[1])
#
# print(f'spark.sql.shuffle.partitions = {spark.conf.get("spark.sql.shuffle.partitions")}')
# --------------------------------------------------------------
# spark.sql.shuffle.partitions = 200
# spark.executor.id 

# --------------------------------------------------------------
# # perfplot
# --------------------------------------------------------------
# import numpy as np
# import pandas as pd
# import perfplot
#
# perfplot.save(
#     "out.png",
#     setup=lambda n: pd.DataFrame(np.arange(n * 3).reshape(n, 3)),
#     n_range=[2**k for k in range(25)],
#     kernels=[
#         lambda df: len(df.index),
#         lambda df: df.shape[0],
#         lambda df: df[df.columns[0]].count(),
#     ],
#     labels=["len(df.index)", "df.shape[0]", "df[df.columns[0]].count()"],
#     xlabel="Number of rows",
# )
# --------------------------------------------------------------