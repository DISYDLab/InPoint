# ## VADER Sentiment Analysis : VADER (Valence Aware Dictionary and sEntiment Reasoner):
# ---------------------------------------------------------------------------------------------------------------------
# is a lexicon and rule-based sentiment analysis tool that is specifically attuned to sentiments expressed in social media.
# VADER uses a combination of A sentiment lexicon is a list of lexical features (e.g., words) which are generally labeled
# according to their semantic orientation as either positive or negative. VADER not only tells about the Positivity and
# Negativity score but also tells us about how positive or negative a sentiment is.

# JAVA_HOME refers to jdk/bin directory. It is used by a java based application.
#import sys, glob, os
#jv = os.environ.get('JAVA_HOME', None)
#print(jv)

#import findspark
#findspark.init()

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.0  pyspark-shell'
#  '--packages org.postgresql:postgresql:42.1.1 pyspark-shell'
# get_ipython().system('export PYTHONPATH="/opt/anaconda/envs/pyspark_env/pkgs/spark-nlp-3.0.0-py38_0/lib/python3.8/site-packages/sparknlp/:$PYTHONPATH"')
# get_ipython().system('export PYTHONPATH="/opt/anaconda/envs/pyspark_env/pkgs/spark-nlp-3.0.0-py38_0/lib/python3.8/site-packages/:$PYTHONPATH"')

# get_ipython().system('echo $PYTHONPATH')
#sys.path.extend(glob.glob(os.path.join(os.path.expanduser("~"), ".ivy2/jars/*.jar")))
#sys.path

# If you want to modify the path to packages from within Python, you can do:
# sys.path.append('/where/module/lives/')
# sys.path.append('/opt/anaconda/envs/pyspark_env/pkgs/spark-nlp-3.0.0-py38_0/lib/python3.8/site-packages/')
#sys.path.append('/opt/anaconda/envs/pyspark_env/lib/python3.7/site-packages/')
#os.environ["TFHUB_CACHE_DIR"] = '/tmp/tfhub'

# Spark NLP
# https://www.gigaspaces.com/blog/natural-language-processing-examples/
import sparknlp.annotator
import pandas as pd
import numpy as np
import json

#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
#os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# SPARK
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Spark NLP
from sparknlp.annotator import *
from sparknlp.base import * # DocumentAssembler
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

# https://www.gigaspaces.com/blog/natural-language-processing-examples/

spark  = SparkSession.builder.appName("SparkNLPandVADER Sentiment Analysis") \
        .config("spark.kryoserializer.buffer.max", "800M") \
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.2.1") \
        .getOrCreate()

        #.master("yarn") \
# sc.stop()

print(spark)
sc = spark.sparkContext
print(sc)
# spark.stop()
#MODEL_NAME='sentimentdl_use_imdb'
MODEL_NAME='sentimentdl_use_twitter'

documentAssembler = DocumentAssembler().setInputCol("text").setOutputCol("document")
# use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
# use = (UniversalSentenceEncoder
# Embeddings
# https://nlp.johnsnowlabs.com/docs/en/models#english---models
# https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/tfhub_use_en_2.4.0_2.4_1587136330099.zip
# The Universal Sentence Encoder encodes text into high-dimensional vectors that can be used for text classification, semantic similarity, 
# clustering and other natural language tasks.

# ===================================================================================

use = (UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")
        .setInputCols(["document"])
        .setOutputCol("sentence_embeddings")
        )
# ====================================================================================
print(sparknlp.version())

# .load("file:///home/hadoopuser/nlp/tfhub")
# use = (UniversalSentenceEncoder
#         .load("hdfs:///user/hadoopuser/nlp/tfhub")
#         .setInputCols(["document"])
#         .setOutputCol("sentence_embeddings")
#         )
# ====================================================================================
sentimentdl = (SentimentDLModel
                .pretrained(name=MODEL_NAME, lang="en")
                .setInputCols(["sentence_embeddings"])
                .setOutputCol("sentiment")
                )

# Create Pipeline
nlpPipeline = Pipeline(
      stages = [
          documentAssembler,
          use,
          sentimentdl
      ])

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("index", IntegerType()),
    StructField("text", StringType())])
# ==================================================================================================
#  LOAD FILE IN SPARK DATAFRAME
# ==================================================================================================
print('read csv file')
# spark_tweetsDF = spark.read.csv('/user/hadoopuser/tweets.csv',header=True, schema = schema).dropna()
# spark_tweetsDF.show(2,truncate=False)

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")
spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled")

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, TimestampType
schema = StructType([
    StructField("text", StringType(), True),
    StructField("favourite_count", IntegerType(), True),
    StructField("retweet_count", IntegerType(), True),
    StructField("created_at", TimestampType(), True)
    ])

# ===================================================================================================================
# ### Create a DataFrame <-- Read Data from CSV file
# ===================================================================================================================
spark_tweetsDF = spark.read.csv('/user/hadoopuser/datasets/mockup_tweets.csv',schema=schema, header=False,escape = '"', multiLine=True)
spark_tweetsDF.show(2,truncate=False)
#  Get current number of partitions of a DataFrame ---> df.rdd.getNumPartitions()
print(spark_tweetsDF.rdd.getNumPartitions())
spark_tweetsDF = spark_tweetsDF.repartition(32).persist()
print(spark_tweetsDF.rdd.getNumPartitions())

# +-----+-------------------------------------------------------------------------------------------------------+
# |index|text                                                                                                   |
# +-----+-------------------------------------------------------------------------------------------------------+
# |0    |97% of the funding for the development of the Oxford/AstraZeneca Covid-19 vaccine came from the public.|
# |1    |Would there be more uptake on AstraZeneca if there was some catchy jingle?                             |
# +-----+-------------------------------------------------------------------------------------------------------+
# only showing top 2 rows

# ### 6. Run the pipeline
# 
empty_df = spark.createDataFrame([['']]).toDF("text")
pipelineModel = nlpPipeline.fit(empty_df)

# df = spark.createDataFrame(pd.DataFrame({"text":text_list}))

result = pipelineModel.transform(spark_tweetsDF)
print(type(result))
# <class 'pyspark.sql.dataframe.DataFrame'>
print(result.printSchema())
# root
#  |-- index: integer (nullable = true)
#  |-- text: string (nullable = true)
#  |-- document: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)
#  |-- sentence_embeddings: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)
#  |-- sentiment: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- annotatorType: string (nullable = true)
#  |    |    |-- begin: integer (nullable = false)
#  |    |    |-- end: integer (nullable = false)
#  |    |    |-- result: string (nullable = true)
#  |    |    |-- metadata: map (nullable = true)
#  |    |    |    |-- key: string
#  |    |    |    |-- value: string (valueContainsNull = true)
#  |    |    |-- embeddings: array (nullable = true)
#  |    |    |    |-- element: float (containsNull = false)

# None
# print(result.select('text', 'sentiment').limit(2).toPandas())

result.select('text', 'sentiment').show(2, truncate=20)
# ---------------------------------------------------------------------------------------------------------
#                                                 text                                          sentiment
# 0  97% of the funding for the development of the ...  [(category, 0, 102, negative, {'sentence': '0'...
# 1  Would there be more uptake on AstraZeneca if t...  [(category, 0, 73, positive, {'sentence': '0',...
# ---------------------------------------------------------------------------------------------------------
from pyspark.sql.functions import col
# result1.select('text', col('sentiment')[0]).toPandas().style.set_properties(subset=['sentiment'], **{'width': '300px'})
# result1.select('text', col('sentiment')[0]['result'], col('sentiment')[0]['metadata']).show(truncate = False)

result_f = (result.select('text', 'sentiment')
            .withColumn('result_nlp', col('sentiment')[0]['result'])
            .withColumn('negative_nlp', col('sentiment')[0]['metadata']['negative'])
            .withColumn('positive_nlp', col('sentiment')[0]['metadata']['positive'])
            .drop('sentiment')
            )

# result1_f.style.set_properties(subset=['text'], **{'width': '300px'}).style.
# print(result_f.limit(2).toPandas().style.set_properties(subset=['text'], **{'width': '300px'}))
# result_f.limit(2).toPandas()
# <pandas.io.formats.style.Styler object at 0x7fe97735352
result_f.show(2, truncate=20)



# sentiment = udf(lambda x: TextBlob(x).sentiment[0])
# spark.udf.register('sentiment', sentiment)
# tweets = tweets.withColumn('sentiment',sentiment('text').cast('double')

import nltk
import csv
import os
# from nltk
# /home/hadoopuser/nltk_data/sentiment/vader_lexicon.zip
nltk.download('vader_lexicon')
# ------------------------------------------------------------
# [nltk_data] Downloading package vader_lexicon to
# [nltk_data]     /home/hadoopuser/nltk_data...
# [nltk_data]   Package vader_lexicon is already up-to-date!
# ------------------------------------------------------------
# ========================================================================================================
#--------------------------------------------------------------------------------------------------------------------
# ### Create and register a udf
#--------------------------------------------------------------------------------------------------------------------
import sys
from pyspark.sql.functions import udf 
# DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType, StructField, StructType, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.types as Types
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def sentiment_scores(sentance: str) -> dict :
    
    
    # Create a SentimentIntensityAnalyzer object.
    sid = SentimentIntensityAnalyzer('file:///home/hadoopuser/nltk_data/sentiment/vader_lexicon.zip/vader_lexicon/vader_lexicon.txt')
    
    # polarity_scores method of SentimentIntensityAnalyzer
    # oject gives a sentiment dictionary.
    # which contains pos, neg, neu, and compound scores.
    r = sid.polarity_scores(sentance)
    return r
#--------------------------------------------------------------------------------------------------------------------
# You can optionally set the return type of your UDF. The default return type is StringType.
# udffactorial_p = udf(factorial_p, LongType())
sentiment_scoresUDF = udf(sentiment_scores, Types.MapType(Types.StringType(), Types.DoubleType()))
#--------------------------------------------------------------------------------------------------------------------
from pyspark.sql.functions import col,sqrt,log,reverse
result_f1 = result_f.withColumn("sentiment", sentiment_scoresUDF(result_f.text))
# t.show()
# print(result_f1.toPandas().style.set_properties(subset=['text'], **{'width': '300px'}))
print(result_f1.printSchema())
result_f1 = (result_f1
                .withColumn('negative_nltk', col('sentiment')['neg'])
                .withColumn('positive_nltk', col('sentiment')['pos'])
                .withColumn('neutral_nltk', col('sentiment')['neu'])
                .withColumn('compound_nltk', col('sentiment')['compound'])
                .drop('sentiment')
                )
# s
result_f1.show(2, truncate=20)

print('before write')
result_f1.write.format("csv").mode("overwrite").options(header="true", escape = '"') \
    .save("/user/hadoopuser/export_datasets/tweets_sentiment_analysis_results.csv")
print('after write')

# ### About the Scoring
# 
#     The compound score is computed by summing the valence scores of each word in the lexicon, adjusted according to the rules, and then normalized to be between -1 (most extreme negative) and +1 (most extreme positive). This is the most useful metric if you want a single unidimensional measure of sentiment for a given sentence. Calling it a 'normalized, weighted composite score' is accurate.
# 
#     It is also useful for researchers who would like to set standardized thresholds for classifying sentences as either positive, neutral, or negative. Typical threshold values (used in the literature cited on this page) are:
# 
#         positive sentiment: compound score >= 0.05
#         neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
#         negative sentiment: compound score <= -0.05
# 
#     **NOTE**: The compound score is the one most commonly used for sentiment analysis by most researchers, including the authors.
# 
#     The pos, neu, and neg scores are ratios for proportions of text that fall in each category (so these should all add up to be 1... or close to it with float operation). These are the most useful metrics if you want to analyze the context & presentation of how sentiment is conveyed or embedded in rhetoric for a given sentence. For example, different writing styles may embed strongly positive or negative sentiment within varying proportions of neutral text -- i.e., some writing styles may reflect a penchant for strongly flavored rhetoric, whereas other styles may use a great deal of neutral text while still conveying a similar overall (compound) sentiment. As another example: researchers analyzing information presentation in journalistic or editorical news might desire to establish whether the proportions of text (associated with a topic or named entity, for example) are balanced with similar amounts of positively and negatively framed text versus being "biased" towards one polarity or the other for the topic/entity.
#     
#         IMPORTANTLY: these proportions represent the "raw categorization" of each lexical item (e.g., words, emoticons/emojis, or initialisms) into positve, negative, or neutral classes; they do not account for the VADER rule-based enhancements such as word-order sensitivity for sentiment-laden multi-word phrases, degree modifiers, word-shape amplifiers, punctuation amplifiers, negation polarity switches, or contrastive conjunction sensitivity.
goodbye_text = f'Spark App: {spark.sparkContext.appName} finished :-)'
print ('*'*len(goodbye_text))
print(goodbye_text)
print ('*'*len(goodbye_text))