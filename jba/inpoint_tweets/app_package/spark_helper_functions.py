# ----------------------------------------------------------------------------------------------------
# NLTK is a leading platform for building Python programs to work with human language data. It provides
# easy-to-use interfaces to over 50 corpora and lexical resources such as WordNet, along with a suite of
# text processing libraries for classification, tokenization, stemming, tagging, parsing, and semantic
# reasoning, wrappers for industrial-strength NLP libraries.
#   • A SentimentAnalyzer is a tool to implement and facilitate Sentiment Analysis tasks using NLTK features and
#     classifiers, especially for teaching and demonstrative purposes.
#   • VADER ( Valence Aware Dictionary for Sentiment Reasoning) is a model used for text sentiment analysis that is
#     sensitive to both polarity (positive/negative) and intensity (strength) of emotion. It is available in the
#     NLTK package and can be applied directly to unlabeled text data.
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd

# ----------------------------------------------------------------------------------------------------
import pyspark.sql.types as Types
from pyspark.sql import functions as F
from pyspark.sql.functions import (PandasUDFType, col, lit, pandas_udf,
                                   to_date, to_timestamp, udf)


# ----------------------------------------------------------------------------------------------------
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
            elif elmnt == 0.0:
                s.append(0)
            else:
                s.append(-1)
        return pd.Series(s)

    return sentiment_eval_pUDF(pdf_series)

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
    # c = split(c, "\\s+") tokenization...
    return c


def create__tweets_sdf_schema(t_lang):
    if t_lang != "en":
        schema = Types.StructType() \
            .add("query_user", 'string', False) \
            .add("id_str", 'string', False) \
            .add("search_words", 'string', False) \
            .add("text_original", 'string', True) \
            .add("text", 'string', True) \
            .add("favourite_count", 'integer', True) \
            .add("retweet_count", 'integer', True) \
            .add("created_at", 'timestamp', True) \
            .add("hashtags", Types.ArrayType(Types.StringType()), True) \
            .add("user_id_str", 'string', False) \
            .add("screen_name", 'string', True) \
            .add("lang", 'string', False)
    else:
        schema = Types.StructType() \
            .add("query_user", 'string', False) \
            .add("id_str", 'string', False) \
            .add("search_words", 'string', False) \
            .add("text", 'string', True) \
            .add("favourite_count", 'integer', True) \
            .add("retweet_count", 'integer', True) \
            .add("created_at", 'timestamp', True) \
            .add("hashtags", Types.ArrayType(Types.StringType()), True) \
            .add("user_id_str", 'string', False) \
            .add("screen_name", 'string', True) \
            .add("lang", 'string', False)
    return schema