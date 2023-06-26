# -------------------------------------------------------------------------------------------------------------------
# # Get Tweets
# 
# This script extracts all the tweets with hashtag #covid-19 related to the day before today (yesterday) and saves them into a .csv file.
# We use the `tweepy` library, which can be installed with the command `pip install tweepy`.
# 
# Firstly, we import the configuration file, called `config.py`, which is located in the same directory of this script.
# -------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------
#SyntaxError: from __future__ imports must occur at the beginning of the file
from __future__ import annotations
from typing import List, Set, Dict, Tuple, Text, Optional, AnyStr
# import mypy 
import sys, glob, os
#print(sys.path)
import mypy
from config import *
import tweepy
import datetime
import logging
logger = logging.getLogger('tweets_search')
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer

print(f"logger.root.level = {logger.root.level}, logger.root.name = {logger.root.name}")
print(f"logger.name = {logger.name}")

format = "%(asctime)s - %(levelname)s - %(message)s"
# logging.basicConfig(format=format, stream=sys.stdout, level = logging.DEBUG)
logging.basicConfig(format=format, stream=sys.stdout, level = logging.INFO)

print(logger.root.level)
# logger.root.level = 10
# print(logger.root.level)

# We setup the connection to our Twitter App by using the `OAuthHandler()` class and its `access_token()` function. 
# Then we call the Twitter API through the `API()` function.
auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify = True)

# api.me()
# api.rate_limit_status()
# -------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------
# ## setup dates (recent 7 days max)
# 
# If today is 2021-06-26 then :
# 
# 1. `time_frame = {timedelta:'2'}` (we get tweets from 2021-0-24 up to 2021-06-25 (today - 1 day))
# 2. `time_frame = {since:'2021-06-23', timedelta:'2'}` 
# 3. `time_frame = {until:'2021-06-25', timedelta:'2'}` (2 & 3 & 4 expressions are equivalent)
# 4. `time_frame = {since:'2021-06-23', until:'2021-06-25'}` -> we get tweets from 2021-06-23 up to 2021-06-24
# 
# `note:` from today we can get a time_frame of 7 days max, i.e since 2021-06-19
# -------------------------------------------------------------------------------------------------------------------
def time_frame1(until:datetime=None, since:datetime=None, timedelta=0) -> tuple[datetime]:
    today = datetime.date.today()
    # print((timedelta))
    # if isinstance(int(timedelta), int):
    #     print(timedelta)
    #     if (1 <= timedelta <=7) and since==None:
    #         today = datetime.date.today()
    #         since= today - datetime.timedelta(days=timedelta)
    #         until= today
    #         logger.warning(f"time_frame: '{until, since}'") # until, since
    if isinstance(timedelta, int) and 0 < timedelta <=7:    
        if until == None and since == None:        
            today = datetime.date.today()
            since= today - datetime.timedelta(days=timedelta)
            until= today
        if until == None and isinstance(since, datetime.date): 
            if 0 < (today - since).days <= 7:
                until= since + datetime.timedelta(days=timedelta)
    return until, since
# (datetime.date(2021, 6, 7), datetime.date(2021, 6, 6))

today = datetime.date.today()
until, since = time_frame1(since= today - datetime.timedelta(days=3), timedelta=2)
print(until, since)

since= today - datetime.timedelta(days=1)
until= today
logger.warning(f"full_text: '{until, since}'")

# -------------------------------------------------------------------------------------------------------------------
logger.debug(f"full_text: '{until, since}'")

# We search for tweets on Twitter by using the `Cursor()` function. 
# We pass the `api.search` parameter to the cursor, as well as the query string, which is specified through the `q` parameter of the cursor.
# The query string can receive many parameters, such as the following (not mandatory) ones:
# * `from:` - to specify a specific Twitter user profile
# * `since:` - to specify the beginning date of search
# * `until:` - to specify the ending date of search
# The cursor can also receive other parameters, such as the language and the `tweet_mode`. If `tweet_mode='extended'`, all the text of the tweet is returned, otherwise only the first 140 characters.

# # example 
# -------------------------------------------------------------------------------------------------------------------
# ### Get all related tweets from `date:since` to `date:until(not_incluted)`
# tweets_list = tweepy.Cursor(api.search, q=['astrazeneca', 'pfizer'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()

# Greek Language = el
# tweets_list = tweepy.Cursor(api.search, q=['coffee island'],since= str(since), until=str(until),tweet_mode='extended', lang='el').items()

# English Language = en
# tweets_list = tweepy.Cursor(api.search, q=['coffee island OR CoffeeIsland'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()

#tweets_list = tweepy.Cursor(api.search, q=['astrazeneca OR pfizer'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()
# search words: coffee and Island in a tweet q= ['coffee', 'island'] or ['coffee island']
# search words: coffee and Island in a tweet q= ['coffee OR island']
tweets_list = tweepy.Cursor(api.search, q=['coffee island'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()
# -------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------
# Now we loop across the `tweets_list`, and, for each tweet, we extract the text, the creation date, the number of retweets and the favourite count. 
# We store every tweet into a list, called `output`.
# -------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------
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
    # "geo": null,"coordinates": null,"place": null,"contributors": null,
    # "is_quote_status": false,"retweet_count": 988,"favorite_count": 3875,
    # "favorited": false,"retweeted": false,"possibly_sensitive": false,"lang": "en"
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
# ---
# ### create pdf from list
pdf = pd.DataFrame(output)
print('create pdf from list')
print(pdf.shape)
print(pdf.head(2))
print(pdf.info())


# Selecting specific columns in a pandas dataframe
print('Selecting specific columns in a pandas dataframe')
print(pdf[['text', 'created_at']].head(2))
print(pdf.count())

pdf[['text', 'created_at']].groupby('created_at').first().count()

# pdf['created_at'] = pdf['created_at'].dt.date
# pdf[['text', 'created_at']].groupby('created_at').first()
# pdf[['text', 'created_at']].groupby('created_at').first().count()
# pdf[['text', 'created_at']].groupby('created_at').count()
# -------------------------------------------------------------------------------------------------------------------
# ### save and read pdf without header
# 
# `index` = By default, when your data is saved, Pandas will include your index. This can be very annoying because when you load up your data again, your index will be there as a new column. I highly recommend setting `index`= false unless you have a specific reason not to.
# 
# `header` = Say you wanted to switch your column names, then you can specify what you want your columns to be called here. This should be a list of the same length as the number of columns in your data.
print('save without header')
pdf.to_csv('output_cof_island.csv', header = False, index = False )
#df.to_csv('output.csv') #mode='a', 

pdf2 = pd.read_csv('output_cof_island.csv', names=['text',	'favourite_count',	'retweet_count','created_at'], parse_dates=['created_at',])
print('read pdf without header')
print(pdf.head(5))
print(pdf2.tail(3))
print(pdf2.shape)
print()
# -------------------------------------------------------------------------------------------------------------------
# ## save a pdf to a csv file with header

#df = pd.DataFrame(output)
print('save a pdf to a csv file with header')
pdf.to_csv('output_cof_island_with_header.csv', mode='a', header=True, index = False)

# -------------------------------------------------------------------------------------------------------------------
# ## Create a pdf from a csv file with header
# ---
# ### pdf
print('Create a pdf from a csv file with header')
pdf_cof_island2 = pd.read_csv('output_cof_island_with_header.csv', parse_dates=['created_at'])
pdf_cof_island2.head(5)

print(pdf_cof_island2.shape)
print()
print(pdf_cof_island2.info())
print('\n\n')
# -------------------------------------------------------------------------------------------------------------------
# #### Convert a pdf datetime column to date
print('Convert a pdf datetime column to date')
print(pdf2.info())
pdf2['created_at'] = pdf2['created_at'].dt.date
print(pdf2.info())
print()
print(pdf2['created_at'][0])
# pdf_cof_island2 = pd.to_csv('output_cof_island.csv', date_format='%Y-%m-%d')
# pdf_cof_island2.head(5)
print(pdf2.describe())

# -------------------------------------------------------------------------------------------------------------------
# ### def sentiment_scores 
# -------------------------------------------------------------------------------------------------------------------
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def sentiment_scores(sentance: str) -> dict :
    # Create a SentimentIntensityAnalyzer object.
    sid = SentimentIntensityAnalyzer()
    # polarity_scores method of SentimentIntensityAnalyzer
    # oject gives a sentiment dictionary.
    # which contains pos, neg, neu, and compound scores.
    r = sid.polarity_scores(sentance);
    return r

# -------------------------------------------------------------------------------------------------------------------
# ### pdf
# -------------------------------------------------------------------------------------------------------------------
# #### create a new column with sentiment_scores
print('create a new column with sentiment_scores')
#df3['rating'] = df3['text'].apply(sid.polarity_scores)
pdf2['rating'] = pdf2['text'].apply(sentiment_scores)

print(pdf2.head(2))
print(pdf.tail(2))
print(pdf2.info())
print()
# https://stackoverflow.com/questions/61608057/output-vader-sentiment-scores-in-columns-based-on-dataframe-rows-of-tweets

print(pdf2)
pdf2['negative_nltk']=[i['neg'] for i in pdf2.rating]
pdf2['positive_nltk']=[i['pos'] for i in pdf2.rating]
pdf2['neutral_nltk']=[i['neu'] for i in pdf2.rating]
pdf2['compound_nltk']=[i['compound'] for i in pdf2.rating]

print(pdf2.head(2))

pdf2['negative_nltk'] = pdf2['rating'].apply(lambda x : x['neg'])
pdf2['positive_nltk'] = pdf2['rating'].apply(lambda x : x['pos'])
pdf2['neutral_nltk'] = pdf2['rating'].apply(lambda x : x['neu'])
pdf2['compound_nltk'] = pdf2['rating'].apply(lambda x : x['compound'])

pdf2 = pdf2.drop('rating', axis=1)
print(pdf2.head(2))
print()

# -------------------------------------------------------------------------------------------------------------------
# show last's row date
# -------------------------------------------------------------------------------------------------------------------
print("show last's row date")
print(pdf2['created_at'][pdf.shape[0]-1])
print(pdf2['text'].count())
print()
print(pdf2.count())
print(pdf2.count(axis=0))
print(pdf2.shape[0])
print(len(pdf.index))
print()
# ---
# ---
# ## Pandas groupBy and aggregate functions 
# 
# `GroupBy` allows you to group rows together based off some column value, for example, you could group together sales data by the day the sale occured, or group repeast customer data based off the name of the customer.
# 
# Once you've performed the GroupBy operation you can use an aggregate function off that data.An `aggregate function` aggregates multiple rows of data into a single output, such as taking the sum of inputs, or counting the number of inputs.
# 
# **`Dataframe Aggregation`**
# 
# A set of methods for aggregations on a DataFrame:
# 
#     agg
#     avg
#     count
#     max
#     mean
#     min
#     pivot
#     sum

# ### Rename a column

# Change name of a specific column
# pdf = pdf.rename(columns={'ncompound_nltk':'compound_nltk'})
# pdf.head(2)
print(pdf.info())
print()
# ### to_datetime
# 
# When a csv file is imported and a Data Frame is made, the Date time objects in the file are read as a string object rather a Date Time object and Hence it’s very tough to perform operations like Time difference on a string rather a Date Time object. Pandas to_datetime() method helps to convert string Date time into Python Date time object.
print('to_datetime')
pdf['created_at'] = pd.to_datetime(pdf['created_at'])

print(pdf.info())
print(pdf.head(2))
print()

# The strftime() method takes one or more format codes as an argument and returns a formatted string based on it.
print('strftime')
pdf['created_at'] = pdf['created_at'].dt.strftime('%Y-%m-%d')
print(pdf.info())
print()

# For a DataFrame, by default the aggregates return results within each column:
# pdf['created_at'].dt.date => Converts Datetime to Date in Pandas df
pdf_agg_byDate =  pdf2.groupby('created_at').agg({'negative_nltk':'sum','positive_nltk':'sum','neutral_nltk':'sum','compound_nltk':'sum', 'created_at':'size'})

print(pdf_agg_byDate.count())

print()
print(pdf_agg_byDate)
print()

pdf_agg_byDate =  pdf2.groupby('created_at').agg(negative_nltk=('negative_nltk','sum'),positive_nltk=('positive_nltk','sum'),neutral_nltk=('neutral_nltk','sum'),compound_nltk=('compound_nltk','sum'), tweets = ('created_at','size'))

pdf_agg_byDate.reset_index(level=0, inplace=True)
print(pdf_agg_byDate.count())
print()

# row 0
print(type(pdf.created_at[0]))
print()

# row 5
print(type(pdf.created_at[5]))
print()

print(pdf_agg_byDate)
print()

print(pdf_agg_byDate.info())
print()

# -------------------------------------------------------------------------------------------------------------------
# ### Divide multiple columns by another column in pandas

# pdf_agg_byDate = (pdf_agg_byDate.apply(['compound_nltk']/pdf_agg_byDate['tweets']
#     .withColumn( 'positive_nltk',f.col('sum(positive_nltk)')/f.col('count(created_at)'))
#     .withColumn( 'negativen_ltk',f.col('sum(negative_nltk)')/f.col('count(created_at)'))
#     .withColumn( 'neutral_nltk',f.col('sum(neutral_nltk)')/f.col('count(created_at)'))
#     .withColumnRenamed('count(created_at)', 'tweets')).drop(*columns_to_drop

pdf_agg_byDate[['negative_nltk','positive_nltk','neutral_nltk','compound_nltk']]=    (pdf_agg_byDate[['negative_nltk','positive_nltk','neutral_nltk','compound_nltk']].divide(pdf_agg_byDate ['tweets'], axis = 'index'))

print(pdf_agg_byDate)
print()

# Sentiment_Evaluation
pdf_agg_byDate['sentiment'] = (pdf_agg_byDate['compound_nltk']
        .apply(lambda comp: 'positive' if comp > 0.05 else 'negative' if comp < -0.05 else 'neutral'))

print(pdf_agg_byDate)
print()

