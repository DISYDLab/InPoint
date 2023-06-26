# # Get Tweets
# 
# This script extracts all the tweets with hashtag #covid-19 related to the day before today (yesterday) and saves them into a .csv file.
# We use the `tweepy` library, which can be installed with the command `pip install tweepy`.
# 
# Firstly, we import the configuration file, called `config.py`, which is located in the same directory of this script.
from __future__ import annotations
import os
jv = os.environ.get('JAVA_HOME', None)
import sys, glob, os
sys.path

import mypy
from config import *
import tweepy
import datetime

import logging

logger = logging.getLogger('tweets_search')

import pandas as pd
# import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# print(f"logger.root.level = {logger.root.level}, logger.root.name = {logger.root.name}")
# print(f"logger.name = {logger.name}")

# format = "%(asctime)s - %(levelname)s - %(message)s"
# logging.basicConfig(format=format, stream=sys.stdout, level = logging.DEBUG)
logging.basicConfig(format=format, stream=sys.stdout, level = logging.INFO)

# print(logger.root.level)
# logger.root.level = 10
# print(logger.root.level)

# We setup the connection to our Twitter App by using the `OAuthHandler()` class and its `access_token()` function. Then we call the Twitter API through the `API()` function.
auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify = True)

# api.me()
# api.rate_limit_status()

# ## setup dates (recent 7 days max)
# 
# 
# If today is 2021-06-26 then :
# 
# 1. `time_frame = {timedelta:'2'}` (we get tweets from 2021-0-24 up to 2021-06-25 (today - 1 day))
# 2. `time_frame = {since:'2021-06-23', timedelta:'2'}` 
# 3. `time_frame = {until:'2021-06-25', timedelta:'2'}` (2 & 3 & 4 expressions are equivalent)
# 4. `time_frame = {since:'2021-06-23', until:'2021-06-25'}` -> we get tweets from 2021-06-23 up to 2021-06-24
# 
# `note:` from today we can get a time_frame of 7 days max, i.e since 2021-06-19

today = datetime.date.today()
since= today - datetime.timedelta(days=1)
until= today
until, since

logger.warning(f"full_text: '{until, since}'")

def time_frame(until=None, since=None, timedelta=0):
    print(until, since, timedelta)
    if isinstance(timedelta, str):
        print(timedelta)
        try:
            timedelta = int(timedelta)
            logger.warning(f"timedelta: '{timedelta}'")
        except (ValueError, TypeError):
            print(f'ValueError: "timedelta = {timedelta}". It must be an int')
    # print((timedelta))
    # if isinstance(int(timedelta), int):
    #     print(timedelta)
    #     if (1 <= timedelta <=7) and since==None:
    #         today = datetime.date.today()
    #         since= today - datetime.timedelta(days=timedelta)
    #         until= today
    #         logger.warning(f"time_frame: '{until, since}'") # until, since
            
               
    # today = datetime.date.today()
    # since= today - datetime.timedelta(days=1)
    # until= today
    # until, since
# (datetime.date(2021, 6, 7), datetime.date(2021, 6, 6))

timedelta = '2'
print(timedelta)
time_frame(timedelta = timedelta)
# print(timedelta)
# time_frame(timedelta = 'a')

# ### type checking with mypy

# def headline(text: str, align: bool = True) -> str:
#     if not align:
#         return f"{text.title()}\n{'-' * len(text)}"
#     else:
#         return f" {text.title()} ".center(50, "o")

# print(headline("python type checking", False))
# print(headline("use mypy", align="center"))

# help(headline)
# from __future__ imports must occur at the beginning of the file
# from __future__ import annotations
from typing import List, Set, Dict, Tuple, Text, Optional, AnyStr
import mypy 
def time_frame1(until:datetime=None, since:datetime=None, timedelta:int=0) -> tuple:
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
time_frame1(since= today - datetime.timedelta(days=3), timedelta=2)

# until:datetime = None
until:datetime = datetime.date.today()
since:datetime=None
timedelta=0

if until is  None:
    print('yes is None')

if isinstance(until, datetime.date):
    print('yes date')

# help(time_frame1)

until, since = time_frame1(timedelta=2)
# print(until, since)

logger.debug(f"full_text: '{until, since}'")
# We search for tweets on Twitter by using the `Cursor()` function. 
# We pass the `api.search` parameter to the cursor, as well as the query string, which is specified through the `q` parameter of the cursor.
# The query string can receive many parameters, such as the following (not mandatory) ones:
# * `from:` - to specify a specific Twitter user profile
# * `since:` - to specify the beginning date of search
# * `until:` - to specify the ending date of search
# The cursor can also receive other parameters, such as the language and the `tweet_mode`. If `tweet_mode='extended'`, all the text of the tweet is returned, otherwise only the first 140 characters.

# # example 
# code tweets = tweepy.Cursor(api.search, tweet_mode=’extended’) 
# for tweet in tweets:
#     content = tweet.full_text

# tweets_list = tweepy.Cursor(api.search, q="#Covid-19 since:" + str(yesterday)+ " until:" + str(today),tweet_mode='extended', lang='en').items()

# tweets_list = tweepy.Cursor(api.search, q=f"#Covid-19 since:{str(yesterday)} until:{str(today)}",tweet_mode='extended', lang='en').items()

# ### Get all related tweets from `date:since` to `date:until(not_incluted)`

# tweets_list = tweepy.Cursor(api.search, q=['astrazeneca', 'pfizer'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()

# Greek Language = el
# tweets_list = tweepy.Cursor(api.search, q=['coffee island'],since= str(since), until=str(until),tweet_mode='extended', lang='el').items()

# English Language = en
# tweets_list = tweepy.Cursor(api.search, q=['coffee island OR CoffeeIsland'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()

tweets_list = tweepy.Cursor(api.search, q=['coffee Island OR CoffeeIsland'],since= str(since), until=str(until),tweet_mode='extended', lang='en').items()

# Now we loop across the `tweets_list`, and, for each tweet, we extract the text, the creation date, the number of retweets and the favourite count. We store every tweet into a list, called `output`.

import time
start = time.time()
output = []
for tweet in tweets_list:
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

logger.info(f"output: '{output}'")
logger.info(f"output_length: '{len(output)}'")

pdf = pd.DataFrame(output)

logger.info(f"pdf.shape: '{pdf.shape}'")
logger.info(f"pdf.head(2): '{pdf.head(2)}'")
print(pdf.head(2))