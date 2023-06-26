import tweepy
import json
import csv
from datetime import date
from datetime import datetime
import time
import nltk
import csv
import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import sys
import pandas as pd
from datetime import datetime

import time
from config import *
import tweepy
import datetime

import sys
import logging

logger = logging.getLogger('tweets_search')
format = "%(asctime)s - %(levelname)s - %(message)s"
# logging.basicConfig(format=format, stream=sys.stdout, level = logging.DEBUG)
logging.basicConfig(format=format, stream=sys.stdout, level = logging.INFO)


# authorization tokens

consumer_key = "pw0ihLFxH3nwDrd4HBd7pqUrc"
consumer_secret = "nh8GxSyT9ebV32pb4urtwlVnE7bxbPwCYYeVnI9TmT51Y71CDk"
access_token = "1360011857969479682-iLrxBUlqdtExwkqiN9iZsHYDXIFTZz"
access_token_secret = "fccgx7QK05sXrURyzcCAPDtZOvEfHtOdo7G5sXHjVshdm"

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)


def scraptweets(search_words, date_since, numTweets, numRuns):
    positive_avg = 0;
    negative_avg = 0;
    neutral_avg = 0;
    cnt = 0;
    output = []
    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
    program_start = time.time()
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()
        tweets = tweepy.Cursor(api.search, q=search_words, since=date_since, tweet_mode='extended', lang='en').items(numTweets)
        for tweet in tweets:
            logger.info(f"tweet_id_str: {'-'*30}")
            logger.info(f"created_at: {tweet.created_at}")
            logger.info(f"full_text: {tweet._json['full_text']}")
        # print(tweet_list)
        noTweets = 0
        # for tweet in tweet_list:
        #     # Pull the values
        #     username = tweet.user.screen_name
        #     acctdesc = tweet.user.description
        #     location = tweet.user.location
        #     following = tweet.user.friends_count
        #     followers = tweet.user.followers_count
        #     totaltweets = tweet.user.statuses_count
        #     usercreatedts = tweet.user.created_at
        #     tweetcreatedts = tweet.created_at
        #     retweetcount = tweet.retweet_count
        #     hashtags = tweet.entities['hashtags']
        #     cnt = cnt + 1



if __name__ == '__main__':
    search_words = "astrazeneca"
    date_since = "2020-11-03"
    numTweets = 20
    numRuns = 2
    a = scraptweets(search_words, date_since, numTweets, numRuns)
    print (a)