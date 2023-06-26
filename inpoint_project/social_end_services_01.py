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
# authorization tokens

consumer_key = ""
consumer_secret = ""
access_token = "-"
access_token_secret = ""
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

    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
    program_start = time.time()
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()
        tweets = tweepy.Cursor(api.search, q=search_words, since=date_since, tweet_mode='extended').items(numTweets)
        tweet_list = [tweet for tweet in tweets]

        noTweets = 0
        for tweet in tweet_list:
            # Pull the values
            username = tweet.user.screen_name
            acctdesc = tweet.user.description
            location = tweet.user.location
            following = tweet.user.friends_count
            followers = tweet.user.followers_count
            totaltweets = tweet.user.statuses_count
            usercreatedts = tweet.user.created_at
            tweetcreatedts = tweet.created_at
            retweetcount = tweet.retweet_count
            hashtags = tweet.entities['hashtags']
            cnt = cnt + 1;
            try:
                text = tweet.retweeted_status.full_text
            except AttributeError:  # Not a Retweet
                text = tweet.full_text
            #print(text)
            r = sid.polarity_scores(text);

            positive_avg = (positive_avg + r['pos'])/cnt;
            negative_avg = (negative_avg + r['neg'])/cnt;
            neutral_avg = (neutral_avg + r['neu'])/cnt;


if __name__ == '__main__':
    search_words = "astrazeneca"
    date_since = "2020-11-03"
    numTweets = 20
    numRuns = 1000
    scraptweets(search_words, date_since, numTweets, numRuns)
