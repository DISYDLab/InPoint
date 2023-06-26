# -----------------------------------------------------------------------------------------------------
import configparser
# -----------------------------------------------------------------------------------------------------

# Tweepy is an open source Python package that gives you a very convenient way to access the Twitter API with Python.
# Tweepy includes a set of classes and methods that represent Twitter's models and API endpoints, and it transparently
# handles various implementation details, such as: Data encoding and decoding.
import tweepy
# -----------------------------------------------------------------------------------------------------
# The sys module provides functions and variables used to manipulate different parts of the
#  Python runtime environment.
import sys
# -----------------------------------------------------------------------------------------------------
# The logging module provides a way for applications to configure different log handlers and a way of routing log
# messages
# to these handlers. This allows for a highly flexible configuration that can deal with a lot of different use cases.
import logging
# Python logging levels
# Levels are used for identifying the severity of an event. There are six logging levels:
#   • CRITICAL || 50
#   • ERROR    || 40
#   • WARNING  || 30
#   • INFO     || 20
#   • DEBUG    || 10
#   • NOTSET   || 0
# If the logging level is set to WARNING, all WARNING, ERROR, and CRITICAL messages are written to the log file or
# console.
# If it is set to ERROR, only ERROR and CRITICAL messages are logged.
# The default level is WARNING, which means that only events of this level and above will be tracked, unless the logging
# package is configured to do otherwise.

# -----------------------------------------------------------------------------------------------------
import time
# -----------------------------------------------------------------------------------------------------
from deep_translator import GoogleTranslator
# -----------------------------------------------------------------------------------------------------


def set_twitter_api(twitter_tokens, logger):
    # ConfigParser is a Python class which implements a basic configuration language for Python
    # programs. It provides a structure similar to Microsoft Windows INI files. ConfigParser
    # allows to write Python programs which can be customized by end users easily.

    # The configuration file consists of sections followed by key/value pairs of options. The
    # section names are delimited with [] characters. The pairs are separated either with : or =.
    #  Comments start either with # or with ;.
    config = configparser.RawConfigParser()
    config.read(filenames=twitter_tokens)
    print(f'config-sections: {config.sections()}')

    # creating 4 variables and assigning them basically saying read these 4 keys from file and assign
    consumer_key = config.get('twitter', 'consumer_key')
    consumer_secret = config.get('twitter', 'consumer_secret')
    access_token = config.get('twitter', 'access_token')
    access_token_secret = config.get('twitter', 'access_token_secret')
    
    # --------------------------------
    # Testing authentication failure
    # access_token_secret = 1
    # --------------------------------
    # Creating the authentication object to Authenticate to Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    # Setting your access token and secret
    auth.set_access_token(access_token, access_token_secret)

    # Create API object - . You can use the API to read and write information related to
    # Twitter entities such as tweets, users, and trends.
    # The Twitter API uses OAuth, a widely used open authorization protocol, to authenticate
    # all the requests.
    api = tweepy.API(auth, wait_on_rate_limit=True)
    try:
        api.verify_credentials()
    except:
        logger.error(f"Error during authentication")
        sys.exit(1)
    else:
        return api


def check_tweets_list(potential_full: list) -> bool:
    if not potential_full:
        # Try to convert argument into a float
        print("list is  empty")
        return False
    else:
        print("list is  not empty")
        return True

def tweets_list_output(tweets_list, logger):
    start = time.time()
    output = []
    for tweet in tweets_list:
        if tweet._json['full_text'].startswith("RT @"):
            text = tweet.retweeted_status.full_text
        else:
            text = tweet._json["full_text"]
        # print(text)
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets

        logger.debug(f"full_text: '{text}'")
        favourite_count = tweet.favorite_count
        retweet_count = tweet.retweet_count
        created_at = tweet.created_at

        line = {'text': text, 'favourite_count': favourite_count,
                'retweet_count': retweet_count, 'created_at': created_at}
        output.append(line)
        logger.info(f"Append list length : {len(output)}")
    end = time.time()
    logger.info(f"elapsed_time: '{end - start}'")
    print(output[:1])
    print(f'output_length = {len(output)}')
    return output


def translate_text(text):
    text = GoogleTranslator(source='auto', target='en').translate(text)
    return text