# Import all needed libraries

import tweepy
import json
import csv
from datetime import date
from datetime import datetime
import time

# authorization tokens
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

# Connect to Twitter API using the secrets
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

# Helper function to save data into a JSON file
# file_name: the file name of the data
# file_content: the data you want to save

#def save_json(file_name, file_content):
    #with open(path + file_name, 'w', encoding='utf-8') as f:
        #json.dump(file_content, f, ensure_ascii=False, indent=4)

# Helper function to handle twitter API rate limit

def limit_handled(cursor, list_name):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print("\nData points in list = {}".format(len(list_name)))
            print('Hit Twitter API rate limit.')
            for i in range(3, 0, -1):
                print("Wait for {} mins.".format(i * 5))
                time.sleep(5 * 60)
        except tweepy.error.TweepError:
            print('\nCaught TweepError exception' )

def get_all_tweets(screen_name):
    alltweets = []
    new_tweets = api.user_timeline(screen_name = screen_name,count=200)
    alltweets.extend(new_tweets)
    oldest = alltweets[-1].id - 1
    while len(new_tweets) > 0:
        print("getting tweets before %s" % (oldest))
        new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
        alltweets.extend(new_tweets)
        oldest = alltweets[-1].id - 1
        print("...%s tweets downloaded so far" % (len(alltweets)))
    outtweets = [[tweet.id_str, tweet.created_at, tweet.text, tweet.favorite_count, tweet.in_reply_to_screen_name, tweet.retweeted] for tweet in alltweets]
    with open('/home/bill/tweets2.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(["id","created_at","text","likes","in reply to","retweeted"])
        writer.writerows(outtweets)
        pass

def get_followers():
    followers_list = []
    cursor = tweepy.Cursor(api.followers, count=200).pages()
    for i, page in enumerate(limit_handled(cursor, followers_list)):
        print("\r"+"Loading"+ i % 5 *".", end='')
        followers_list += page
        followers_list = [x._json for x in followers_list]
        
def save_json(followers_data, followers_list):
    with open('/home/bill/followers_data.json', 'w', encoding='utf-8') as f:
        json.dump(followers_list, f, ensure_ascii=False, indent=4)

def get_friends():
    friends_list = []
    cursor = tweepy.Cursor(api.friends, count=200).pages()
    for i, page in enumerate(limit_handled(cursor, friends_list)):
        print("\r"+"Loading"+ i % 5 *".", end='')
        friends_list += page
        friends_list = [x._json for x in friends_list]
        
def save_json(friends_data, friends_list):
    with open('/home/bill/friends_data.json', 'w', encoding='utf-8') as f:
        json.dump(friends_list, f, ensure_ascii=False, indent=4)
     
def todays_stats(dict_name):
    info = api.me()
    followers_cnt = info.followers_count
    following_cnt = info.friends_count
    today = date.today()
    d = today.strftime("%b %d, %Y")
    if d not in dict_name:
        dict_name[d] = {"followers":followers_cnt, "following":following_cnt}
        save_json("follower_history.json", dict_name)
    else:
        print('Today\'s stats already exist')

#Data Collection main script
if __name__ == '__main__':
    get_all_tweets("Activision")
    #with open('/home/bill/follower_history.json', 'w', encoding='utf-8') as json_file:
      #history = json.load(json_file)
    #todays_stats(history)
    get_followers()
    get_friends()
