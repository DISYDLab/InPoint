# Import all needed libraries
import pandas as pd
import tweepy
import json
import csv
from neo4j import GraphDatabase
from datetime import date
from datetime import datetime
import time
from multiprocessing import Process


# authorization tokens
consumer_key = "pw0ihLFxH3nwDrd4HBd7pqUrc"
consumer_secret = "nh8GxSyT9ebV32pb4urtwlVnE7bxbPwCYYeVnI9TmT51Y71CDk"
access_token = "1360011857969479682-iLrxBUlqdtExwkqiN9iZsHYDXIFTZz"
access_token_secret = "fccgx7QK05sXrURyzcCAPDtZOvEfHtOdo7G5sXHjVshdm"

# Connect to Twitter API using the secrets
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)



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

    LIMIT = 0;


    while len(new_tweets) > 0 and LIMIT < 2000:
        print("getting tweets before %s" % (oldest))
        new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
        alltweets.extend(new_tweets)
        oldest = alltweets[-1].id - 1
        print("...%s tweets downloaded so far" % (len(alltweets)))
        LIMIT = LIMIT + 200;

    outtweets = [[tweet.id_str, tweet.created_at, tweet.text, tweet.favorite_count, tweet.in_reply_to_screen_name, tweet.retweeted] for tweet in alltweets]
    with open('/home/bill/Desktop/tweets.csv', 'w') as f: #BILL, change this path
        writer = csv.writer(f)
        writer.writerow(["id","created_at","text","likes","in reply to","retweeted"])
        writer.writerows(outtweets)
        pass

def scraptweets(TARGETUSER, search_words, date_since, numTweets, numRuns):


    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
    program_start = time.time()
    search_words = search_words.split(',');
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()

        # Collect tweets using the Cursor object
        # .Cursor() returns an object that you can iterate or loop over to access the data collected.
        # Each item in the iterator has various attributes that you can access to get information about each tweet
        tweets = tweepy.Cursor(api.user_timeline, screen_name = TARGETUSER, since=date_since, tweet_mode='extended').items(numTweets)
        #tweets = api.search("coffee from:CoffeeIsland_GR")
        # Store these tweets into a python list
        tweet_list = [tweet for tweet in tweets]
        # Obtain the following info (methods to call them out):
        # user.screen_name - twitter handle
        # user.description - description of account
        # user.location - where is he tweeting from
        # user.friends_count - no. of other users that user is following (following)
        # user.followers_count - no. of other users who are following this user (followers)
        # user.statuses_count - total tweets by user
        # user.created_at - when the user account was created
        # created_at - when the tweet was created
        # retweet_count - no. of retweets
        # (deprecated) user.favourites_count - probably total no. of tweets that is favourited by user
        # retweeted_status.full_text - full text of the tweet
        # tweet.entities['hashtags'] - hashtags in the tweet
        # Begin scraping the tweets individually:
        noTweets = 0
        mytweets = []
        for tweet in tweet_list:

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
            try:
                text = tweet.retweeted_status.full_text
            except AttributeError:  # Not a Retweet
                text = tweet.full_text

            if any(word in text for word in search_words):
                ith_tweet = [username, acctdesc, location, following, followers, totaltweets,
                             usercreatedts, tweetcreatedts, retweetcount, text, hashtags]
                # Append to dataframe - db_tweets
                db_tweets.loc[len(db_tweets)] = ith_tweet
                #print(text)
                mytweets.append(text)
                # increase counter - noTweets
            noTweets += 1

        # Run ended:
        end_run = time.time()
        duration_run = round((end_run-start_run)/60, 2)

        print('no. of tweets scraped for run {} is {}'.format(i + 1, noTweets))
        print('time take for {} run to complete is {} mins'.format(i+1, duration_run))

        #time.sleep(920) #15 minute sleep time
        return '---'.join(mytweets);



def get_followers(TARGETUSER, search_words):
    print('TARGETUSER = ', TARGETUSER)
    user = api.get_user(TARGETUSER)
    ID = user.id_str

    insert_node(TARGETUSER, ID, search_words)

    ids = []
    for page in tweepy.Cursor(api.followers_ids, screen_name=TARGETUSER).pages():
        ids.extend(page)
        #break;
        time.sleep(60)

    allfriends = [];
    print('ids = ', ids)
    start = 0;
    end = 0;
    while (end < len(ids)) and (end < 100):
        end = end + 10;
        if (end > len(ids)):
            end = len(ids);

        done = False;
        while (not done):
            try:
                friends = [[user.screen_name, user.id] for user in api.lookup_users(user_ids=ids[start:end])]
                done = True;
            except:
                print('twitter is stalling')
                time.sleep(10);
        print('end = ', end)
        allfriends.extend(friends);
        start = end;

    return allfriends;

def create_graph(TARGETUSER, d, search_words):
    if (d > 0):
        user = api.get_user(TARGETUSER)
        ID = user.id_str
        #search_words = "καφές, ζάχαρη";

        insert_node(TARGETUSER, ID, search_words)
        friends = get_followers(TARGETUSER, search_words);
        print('friends = ', friends)
        for name, idf in friends:
            print(name, idf)
            insert_node(name, idf, search_words)
            create_connection(int(ID), int(idf));
            p = Process(target=create_graph, args=(name,d-1, search_words, ));
            p.start()
            p.join()
            #create_graph(name, d-1);
            #if ('eellak' not in name):
            #    friends = get_followers(name);
    else:
        return 0;

def get_friends(TARGETUSER):
    friends_list = []
    userid = list((api.followers_ids(user_id = 'TARGETUSER')));
    #f = open('/home/bill/Desktop/friends.csv', 'w'); #BILL, change this path
    #writer = csv.writer(f)
    #writer.writerow(["follower"])
    for user in tweepy.Cursor(api.friends, user_id ='TARGETUSER').items():
        user_id = list((api.get_user(user_id = 'TARGETUSER')));
        for user in tweepy.Cursor(api.followers_ids, user_id = 'TARGETUSER').items():
            insert_node(api.followers_ids, user_id = 'TARGETUSER');
            print('friend has followers: ' + user.screen_name);
            create_connection()
        #writer.writerow([user.screen_name]);


def todays_stats(dict_name):
    info = api.me()
    followers_cnt = info.followers_count
    following_cnt = info.friends_count
    today = date.today()
    d = today.strftime("%b %d, %Y")
    if d not in dict_name:
        dict_name[d] = {"followers":followers_cnt, "following":following_cnt}

    else:
        print('Today\'s stats already exist')

    print(dict_name)


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

#create database connection and insert node function

def insert_node(nodename, nodeid, search_words):
    conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="1234")
    print("Connection established-listening to port: 7687")
    qexist = "MATCH (u:User3) WHERE u.myid = '" + str(nodeid) + "' RETURN u";
    print(qexist)


    date_since = "2015-11-03"
    numTweets = 2500
    numRuns = 1
    # Call the function scraptweets
    mytweets = scraptweets(nodename, search_words, date_since, numTweets, numRuns)
    print('mytweets = ', mytweets)


    qe = conn.query(qexist);
    print(qe)
    if not qe:
        q = "CREATE (n:User3 {name: '" + str(nodename) + "', myid: '" + str(nodeid) + "', mytweets: '" + mytweets  + "' })";
        #CREATE (n:Person {name: 'Andy', title: 'Developer'})
        print(q)
        a = conn.query(q);
        return a;
    else:
        return -1;

#insert_node("george", 460);

#create relationships with specific ids

def create_connection(nodestartid, nodeendid):
    conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="1234")
    print("Connection established-listening to port: 7687")
    q1 = "MATCH (a:User3), (b:User3) WHERE a.myid = '" + str(nodestartid) + "' AND b.myid = '" + str(nodeendid) + "' CREATE (a)-[r:Followed_by]->(b) RETURN type(r)";
    print(q1)
    b = conn.query(q1);
    return b;

#create_connection(452, 455);



if __name__ == '__main__':
    #create_connection(460, 3997011574)
    #get_all_tweets("vtsakan")
    search_words = "καφές, ζάχαρη";
    p = Process(target=create_graph, args=('CoffeeIsland_GR',3, search_words, ));
    p.start()
    #p.join()


    #date_since = "2015-11-03"
    #numTweets = 2500
    #numRuns = 1
    # Call the function scraptweets
    #scraptweets(search_words, date_since, numTweets, numRuns)
