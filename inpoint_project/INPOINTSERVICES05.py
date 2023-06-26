import csv
from parsel import Selector
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup, SoupStrainer
import flask
from flask import request, jsonify, Response
import json
from multiprocessing import Process, Queue, Value
import time
import urllib.request
from bs4.element import Comment
import httplib2
from urllib.parse import urlparse
import pymongo
import nltk
from nltk.corpus import stopwords
import re
import string
from collections import Counter
from flask import render_template
import networkx as nx
import random
#from sklearn.fesudo import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.util import ngrams, bigrams, trigrams
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
import matplotlib.pyplot as plt
from nltk.tokenize import sent_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import tweepy
import pandas as pd
from neo4j import GraphDatabase
from linkedin_scraper import Person, Company, actions
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from deep_translator import GoogleTranslator
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bson.json_util import dumps
import matplotlib.pyplot as plt
import io
import numpy as np
import textstat


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
from deep_translator import GoogleTranslator
from flask_cors import CORS

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()

nltk.download("stopwords")
nltk.download('punkt')

global startingpoint;

pqueue = Queue();


# authorization tokens
#consumer_key = "pw0ihLFxH3nwDrd4HBd7pqUrc"
#consumer_secret = "nh8GxSyT9ebV32pb4urtwlVnE7bxbPwCYYeVnI9TmT51Y71CDk"
#access_token = "1360011857969479682-iLrxBUlqdtExwkqiN9iZsHYDXIFTZz"
#access_token_secret = "fccgx7QK05sXrURyzcCAPDtZOvEfHtOdo7G5sXHjVshdm"
#consumer_key = "f9n1D1rcKgULFRTKdelDUOtz2"
#consumer_secret = "WrcBiZ2l1RXumwNE7IkpyV6Yj7bjuPteUj47FgswkLlSvISnSK"
#access_token = "1415070882683293707-fZTh0jLU9opO5WKzGJW6vVkNKVvrRf"
#access_token_secret = "FnhwJ8YMkubAfZz8tVZBd0CRcnlzf0G4PPt0qFlnEOrIE"

# Connect to Twitter API using the secrets
#auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
#auth.set_access_token(access_token, access_token_secret)

#api = tweepy.API(auth, wait_on_rate_limit=True)



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
    conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="11111111")
    print("Connection established-listening to port: 7687")
    qexist = "MATCH (u:User8) WHERE u.myid = '" + str(nodeid) + "' RETURN u";
    #print(qexist)


    date_since = "2015-11-03"
    numTweets = 2500
    numRuns = 1
    # Call the function scraptweets
    mytweets, myfulltweets = scraptweets(nodename, search_words, date_since, numTweets, numRuns)
    #print('mytweets = ', mytweets)
    SQNEG01 = [];
    SQNEU01 = [];
    SQPOS01 = [];
    SQCOM01 = [];
    totalretweets = 0;
    for t in myfulltweets:
        totalretweets = totalretweets + int(t.retweet_count)
    print('totalretweets = ', totalretweets)

    mytweetstext = ""
    for t in mytweets:
        mytweetstext = mytweetstext + t;
        r = sid.polarity_scores(t);
        SQNEG01.append(r['neg']);
        SQNEU01.append(r['neu']);
        SQPOS01.append(r['pos']);
        SQCOM01.append(r['compound']);
        print(r)

    if (len(SQNEG01) > 0):
        SENTOUT = 'neg=' + str(sum(SQNEG01)/len(SQNEG01)) + ' - neu=' + str(sum(SQNEU01)/len(SQNEU01)) + ' - pos=' + str(sum(SQPOS01)/len(SQPOS01)) + ' - com=' + str(sum(SQCOM01)/len(SQCOM01))
    else:
        SENTOUT = '-'
    print(SENTOUT)


    qe = conn.query(qexist);
    print(qe)
    if not qe:
        q = "CREATE (n:User8 {name: '" + str(nodename) + "', myid: '" + str(nodeid) + "', totalretweets: '"+str(totalretweets)+"', SENTOUT: '" + SENTOUT  + "', mytweetstext: '" +str(mytweetstext)+ "' })";
        #print(q)
        #CREATE (n:Person {name: 'Andy', title: 'Developer'})
        print(q)
        a = conn.query(q);
        return a;
    else:
        return -1;

#insert_node("george", 460);

#create relationships with specific ids

def create_connection(nodestartid, nodeendid):
    conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="11111111")
    print("Connection established-listening to port: 7687")
    q1 = "MATCH (a:User8), (b:User8) WHERE a.myid = '" + str(nodestartid) + "' AND b.myid = '" + str(nodeendid) + "' CREATE (a)-[r:Followed_by]->(b) RETURN type(r)";
    #print(q1)
    b = conn.query(q1);
    return b;


def scraptweets(TARGETUSER, search_words, date_since, numTweets, numRuns):
    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
    tweet_list = []
    program_start = time.time()
    search_words = search_words.split(',');
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()

        # Collect tweets using the Cursor object
        # .Cursor() returns an object that you can iterate or loop over to access the data collected.
        # Each item in the iterator has various attributes that you can access to get information about each tweet
        #tweets = tweepy.Cursor(api.user_timeline, screen_name = TARGETUSER, since=date_since, tweet_mode='extended').items(numTweets)

        try:
            for tweet in api.user_timeline(screen_name = TARGETUSER, since=date_since, tweet_mode='extended'):
                tweet_list.append(tweet)
            #tweets = api.search("coffee from:CoffeeIsland_GR")
            # Store these tweets into a python list
            #tweet_list = [tweet for tweet in tweets]
            #print(tweet_list)
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
                    translated = GoogleTranslator(source='auto', target='en').translate(text)

                    mytweets.append(translated)
                    # increase counter - noTweets
                noTweets += 1

            # Run ended:
            end_run = time.time()
            duration_run = round((end_run-start_run)/60, 2)

            print('no. of tweets scraped for run {} is {}'.format(i + 1, noTweets))
            print('time take for {} run to complete is {} mins'.format(i+1, duration_run))

            #time.sleep(920) #15 minute sleep time
            return [mytweets, tweet_list];
        except:
            return [[],[]]


def get_followers(TARGETUSER, search_words, api):
    #print('TARGETUSER = ', TARGETUSER)

    #consumer_key = "pw0ihLFxH3nwDrd4HBd7pqUrc"
    #consumer_secret = "nh8GxSyT9ebV32pb4urtwlVnE7bxbPwCYYeVnI9TmT51Y71CDk"
    #access_token = "1360011857969479682-iLrxBUlqdtExwkqiN9iZsHYDXIFTZz"
    #access_token_secret = "fccgx7QK05sXrURyzcCAPDtZOvEfHtOdo7G5sXHjVshdm"
    #consumer_key = "f9n1D1rcKgULFRTKdelDUOtz2"
    #consumer_secret = "WrcBiZ2l1RXumwNE7IkpyV6Yj7bjuPteUj47FgswkLlSvISnSK"
    #access_token = "1415070882683293707-fZTh0jLU9opO5WKzGJW6vVkNKVvrRf"
    #access_token_secret = "FnhwJ8YMkubAfZz8tVZBd0CRcnlzf0G4PPt0qFlnEOrIE"
    #nltk.download('vader_lexicon')
    #sid = SentimentIntensityAnalyzer()

    #auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    #auth.set_access_token(access_token, access_token_secret)

    #api = tweepy.API(auth, wait_on_rate_limit=True)


    #user = api.get_user(TARGETUSER)
    #ID = user.id_str

    #insert_node(TARGETUSER, ID, search_words)

    ids = []
    #for page in tweepy.Cursor(api.followers_ids, screen_name=TARGETUSER).pages():
    #    ids.extend(page)
    #    #break;
    #    time.sleep(60)
    ids = api.followers_ids(TARGETUSER)
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

def create_graph(TARGETUSER, d, search_words, q):

    G = nx.DiGraph()
    consumer_key = "pw0ihLFxH3nwDrd4HBd7pqUrc"
    consumer_secret = "nh8GxSyT9ebV32pb4urtwlVnE7bxbPwCYYeVnI9TmT51Y71CDk"
    access_token = "1360011857969479682-iLrxBUlqdtExwkqiN9iZsHYDXIFTZz"
    access_token_secret = "fccgx7QK05sXrURyzcCAPDtZOvEfHtOdo7G5sXHjVshdm"
    #consumer_key = "f9n1D1rcKgULFRTKdelDUOtz2"
    #consumer_secret = "WrcBiZ2l1RXumwNE7IkpyV6Yj7bjuPteUj47FgswkLlSvISnSK"
    #access_token = "1415070882683293707-fZTh0jLU9opO5WKzGJW6vVkNKVvrRf"
    #access_token_secret = "FnhwJ8YMkubAfZz8tVZBd0CRcnlzf0G4PPt0qFlnEOrIE"
    nltk.download('vader_lexicon')
    sid = SentimentIntensityAnalyzer()

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)
    #G.add_edge('A', 'B')

    if (d > 0):
    	#try:
        TARGETUSER="523640242"#CoffeeIsland_GR
        user = api.get_user(TARGETUSER)
        ID = user.id_str
        #search_words = "καφές, ζάχαρη";

        insert_node(TARGETUSER, ID, search_words)
        G.add_edge(str(TARGETUSER), str(ID))

        friends = get_followers(TARGETUSER, search_words, api);
        print('friends = ', friends)
        for name, idf in friends:
            print(name, idf)
            #insert_node(name, idf, search_words)
            G.add_edge(str(ID), str(name))
            #create_connection(int(ID), int(idf));
            #p = Process(target=create_graph, args=(name,d-1, search_words, ));
            #p.start()
            #p.join()
            #create_graph(name, d-1);
            #if ('eellak' not in name):
            #    friends = get_followers(name);


    q.put(G)

    return 0;


def get_jaccard_sim(str1, str2):
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))

def insertcommunities2mongo(communities):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    #mytext = mytext.split('/n')

    #dblist = myclient.list_database_names()
    #if "inpoint" in dblist:
    #    print("The database exists.")S
    mycollection = mydb['communities'];
    mydict = { "content": communities }
    x = mycollection.insert_one(mydict);


    #nltk_tokens = word_tokenize(x)

    return x;

def insert2mongo(mycoll, myurl, mykeywords, mytext):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    #mytext = mytext.split('/n')

    #dblist = myclient.list_database_names()
    #if "inpoint" in dblist:
    #    print("The database exists.")
    print('mycoll = ', mycoll)
    mycollection = mydb[mycoll];
    collist = mydb.list_collection_names();
    mydict = { "mykeywords": mykeywords, "content": mytext }
    x = mycollection.insert_one(mydict);


    #nltk_tokens = word_tokenize(x)

    return x;

def insertSentiment2mongo(requestid, searchwords, positive_avg, negative_avg, neutral_avg):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    mycollection = mydb['tweeter_sentiment_' + searchwords];
    collist = mydb.list_collection_names();
    mydict = { "requestid":requestid,"searchwords":searchwords, "time": datetime.now(), "sentiment": [positive_avg, negative_avg, neutral_avg] }
    x = mycollection.insert_one(mydict);

    return x;

def getSentiment(requestid, searchwords):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    mycollection = mydb['tweeter_sentiment_' + searchwords];
    collist = mydb.list_collection_names();

    myquery = { "requestid": requestid }
    mydoc = mycollection.find(myquery)

    res = []
    for x in mydoc:
        res.append(x)

    print(res)

    return res;


def insertTweet2mongo(requestid, searchwords, positive, negative, neutral, text):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    mycollection = mydb['tweeter_sentiment_' + searchwords];
    collist = mydb.list_collection_names();
    mydict = { "requestid":requestid,"searchwords":searchwords, "time": datetime.now(), "sentiment": [positive, negative, neutral], "ttext":text }
    x = mycollection.insert_one(mydict);
    return x;


def selectfrommongo(mycoll, myurl ):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    #dblist = myclient.list_database_names()
    #if "inpoint" in dblist:
    #    print("The database exists.")S
    mycollection = mydb[mycoll];
    print('mycollection = ', mycollection)
    collist = mydb.list_collection_names();
    print(collist)

    collec = mycollection.find()



    #nltk_tokens = word_tokenize(x)

    return collec;

def getRank(myurl):
    domain = urlparse(myurl).netloc
    print('https://www.alexa.com/siteinfo/'+domain) # --> www.example.test
    html = urllib.request.urlopen('https://www.alexa.com/siteinfo/'+domain).read()
    soup = BeautifulSoup(html, 'html.parser');
    mydivs = soup.find_all("div", {"class": "rankmini-rank"})
    c = 0;
    try:
        for r in mydivs[0]:
            c = c + 1;
            if c == 3:
                r = r.replace(",", ".")
                return float(r);
    except:
        return 0;

app = flask.Flask(__name__)
CORS(app)
app.config["DEBUG"] = True

def monitorwebpage(myurl):
    print('monitor', myurl);
    while (True):
        html = urllib.request.urlopen(myurl).read()
        previous = text_from_html(html);
        time.sleep(30);
        now = text_from_html(html);
        if (now != previous):
            print('[',myurl,']', 'CHANGES DETECTED')
        else:
            print('[',myurl,']', 'NO CHANGES');

    print('end')
    return 0;

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)



def commonwords(docs):


    # Flatten the list of lists into a single list of documents
    all_docs = [doc[0] for doc in docs if doc]  # This only includes sublists that have at least one element

    # Concatenate all documents into one string:
    all_docs_str = " ".join(all_docs)

    # Now, let's tokenize the string into words:
    words = word_tokenize(all_docs_str)


    # We might want to remove the common words (a.k.a stopwords) and punctuation
    # nltk.download('stopwords') # Uncomment this line if you haven't downloaded the stopwords
    # nltk.download('punkt') # Uncomment this line if you haven't downloaded the punkt tokenizer

    stop_words = set(stopwords.words('english'))
    stop_words.update([".", ",", "!", "?", ")", "(", "^", "&", "@", "*", "[", "]", ":", "''", "$", ";", "}", "{"]) # We add some punctuation to the stopword list

    filtered_words = [word for word in words if word.casefold() not in stop_words]

    # Now, let's get the 10 most common words:
    word_counts = Counter(filtered_words)
    common_words = word_counts.most_common(10)
    return common_words

def getPAGEINFO(startingpoint, myurl, mykeywords):
    print(myurl)
    try:
        html = urllib.request.urlopen(myurl, timeout=10).read()
        print(text_from_html(html));
        #insert2mongo(startingpoint, myurl, mykeywords, text_from_html(html));
        #mdoc = getDocumentAnalytics(myurl);

        return [text_from_html(html)];
    except:
        return []


def getINFO(myurl, mykeywords):


    print(myurl)
    html = urllib.request.urlopen(myurl).read()
    print(text_from_html(html));
    global startingpoint;
    insert2mongo(startingpoint, myurl, mykeywords, text_from_html(html));
    mdoc = getDocumentAnalytics(myurl);
    print('mdoc = ', mdoc)

    parser = 'html.parser'  # or 'lxml' (preferred) or 'html5lib', if installed
    resp = urllib.request.urlopen(myurl)
    soup = BeautifulSoup(resp, parser, from_encoding=resp.info().get_param('charset'))

    item = soup.find_all('a', href=True);
    links = [item['href'] for item in soup.select('[href^=http]')]

    for link in soup.find_all('a', href=True):
        print(link['href']);
        exist = False;
        for i in range(pqueue.qsize()):
            l = pqueue.get();
            pqueue.put(l)
            if (l == link['href']):
                exist = True;



        if (not exist) and ('#' not in link['href']) and (link['href'] not in myurl):
            print('*******************************************************')
            print(link['href'])
            gr = getRank(link['href']);
            print('*******************************************************')
            if (gr > 10):
                p = Process(target=getINFO, args=(link['href'],))
                pqueue.put(link['href']);
                p.start()
                p.join()


    return 0;


def getProfiles(myquery, q):
    driver = webdriver.Chrome("/usr/bin/chromedriver")
    driver.get('https://www.linkedin.com/')


    email = "vasilistsakanikas@gmail.com"
    password = ""

    actions.login(driver, email, password) # if email and password isnt given, it'll prompt in terminal
    print('check1')
    driver.get('https://www.google.com/?&hl=en-GB')

    driver.find_element(By.XPATH, "//*[text()='Accept all']").click()#consent...


    #back to the main page
    #driver.switch_to_default_content()
    search_query = driver.find_element_by_name('q')
    #print('search_query = ', search_query)
    sleep(5)
    #search_query.send_keys('site:linkedin.com/in AND "used cars" AND "Patra"');
    search_query.send_keys('site:linkedin.com/in ' + myquery);
    print('OK')

    search_query.send_keys(Keys.RETURN)
    sleep(0.5)


    #urls = driver.find_elements_by_xpath('//*[@class = "g"]/ul')

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    # soup = BeautifulSoup(r.text, 'html.parser')
    urls = []
    search = soup.find_all('div', class_="g")
    for h in search:
        urls.append(h.a.get('href'))


    print(urls)
    #urls = [url.get_attribute('href') for url in urls]
    sleep(0.5)
    results = urls;
    '''
    for url in urls:
        sleep(2)
        pr = {};
        driver.get(url)


        sel = Selector(text = driver.page_source)

        name = sel.xpath('//*[@class = "text-body-medium break-words"]/text()').extract_first()
        if (name):
            name = name.split()
        else:
            name = ' '
        name = ' '.join(name)
        print(name);

        position = sel.xpath('//*[@class = "t-16 t-black t-bold"]/text()').extract_first()
        if (position):
            position = position.split()
        else:
            position = ' '
        position = ' '.join(position)

        experience = sel.xpath('//*[@class = "pv-top-card-v3--experience-list"]')
        company = experience.xpath('./li[@data-control-name = "position_see_more"]//span/text()').extract_first()
        company = ''.join(company.split()) if company else None
        education = experience.xpath('.//li[@data-control-name = "education_see_more"]//span/text()').extract_first()
        education = ' '.join(education.split()) if education else None

        #location = ' '.join(sel.xpath('//*[@class = "t-16 t-black t-normal inline-block"]/text()').extract_first().split())

        url = driver.current_url;
        pr['Name'] = name;
        pr['Position'] = position;
        pr['Company'] = company;
        pr['Education'] = education;
        #pr['Location'] = location;
        pr['URL'] = url;

        print('\n')
        print('Name: ', name)
        print('Position: ', position)
        print('Company: ', company)
        print('Education: ', education)
        #print('Location: ', location)
        print('URL: ', url)
        print('\n')

        results.append(pr);
    '''

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    #mytext = mytext.split('/n')

    #dblist = myclient.list_database_names()
    #if "inpoint" in dblist:
    #    print("The database exists.")S
    mycollection = mydb['linkedin'];
    #collist = mydb.list_collection_names();
    mydict = { "searchwords": myquery, "results": results }
    x = mycollection.insert_one(mydict);

    q.put(results)
    driver.quit()


def getClusters(myurl):
    global startingpoint;
    startingpoint = myurl;
    Xdoc = []
    X = selectfrommongo(startingpoint, myurl);
    for doc in X:
        Xdoc.append(doc['content'])
        print('doc = ', doc['content']);

    vectorizer = TfidfVectorizer(stop_words={'english'});
    X = vectorizer.fit_transform(Xdoc);
    Sum_of_squared_distances = []
    K = range(2,6)
    for k in K:
        km = KMeans(n_clusters=k, max_iter=200, n_init=10)
        km = km.fit(X)
        Sum_of_squared_distances.append(km.inertia_)
    true_k = 6
    model = KMeans(n_clusters=true_k, init='k-means++', max_iter=200, n_init=10)
    model.fit(X)
    labels=model.labels_
    print(labels)
    return 0;


def getDocumentAnalytics(myurl):
    stop_words = set(stopwords.words("english"));
    print(stop_words);
    #global startingpoint;

    #print('startingpoint = ', startingpoint)
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"];
    mycollection = mydb[myurl];
    myquery = { "url": myurl }
    mydoc = mycollection.find(myquery)

    for x in mydoc:
        print('xcont = ', x['content'])
        x = x['content'];
        x = " ".join([word for word in x.split() if word not in stop_words]);
        x = x.encode(encoding="ascii", errors="ignore")
        x = x.decode()
        x = " ".join([word for word in x.split()]);
        x = re.sub("@\S+", "", x)
        x = re.sub("\$", "", x);
        x = re.sub("https?:\/\/.*[\r\n]*", "", x);
        x = re.sub("#", "", x);

        #tokenized_text=sent_tokenize(x)
        tokenized_word = word_tokenize(x)
        print(tokenized_word)
        fdist = FreqDist(tokenized_word)
        fd_t10 = fdist.most_common(10);
        print(fd_t10)
        #fd_t10.plot(10, cumulative=False)
        #plt.show()

    return fd_t10;


def getClusters2(docs, urls):
    # Initialize TF-IDF vectorizer
    print('docs = ', docs)

    all_docs = [doc[0] for doc in docs if doc]



    # Initialize TF-IDF vectorizer
    vectorizer = TfidfVectorizer(stop_words='english')

    # Transform documents to numerical data
    X = vectorizer.fit_transform(all_docs)

    # Define number of clusters
    num_clusters = 3

    # Perform k-means clustering
    kmeans = KMeans(n_clusters=num_clusters, random_state=0).fit(X)

    # Get cluster assignments for each document
    clusters = kmeans.predict(X)

    # Initialize lists to hold urls for each cluster
    clustered_urls = {i: [] for i in range(num_clusters)}

    # Assign each url to appropriate cluster
    for i, cluster in enumerate(clusters):
        clustered_urls[cluster].append(urls[i])

    # Print urls for each cluster
    for i in range(num_clusters):
        print(f"URLs in cluster {i}: {clustered_urls[i]}")


    return clustered_urls;

def bestreadabilityindex(docs, urls):
    min_index = float('inf')
    best_url = None

    for doc, url in zip(docs, urls):
        doc_str = ' '.join(doc)
        fk_score = textstat.flesch_kincaid_grade(doc_str)

        if fk_score < min_index:
            min_index = fk_score
            best_url = url

    return best_url;


def getDocumentAnalytics2(texts, urls):
    print('---------------------------------------------------------------------')

    stop_words = set(stopwords.words("english"));

    common_words = commonwords(texts)
    bestwebsite = bestreadabilityindex(texts, urls)
    clust = getClusters2(texts, urls)
    print(bestwebsite)

    res = {"commonwords":common_words, "most_readable_website(Flesch-Kincaid index)":bestwebsite, "website_clusters":clust}


    return res;


def scraptweets_sentiment(requestid, fromuser,htgs, search_words, date_since, numTweets, numRuns, q):
    consumer_key = "pw0ihLFxH3nwDrd4HBd7pqUrc"
    consumer_secret = "nh8GxSyT9ebV32pb4urtwlVnE7bxbPwCYYeVnI9TmT51Y71CDk"
    access_token = "1360011857969479682-iLrxBUlqdtExwkqiN9iZsHYDXIFTZz"
    access_token_secret = "fccgx7QK05sXrURyzcCAPDtZOvEfHtOdo7G5sXHjVshdm"
    #consumer_key = "f9n1D1rcKgULFRTKdelDUOtz2"
    #consumer_secret = "WrcBiZ2l1RXumwNE7IkpyV6Yj7bjuPteUj47FgswkLlSvISnSK"
    #access_token = "1415070882683293707-fZTh0jLU9opO5WKzGJW6vVkNKVvrRf"
    #access_token_secret = "FnhwJ8YMkubAfZz8tVZBd0CRcnlzf0G4PPt0qFlnEOrIE"
    nltk.download('vader_lexicon')
    sid = SentimentIntensityAnalyzer()

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    positive_avg = 0;
    negative_avg = 0;
    neutral_avg = 0;
    cnt = 0;

    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
    program_start = time.time()
    tweet_list = []
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()
        tmp = htgs
        print(tmp)
        hashtagsl = tmp.split(',');
        qh = ' OR ';
        i = 0;
        for h in hashtagsl:
            if (i < len(hashtagsl) - 1):
                qh = qh + '#' + h + ' OR '
            else:
                qh = qh + '#' + h;
            i = i + 1;
        print(qh)

        print(search_words + qh + ' from:'+fromuser)
        #for tweet in api.search(q=search_words + qh + ' from:'+fromuser, tweet_mode='extended', rpp=10):4
        cntt = 0;
        #print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' + str(api.get_user("57741058")))
        for tweet in api.search(q=search_words + qh, tweet_mode='extended', rpp=10):
        #for tweet in api.search(q="#coffeeisland", tweet_mode='extended', rpp=10):
            tweet_list.append(tweet)
            cntt = cntt + 1
            print('cntt=', cntt)
            if (cntt > 10):
                break
            #print(tweet)


        #tweets = tweepy.Cursor(api.search, q=search_words, since=date_since, tweet_mode='extended').items(numTweets)
        #tweet_list = [tweet for tweet in tweets]

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
                text = GoogleTranslator(source='auto', target='en').translate(tweet.retweeted_status.full_text)
            except AttributeError:  # Not a Retweet
                text = GoogleTranslator(source='auto', target='en').translate(tweet.full_text)

            #try:
            #    text = tweet.retweeted_status.full_text
            #except AttributeError:  # Not a Retweet
            #    text = tweet.full_text
            #print(text)
            r = sid.polarity_scores(text);
            print(r)
            positive_avg = positive_avg + r['pos'];
            negative_avg = negative_avg + r['neg'];
            neutral_avg = neutral_avg + r['neu'];
            insertTweet2mongo(requestid, search_words, r['pos'], r['neg'], r['neu'], text);
        if (cnt > 0):
            insertSentiment2mongo(requestid, search_words, positive_avg/cnt, negative_avg/cnt, neutral_avg/cnt)


    #buf = create_mygraph()
    #q.put(buf);
    print('returning.......')
    if (cnt > 0):
        q.put([positive_avg/cnt, negative_avg/cnt, neutral_avg/cnt]);
        return positive_avg/cnt, negative_avg/cnt, neutral_avg/cnt
    else:
        q.put([-2, -2, -2]);
        return -2, -2, -2

def getProximityCommunities():
    conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="11111111")
    print("Connection established-listening to port: 7687")
    q1 = "CALL gds.graph.create('myGraph', 'User8', 'Followed_by')";
    #print(q1)
    try:
        b = conn.query(q1);
    except:
        print('done that')

    q2 = "CALL gds.louvain.stream('myGraph')"
    b = conn.query(q2);
    print(b)
    insertcommunities2mongo(b)
    return b;


def getProximityCommunities2():
    conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="1234")
    print("Connection established-listening to port: 7687")

    random_graph_name = ''.join(random.choice(string.ascii_letters) for _ in range(5))

    q1 = "CALL gds.graph.create('" + random_graph_name + "', 'User8', 'Followed_by')";
    print(q1)
    try:
        b = conn.query(q1);
    except:
        print('done that')

    q2 = "CALL gds.louvain.stream('" + random_graph_name + "')"
    records = conn.query(q2);
    grouped_records = {}

    for record in records:
        # extract the communityId from the record
        communityId = record['communityId']

        # if this communityId is not already a key in the dictionary, add it with an empty list as its value
        if communityId not in grouped_records:
            grouped_records[communityId] = []

        # add the record to the list of records for this communityId
        grouped_records[communityId].append(record)
    #print(grouped_records)

    for communityId, records in grouped_records.items():
        print(f"CommunityId: {communityId}, Number of Records: {len(records)}")


    #insertcommunities2mongo(b)
    return {communityId: len(records) for communityId, records in grouped_records.items()};


def create_mygraph(keywords, res):
    x = np.linspace(0, 10, 100)
    y = np.sin(x)
    fig, ax = plt.subplots()
    #ax.plot(x, y)
    ax.set_title('Sentiment analysis on: '+keywords)
    #ax.set_xlabel('X Axis')
    #ax.set_ylabel('Y Axis')
    categories = ['positive', 'negative', 'neutral']
    values = res;
    ax.bar(categories, values, color ='maroon', width = 0.4)
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)

    return buf

@app.route('/api/v1/inpoint/services/', methods=['GET'])
def home():
    return "<h1>INPOINT SERVICES API v01</h1>";

@app.route('/api/v1/inpoint/services/getLinkedinProfiles', methods=['GET'])
def getLinkedinProfiles():
    myquery =  request.args['searchwords'];
    q  = Queue()

    p = Process(target=getProfiles, args=(myquery, q));
    p.start()
    p.join()

    results = q.get()

    #return Response(json.dumps(results), mimetype='application/json');
    return render_template('urls.html', urls=results)


@app.route('/api/v1/inpoint/services/monitor', methods=['GET'])
def monitorpage():
    myurl =  request.args['myurl'];
    #print('Monitoring ', myurl)
    p = Process(target=monitorwebpage, args=(myurl,))
    p.start()
    #p.join()
    return Response(json.dumps('results'), mimetype='application/json');


@app.route('/api/v1/inpoint/services/createcollection', methods=['GET'])
def createcollection():
    global startingpoint;
    mykeywords =  request.args['searchwords'];

    ############keywords to urls#########################################
    driver = webdriver.Chrome("/usr/bin/chromedriver")
    driver.get('https://www.google.com/?&hl=en-GB')
    driver.find_element(By.XPATH, "//*[text()='Accept all']").click()#consent...
    search_query = driver.find_element("name", 'q')
    sleep(5)
    search_query.send_keys(mykeywords);
    print('OK');

    search_query.send_keys(Keys.RETURN)
    sleep(0.5)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    urls = []
    search = soup.find_all('div', class_="g")
    for h in search:
        urls.append(h.a.get('href'))

    print(urls)
    myurl = urls[0];
    #####################################################################
    #myurl =  request.args['myurl'];
    startingpoint = myurl;
    p = Process(target=getINFO, args=(myurl,mykeywords,))
    p.start()
    #p.join()
    return Response(json.dumps('finish'), mimetype='application/json');


@app.route('/api/v1/inpoint/services/createcollection2', methods=['GET'])
def createcollection2():
    mykeywords =  request.args['searchwords'];
    mykeywords = GoogleTranslator(source='auto', target='en').translate(mykeywords)

    ############keywords to urls#########################################
    driver = webdriver.Chrome("/usr/bin/chromedriver")
    driver.get('https://www.google.com/?&hl=en-GB')
    driver.find_element(By.XPATH, "//*[text()='Accept all']").click()#consent...
    search_query = driver.find_element("name", 'q')
    sleep(5)
    search_query.send_keys(mykeywords);
    print('OK');

    search_query.send_keys(Keys.RETURN)
    sleep(0.5)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    urls = []
    docprops = []
    search = soup.find_all('div', class_="g")

    for h in search:
        startingpoint = h.a.get('href')
        break;

    texts = []
    for h in search:
        urls.append(h.a.get('href'))
        t = getPAGEINFO(startingpoint, h.a.get('href'), mykeywords)
        texts.append(t)

    docprops = getDocumentAnalytics2(texts, urls);

    return Response(json.dumps(docprops), mimetype='application/json');

@app.route('/api/v1/inpoint/services/clustercreatecollection', methods=['GET'])
def clustercreatecollection():
    global startingpoint;
    mykeywords =  request.args['mykeywords'];
    startingpoint = mykeywords;
    p = Process(target=getClusters, args=(myurl,))
    p.start()
    #p.join()
    return Response(json.dumps('finish'), mimetype='application/json');

@app.route('/api/v1/inpoint/services/proximitygraph', methods=['GET'])
def proximitygraph():
    q  = Queue()
    #twaccount = request.args['twaccount'];
    twaccount = 'CoffeeIsland_GR'
    depth = 1#int(request.args['depth']);
    search_words = request.args['searchwords'];
    p = Process(target=create_graph, args=(twaccount,depth, search_words, q));
    p.start()
    p.join()
    G = q.get()

    fig, ax = plt.subplots()
    Gtmp = nx.DiGraph()
    Gtmp = G

    nx.draw(G, with_labels=True)

    #ax.plot(x, y)
    ax.set_title('proximity Graph')
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)



    # save as a .png
    #plt.savefig("graph.png")



    #return Response(json.dumps('finish'), mimetype='application/json');
    return Response(buf.getvalue(), content_type='image/png')


@app.route('/api/v1/inpoint/services/tweetsentiment', methods=['GET'])
def tweetsentiment():
    #http://195.251.13.83:5000/api/v1/inpoint/services/tweetsentiment?requestid=1&fromuser=%27vtsakan%27&fromuser&hashtags=%27test%27&searchwords=ukrain&numTweets=100&numRuns=1
    #requestid = request.args['requestid'];
    requestid = 1;
    searchwords = request.args['searchwords'];
    #hashtags = request.args['hashtags'];
    hashtags = ''
    #fromuser = request.args['fromuser'];
    fromuser = 'CoffeeIsland_GR'
    #numTweets = int(request.args['numTweets']);
    numTweets = 100;
    #numRuns = int(request.args['numRuns']);
    numRuns = 1;
    num = Value('d', 0.0)
    q  = Queue()

    p = Process(target=scraptweets_sentiment, args=(requestid, fromuser, hashtags, searchwords, 'date_since', numTweets, numRuns, q));
    p.start()
    p.join()
    #time.sleep(5)
    res = q.get()
    buf = create_mygraph(searchwords, res)
    return Response(buf.getvalue(), content_type='image/png')


@app.route('/api/v1/inpoint/services/getsentimentresults', methods=['GET'])
def getsentimentresults():
    requestid = request.args['requestid'];
    searchwords = request.args['searchwords'];
    p = getSentiment(requestid, searchwords);
    print(p)
    return dumps(p)


@app.route('/api/v1/inpoint/services/getCommunities2', methods=['GET'])
def getCommunities2():
    res = getProximityCommunities2()
    return Response(json.dumps(res), mimetype='application/json');


@app.route('/api/v1/inpoint/services/getCommunities', methods=['GET'])
def getCommunities():

    p = Process(target=getProximityCommunities, args=( ));
    p.start()
    return Response(json.dumps('finish'), mimetype='application/json');

if __name__ == "__main__":
    app.run(host='0.0.0.0')
