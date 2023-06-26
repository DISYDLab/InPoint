import csv
from parsel import Selector
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup, SoupStrainer
import flask
from flask import request, jsonify, Response
import json
from multiprocessing import Process, Queue
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
from sklearn.feature_extraction.text import CountVectorizer
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

nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()

nltk.download("stopwords")
nltk.download('punkt')

global startingpoint;

pqueue = Queue();


# authorization tokens
consumer_key = "pw0ihLFxH3nwDrd4HBd7pqUrc"
consumer_secret = "nh8GxSyT9ebV32pb4urtwlVnE7bxbPwCYYeVnI9TmT51Y71CDk"
access_token = "1360011857969479682-iLrxBUlqdtExwkqiN9iZsHYDXIFTZz"
access_token_secret = "fccgx7QK05sXrURyzcCAPDtZOvEfHtOdo7G5sXHjVshdm"

# Connect to Twitter API using the secrets
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)



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
    qexist = "MATCH (u:User4) WHERE u.myid = '" + str(nodeid) + "' RETURN u";
    print(qexist)


    date_since = "2015-11-03"
    numTweets = 2500
    numRuns = 1
    # Call the function scraptweets
    mytweets = scraptweets(nodename, search_words, date_since, numTweets, numRuns)
    print('mytweets = ', mytweets)
    SQNEG01 = [];
    SQNEU01 = [];
    SQPOS01 = [];
    SQCOM01 = [];
    for t in mytweets:
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
        q = "CREATE (n:User4 {name: '" + str(nodename) + "', myid: '" + str(nodeid) + "', SENTOUT: '" + SENTOUT  + "' })";
        #CREATE (n:Person {name: 'Andy', title: 'Developer'})
        print(q)
        a = conn.query(q);
        return a;
    else:
        return -1;

#insert_node("george", 460);

#create relationships with specific ids

def create_connection(nodestartid, nodeendid):
    conn = Neo4jConnection(uri="bolt://ubuntu:7687", user="neo4j", pwd="1234")
    print("Connection established-listening to port: 7687")
    q1 = "MATCH (a:User4), (b:User4) WHERE a.myid = '" + str(nodestartid) + "' AND b.myid = '" + str(nodeendid) + "' CREATE (a)-[r:Followed_by]->(b) RETURN type(r)";
    print(q1)
    b = conn.query(q1);
    return b;


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
        return mytweets;



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


def get_jaccard_sim(str1, str2):
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))


def insert2mongo(mycoll, myurl, mytext):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"]

    #mytext = mytext.split('/n')

    #dblist = myclient.list_database_names()
    #if "inpoint" in dblist:
    #    print("The database exists.")S
    mycollection = mydb[mycoll];
    collist = mydb.list_collection_names();
    mydict = { "url": myurl, "content": mytext }
    x = mycollection.insert_one(mydict);


    #nltk_tokens = word_tokenize(x)

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


def getINFO(myurl):
    html = urllib.request.urlopen(myurl).read()
    print(text_from_html(html));
    global startingpoint;
    insert2mongo(startingpoint, myurl, text_from_html(html));
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
    global startingpoint;

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["inpoint"];
    mycollection = mydb[startingpoint];
    myquery = { "url": myurl }
    mydoc = mycollection.find(myquery)

    for x in mydoc:
        #print(x['content'])
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


@app.route('/api/v1/inpoint/services/', methods=['GET'])
def home():
    return "<h1>INPOINT SERVICES API v01</h1>";

@app.route('/api/v1/inpoint/services/getLinkedinProfiles', methods=['GET'])
def getLinkedinProfiles():
    myquery =  request.args['myquery'];
    driver = webdriver.Chrome("/usr/bin/chromedriver")
    driver.get('https://www.linkedin.com/')


    email = "vasilistsakanikas@gmail.com"
    password = "6972745067"

    actions.login(driver, email, password) # if email and password isnt given, it'll prompt in terminal



    #username = driver.find_element_by_name("session_key")
    #username.send_keys('vasilistsakanikas@gmail.com')
    #sleep(0.5)

    #password = driver.find_element_by_name('session_password')
    #password.send_keys('6972745067')
    #sleep(0.5)

    #sign_in_button = driver.find_element_by_class_name('sign-in-form__submit-button')
    #sign_in_button.click()
    #sleep(2)

    driver.get('https://www.google.com/')
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
    results = [];

    for url in urls:
        sleep(2)
        pr = {};
        driver.get(url)


        sel = Selector(text = driver.page_source)

        name = sel.xpath('//*[@class = "text-body-medium break-words"]/text()').extract_first().split()
        name = ' '.join(name)
        print(name);

        position = sel.xpath('//*[@class = "t-16 t-black t-bold"]/text()').extract_first().split()
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

    return Response(json.dumps(results), mimetype='application/json');
    driver.quit()

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
    myurl =  request.args['myurl'];
    startingpoint = myurl;
    p = Process(target=getINFO, args=(myurl,))
    p.start()
    #p.join()
    return Response(json.dumps('finish'), mimetype='application/json');

@app.route('/api/v1/inpoint/services/clustercreatecollection', methods=['GET'])
def clustercreatecollection():
    global startingpoint;
    myurl =  request.args['myurl'];
    startingpoint = myurl;
    p = Process(target=getClusters, args=(myurl,))
    p.start()
    #p.join()
    return Response(json.dumps('finish'), mimetype='application/json');

@app.route('/api/v1/inpoint/services/proximitygraph', methods=['GET'])
def proximitygraph():
    search_words = request.args['searchwords'];
    p = Process(target=create_graph, args=('CoffeeIsland_GR',3, search_words, ));
    p.start()
    return Response(json.dumps('finish'), mimetype='application/json');

app.run()
