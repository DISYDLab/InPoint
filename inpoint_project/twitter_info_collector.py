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
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import multiprocessing as mp
from multiprocessing import Process, Queue
import time
import sys
import pandas as pd
from geopy.geocoders import Nominatim


poslon = mp.Queue()
poslat = mp.Queue()
posarea = mp.Queue()
pospopul = mp.Queue()

neglon = mp.Queue()
neglat = mp.Queue()
negarea = mp.Queue()
negpopul = mp.Queue()

geolocator = Nominatim(user_agent="twitter-location")
location = geolocator.geocode("175 5th Avenue NYC")
print(location.address)
print((location.latitude, location.longitude))


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

# Helper function to save data into a JSON file
# file_name: the file name of the data
# file_content: the data you want to save

#def save_json(file_name, file_content):
    #with open(path + file_name, 'w', encoding='utf-8') as f:
        #json.dump(file_content, f, ensure_ascii=False, indent=4)

# Helper function to handle twitter API rate limit


def plotmymap():
    plat = []
    plon = []
    ppopulation = []
    parea = []

    nlat = []
    nlon = []
    npopulation = []
    narea = []

    #lat.append(37.5)
    #lon.append(-78.4927721)

    #lat = cities['latd'].values
    #lon = cities['longd'].values
    #population = cities['population_total'].values
    fig = plt.figure(num=None, figsize=(12, 8) )
    while (True):

        m = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,llcrnrlon=-180,urcrnrlon=180,resolution='c')
        m.drawcoastlines(color='blue')
        #m.fillcontinents(color='tan',lake_color='lightblue')
        # draw parallels and meridians.
        m.drawparallels(np.arange(-90.,91.,30.),labels=[True,True,False,False],dashes=[2,2])
        m.drawmeridians(np.arange(-180.,181.,60.),labels=[False,False,False,True],dashes=[2,2])
        m.drawmapboundary(fill_color='lightblue')

        # make legend with dummy points
        #for a in [100, 300, 500]:
        plt.scatter([], [], c='k', alpha=0.5)
        #plt.legend(scatterpoints=1, frameon=False,
        #           labelspacing=1, loc='lower left');
        plt.colorbar(label=r'$({\rm sentiment})$');
        plt.clim(0, 1)

        plt.title("Twitter map")
        m.scatter(plon, plat, latlon=True,c=np.uint8(ppopulation), s=parea, cmap='Blues', alpha=0.5)
        m.scatter(nlon, nlat, latlon=True,c=np.uint8(npopulation), s=narea, cmap='Reds', alpha=0.5, marker = "X")

        plon.append(poslon.get())
        plat.append(poslat.get())
        ppopulation.append(pospopul.get())
        parea.append(posarea.get())

        nlon.append(neglon.get())
        nlat.append(neglat.get())
        npopulation.append(negpopul.get())
        narea.append(negarea.get())

        plt.draw()
        plt.pause(0.05)

        #time.sleep(0.1)
        fig.clear()
        #time.sleep(1)


def scraptweets(search_words, date_since, numTweets, numRuns):
    db_tweets = pd.DataFrame(columns = ['username', 'acctdesc', 'location', 'following',
                                        'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                        'retweetcount', 'text', 'hashtags']
                                )
    program_start = time.time()
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()
        # Collect tweets using the Cursor object
        # .Cursor() returns an object that you can iterate or loop over to access the data collected.
        # Each item in the iterator has various attributes that you can access to get information about each tweet
        tweets = tweepy.Cursor(api.search, q=search_words, lang="en", since=date_since, tweet_mode='extended').items(numTweets)
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
            print(tweet.coordinates)
            print('________________________________________________________________________________')
            try:
                text = tweet.retweeted_status.full_text
            except AttributeError:  # Not a Retweet
                text = tweet.full_text
            print(text)

            r = sid.polarity_scores(text);
            if (location is not None):
                loc = geolocator.geocode(location)
                print(location)
                if (loc is not None):
                    print((loc.latitude, loc.longitude))
                    poslon.put(loc.longitude);
                    poslat.put(loc.latitude);
                    if (float(r['pos']) > float(r['neg'])):
                        poslon.put(loc.longitude);
                        poslat.put(loc.latitude);
                        posarea.put(50);
                        pospopul.put(5000+10000*float(r['pos']));
                    else:
                        neglon.put(loc.longitude);
                        neglat.put(loc.latitude);
                        negarea.put(50);
                        negpopul.put(3000+10000*float(r['neg']));

        print(location)
        ith_tweet = [username, acctdesc, location, following, followers, totaltweets, usercreatedts, tweetcreatedts, retweetcount, text, hashtags]
        db_tweets.loc[len(db_tweets)] = ith_tweet
        noTweets += 1

        end_run = time.time()
        duration_run = round((end_run-start_run)/60, 2)

        print('no. of tweets scraped for run {} is {}'.format(i + 1, noTweets))
        print('time take for {} run to complete is {} mins'.format(i+1, duration_run))

        time.sleep(2) #15 minute sleep time
        # Once all runs have completed, save them to a single csv file:
    from datetime import datetime
    # Obtain timestamp in a readable format
    to_csv_timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')
    # Define working path and filename
    path = os.getcwd()
    filename = path + '' + to_csv_timestamp + '__tweets.csv'
    # Store dataframe in csv with creation date timestamp
    db_tweets.to_csv(filename, index = False)

    program_end = time.time()
    print('Scraping has completed!')
    print('Total time taken to scrap is {} minutes.'.format(round(program_end - program_start)/60, 2))



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
        new_tweets = api.user_timeline(screen_name = screen_name,count=3,max_id=oldest)
        alltweets.extend(new_tweets)
        for t in new_tweets:
            r = sid.polarity_scores(t.text);
            #print(t.coordinates['coordinates'][0],t.coordinates['coordinates'][1]);
            user = api.get_user(t.user.id)
            print(user.location)
            print(r)
        oldest = alltweets[-1].id - 1
        print("...%s tweets downloaded so far" % (len(alltweets)))
        LIMIT = LIMIT + 200;

    outtweets = [[tweet.id_str, tweet.created_at, tweet.text, tweet.favorite_count, tweet.in_reply_to_screen_name, tweet.retweeted] for tweet in alltweets]
    with open('/home/vtsakan/Desktop/tweets.csv', 'w') as f: #BILL, change this path
        writer = csv.writer(f)
        writer.writerow(["id","created_at","text","likes","in reply to","retweeted"])
        writer.writerows(outtweets)
        pass

def get_followers(TARGETUSER):
    followers_list = []
    f = open('/home/vtsakan/Desktop/followers.csv', 'w'); #BILL, change this path
    writer = csv.writer(f)
    writer.writerow(["follower"])
    for user in tweepy.Cursor(api.followers, screen_name=TARGETUSER).items():
        print('follower: ' + user.screen_name);
        writer.writerow([user.screen_name])

def get_friends(TARGETUSER):
    friends_list = []
    f = open('/home/vtsakan/Desktop/friends.csv', 'w'); #BILL, change this path
    writer = csv.writer(f)
    writer.writerow(["follower"])
    for user in tweepy.Cursor(api.friends, screen_name=TARGETUSER).items():
        print('friend: ' + user.screen_name);
        writer.writerow([user.screen_name]);


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

#Data Collection main script
if __name__ == '__main__':
    #get_all_tweets("cocacola")
    #get_followers("vtsakan")
    #get_friends("vtsakan")
    #t = {}
    #todays_stats(t)


    # Initialise these variables:
    search_words = "#greece"
    date_since = "2020-11-03"
    numTweets = 20
    numRuns = 100
    # Call the function scraptweets
#    scraptweets(search_words, date_since, numTweets, numRuns)

    p = Process(target=scraptweets, args=(search_words, date_since, numTweets, numRuns,))
    p.start()

    map = Process(target=plotmymap)
    map.start()


    p.join()
