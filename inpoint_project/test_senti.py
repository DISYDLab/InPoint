import nltk
import csv
import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer


nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()

r = sid.polarity_scores('Goodmorning to everyone');

print(r);
