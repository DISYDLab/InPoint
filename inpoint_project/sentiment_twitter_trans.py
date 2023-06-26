import nltk
import csv
import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from googletrans import Translator

translator = Translator()
result = translator.translate('Goodmorning my friend', src='en', dest='el')

print(result.src)
print(result.dest)
print(result.text)



nltk.download('vader_lexicon')
sid = SentimentIntensityAnalyzer()
#print(sid.polarity_scores('not so good experiences partly, because it often did not work --> unsatisfactory many repetitions due to technical problems '))

date = [];
country = [];
Q1 = [];


with open('hate_speech.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t', quotechar='|');
    for row in reader:
        if (row):
            Q1.append(row[0]);

out_csv =  open('sentiment_analysis_hate.csv', mode='w');
fieldnames = ['NEGATIVE', 'NEUTRAL', 'POSITIVE', 'COMPOUND' ];

writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
writer.writeheader();

SQNEG01 = [];
SQNEU01 = [];
SQPOS01 = [];
SQCOM01 = [];
for a in Q1:
    r = sid.polarity_scores(a);
    SQNEG01.append(r['neg']);
    SQNEU01.append(r['neu']);
    SQPOS01.append(r['pos']);
    SQCOM01.append(r['compound']);


for i in range(len(Q1)):
    writer.writerow({'NEGATIVE':SQNEG01[i], 'NEUTRAL':SQNEU01[i], 'POSITIVE':SQPOS01[i], 'COMPOUND':SQCOM01[i]});

out_csv.close();
