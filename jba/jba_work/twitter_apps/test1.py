import site
import sys
import os
path = '/home/hadoopuser/nltk_data/sentiment'
os.environ['PATH'] += ':'+path
# python -m site --user-site
# sys.path.append('/home/hadoopuser/nltk_data')
print(sys.path)
print(site.USER_SITE)