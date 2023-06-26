# import site
# import sys
# import os
# path = '/home/hadoopuser/nltk_data/sentiment'
# os.environ['PATH'] += ':'+path
# # python -m site --user-site
# # sys.path.append('/home/hadoopuser/nltk_data')
# print(sys.path)
# print(site.USER_SITE)
# print('OK')
print(f"{'*'*60}")

success_msg = 'success - tweets results'
print(f' {"="*(len(success_msg)+20)}')
print(f'\n{" " *10 + success_msg}\n')
print(f' {"="*(len(success_msg)+20)}')

import datetime
date_since='2021-07-11'
date_until='2021-06-21'
# date_since='its time'

# The string needs to be in a certain format in order to convert it in a datetime object
# https://www.programiz.com/python-programming/datetime/strptime
try:
    date_since_obj = datetime.datetime.strptime(date_since, '%Y-%m-%d')
    date_until_obj = datetime.datetime.strptime(date_until, '%Y-%m-%d')
    print(isinstance(date_since_obj, datetime.datetime))
except Exception as e:
    print(f'{date_since} is not a valid date')
else:
    if (date_since_obj <= date_until_obj):
        date_test_ok= True
    else:
        date_test_ok= False

    # print("Yeah ! Your answer is :", type(datetime.datetime.strptime(date_since, '%Y-%m-%d')) is datetime.date)
# type(datetime.datetime.strptime(date_since, '%Y-%m-%d')) is datetime.date
print(date_test_ok)
d = datetime.date(2012, 9, 1)
print (type(d) is datetime.date)
print(isinstance(d, datetime.date))
print(datetime.date.today())
print(datetime.datetime.now())