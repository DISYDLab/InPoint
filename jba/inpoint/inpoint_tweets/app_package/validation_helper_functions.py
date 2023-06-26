# -----------------------------------------------------------------------------------------------------
# The datetime module of Python helps us handle time-related information on any precision level.
# 4 Must-Know Objects: date, timedelta, time, datetime (is kind of a combination of date and time).
# The datetime object provides the flexibility of using date only, or date and time combined.
import datetime  # (datetime.datetime, datetime.timezone)
from datetime import date
# -----------------------------------------------------------------------------------------------------

from errors import InvalidParameter
# -----------------------------------------------------------------------------------------------------
from datatest import validate
# -----------------------------------------------------------------------------------------------------
from flask import request

# ----------------------------------------------------------------------------------------------------
# pytz brings the Olson tz database into Python. This library allows accurate and cross platform timezone calculations
# using Python 2.4 or higher. It also solves the issue of ambiguous times at the end of daylight saving time, which you
# can read more about in the Python Library Reference (datetime.tzinfo).
from pytz import timezone


def strftime_format(format):
    def func(value):
        try:
            datetime.datetime.strptime(value, format)
        except ValueError:
            return False
        return True

    func.__doc__ = f'should use date format {format}'
    return func


def date_str_to_date_type(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()


def validate_dates(message1, date_since, date_until, search_tweets=True):

    dates = [date_since, date_until]
    validate(dates, strftime_format('%Y-%m-%d'))
    if message1 != "":
        status = 400
        raise InvalidParameter('Invalid request values', status)

    date_since_date_type = date_str_to_date_type(date_since)
    date_until_date_type = date_str_to_date_type(date_until)
    if search_tweets:
        delta1 = date_until_date_type - date_since_date_type
        delta2 = date.today() - date_since_date_type

        if (date_until_date_type <= date_since_date_type) or (delta2 > datetime.timedelta(days=7)):
            message = f"""
            Wrong dates, date_until: {date_until_date_type} must be greater than date_since: {date_since_date_type} and must be in a time-window of 0-7 days from today
            """
            print(message.strip())
            raise InvalidParameter(message.strip(), 400)

    else:
        if (date_until_date_type <= date_since_date_type):
            message = f"""
            Wrong dates, date_until: {date_until_date_type} must be greater than date_since: {date_since_date_type}
            """
            print(message.strip())
            raise InvalidParameter(message.strip(), 400)

def missing_query_arguments_or_values_check():

    try:
        # POST Method
        # tweets_data = request.get_json()

        # -------------------------------------------
        # GET Method
        tweets_data = request.args
        # The conversion from a http request string sent by client to another type is 
        # referred to as data binding
        tweets_data = tweets_data.to_dict(flat=True)
        # -------------------------------------------
        print(tweets_data)
    except Exception as e:
        status = 400
        raise InvalidParameter(
            """please try with a valid query: i.e:{"user": "jba", "search_words": "CoffeeIsland_GR OR (coffee island)", "date_since": "2022-01-19", "date_until": "2022-01-21","lang":"el"}""".strip(
            ), status
        )

    missing_arguments = []
    empty_value_arguments = []
    tweets_data_arguments = ['user', "search_words",
                             "date_since", "date_until", "lang"]
    for arg in tweets_data_arguments:
        if not arg in tweets_data.keys():
            missing_arguments.append(arg)
        elif tweets_data[arg] == '':
            empty_value_arguments.append(arg)

    if missing_arguments and empty_value_arguments:
        print(
            f'Missing arguments: {missing_arguments} and arguments with no_value: {empty_value_arguments}')
        status = 400
        raise InvalidParameter(
            f'Missing arguments: {missing_arguments} and arguments with no_value: {empty_value_arguments}. Please try with a valid query', status)

    if missing_arguments:
        print(f'Missing arguments: {missing_arguments}')
        status = 400
        raise InvalidParameter(
            f'Missing arguments: {missing_arguments}. Please try with a valid query', status)
    if empty_value_arguments:
        print(f'Arguments with no_value: {empty_value_arguments}')
        status = 400
        raise InvalidParameter(
            f'Arguments with no_value: {empty_value_arguments}. Please try with a valid query', status)
    return tweets_data


    # "Z time" or "Zulu Time" or "UTC time"
    ########################################
def date_str_to_datetime_UTC_str(date_since, date_until):
    """
    from date string to datetime UTC string ( Coordinated Universal Time)
    creates datetime_since_UTC_str and datetime_until_UTC_str as in the following example:
    date_since = 2022-01-21, date_until = 2022-01-23
    datetime_since_UTC_str = '2022-01-21T00:00:00+00:00'
    datetime_until_UTC_str = '2022-01-23T23:59:59.999999+00:00'
    """

    # The strptime() method creates a datetime object from a given string (representing date and time).
    # A datetime object d is aware if both of the following hold:
    # d.tzinfo is not None
    # d.tzinfo.utcoffset(d) does not return None
    # print(date_since_obj.tzinfo) # None
    # print(datetime.tzinfo.tcoffset(date_since_obj))
    # The ISO format for timestamps has a 'T' separating the date from the time part.
    # date_since=2021-07-07T00:00:00Z
    # date_until=2021-07-14T23:59:59.999999Z
    datetime_since_UTC_str = datetime.datetime.strptime(
        date_since, '%Y-%m-%d').isoformat(sep=" ")

    datetime_until_obj = datetime.datetime.strptime(date_until, '%Y-%m-%d')
    datetime_until_UTC_str = (datetime.datetime
                              .combine(datetime_until_obj, datetime.datetime.max.time())).isoformat(sep=" ")

    return datetime_since_UTC_str, datetime_until_UTC_str


def date_str_to_datetime_EEST_str(date_since, date_until):
    """
    from date string to datetime EEST string (European East Time)
    creates datetime_since_EEST_str and datetime_until_EEST_str as in the following example
    date_since = 2022-01-21, date_until = 2022-01-23
    datetime_since_EEST_str = '2022-01-21T00:00:00+02:00'
    datetime_until_EEST_str = '2022-01-23T23:59:59.999999+02:00'
    """
    datetime_since_obj = datetime.datetime.strptime(date_since, '%Y-%m-%d')
    dt_athens_since = timezone('Europe/Athens').localize(datetime_since_obj)
    datetime_since_EEST_str = dt_athens_since.isoformat()

    datetime_until_obj = datetime.datetime.strptime(date_until, '%Y-%m-%d')
    datetime_until_obj = (datetime.datetime
                          .combine(datetime_until_obj, datetime.datetime.max.time()))
    dt_athens_until = timezone('Europe/Athens').localize(datetime_until_obj)
    datetime_until_EEST_str = dt_athens_until.isoformat()
    return datetime_since_EEST_str, datetime_until_EEST_str