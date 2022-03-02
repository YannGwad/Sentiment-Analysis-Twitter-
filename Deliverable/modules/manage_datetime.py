from datetime import datetime
from datetime import timedelta
import time
import pandas as pd

def get_fr_time():

  # Check Time Zone
  zone = time.tzname[0]
  print('---------------')
  print('time zone : ',zone)
  # Get today's date and time
  now = datetime.now()
  if zone == 'UTC':
    hour_fr = int(now.strftime('%H'))+1
    now = now.replace(hour = hour_fr)
    timefr = now.strftime("%Y-%m-%dT%H:%M:%SZ")

  return(timefr)

#Input should be datetime format
def get_datetime_minus_1s(date_time):

  date_time = date_time + timedelta(seconds=-1)
  return date_time

def get_datetime_plus_1s(date_time):

  date_time = date_time + timedelta(seconds=+1)
  return date_time