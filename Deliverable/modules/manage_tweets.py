#################################################################################################################
# Function request tweets : allow to get 100 more recent tweet within time range defined by [start_time, end_time]
#################################################################################################################
'''
parameter format
start_time & end_time : 'YYYY-MM-DDTHH:MM:SSZ',
client_key & client_secret : twitter app keys and tokens format (https://developer.twitter.com/en/portal/dashboard)
'''
import base64
import requests
import json
import pandas as pd

def request_tweets(start_time, end_time, nb_tweets, client_key, client_secret):

  # Parameters
  salto_id = '1007261596706639874'      # Salto Twitter ID*
  # Reformat the keys and encode them
  key_secret = '{}:{}'.format(client_key, client_secret).encode('ascii')
  # Transform from bytes to bytes that can be printed
  b64_encoded_key = base64.b64encode(key_secret)
  # Transform from bytes back into Unicode
  b64_encoded_key = b64_encoded_key.decode('ascii')

  # Twitter url & authintification parameters
  base_url = 'https://api.twitter.com/'
  auth_url = '{}oauth2/token'.format(base_url)
  auth_headers = {
    'Authorization': 'Basic {}'.format(b64_encoded_key),
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
  }
  auth_data = {
      'grant_type': 'client_credentials'
  }
  auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)

  access_token = auth_resp.json()['access_token']

  search_headers = {
    'Authorization': 'Bearer {}'.format(access_token)    
  }
  search_params = {"tweet.fields": "created_at",
                  'max_results' : nb_tweets,
                  'start_time' : start_time,
                  'end_time' : end_time}
  
  # Create the URL
  search_url = "https://api.twitter.com/2/users/{}/mentions".format(salto_id)
  
  # Execute the get request
  search_resp = requests.get(search_url, headers=search_headers, params=search_params)
  # Get the data from the request
  Data = json.loads(search_resp.content)
  # Tranform Data to DataFrame
  created_at = []
  id = []
  text = []

  #If data is not empty, return Data within a Dataframe df and the status 'ok'
  if Data['meta']['result_count'] > 0: 
    for i in range(len(Data['data'])):
      created_at .append(Data['data'][i]['created_at'])
      id.append(Data['data'][i]['id'])
      text.append(Data['data'][i]['text'])
    d = {
        'id': id,
        'text': text,
        'created_at' : created_at
        }
    df = pd.DataFrame(data=d,columns=['id','text','created_at'])
    status = 'ok'
  else:
    #If data is not empty, return empty df and the status 'nok'
    status = 'nok'
    df = pd.DataFrame(columns=['id','text','created_at'])
  return (df,Data)


############################################################################
# Function clean tweets : remove punctuations, links, emojis, and stop words
############################################################################

import re
import string
import emoji
# Natural Language Processing Toolkit
from nltk.tokenize import word_tokenize # to create word tokens
#from emoji.unicode_codes import EMOJI_UNICODE


def clean_tweets(tweet,stop_words):
    tweet = tweet.lower()  #has to be in place
    # Remove urls, transform the emojis into french sentences
    tweet = re.sub(r"http\S+|www\S+|https\S+", '', tweet, flags=re.MULTILINE)
    tweet = emoji.demojize(tweet, language='fr')
    tweet = tweet.replace(":"," ")
    #tweet = tweet.replace("_"," ")

    # Remove user @ references, '#', and other punctuations from tweet
    tweet = re.sub('[\.\#\@\!\?\(\)\,]', ' ', tweet)
    tweet = re.sub('[^A-Za-zëéàèç_]+', ' ', tweet)
    # Remove stopwords
    tweet_tokens = word_tokenize(tweet)  # convert string to tokens
    filtered_words = [w for w in tweet_tokens if w not in stop_words]

    # Remove punctuations
    unpunctuated_words = [char for char in filtered_words if char not in string.punctuation]
    unpunctuated_words = ' '.join(unpunctuated_words)

    return "".join(unpunctuated_words)  # join words with a space in between them