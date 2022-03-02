import pandas as pd
import datetime
from datetime import datetime
from nltk.corpus import stopwords
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('words')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
import string
from tqdm import tqdm
tqdm.pandas()

from modules.manage_bigquery_database import connect_BigQuery_database,request_last_tweet,import_to_BigQuery,request_all_tweet,bigquery_format
from modules.manage_datetime import get_datetime_minus_1s, get_datetime_plus_1s
from modules.manage_tweets import request_tweets, clean_tweets
from modules.manage_cloud_natural_language import connect_cloud_natural_language, get_sentiment, get_polarity
from modules.manage_movie_entity import get_movie_entities, movie_selection, clean_movie

def tweets_sentiment_analysis(data, context):
#############################################
############## Parameters ###################
#############################################

# ------- Twitter API logs ---------
  url_log = 'logs/'
  file_log = 'loggin.txt'
  f = open(url_log+file_log, 'r')
  Lines = f.readlines()
  client_key = Lines[0].strip('\n').strip('client_key:')
  client_secret = Lines[1].strip('\n').strip('client_secret:')

# ------- Twitter API parameters -----
# DO NOT MODIFY !!!
  nb_tweets_per_request = 100 # number of tweets per request (max authorized = 100)
  tot_nb_tweets_retrieved = 0 # variable declaration : total nb of tweets retreived (for all iterations in the loop)
  nb_tweets_retrieved = 0 # variable declaration : nb of tweets retreive per request (per iteration in the loop)

# ------ DataBase parameters -------
  # DO NOT MODIFY !!!
  project_id = 'salto-datalab-pid2'
  dataset_id = 'Twitter_database' 
  table_id = 'Tweets'
  data_id = project_id + '.' + dataset_id + '.' + table_id

# ------ Cloud Natural language parameters ------
  '''Defining a score range within polarity is considered to be neutral
  if score in [max_negative,min_positive], polarity is set to neutral'''
# Parameters below can be modified. CNL API returns a score between -1 and 1
# You can consider a tweet as positive from the moment the score is postive or above min_positive
# You can consider a tweet as negative from the moment the score is negative or below min_positive
# Other tweets are considered neutral
  min_positive = 0
  max_negative = 0

# ------ Nammed entity recognition parameters ------
# define the score limit [0 100] to validate the matching between a tweet and a movie in the movies list.
  fuzzy_score_min = 90
# list of movies to be removed because they generate errors in entity detection.
  list_badwords = ['Vu','Face-à-face']
  url_movies = 'movies/'
  file_movie = 'salto_programs.csv'
  list_movies = pd.read_csv(url_movies+file_movie)['0'].to_list()

# ------ Tweets cleaning parameters ------
  stop_words = list(stopwords.words('french'))
  alphabets = list(string.ascii_lowercase)
  delete_custom = ['salto_fr','salto','fr','ça','plus','tf','si','avant',
                  'tout','quand','fait','voir','cette','comme','tre','avoir',
                  'déjà','aussi','va','très','dès','après','vu','peut','donc',
                  'alors','là','vf','fran','ais','temp',
                  'jbkechi','avp','vais','us','ca','tou','aise','san',
                  'quoi','comment','juste','peu','sans','chez','trop','tous','car']
  stop_words = stop_words + alphabets + delete_custom


#############################################
############# Get new Tweets ################
#############################################

# ------ BigQuery Server Connexion ------
  bigquery_client = connect_BigQuery_database(url_log)

# ------ Load newest database tweet ------
# answer provides information that the check in database is correct.
# There is no mismatch between the higest key and the last tweet datetime
  last_tweet,answer  = request_last_tweet(bigquery_client)
  print('{} tweets in BigQuery DataBase'.format(last_tweet.key[0]))
  if answer == 'yes': # program not aborted following last database tweet check

# ------ Time range definition ----------
    '''Define time range for iteration (Twitter Time format)
    end_time = datetime.now (we want to retrieve all new tweets until the datetime request)
    end_time = get_fr_time()'''

    now = datetime.now()
    end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    '''start_time initialization = date of the most recent tweet (last_tweet) from the database plus 1 second'''
    start_time = get_datetime_plus_1s(last_tweet.created_at)
    start_time = start_time[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    print('start time ',start_time.replace('Z','').split('T'))
    print('end time ',end_time.replace('Z','').split('T'))

# ------- Request tweets using Twitter API v2 ----
# ------------------------------------------------

    df_new_tweets = pd.DataFrame(columns=['id','text','created_at']) # Initialize new tweets DataFrame

    '''Loop is built to stop running as soon as the parameter end time >= start_time
    This means that we covered the entire timeline between the last tweet datetime in the database
    and the current datetime'''
    while pd.to_datetime(end_time) >= pd.to_datetime(start_time): 
      try: # Request function
        df,status = request_tweets(start_time, end_time, nb_tweets_per_request, client_key, client_secret)
        tot_nb_tweets_retrieved += len(df)
        nb_tweets_retrieved = len(df)
        df_new_tweets = pd.concat([df_new_tweets,df])
      except:
        print('Error Requesting tweets from Twitter API v2')
        break

# Define new end time to loop
      if nb_tweets_retrieved > 0 : # Check if dataframe contains information, else, there is no new tweet
        if nb_tweets_retrieved < nb_tweets_per_request: # Check if all data have been retrieved
          # Modify end time by date of oldest tweet retrieved - minus 1 second to avoid overlapping
          print('Total {} Number of tweets retrieved'.format(tot_nb_tweets_retrieved))
          break
        else: # if the number of retrieved tweets < 100 this means there are probably other new tweets
          end_time = get_datetime_minus_1s(pd.to_datetime(df['created_at']).min()) # we define new end time to loop
          end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
          print('new end time :',end_time.replace('Z','').split('T'))
      else:
        print('No new tweet mentionning Salto_fr')
        break

    if df_new_tweets.empty == False :

# ------- Adding Keys -------
# We get the last tweet key in the database and we start incrementing keys for new tweets
# Last tweet has the higest key
      df_new_tweets = df_new_tweets.sort_values(by='created_at',ascending=False)
      df_new_tweets['key'] = [x for x in range(last_tweet.key[0]+1,last_tweet.key[0]+len(df_new_tweets)+1)][::-1]
      df_new_tweets = df_new_tweets[['key','id','text','created_at']]
    
#####################################################
############# Sentiment analysis ####################
#####################################################

      list_text = df_new_tweets.text.values.tolist()
      list_key = df_new_tweets.key.to_list()
      list_id = df_new_tweets.id.to_list()
      list_created_at = df_new_tweets.created_at.to_list()

# ------ Cloud Natural Language instanciation ------
      cnl_client = connect_cloud_natural_language(url_log)

      error = 'no'
      print('Google Natural Language processing :')
      try:
        results = [get_sentiment(row,cnl_client) for row in tqdm(list_text,mininterval=1)]
      except:
        print('Error calling Google Natural Language API')
        error = 'yes'

      if error == 'no':
        list_score, list_magnitude = zip(*results) # Unpacking the result into 2 lists
        list_polarity = [get_polarity(x,min_positive,max_negative) for x in list_score]
        df = list(zip(list_key, list_id, list_text, list_created_at, list_score, list_magnitude, list_polarity))
        df = pd.DataFrame(df, columns = ['key','id','text', 'created_at','score',
          'magnitude','polarity'])

#####################################################
############# Nammed Entity Recognition #############
#####################################################
        print('Movies entity recognition processing')
        df[['fuzzy_method','fuzzy_score','fuzzy_movie']] = df.progress_apply(
            lambda row : pd.Series(get_movie_entities(row['text'], list_movies,fuzzy_score_min)), axis=1)
        df = df[['key','id','text', 'created_at','score','magnitude',
        'polarity','fuzzy_method','fuzzy_score','fuzzy_movie']]

        #Get only the first movie in the list if multiple movies are returned
        df = movie_selection(df)

        df[['fuzzy_method','fuzzy_score','fuzzy_movie']] = df.apply(
        lambda row : pd.Series(clean_movie(row, list_badwords)), axis=1)

####################################################
############ Clean Tweets for Wordcloud ############
####################################################

        df['text_clean'] = df.text.apply(lambda x : clean_tweets(x,stop_words))


#------- Formatting data to BigQuery format -------
        df_to_bigquery = bigquery_format(df)

#-------- Save Tweets in BigQuery Database -------------
        try:
          import_to_BigQuery(bigquery_client,data_id,df_to_bigquery)
          print('New tweets loaded sucessfully in BigQuery DataBase')

# ---- Check number of tweets in database -----
          print('{} tweets in BigQuery DataBase'.format(df_to_bigquery.key.max()))
      
        except:
          print('Error importing tweets to BigQuery Database')

