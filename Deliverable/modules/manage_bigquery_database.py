from google.cloud import bigquery
import pandas as pd

###########################################################
# Function create connection : Connect to BigQuery DataBase
###########################################################
def connect_BigQuery_database(url_keys):
  
  #Create connexion to BigQuery Server
  keys_file = 'salto-datalab-pid2-949a6261cdbe.json'
  service_account_file_path = url_keys + keys_file
  bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
  return(bigquery_client)

#################################################
# Function Request newest tweet in BigQuery table
#################################################
def request_last_tweet(bigquery_client):

  # get oldest tweet by date
  df_lastdate = bigquery_client.query('''
    SELECT key, id, text, created_at
    FROM `salto-datalab-pid2.Twitter_database.Tweets`
    WHERE created_at = (SELECT MAX(created_at) AS last_date
                    FROM `salto-datalab-pid2.Twitter_database.Tweets`);  
  ''').to_dataframe()
  # get oldest tweet by key
  df_lastkey = bigquery_client.query('''
    SELECT key, id, text, created_at
    FROM `salto-datalab-pid2.Twitter_database.Tweets`
    WHERE key = (SELECT MAX(key) AS last_key
                    FROM `salto-datalab-pid2.Twitter_database.Tweets`);  
  ''').to_dataframe()

  '''We check here if there is a mismatch in the database
  Normally, a key is assigned to each new tweet in ascending order
  This means the most recent tweet should have the highest key value
  If not, there is a coding bug that should be solved
  '''
  check = ""
  if df_lastdate.created_at.values[0] == df_lastkey.created_at.values[0]:
    print('Last tweet verified : no error found')
    check = 'yes'
  elif df_lastdate.created_at.values[0] >  df_lastkey.created_at.values[0]:
    check = ""
    print(' Warning : the highest key does not match with most recent datetime')
    print(' Warning : this new request may lead to an overlapping of tweets in database')
    while (check != 'yes') | (check != 'no'):
      check = input('Warning : do you want to continue?')
      if (check != 'yes') | (check != 'no'):
        print('No valid answer')
      elif check == 'no' :
        print('Program aborted')
      else:
        print('Program ongoing')
  else :
    check = ""
    print(' Warning : the highest key does not match with most recent datetime')
    print(' Warning : no overlapping risk but investigation needed')
    while (check != 'yes') | (check != 'no'):
      check = input('Warning : do you want to continue?')
      if (check != 'yes') | (check != 'no'):
        print('No valid answer')
      elif check == 'no' :
        print('Program aborted')
      else:
        print('Program ongoing')
  return df_lastkey,check

#############################################
# Function import new lines to bigquery table
#############################################
def import_to_BigQuery(bigquery_client,data_id,df):

  job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        bigquery.SchemaField("key", 'INTEGER',mode="REQUIRED"),
        bigquery.SchemaField("id", "STRING",mode="NULLABLE"),
        bigquery.SchemaField("text", "STRING",mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP",mode="NULLABLE"),
        bigquery.SchemaField("score", "FLOAT",mode="NULLABLE"),
        bigquery.SchemaField("magnitude", "FLOAT",mode="NULLABLE"),
        bigquery.SchemaField("polarity", "STRING",mode="NULLABLE"),
        bigquery.SchemaField("fuzzy_method", "STRING",mode="NULLABLE"),
        bigquery.SchemaField("fuzzy_score", "FLOAT",mode="NULLABLE"),
        bigquery.SchemaField("fuzzy_movie", "STRING",mode="NULLABLE"),
        bigquery.SchemaField("text_clean", "STRING",mode="NULLABLE")
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows with "WRITE_APPEND"
    write_disposition="WRITE_APPEND",
  )

  job = bigquery_client.load_table_from_dataframe(
      df, data_id, job_config=job_config
  )  # Make an API request.


###############################################
# Function Request all tweets in BigQuery table
###############################################
def request_all_tweet(bigquery_client):

  df = bigquery_client.query('''
    SELECT *
    FROM `salto-datalab-pid2.Twitter_database.Tweets`  
  ''').to_dataframe()

  return df

##############################################
# Formatting new data to match BigQuery format
##############################################

def bigquery_format(df):

  df.key = df.key.astype(int)
  df.id = df.id.astype(str)
  df.text = df.text.astype(str)
  df.created_at = pd.to_datetime(df.created_at)
  df.score = df.score.astype(float)
  df.magnitude = df.magnitude.astype(float)
  df.polarity = df.polarity.astype(str)
  df.fuzzy_method = df.fuzzy_method.astype(str)
  df.fuzzy_score = df.fuzzy_score.astype(float)
  df.fuzzy_movie = df.fuzzy_movie.astype(str)
  df.text_clean = df.text_clean.astype(str)
  return df  

##################################################################################
# Function put dataframe in Bigquery table (create or overwrite if already exists)
##################################################################################

def Overwrite_BigQuery_table(bigquery_client,data_id,df):

  job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
      bigquery.SchemaField("key", 'INTEGER',mode="REQUIRED"),
      bigquery.SchemaField("id", "STRING",mode="NULLABLE"),
      bigquery.SchemaField("text", "STRING",mode="NULLABLE"),
      bigquery.SchemaField("created_at", "TIMESTAMP",mode="NULLABLE"),
      bigquery.SchemaField("score", "FLOAT",mode="NULLABLE"),
      bigquery.SchemaField("magnitude", "FLOAT",mode="NULLABLE"),
      bigquery.SchemaField("polarity", "STRING",mode="NULLABLE"),
      bigquery.SchemaField("fuzzy_method", "STRING",mode="NULLABLE"),
      bigquery.SchemaField("fuzzy_score", "FLOAT",mode="NULLABLE"),
      bigquery.SchemaField("fuzzy_movie", "STRING",mode="NULLABLE"),
        bigquery.SchemaField("text_clean", "STRING",mode="NULLABLE")
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows with "WRITE_APPEND"
    write_disposition="WRITE_TRUNCATE",
  )

  job = bigquery_client.load_table_from_dataframe(
      df, data_id, job_config=job_config
  )