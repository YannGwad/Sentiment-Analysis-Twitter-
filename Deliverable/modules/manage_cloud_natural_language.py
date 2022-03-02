from google.cloud import language
from google.oauth2 import service_account

################################################################
# Function create connection : Connect to Cloud Natural Language
################################################################
def connect_cloud_natural_language(url_keys):
  
  #Create connexion to BigQuery Server
  keys_file = 'salto-datalab-pid2-949a6261cdbe.json'
  service_account_file_path = url_keys + keys_file
  creds = service_account.Credentials.from_service_account_file(service_account_file_path)
  cnl_client = language.LanguageServiceClient(credentials=creds)
  return(cnl_client)

############################################################
# Function get_sentiment : Get parameters score & magnitude 
############################################################

def get_sentiment(text,client):  
    
    document = language.Document(
            content=text,
            type_=language.Document.Type.PLAIN_TEXT)
    annotations = client.analyze_sentiment(document=document)
    score = annotations.document_sentiment.score
    magnitude = annotations.document_sentiment.magnitude
    return score, magnitude

##############################################################################
# Function get_polarity : Get sentiment polarity (neutral, positive, negative)
##############################################################################

def get_polarity(score, min_positive, max_negative):

  if score > min_positive : return 'positive'
  elif score < max_negative: return 'negative'
  else : return 'neutral'