from fuzzywuzzy import fuzz
import numpy as np

##########################################################################
# Function get movie entity : Check matching between tweet and movies list
##########################################################################
'''
This function use 4 fuzzy method to detect movie pattern in each tweet : ratio, partial_ratio, token_sort_ratio, token_set_ratio
please refer to : https://www.datacamp.com/community/tutorials/fuzzy-string-python
4 scores are returned and we keep the highest one which means a better matching
Then, if the score is higher than Fuzzy_score_limit, we are pretty confident that there is a match between a tweet and a movie recorded in the movies list
We return the methods used to get the results, the scores and the movies found
'''
def get_movie_entities(text,list_movies,fuzzy_score_min):
  list_scores = []
  list_methods = []
  list_matches = []
  for i in range(len(list_movies)):
    
    list_ratios = [fuzz.ratio(text.lower(),list_movies[i].lower()),
                   fuzz.partial_ratio(text.lower(),list_movies[i].lower()),
                   fuzz.token_sort_ratio(text,list_movies[i]),
                   fuzz.token_set_ratio(text,list_movies[i])]
    
    # If the score is high enought
    if max(list_ratios) > fuzzy_score_min:
      list_scores.append(max(list_ratios))
      list_matches.append(list_movies[i])
      if list_ratios.index(max(list_ratios)) == 0:
        list_methods.append('ratio')
      elif list_ratios.index(max(list_ratios)) == 1:
        list_methods.append('partial ratio')
      elif list_ratios.index(max(list_ratios)) == 2:
        list_methods.append('token sort ratio')
      else :
        list_methods.append('token set ratio')

  return list_methods,list_scores,list_matches

####################################################################################################
# Function salto movie selection : get the first movie if multiple matches with Salto movie database
####################################################################################################

def movie_selection(df):
#Get only the first movie in the list if multiple movies are returned
  df.fuzzy_method = df.fuzzy_method.apply(lambda x : np.nan if len(x) == 0 else x[0])
  df.fuzzy_score = df.fuzzy_score.apply(lambda x : np.nan if len(x) == 0 else x[0])
  df.fuzzy_movie = df.fuzzy_movie.apply(lambda x : np.nan if len(x) == 0 else x[0])
  return df


def clean_movie(df,list_badwords):
  if df.fuzzy_movie in list_badwords:
    return np.nan, np.nan, np.nan
  else:
    return df.fuzzy_method,df.fuzzy_score, df.fuzzy_movie