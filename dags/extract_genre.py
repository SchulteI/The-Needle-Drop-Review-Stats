#Custom method to retrieve authentication token for Spotify Web API from public and private key
from spotify_auth import get_auth_token
import requests 
import pandas as pd
import numpy as np
import datetime as dt


def get_album_id(path):
    #Remove duplicate index column when reading csv
    reviews = pd.read_csv(path, index_col=0)
    
    #Removes reviews for EPs and Singles
    reviews = reviews.loc[reviews['project_type']=='album']
    reviews.dropna(inplace=True)
    
    #Remove duplicate entries with spotify id as the key and remove other currently unused columns
    reviews.drop_duplicates(subset='spotify_id', inplace=True) 
    reviews.drop(columns = ['youtube_id', 'project_type', 'project_art', 'tracks'], inplace=True)
    
    return reviews
    
def get_album_genres(albumIds, genres):
    tokenHeader = {
    "Authorization": "Bearer " + str(get_auth_token())
    }
    
    #Create dictionary to store genres retrieved from Spotify API
    albumGenres = {'spotify_id': albumIds, 'genres': []}
    
    #Output file to write missing data entries to
    with open('/opt/airflow/dags/output_' + dt.date.today().strftime('%m%d%Y')+'.txt', 'w') as f:
        
        for id in range(0, len(albumIds), 1):
            
            #Retrieve album info from Spotify
            albumReq = requests.get("https://api.spotify.com/v1/albums/" + str(albumIds[id]), headers=tokenHeader).json()

            #Get artist id from album info since genre tags are stored under the artist info
            artistId = albumReq['artists'][0]['id']
            
            #Retrieve artist info from Spotify and get a list of associated genres
            artistReq = requests.get("https://api.spotify.com/v1/artists/" + str(artistId), headers=tokenHeader).json()
            subGenres = artistReq['genres']

            #Some of the artist dont have any genres associated with their profile, add place holder nan and go to next iteration of the loop if that is the case
            if len(subGenres) == 0:
                print("No sub-genres found for " + artistId, file=f)
                albumGenres['genres'].append(np.nan)
                continue
            
            else:
                #Empty list to store the genres listed in create_database() in accordance with the genres retrieved from spotify
                simplifiedGenres = []
                
                #Genres from spotify are more detailed ie. art pop, doom metal, etc. 
                #Split genre strings on whitespace and store result in set to make lookup faster
                genreSet = []
                for genre in subGenres:
                    genreSet.extend(genre.split())
            
                genreSet = set(genreSet)

                #Loop through genres defined in create_database() to see if they match any retrieved from Spotify and save valid genres in a list
                for genre in genres:
                    if genre in genreSet:
                        simplifiedGenres.append(genre)

            #Add genres to dictionary if found, otherwise add place holder nan
            if len(simplifiedGenres) == 0:
                print("No genre found for " + artistId, file=f)
                albumGenres['genres'].append(np.nan)
            else:   
                albumGenres['genres'].append(simplifiedGenres)
    try:
        print("All genres matched to album")
        return pd.DataFrame.from_dict(albumGenres)
    except:
        print('Error occured when converting genre dictionary to dataframe. Check for mismatched entries.')

#A few checks to make sure we have retrieved all the data we can
def check_if_valid(df:pd.DataFrame, albumIds) -> bool:
    if df.empty:
        raise Exception("No data retrieved. Terminating execution.")
    else:
        pass
    
    if pd.Series(df['spotify_id']).is_unique:
        pass
    else:
        raise Exception("Primary Key check violated.")
        
    if len(list(df['spotify_id'])) != len(albumIds):
        raise Exception("Not all album data retrieved.")
    else:
        pass


def create_database():
    
    #Set of genres we are intrested in. Results in a lot of albums skipped since genres like hip hop cant be checked for with current code logic
    genreClassifications = {'pop', 'rock', 'jazz', 'metal', 'country', \
                            'folk', 'indie', 'rap', 'punk'}

    reviewsCsvPath = '/opt/airflow/dags/reference_tables/albums.csv'
    jsonFile = '/opt/airflow/dags/database/matched_score_and_genre.json'
    
    reviewsDf = get_album_id(reviewsCsvPath)
    genresDf = get_album_genres(list(reviewsDf['spotify_id']), genreClassifications)
    
    #Makes sure no nessecary data is missing from the genre retrieval
    check_if_valid(genresDf, list(reviewsDf['spotify_id']))
    
    #Drop placeholder nan entries
    genresDf.dropna(inplace=True)
    
    #Inner merge since retaining extra entries on the review side does not have any use at the moment
    scoreGenreMatch = reviewsDf.merge(genresDf, how='inner', on='spotify_id')

    #Write final dataframe to json
    scoreGenreMatch.to_json(jsonFile, orient='records')