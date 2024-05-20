import pandas as pd
import json
from pymongo import MongoClient

# Read Excel file
excel_file_path = 'C:\\Users\\janid\\OneDrive\\Documents\\Uni\\CCCU 23 24\\Advanced OS\\Assignment 2\\Dataset\\Movies_Data2.xlsx'
excel_data = pd.ExcelFile(excel_file_path)
sheet_names = excel_data.sheet_names

# Initialize dictionaries to store data from each sheet
movies_data = {}
country_data = {}
artist_data = {}
role_data = []
internet_user_data = {}
score_movie_data = []

# Process each sheet
for sheet_name in sheet_names:
    df = excel_data.parse(sheet_name)
    #df = df.fillna('')  # Replace NaN values with empty strings
    print(f"Processing sheet: {sheet_name}")
    #print(df.head())  # Print the first few rows to check the data

    if sheet_name == 'Movie Table':
        for _, row in df.iterrows():
            movies_data[row['movieId']] = {
                'movieId': row['movieId'],
                'title': row['title'],
                'year': int(row['year']),
                'genre': row['genre'],
                'summary': row['summary'],
                'producer': {
                    'producerId': row['producerId']
                },
                'countryCode': row['countryCode'],
                'roles': [],
                'scores': []
            }
        print(f"Movies data: {movies_data}")
    elif sheet_name == 'Country Table':
        for _, row in df.iterrows():
            country_data[row['code']] = {
                'code': row['code'],
                'name': row['name'],
                'language': row['language']
            }
        print(f"Country data: {country_data}")
    elif sheet_name == 'Artist Table':
        for _, row in df.iterrows():
            artist_data[row['artistId']] = {
                'artistId': row['artistId'],
                'surname': row['surname'],
                'name': row['name'],
                'DOB': row['DOB']
            }
        print(f"Artist data: {artist_data}")
    elif sheet_name == 'Role Table':
        for _, row in df.iterrows():
            role = {
                'movieId': row['movieId'],
                'actorId': row['actorId'],
                'roleName': row['roleName'],
                'artistId': row['actorId'] 
            }
            role_data.append(role)
        print(f"Role data: {role_data}")
    elif sheet_name == 'Internet_user Table':
        for _, row in df.iterrows():
            internet_user_data[row['email']] = {
                'email': row['email'],
                'surname': row['surname'],
                'name': row['name'],
                'region': row['region']
            }
        print(f"Internet user data: {internet_user_data}")
    elif sheet_name == 'Score_movie Table':
        for _, row in df.iterrows():
            score = {
                'email': row['email'],
                'movieId': row['movieId'],
                'score': float(row['score'])
            }
            score_movie_data.append(score)
        print(f"Score movie data: {score_movie_data}")

# Combine data into the desired MongoDB document structure
for role in role_data:
    movie_id = role['movieId']
    actor_id = role['actorId']
    artist = artist_data.get(actor_id)
    if artist is not None:
        role_entry = {
            'actorId': actor_id,
            'roleName': role['roleName'],
            'artist': artist
        }
        if movie_id in movies_data:
            movies_data[movie_id]['roles'].append(role_entry)
        else:
            print(f"Movie ID {movie_id} not found for role {role_entry}")
    else:
        print(f"Artist ID {actor_id} not found for role {role}")

for score in score_movie_data:
    movie_id = score['movieId']
    user_email = score['email']
    score_entry = {
        'email': user_email,
        'score': score['score']
    }
    if movie_id in movies_data:
        movies_data[movie_id]['scores'].append(score_entry)
    else:
        print(f"Movie ID {movie_id} not found for score {score_entry}")

for movie_id, movie in movies_data.items():
    country_code = movie['countryCode']
    if country_code in country_data:
        movie['country'] = country_data[country_code]
    else:
        print(f"Country code {country_code} not found for movie {movie}")
    del movie['countryCode']

# Convert the movies_data dictionary into a list of movies
movies_list = list(movies_data.values())

# Print the final document
#print(json.dumps(movies_list, indent=2))

# Connect to MongoDB and insert the document
mongo_url = "mongodb+srv://Janidu:JanBas12@janiducluster.wrpfosj.mongodb.net/moviesdb?retryWrites=true&w=majority&appName=JaniduCluster"
client = MongoClient(mongo_url)

# Select the database and collection
db = client['MovieDB']
collection = db['movies']

# Insert each movie into the collection
for movie in movies_list:
    collection.insert_one(movie)

print("Data successfully inserted into MongoDB")
