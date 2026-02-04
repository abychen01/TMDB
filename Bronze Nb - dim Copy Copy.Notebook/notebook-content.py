# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "127e6c72-d96b-4985-ab2f-e39b41092274",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "3fc15e8d-b7ec-4d57-abd0-49cbf308b40a",
# META       "known_lakehouses": [
# META         {
# META           "id": "127e6c72-d96b-4985-ab2f-e39b41092274"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Define a dictionary for Bronze layer table names
# - This dictionary maps keys to table paths for easier reference in the code

bronze_dict = {
    "b_countries": "Bronze_LH.countries",
    "b_fact_tv": "Bronze_LH.fact_tv",
    "b_fact_movies": "Bronze_LH.fact_movies",
    "b_genre_tv": "Bronze_LH.genre_tv",
    "b_genre_movie": "Bronze_LH.genre_movie",
    "b_languages": "Bronze_LH.languages"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary libraries for API requests and Spark DataFrame operations
# - requests: For making HTTP requests to the TMDB API
# - pyspark.sql.types: For defining DataFrame schemas

import requests
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define schemas for dimension tables

genre_movie_schema = StructType([
    StructField("id", IntegerType(),True),
    StructField("name", StringType(),True)
])

genre_tv_schema = StructType([
    StructField("id", IntegerType(),True),
    StructField("name", StringType(),True)
])

country_schema = StructType([
    StructField("iso_3166_1",StringType(),True),
    StructField("english_name",StringType(),True),
    StructField("native_name",StringType(),True)
])

language_schema = StructType([
    StructField("iso_639_1",StringType(),True),
    StructField("english_name",StringType(),True),
    StructField("name",StringType(),True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set up headers for TMDB API requests with API key
# - Headers include content type and authorization token for API access

headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIwMmZjZGM2ZGQxZmIxOTNlNjQ2MjU5MGU0ZmUwZWM2NCIsIm5iZiI6MTc1MTg4OTMxNy45ODIsInN1YiI6IjY4NmJiNWE1NzFiNzVhZDM3NGE5NWJmZiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.DrBLlqA8g9mlH2zJC0c60vogL1jmcGNH2oMdg2qhP3s"
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GENRE MOVIE
# Fetch movie genres from TMDB API and store in Bronze layer

url = "https://api.themoviedb.org/3/genre/movie/list?language=en"

genre_movie_list = []

try:

    response = requests.get(url, headers=headers)
    genre_movie_data = response.json()['genres']

    for x in genre_movie_data:
        genre_movie_dict = {
            "id": x.get('id',"N/A"),
            "name": x.get('name',"N/A")
        }
        genre_movie_list.append(genre_movie_dict)
except Exception as e:
    print(f"Fail------{e}")

genre_movie_df = spark.createDataFrame(genre_movie_list, schema=genre_movie_schema)
genre_movie_df.write.format("delta").mode("overwrite")\
                .saveAsTable(bronze_dict['b_genre_movie'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_LH.languages LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GENRE TV
# Fetch TV genres from TMDB API and store in Bronze layer

url = "https://api.themoviedb.org/3/genre/tv/list?language=en"

genre_tv_list = []

try:

    response = requests.get(url, headers=headers)
    genre_tv_data = response.json()['genres']

    for x in genre_tv_data:
        genre_tv_dict = {
            "id": x.get('id',"N/A"),
            "name": x.get('name',"N/A")
        }
        genre_tv_list.append(genre_tv_dict)
except Exception as e:
    print(f"Fail------{e}")

genre_tv_df = spark.createDataFrame(genre_tv_list, schema=genre_tv_schema)
genre_tv_df.write.format("delta").mode("overwrite")\
            .saveAsTable(bronze_dict['b_genre_tv'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GENRE COUNTRIES
# Fetch countries from TMDB API and store in Bronze layer

url = "https://api.themoviedb.org/3/configuration/countries?"
country_list = []

try:
    response = requests.get(url, headers=headers)
    country_data = response.json()

    for x in country_data:

        country_dict = {
            "iso_3166_1": x.get('iso_3166_1',"N/A"),
            "english_name": x.get('english_name',"N/A"),
            "native_name": x.get('native_name',"N/A")
        }
        country_list.append(country_dict)

except Exception as e:
    print(f"Fail....{e}")

country_df = spark.createDataFrame(country_list,schema=country_schema)
country_df.write.format("delta").mode("overwrite")\
            .saveAsTable(bronze_dict['b_countries'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GENRE LANGUAGES
# Fetch languages from TMDB API and store in Bronze layer

url = "https://api.themoviedb.org/3/configuration/languages"
language_list = []

try:
    response = requests.get(url, headers=headers)
    language_data = response.json()

    for x in language_data:

        language_dict = {
            "iso_639_1": x.get('iso_639_1',"N/A"),
            "english_name": x.get('english_name',"N/A"),
            "name": "N/A" if x['name']=="" else x.get('name',"N/A")
        }
        language_list.append(language_dict)

except Exception as e:
    print(f"Fail....{e}")

language_df = spark.createDataFrame(language_list,schema=language_schema)
language_df.write.format("delta").mode("overwrite")\
            .saveAsTable(bronze_dict['b_languages'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # List all tables in the current Lakehouse
# tables = spark.catalog.listTables()
# 
# # Loop through each table and drop it
# for table in tables:
#     table_name = table.name
#     try:
#         spark.sql(f"DROP TABLE IF EXISTS {table_name}")
#         print(f"Successfully dropped table: {table_name}")
#     except Exception as e:
#         print(f"Failed to drop table {table_name}: {str(e)}")
