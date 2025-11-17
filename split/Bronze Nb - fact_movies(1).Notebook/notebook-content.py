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

# MARKDOWN ********************

# ##### 

# MARKDOWN ********************

# ##### Parameters

# PARAMETERS CELL ********************

start = 0
end = 500000

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Table defs

# CELL ********************

# Define dictionaries for table names across Bronze, Silver, and Gold Lakehouses

bronze_dict = {
    "b_countries": "Bronze_LH.countries",
    "b_fact_tv": "Bronze_LH.fact_tv",
    "b_fact_movies": "Bronze_LH.fact_movies",
    "b_genre_tv": "Bronze_LH.genre_tv",
    "b_genre_movie": "Bronze_LH.genre_movie",
    "b_languages": "Bronze_LH.languages"
}

silver_dict = {
    "s_countries": "Silver_LH.countries",
    "s_fact_tv": "Silver_LH.fact_tv",
    "s_fact_movies": "Silver_LH.fact_movies",
    "s_genre_combined": "Silver_LH.genre_combined",
    "s_languages": "Silver_LH.languages"
}

gold_dict = {
    "g_countries": "Gold_LH.countries",
    "g_fact_tv": "Gold_LH.fact_tv",
    "g_fact_movies": "Gold_LH.fact_movies",
    "g_genre_tv_bridge": "Gold_LH.genre_tv_bridge",
    "g_genre_movie_bridge": "Gold_LH.genre_movie_bridge",
    "g_languages": "Gold_LH.languages",
    "g_genre_combined": "Gold_LH.genre_combined"
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Imports

# CELL ********************

# Import libraries for API requests, data processing, and Delta table operations

import requests, time, json, datetime
from pprint import pprint
from pyspark.sql.types import StringType,StructField,StructType, \
    IntegerType, FloatType, LongType, BooleanType, DateType, TimestampType
from pyspark.sql.functions import col, lit, desc,asc, year, row_number, count
from notebookutils import mssparkutils
from delta.tables import DeltaTable
from pyspark.sql.window import Window  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Schema defs

# CELL ********************

# Define schemas for movie and TV show tables

movie_schema = StructType([
    StructField("Adult", BooleanType(), True),                 
    StructField("Backdrop_Path", StringType(), True),       
    StructField("Genre_IDs", StringType(), True),            
    StructField("Movie_ID", IntegerType(), True),
    StructField("IMDB_ID", StringType(), True),
    StructField("Original_Language", StringType(), True),
    StructField("Origin_Country", StringType(), True),
    StructField("Original_Title", StringType(), True),
    StructField("Overview", StringType(), True),
    StructField("Popularity", FloatType(), True),
    StructField("Poster_Path", StringType(), True),
    StructField("Release_Date", StringType(), True),          
    StructField("Revenue", LongType(), True),
    StructField("Runtime", IntegerType(), True),
    StructField("Budget", LongType(), True),
    StructField("Status", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Video", StringType(), True),                 
    StructField("Vote_Average", FloatType(), True),
    StructField("Vote_Count", IntegerType(), True)
])

tv_schema = StructType([
    StructField("Adult", BooleanType(), True),
    StructField("Backdrop_Path", StringType(), True),
    StructField("Episode_Run_Time", IntegerType(), True),
    StructField("First_Air_Date", StringType(), True),
    StructField("Genres", StringType(), True),
    StructField("TV_ID", IntegerType(), True),
    StructField("In_Production", BooleanType(), True),
    StructField("Last_Air_Date", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Number_Of_Episodes", IntegerType(), True),
    StructField("Number_Of_Seasons", IntegerType(), True),
    StructField("Origin_Country", StringType(), True),
    StructField("Original_Language", StringType(), True),
    StructField("Original_Name", StringType(), True),
    StructField("Overview", StringType(), True),
    StructField("Popularity", FloatType(), True),
    StructField("Poster_Path", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Vote_Average", FloatType(), True),
    StructField("Vote_Count", IntegerType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### API Header def

# CELL ********************

# Set up headers for TMDB API requests with API key

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

##### Logs table creation

schema = StructType([
    StructField("Date",TimestampType()),
    StructField('ID', IntegerType()),
    StructField('Table',StringType()),
    StructField('Status',StringType())  
])
lists = {}
temp_df = spark.createDataFrame(lists,schema=schema)
temp_df.write.format("delta").saveAsTable("logs")

spark.sql("TRUNCATE TABLE logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

movies_list = []
fail_count = 0

for id in range(start,end+1):

    url = f"https://api.themoviedb.org/3/movie/{id}?language=en-US"

    try:
        response = requests.get(url, headers=headers)
        time = datetime.datetime.now()

        if response.status_code == 200:
            data = response.json()
            #pprint(data)
            
            movie_dict = {
                    "Adult": data.get("adult", False),
                    "Backdrop_Path": data.get("backdrop_path", None),
                    "Genre_IDs": [genre['id'] for genre in data.get("genres", [])],
                    "Movie_ID": data.get("id", 0),
                    "IMDB_ID": data.get("imdb_id", None),
                    "Original_Language": data.get("original_language", None),
                    "Origin_Country": data.get("origin_country", []),
                    "Original_Title": data.get("original_title", None),
                    "Overview": data.get("overview", None),
                    "Popularity": data.get("popularity", 0.0),
                    "Poster_Path": data.get("poster_path", None),
                    "Release_Date": data.get("release_date", None),
                    "Revenue": data.get("revenue", 0),
                    "Runtime": data.get("runtime", 0),
                    "Budget": data.get("budget", 0),
                    "Status": data.get("status", None),
                    "Title": data.get("title", None),
                    "Video": data.get("video", False),
                    "Vote_Average": data.get("vote_average", 0.0),
                    "Vote_Count": data.get("vote_count", 0)
                }
                

            movies_list.append(movie_dict)
            if id%500 == 0:
                spark.sql(f"""
                            INSERT INTO logs (ID, Status, Table, Date) 
                            VALUES
                            ({id},'Success','fact_movies','{time}')
                """)

        else: 
            if id%500 == 0:
                 spark.sql(f"""
                            INSERT INTO logs (ID, Status, Table, Date) 
                            VALUES
                            ({id},'Fail','fact_movies','{time}')
                """)
            fail_count = fail_count + 1

    except Exception as e:
        print(e)
        continue


movie_df1 = spark.createDataFrame(movies_list,schema=movie_schema)
print(fail_count)
display(movie_df1)
#movie_df1.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable("temp")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


count2 = movie_df1.count()
rows = movie_df1.take(count2)
movie_data2 = []

for index, row in enumerate(rows):
    movie_id = row.Movie_ID

    url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?language=en-US"

    try:
        response = requests.get(url,headers=headers)
        response.raise_for_status()

        data = response.json()

        if data.get("cast") and len(data["cast"]) > 0:
            name = data["cast"][0].get("name", "N/A")
            gender = data["cast"][0].get("gender", "N/A")
        else:
            name = "N/A"
            gender = "N/A"

        movie_data2.append((movie_id,name,gender))

    except Exception as e:
        print(e)

actor_df = spark.createDataFrame(movie_data2,["Movie_ID","Name","Gender"])
movie_df1 = movie_df1.join(actor_df,"Movie_ID","left")
display(movie_df1)
movie_df1.write.format("delta").mode("overwrite").option("mergeSchema", True).saveAsTable("fact_movies")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#testing....
'''
tv_list = []
fail_count = 0

# max = 199989

for id in range(1,100):

    url = f"https://api.themoviedb.org/3/tv/{id}?language=en-US"

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            #pprint(data)
            
            tv_dict = {
                "Adult": data.get("adult", False),
                "Backdrop_Path": data.get("backdrop_path", None),
                "Episode_Run_Time": data.get("episode_run_time", []),
                "First_Air_Date": data.get("first_air_date", None),
                "Genres": data.get("genres", []),
                "TV_ID": data.get("id", 0),
                "In_Production": data.get("in_production", False),
                "Last_Air_Date": data.get("last_air_date", None),
                "Last_Episode_To_Air": data.get("last_episode_to_air", {}),
                "Name": data.get("name", None),
                "Number_Of_Episodes": data.get("number_of_episodes", 0),
                "Number_Of_Seasons": data.get("number_of_seasons", 0),
                "Origin_Country": data.get("origin_country", []),
                "Original_Language": data.get("original_language", None),
                "Original_Name": data.get("original_name", None),
                "Overview": data.get("overview", None),
                "Popularity": data.get("popularity", 0.0),
                "Poster_Path": data.get("poster_path", None),
                "Status": data.get("status", None),
                "Type": data.get("type", None),
                "Vote_Average": data.get("vote_average", 0.0),
                "Vote_Count": data.get("vote_count", 0)
            }
            tv_list.append(tv_dict)

    except Exception as e:
        print(f"Fail.....{e} || ")

tv_df = spark.createDataFrame(tv_list,schema = tv_schema)

print(fail_count)
display(tv_df)
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
