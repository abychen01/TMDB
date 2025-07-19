# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "639eaa96-065c-434f-be2d-d1ad81de7f33",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "6eb1325f-b953-490a-b555-06b17f8521c8",
# META       "known_lakehouses": [
# META         {
# META           "id": "639eaa96-065c-434f-be2d-d1ad81de7f33"
# META         }
# META       ]
# META     }
# META   }
# META }

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

# CELL ********************

# Import libraries for API requests, data processing, and Delta table operations

import requests, time, json
from pprint import pprint
from pyspark.sql.types import StringType,StructField,StructType, IntegerType
from pyspark.sql.functions import col, lit, desc,asc, year, row_number, count
from notebookutils import mssparkutils
from delta.tables import DeltaTable
from pyspark.sql.window import Window  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define schemas for movie and TV show tables

movie_schema = StructType([
    StructField("Adult", StringType(), True),
    StructField("Backdrop_Path", StringType(), True),
    StructField("Genre_IDs", StringType(), True),
    StructField("Movie_ID", StringType(), True),
    StructField("Original_Language", StringType(), True),
    StructField("Original_Title", StringType(), True),
    StructField("Overview", StringType(), True),
    StructField("Popularity", StringType(), True),
    StructField("Poster_Path", StringType(), True),
    StructField("Release_Date", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Video", StringType(), True),
    StructField("Vote_Average", StringType(), True),
    StructField("Vote_Count", StringType(), True)
])

tv_schema = StructType([
    StructField("Adult", StringType(), True),
    StructField("Backdrop_Path", StringType(), True),
    StructField("First_Air_Date", StringType(), True),
    StructField("Genre_IDs", StringType(), True),
    StructField("TV_ID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Origin_Country", StringType(), True),
    StructField("Original_Language", StringType(), True),
    StructField("Original_Name", StringType(), True),
    StructField("Overview", StringType(), True),
    StructField("Popularity", StringType(), True),
    StructField("Poster_Path", StringType(), True),
    StructField("Vote_Average", StringType(), True),
    StructField("Vote_Count", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Fetch initial movie data from TMDB API and store in a temporary table

movies_list = []

for page_no in range(1,400):
    url = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&page={page_no}&sort_by=popularity.desc&year=2025"

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            #print("success")
            results = response.json()['results']

            for movie in results:
                movie_dict = {
                            "Adult": str(movie.get("adult", "N/A")),
                            "Backdrop_Path": str(movie.get("backdrop_path", "N/A")),
                            "Genre_IDs": str(movie.get("genre_ids", "N/A")),
                            "Movie_ID": str(movie.get("id", "N/A")),
                            "Original_Language": str(movie.get("original_language", "N/A")),
                            "Original_Title": str(movie.get("original_title", "N/A")),
                            "Overview": str(movie.get("overview", "N/A")),
                            "Popularity": str(movie.get("popularity", "N/A")),
                            "Poster_Path": str(movie.get("poster_path", "N/A")),
                            "Release_Date": str(movie.get("release_date", "N/A")),
                            "Title": str(movie.get("title", "N/A")),
                            "Video": str(movie.get("video", "N/A")),
                            "Vote_Average": str(movie.get("vote_average", "N/A")),
                            "Vote_Count": str(movie.get("vote_count", "N/A"))
                }
                movies_list.append(movie_dict)

        else:
            print(f"Fail.....{response.status_code} || {response.text}")

    except Exception as e:
        print(f"Page {page_no}: Error while calling API: {e}")
        continue


movie_df = spark.createDataFrame(movies_list,schema=movie_schema)

movie_df = movie_df.sort(desc(col("Vote_Count")))
movie_df = movie_df.drop_duplicates(["Movie_ID"])

movie_df.write.format("delta").mode("overwrite")\
        .saveAsTable("temp_fact_movies")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enrich temporary movie data with additional details (budget, revenue, etc.)
# - Fetches details for each movie and updates the temporary table

movie_df2 = spark.read.table("temp_fact_movies")
count = movie_df2.count()
rows = movie_df2.take(count)
movie_data1 = []

for index, row in enumerate(rows):
    movie_id = row.Movie_ID
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        
        budget = data.get("budget", 0)
        imdb_id = data.get("imdb_id","N/A")
        origin_country = data.get("origin_country","N/A")
        revenue = data.get('revenue',None)
        runtime = data.get('runtime',None)
        
        movie_data1.append((movie_id,budget,imdb_id,origin_country,revenue,runtime))
    

    except Exception as e:
        print(e)

temp_df1 = spark.createDataFrame(movie_data1,\
            ["Movie_ID","budget","imdb_id","origin_country","revenue","runtime"])

movie_df2 = movie_df2.join(temp_df1,"Movie_ID","left")

movie_df2.write.format("delta").mode("overwrite")\
        .option("overwriteSchema",True).saveAsTable("temp_fact_movies")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# movie_df3 = movie_df3.drop_duplicates(["Movie_ID"])
# count2 = movie_df3.count()


# CELL ********************

# Enrich temporary movie data with cast information (lead actor and gender)
# - Fetches cast details and updates the temporary table

movie_df3 = spark.read.table("temp_fact_movies")
count2 = movie_df3.count()
rows = movie_df3.take(count2)
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
movie_df3 = movie_df3.join(actor_df,"Movie_ID","left")

movie_df3.write.format("delta").mode("overwrite")\
        .option("overwriteSchema",True).saveAsTable("temp_fact_movies")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge enriched movie data into the Bronze layer fact_movies table
# - Deduplicates, merges updates, and inserts new records

from delta.tables import DeltaTable
from pyspark.sql.functions import col, desc

# Read tables
movie_df4 = spark.read.table(bronze_dict['b_fact_movies'])
movie_df_update = spark.read.table("temp_fact_movies")

# Deduplicate by Movie_ID, keeping the row with the highest Vote_Count
movie_df4_sorted = movie_df4.sort(desc(col("Vote_Count")))
movie_df4 = movie_df4_sorted.drop_duplicates(["Movie_ID"])

movie_df_update_sorted = movie_df_update.sort(desc(col("Vote_Count")))
movie_df_update = movie_df_update_sorted.drop_duplicates(["Movie_ID"])

# Setup Delta table
delta_table = DeltaTable.forName(spark, bronze_dict['b_fact_movies'])

# Define merge logic
source_key = "Movie_ID"
target_key = "Movie_ID"
columns = [col for col in movie_df_update.columns if col != source_key]

update_condition = " OR ".join([f"target.{col} != source.{col}" for col in columns])

update_set = {col: f"source.{col}" for col in columns}

insert = {target_key: f"source.{source_key}", **{col: f"source.{col}" for col in columns}}

# Perform merge with error handling
try:
    merge = delta_table.alias("target")\
        .merge(
            source=movie_df_update.alias("source"),
            condition=f"target.{target_key} = source.{source_key}"
        )\
        .whenMatchedUpdate(
            condition=update_condition,
            set=update_set
        )\
        .whenNotMatchedInsert(
            values=insert
        )

    merge_result = merge.execute()
    print("Merge executed successfully")
    spark.sql("DROP TABLE temp_fact_movies")
    
except Exception as e:
    print(f"Merge failed: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch initial TV show data from TMDB API for 2025 and store in a temporary table
# - Fetches data across multiple pages, sorts, removes duplicates, and saves

url = "https://api.themoviedb.org/3/discover/tv?first_air_date_year=2025&include_adult=false&include_null_first_air_dates=false&language=en-US&page=1&sort_by=popularity.desc"
tv_list = []

response = requests.get(url, headers=headers)
total_pages = response.json()['total_pages']

for x in range(1,total_pages+3):

    url = f"https://api.themoviedb.org/3/discover/tv?first_air_date_year=2025&include_adult=false&include_null_first_air_dates=false&language=en-US&page={x}&sort_by=popularity.desc"
    
    try:
        response = requests.get(url, headers=headers)
        tv_data = response.json()['results']

        for y in tv_data:
            
            tv_dict = {
                    "Adult": str(y.get("adult", "N/A")),
                    "Backdrop_Path": str(y.get("backdrop_path", "N/A")),
                    "First_Air_Date": str(y.get("first_air_date", "N/A")),
                    "Genre_IDs": str(y.get("genre_ids", "N/A")),
                    "TV_ID": str(y.get("id", "N/A")),
                    "Name": str(y.get("name", "N/A")),
                    "Origin_Country": str(y.get("origin_country", "N/A")),
                    "Original_Language": str(y.get("original_language", "N/A")),
                    "Original_Name": str(y.get("original_name", "N/A")),
                    "Overview": str(y.get("overview", "N/A")),
                    "Popularity": str(y.get("popularity", "N/A")),
                    "Poster_Path": str(y.get("poster_path", "N/A")),
                    "Vote_Average": str(y.get("vote_average", "N/A")),
                    "Vote_Count": str(y.get("vote_count", "N/A"))
                }
            tv_list.append(tv_dict)

    except Exception as e:
        print(f"Fail.....{e} || ")

tv_df = spark.createDataFrame(tv_list,schema = tv_schema)

tv_df = tv_df.sort(desc(col("Vote_Count")))
tv_df = tv_df.drop_duplicates(["TV_ID"])

tv_df.write.format("delta").mode("overwrite")\
        .saveAsTable("temp_fact_tv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enrich temporary TV show data with additional details (runtime, seasons, etc.)
# - Fetches details for each TV show and updates the temporary table

tv2_list = []
tv2_df = spark.read.table("temp_fact_tv")
count = tv2_df.count()
rows = tv2_df.take(count)

for index, row in enumerate(rows):
    
    tv_id = row.TV_ID
    url = f"https://api.themoviedb.org/3/tv/{tv_id}?language=en-US"

    try:
        response = requests.get(url,headers = headers)
        response.raise_for_status()

        data = response.json()
        episode_run_time = data.get('episode_run_time',None)
        number_of_seasons = data.get('number_of_seasons', None)
        number_of_episodes = data.get('number_of_episodes', None)
        type = data.get('type',"N/A")

        tv2_list.append((tv_id,episode_run_time,number_of_seasons,type,number_of_episodes))


    except Exception as e:
        print(e)

temptv_df = spark.createDataFrame(tv2_list,\
            ["TV_ID","episode_run_time","number_of_seasons","type","number_of_episodes"])

tv2_df = tv2_df.join(temptv_df,"TV_ID","left")

tv2_df.write.format("delta").mode("overwrite")\
            .option("overwriteSchema",True).saveAsTable("temp_fact_tv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge enriched TV show data into the Bronze layer fact_tv table
# - Deduplicates, merges updates, and inserts new records

# Read tables
tv_df4 = spark.read.table(bronze_dict['b_fact_tv'])
tv_df_update = spark.read.table("temp_fact_tv")

# Deduplicate by Movie_ID, keeping the row with the highest Vote_Count
tv_df4_sorted = tv_df4.sort(desc(col("Vote_Count")))
tv_df4 = tv_df4_sorted.drop_duplicates(["TV_ID"])

tv_df_update_sorted = tv_df_update.sort(desc(col("Vote_Count")))
tv_df_update = tv_df_update_sorted.drop_duplicates(["TV_ID"])

# Setup Delta table
delta_table = DeltaTable.forName(spark, bronze_dict['b_fact_tv'])

# Define merge logic
source_key = "TV_ID"
target_key = "TV_ID"
columns = [col for col in tv_df_update.columns if col != source_key]

update_condition = " OR ".join([f"target.{col} != source.{col}" for col in columns])

update_set = {col: f"source.{col}" for col in columns}

insert = {target_key: f"source.{source_key}", **{col: f"source.{col}" for col in columns}}

# Perform merge with error handling
try:
    merge = delta_table.alias("target")\
        .merge(
            source=tv_df_update.alias("source"),
            condition=f"target.{target_key} = source.{source_key}"
        )\
        .whenMatchedUpdate(
            condition=update_condition,
            set=update_set
        )\
        .whenNotMatchedInsert(
            values=insert
        )

    merge_result = merge.execute()
    print("Merge executed successfully")
    spark.sql("DROP TABLE temp_fact_tv")
    
except Exception as e:
    print(f"Merge failed: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Combine all dictionaries and exit the notebook with output
# - Merges Bronze, Silver, and Gold dictionaries for pipeline reference

output = bronze_dict | silver_dict | gold_dict

mssparkutils.notebook.exit(json.dumps(output))

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
