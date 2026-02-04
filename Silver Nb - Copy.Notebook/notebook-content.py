# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "536e613a-6818-4725-a8b8-258ae5a7aec7",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "3fc15e8d-b7ec-4d57-abd0-49cbf308b40a",
# META       "known_lakehouses": [
# META         {
# META           "id": "536e613a-6818-4725-a8b8-258ae5a7aec7"
# META         },
# META         {
# META           "id": "127e6c72-d96b-4985-ab2f-e39b41092274"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### Pipeline parameters

# PARAMETERS CELL ********************

# Define variables for Bronze and Silver table names (passed via pipeline)

b_countries = ""
b_fact_tv = ""
b_fact_movies = ""
b_genre_tv = ""
b_genre_movie = ""
b_languages = ""

s_countries = ""
s_fact_tv = ""
s_fact_movies = ""
s_genre_combined = ""
s_languages = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Imports

# CELL ********************

# Import libraries for Spark operations, date handling

from pyspark.sql.types import cast
from pyspark.sql.functions import col, when, substring, year, length
from datetime import date
from notebookutils import mssparkutils
import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

b_countries = "Bronze_LH.countries"
b_fact_tv = "Bronze_LH.fact_tv"
b_fact_movies = "Bronze_LH.fact_movies"
b_genre_tv = "Bronze_LH.genre_tv"
b_genre_movie = "Bronze_LH.genre_movie"
b_languages = "Bronze_LH.languages"

s_countries = "Silver_LH.countries"
s_fact_tv = "Silver_LH.fact_tv"
s_fact_movies = "Silver_LH.fact_movies"
s_genre_tv = "Silver_LH.genre_tv"
s_genre_movie = "Silver_LH.genre_movie"
s_languages = "Silver_LH.languages"
s_genre_combined = "Silver_LH.genre_combined"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# '''
# df_countries = spark.read.table(b2_countries)
# df_tv = spark.read.table(s2_fact_tv)
# df3 = spark.read.table(s2_fact_movies)
# df_genre_tv = spark.read.table(b2_genre_tv).withColumnRenamed("id","tv_id")
# df_genre_movie = spark.read.table(b2_genre_movie).withColumnRenamed("id","movie_id")
# df_languages = spark.read.table(b2_languages)
# '''
# 
# 
# #display(df_countries)
# display(spark.read.table(b2_fact_movies))
# #display(df3)
# #display(df_languages)
# #display(df_genre_combined)
# 


# MARKDOWN ********************

# ##### fact_movie transformations

# CELL ********************

df_movie = spark.read.table(b_fact_movies)


cols_list = [c for c in df_movie.columns if c not in ['Origin_Country','Movie_ID','Release_Date',\
                'Popularity','Vote_Average','Vote_Count','Gender','Adult','Video']]

df_movie = df_movie.select(
            *[col(c) for c in cols_list],
            substring(df_movie.Origin_Country,2,2).alias('Origin_Country'),
            df_movie.Movie_ID.cast("int").alias('Movie_ID'),
            df_movie.Release_Date.cast("date").alias('Release_Date'),
            df_movie.Popularity.cast("float").alias('Popularity'),
            df_movie.Vote_Average.cast("float").alias('Vote_Average'),
            df_movie.Vote_Count.cast("float").alias('Vote_Count'),
            when(df_movie.Gender == 1,"Female")\
                .when(df_movie.Gender == 2,"Male")\
                .otherwise('N/A').alias('Gender')
)

df_movie.write.format("delta").mode("overwrite")\
        .option("overwriteSchema",True).saveAsTable(s_fact_movies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### fact_tv transformations

# CELL ********************

# Transform TV show data from Bronze to Silver layer
# - Drops unnecessary columns, extracts country code, casts types

df_tv = spark.read.table(b_fact_tv)

cols_list = [c for c in df_tv.columns if c not in \
                ['Origin_Country','Episode_Run_Time','TV_ID','First_Air_Date','Last_Air_Date',\
                'Popularity','Vote_Average','Vote_Count','Adult']]

df_tv = df_tv.select(
                *[col(c) for c in cols_list],
                df_tv.Origin_Country.substr(2,2).alias('Origin_Country'),
                when(length(df_tv.Episode_Run_Time)>2, df_tv.Episode_Run_Time.substr(2,2))\
                        .otherwise('').cast('int').alias('Episode_Run_Time'),
                df_tv.TV_ID.cast("int").alias('TV_ID'), 
                df_tv.First_Air_Date.cast("date").alias('First_Air_Date'),
                df_tv.Last_Air_Date.cast("date").alias('Last_Air_Date'),
                df_tv.Popularity.cast("float").alias('Popularity'),
                df_tv.Vote_Average.cast("float").alias('Vote_Average'),
                df_tv.Vote_Count.cast("float").alias('Vote_Count')

)

df_tv.write.format("delta").option("overwriteSchema",True).mode("overwrite").saveAsTable(s_fact_tv)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### other transformations

# CELL ********************

# Transform dimension tables on the 15th or 28th

if True:

    df_countries = spark.read.table(b_countries)
    df_countries = df_countries.withColumnRenamed("iso_3166_1","country_id")
    df_countries.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable(s_countries)

    df_languages = spark.read.table(b_languages)
    df_languages = df_languages.withColumnRenamed("iso_639_1","language_id")
    df_languages.write.format("delta").mode("overwrite")\
                .option("overwriteSchema", "true").saveAsTable(s_languages)

    df_genre_tv = spark.read.table('Bronze_LH.genre_tv').withColumnRenamed("id","tv_id")\
                        .write.format("delta").mode("overwrite").saveAsTable('Silver_LH.genre_tv')
    df_genre_movie = spark.read.table('Bronze_LH.genre_movie').withColumnRenamed("id","movie_id")\
                        .write.format("delta").mode("overwrite").saveAsTable('Silver_LH.genre_movie')

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
