# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ad0ef244-eccd-4390-9f4a-899dd2b819f3",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "b8e7a887-498e-4e85-af11-885c32a43aa5",
# META       "known_lakehouses": [
# META         {
# META           "id": "d54a4800-b077-4df7-a53b-4a79430916a4"
# META         },
# META         {
# META           "id": "ad0ef244-eccd-4390-9f4a-899dd2b819f3"
# META         }
# META       ]
# META     }
# META   }
# META }

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

# CELL ********************

# Import libraries for Spark operations, date handling

from pyspark.sql.types import cast
from pyspark.sql.functions import col, when, substring
from datetime import date
from notebookutils import mssparkutils
import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# b2_countries = "Bronze_LH.countries"
# b2_fact_tv = "Bronze_LH.fact_tv"
# b2_fact_movies = "Bronze_LH.fact_movies"
# b2_genre_tv = "Bronze_LH.genre_tv"
# b2_genre_movie = "Bronze_LH.genre_movie"
# b2_languages = "Bronze_LH.languages"
# 
# s2_countries = "Silver_LH.countries"
# s2_fact_tv = "Silver_LH.fact_tv"
# s2_fact_movies = "Silver_LH.fact_movies"
# s2_genre_tv = "Silver_LH.genre_tv"
# s2_genre_movie = "Silver_LH.genre_movie"
# s2_languages = "Silver_LH.languages"
# s2_genre_combined = "Silver_LH.genre_combined"

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


# CELL ********************

# Transform movie data from Bronze to Silver layer
# - Drops unnecessary columns, extracts first origin_country, casts types, maps gender

df_movie = spark.read.table(b_fact_movies)

df_movie = df_movie.drop(col("Adult"),col("Video"))\
        .withColumn("origin_country", substring(df_movie.origin_country,2,2))\
        .withColumn("Movie_ID",df_movie.Movie_ID.cast("int"))\
        .withColumn("Release_Date",df_movie.Release_Date.cast("date"))\
        .withColumn("Popularity",df_movie.Popularity.cast("float"))\
        .withColumn("Vote_Average",df_movie.Vote_Average.cast("float"))\
        .withColumn("Vote_Count",df_movie.Vote_Count.cast("float"))\
        .withColumn("Gender", when(df_movie.Gender == 0,"N/A")\
                                .when(df_movie.Gender == 1,"Female")\
                                .when(df_movie.Gender == 2,"Male"))

df_movie.write.format("delta").mode("overwrite")\
        .option("overwriteSchema",True).saveAsTable(s_fact_movies)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform TV show data from Bronze to Silver layer
# - Drops unnecessary columns, extracts country code, casts types

df_tv = spark.read.table(b_fact_tv)

df_tv = df_tv.drop(col("Adult"))\
        .withColumn("Origin_Country", df_tv.Origin_Country.substr(3,2))\
        .withColumn("episode_run_time", df_tv.episode_run_time[0])\
        .withColumn("TV_ID",df_tv.TV_ID.cast("int"))\
        .withColumn("First_Air_Date",df_tv.First_Air_Date.cast("date"))\
        .withColumn("Popularity",df_tv.Popularity.cast("float"))\
        .withColumn("Vote_Average",df_tv.Vote_Average.cast("float"))\
        .withColumn("Vote_Count",df_tv.Vote_Count.cast("float"))

df_tv.write.format("delta").option("overwriteSchema",True).mode("overwrite").saveAsTable(s_fact_tv)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform dimension tables on the 15th or 28th

if date.today().day in (15,28):

    df_countries = spark.read.table(b_countries)
    df_countries = df_countries.withColumnRenamed("iso_3166_1","country_id")
    df_countries.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable(s_countries)

    df_languages = spark.read.table(b_languages)
    df_languages = df_languages.withColumnRenamed("iso_639_1","language_id")
    df_languages.write.format("delta").mode("overwrite")\
                .option("overwriteSchema", "true").saveAsTable(s_languages)

    df_genre_tv = spark.read.table(b_genre_tv).withColumnRenamed("id","tv_id")
    df_genre_movie = spark.read.table(b_genre_movie).withColumnRenamed("id","movie_id")

    df_join = df_genre_movie.join(df_genre_tv,"name","outer")
    df_genre = df_join.withColumn("id", when(col("movie_id").isNull(),df_join.tv_id)\
                    .otherwise(df_join.movie_id))
                    
    df_genre = df_genre.drop(col("movie_id"),col("tv_id"))

    df_genre.write.format("delta").mode("overwrite").saveAsTable(s_genre_combined)

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

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
