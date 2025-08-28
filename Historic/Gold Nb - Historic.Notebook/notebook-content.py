# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3fca51c1-7db2-47bf-b6cd-aca59bf08c94",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "6eb1325f-b953-490a-b555-06b17f8521c8",
# META       "known_lakehouses": [
# META         {
# META           "id": "3fca51c1-7db2-47bf-b6cd-aca59bf08c94"
# META         },
# META         {
# META           "id": "aa41f520-c88b-4137-b12c-24acc7631bd6"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Define variables for Silver and Gold table names (passed via pipeline)

s_countries = ""
s_fact_tv = ""
s_fact_movies = ""
s_genre_combined = ""
s_languages = ""

g_countries = ""
g_fact_tv = ""
g_fact_movie = ""
g_genre_combined = ""
g_languages = ""
g_genre_movie_bridge = "Gold_LH.genre_movie_bridge"
g_genre_tv_bridge = "Gold_LH.genre_tv_bridge"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import libraries for Spark operations

from pyspark.sql.functions import split, regexp_replace, col, explode
from pyspark.sql.types import ArrayType, IntegerType, cast
from datetime import date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create bridge table for movie genres from Silver layer data

df_movie = spark.read.table(s_fact_movies)

df_movie = df_movie.withColumn("Genre_IDs", \
        split(regexp_replace(col("Genre_IDs"), "[\\[\\]]", ""),",\\s*")\
        .cast(ArrayType(IntegerType())))\

df_movie_exp = df_movie.select("Movie_ID",explode(col("Genre_IDs")).alias("Genre_ID"))

df_movie_exp.write.format("delta").mode("overwrite")\
        .option("overwriteSchema",True).saveAsTable(g_genre_movie_bridge)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create bridge table for TV genres from Silver layer data

df_tv = spark.read.table(s_fact_tv)

df_tv = df_tv\
            .withColumn("Genre_IDs",split(regexp_replace(col("Genre_IDs"), "[\\[\\]]", ""),",\\s*")\
            .cast(ArrayType(IntegerType())))


df_tv_bridge = df_tv.select("TV_ID",explode(col("Genre_IDs")).alias("Genre_IDs"))

df_tv_bridge.write.format("delta").mode("overwrite").saveAsTable(g_genre_tv_bridge)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Copy dimension tables from Silver to Gold layer

spark.read.table(s_countries).write.format("delta")\
        .mode("overwrite").saveAsTable(g_countries)

spark.read.table(s_languages).write.format("delta")\
        .mode("overwrite").saveAsTable(g_languages)

spark.read.table(s_genre_combined).write\
        .format("delta").mode("overwrite").saveAsTable(g_genre_combined)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Copy fact tables from Silver to Gold layer

df_fact_movies = spark.read.table(s_fact_movies)
df_fact_tv = spark.read.table(s_fact_tv)

df_fact_movies.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable(g_fact_movie)

df_fact_tv.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable(g_fact_tv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
