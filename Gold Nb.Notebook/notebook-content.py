# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f908ebc-d61f-4264-97d2-eaa9e2d1b9c3",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "b8e7a887-498e-4e85-af11-885c32a43aa5",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad0ef244-eccd-4390-9f4a-899dd2b819f3"
# META         },
# META         {
# META           "id": "5f908ebc-d61f-4264-97d2-eaa9e2d1b9c3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### Pipeline parameters

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

# MARKDOWN ********************

# ##### Imports

# CELL ********************

# Import libraries for Spark operations and date handling

from pyspark.sql.functions import split, regexp_replace, col, explode
from pyspark.sql.types import ArrayType, IntegerType, cast
from datetime import date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### fact_movie genre transformation

# CELL ********************

# Update movie genre bridge table on the 15th or 28th
# - Converts Genre_IDs to array, explodes it, and saves if condition met

df_movie = spark.read.table(s_fact_movies)
df_genre_movie = spark.read.table('Silver_LH.genre_movie')


df_movie = df_movie.withColumn("Genre_IDs", \
        split(regexp_replace(col("Genre_IDs"), "[\\[\\]]", ""),",\\s*")\
        .cast(ArrayType(IntegerType())))\

df_movie_exp = df_movie.select("Movie_ID",explode(col("Genre_IDs")).alias("Genre_ID"))

df_movie_exp.write.format("delta").mode("overwrite").option("overwriteSchema",True)\
                .saveAsTable('Gold_LH.genre_movie_bridge')
                
df_genre_movie.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true").saveAsTable('Gold_LH.genre_movie')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### fact_tv genre transformation

# CELL ********************


# Update TV genre bridge table on the 15th or 28th
# - Converts Genre_IDs to array, explodes it, and saves if condition met

df_tv = spark.read.table('Silver_LH.tv_temp')
df_genre_tv = spark.read.table('Silver_LH.genre_tv')

df_tv = df_tv\
            .withColumn("Genres",split(regexp_replace(col("Genres"), "[\\[\\]]", ""),",\\s*")\
            .cast(ArrayType(IntegerType())))


df_tv_bridge = df_tv.select("TV_ID",explode(col("Genres")).alias("Genres"))

df_tv_bridge.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
                .saveAsTable('Gold_LH.genre_tv_bridge')

df_genre_tv.write.format("delta").mode("overwrite")\
    .option("overwriteSchema", "true").saveAsTable('Gold_LH.genre_tv')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Countries and Languages transfer

# CELL ********************

# Copy dimension tables to Gold layer on the 15th or 28th
# - Transfers countries and languages if condition met

if date.today().day in (15,28):

        spark.read.table(s_countries).write.format("delta")\
                .mode("overwrite").saveAsTable(g_countries)

        spark.read.table(s_languages).write.format("delta")\
                .mode("overwrite").option("overwriteSchema",True).saveAsTable(g_languages)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### fact_movies and fact_tv transfer

# CELL ********************

# Copy fact tables from Silver to Gold layer daily

df_fact_movies = spark.read.table('Silver_LH.fact_movies')
df_fact_tv = spark.read.table('Silver_LH.fact_tv')

df_fact_movies.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable('Gold_LH.fact_movies')

df_fact_tv.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable('Gold_LH.fact_tv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Copy fact tables from Silver to Gold layer daily
# 
# df_fact_movies = spark.read.table(s_fact_movies)
# df_fact_tv = spark.read.table(s_fact_tv)
# 
# df_fact_movies.write.format("delta").option("overwriteSchema",True)\
#                 .mode("overwrite").saveAsTable(g_fact_movie)
# 
# df_fact_tv.write.format("delta").option("overwriteSchema",True)\
#                 .mode("overwrite").saveAsTable(g_fact_tv)

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
