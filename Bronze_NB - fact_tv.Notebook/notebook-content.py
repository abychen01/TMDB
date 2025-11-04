# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d54a4800-b077-4df7-a53b-4a79430916a4",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "b8e7a887-498e-4e85-af11-885c32a43aa5",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f908ebc-d61f-4264-97d2-eaa9e2d1b9c3"
# META         },
# META         {
# META           "id": "d54a4800-b077-4df7-a53b-4a79430916a4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### Imports

# CELL ********************

# Import libraries for API requests, data processing, and Delta table operations

import requests, time, json, datetime
from pprint import pprint
from pyspark.sql.types import StringType,StructField,StructType, IntegerType, \
    FloatType, LongType, BooleanType, DateType
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

# ##### Schema def

# CELL ********************

tv_schema = StructType([
    StructField("Adult", BooleanType(), True),
    StructField("Backdrop_Path", StringType(), True),
    StructField("Episode_Run_Time", StringType(), True),
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

# ##### API def

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

# MARKDOWN ********************

# ##### API call

# CELL ********************

#testing....
#350000 - total number of rows

tv_list = []
fail_count = 0

max = 350000

for id in range(1,max):

    url = f"https://api.themoviedb.org/3/tv/{id}?language=en-US"

    try:
        response = requests.get(url, headers=headers)
        time = datetime.datetime.now()

        if response.status_code == 200:
            data = response.json()
            #pprint(data)
            
            tv_dict = {
                "Adult": data.get("adult", False),
                "Backdrop_Path": data.get("backdrop_path", None),
                "Episode_Run_Time": data.get("episode_run_time", []),
                "First_Air_Date": data.get("first_air_date", None),
                "Genres": [genre.get("id") for genre in data.get("genres", [])],
                "TV_ID": data.get("id", 0),
                "In_Production": data.get("in_production", False),
                "Last_Air_Date": data.get("last_air_date", None),
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

        if id%500 == 0:
                spark.sql(f"""
                            INSERT INTO logs (ID, Status, Table, Date) 
                            VALUES
                            ({id},'Success','fact_tv','{time}')
                """)

        else: 
            if id%500 == 0:
                 spark.sql(f"""
                            INSERT INTO logs (ID, Status, Table, Date) 
                            VALUES
                            ({id},'Fail','fact_tv','{time}')
                """)
            fail_count = fail_count + 1


    except Exception as e:
        print(f"Fail.....{e} || ")
        continue

tv_df = spark.createDataFrame(tv_list,schema = tv_schema)
print(fail_count)

tv_df.write.format("delta").mode("overwrite").option("mergeSchema", True).saveAsTable("fact_tv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
