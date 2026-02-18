hhihhhv# TMDB Data Engineering Project

This project is a data engineering pipeline that collects movie and TV show data from The Movie Database (TMDB) API and processes it using a medallion architecture (Bronze, Silver, Gold layers). It is implemented using Microsoft Fabric with PySpark notebooks and orchestrated via pipelines. The project is divided into two main parts: **Historic ETL** for initial data collection and **Daily ETL** for incremental updates.

<a href="https://app.fabric.microsoft.com/view?r=eyJrIjoiMWNkOWE3MzYtNDhiYy00MDI2LWJlMzQtZDM0M2IxOTgwZjA2IiwidCI6IjZkYWRkOGM5LTMxMGEtNGE2Ni05MzRhLWQ5MGI1OTk5YjViMCJ9" target="_blank">Power BI report</a>


## Project Overview

The pipeline fetches data such as movies, TV shows, genres, countries, and languages from the TMDB API for the year 2025. The data is processed and stored in Lakehouses (LH) following the medallion architecture:
- **Bronze Layer**: Raw data ingestion from the TMDB API.
- **Silver Layer**: Cleaned and transformed data with schema enforcement.
- **Gold Layer**: Aggregated and enriched data for analytics, including bridge tables for relationships.

The project includes two ETL processes:
1. **Historic ETL**: Collects all available data and processes it through the medallion layers.
2. **Daily ETL**: Updates fact tables daily and dimension tables on the 15th and 28th of each month.

## Repository Structure

- **Notebooks**:
  - `Bronze Nb Historic.ipynb`: Ingests all historical data into the Bronze layer.
  - `Bronze Nb - dim.ipynb`: Updates dimension tables on specific days.
  - `Bronze Nb - fact.ipynb`: Updates fact tables daily.
  - `Silver Nb - Historic.ipynb`: Processes historical data for the Silver layer.
  - `Silver Nb -.ipynb`: Processes daily updates for the Silver layer.
  - `Gold Nb - Historic.ipynb`: Aggregates historical data for the Gold layer.
  - `Gold Nb.ipynb`: Aggregates daily updates for the Gold layer.

- **Pipelines**:
  - `ETL historic pipeline.png`: Diagram of the historic ETL pipeline.
  - `ETL pipeline.png`: Diagram of the daily ETL pipeline.

- **Screenshots**:
  - `final_relationship_model.png`: The final data model (referenced but not provided).

## Prerequisites

- **Microsoft Fabric**: Workspace with Lakehouses (`Bronze_LH`, `Silver_LH`, `Gold_LH`).
- **TMDB API Key**: Obtain from [TMDB](https://www.themoviedb.org/documentation/api) and add it to Azure Key Vault.
- **Python Libraries**: `requests`, `pyspark`, `delta`.
- **Fabric Data Pipeline**: For pipeline orchestration.
- **Fabric DataFlow Gen 2**: For date table setup using Power Query.

## Medallion Architecture

### Bronze Layer
- **Purpose**: Stores raw data fetched from the TMDB API.
- **Tables**:
  - `countries`: List of countries.
  - `languages`: List of languages.
  - `genre_movie`: Movie genres.
  - `genre_tv`: TV show genres.
  - `fact_movies`: Movie details.
  - `fact_tv`: TV show details.

### Silver Layer
- **Purpose**: Cleans and transforms Bronze data.
- **Transformations**:
  - Drops unnecessary columns (e.g., `Adult`, `Video`).
  - Converts data types (e.g., `Movie_ID` to integer, `Release_Date` to date).
  - Standardizes values (e.g., gender mapping).
  - Combines movie and TV genres into `genre_combined`.

### Gold Layer
- **Purpose**: Aggregates data for business use.
- **Transformations**:
  - Creates bridge tables (`genre_movie_bridge`, `genre_tv_bridge`) for many-to-many relationships.
  - Copies dimension tables (`countries`, `languages`, `genre_combined`) as-is.
  - Stores fact tables (`fact_movies`, `fact_tv`) for reporting.

## ETL Pipelines

### Historic ETL Pipeline
Fetches data from TMDB and processes it through the medallion layers.

1. **Bronze Activity**: Runs `Bronze Nb Historic.ipynb` to fetch and store raw data.
2. **Silver Activity**: Runs `Silver Nb - Historic.ipynb` to transform data into the Silver layer.
3. **Gold Activity**: Runs `Gold Nb - Historic.ipynb` to aggregate data into the Gold layer.

**Pipeline Diagram**:
<img width="1414" alt="image" src="https://github.com/abychen01/TMDB/blob/38eafa2b12bfa801cf35d98a8e90937dc6f2265f/ETL%20Historic%20pipeline.png" />

### Daily ETL Pipeline
Updates fact tables daily and dimension tables on the 15th and 28th of each month.

1. **Lookup Activity**: Executes SQL query `SELECT DAY(CAST(GETDATE() AS DATE)) AS day` to get the current day.
2. **If Condition Activity**: Checks if the day is 15 or 28:
   ```
   @or(
       equals(activity('Lookup1').output.firstRow.day, 15),
       equals(activity('Lookup1').output.firstRow.day, 28)
   )
   ```
   - **If True**: Runs `Bronze Nb - dim.ipynb` to update dimension tables.
   - **If False**: Skips to the Bronze Fact Activity.
3. **Bronze Fact Activity**: Runs `Bronze Nb - fact.ipynb` to update fact tables.
4. **Silver Activity**: Runs `Silver Nb -.ipynb` to transform updated data.
5. **Gold Activity**: Runs `Gold Nb.ipynb` to aggregate data into the Gold layer.

**Pipeline Diagram**:

<img width="1414" alt="image" src="https://github.com/abychen01/TMDB/blob/38eafa2b12bfa801cf35d98a8e90937dc6f2265f/ETL%20pipeline.png" />


## Data Model

The final data model includes:
- **Fact Tables**: `fact_movies`, `fact_tv`.
- **Dimension Tables**: `countries`, `languages`, `genre_combined`.
- **Bridge Tables**: `genre_movie_bridge`, `genre_tv_bridge`.

**Data Model Diagram**:
<img width="1414" alt="image" src="https://github.com/abychen01/TMDB/blob/38eafa2b12bfa801cf35d98a8e90937dc6f2265f/Relationship%20Model.png" />


**Data Model Relationships**:
<img width="1000" alt="image" src="https://github.com/abychen01/TMDB/blob/1ab574e13f775ef3d7f287a5de719c5bd018ebf1/Model%20Relationships.png"/>

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/abychen01/TMDB.git
   ```
2. **Replace the TMDB API Key**: Update the `headers` variable in the notebooks with your API key.
3. **Deploy Notebooks**: Upload the notebooks to your Fabric workspace.
4. **Configure Lakehouses**:
   - `Bronze_LH`: For raw data.
   - `Silver_LH`: For transformed data.
   - `Gold_LH`: For aggregated data.
5. **Set Up Pipelines**: Configure pipelines in Fabric Data Pipeline using the provided screenshots.
6. **Run Historic ETL**: Execute the historic pipeline to populate the initial dataset.
7. **Schedule Daily ETL**: Schedule the daily pipeline to run daily.

## Usage

- **Historic Data**: Trigger the historic ETL pipeline to fetch all data.
- **Daily Updates**: The daily ETL pipeline updates fact tables daily and dimension tables on the 15th and 28th.

## Code Comments

Each notebook includes detailed comments explaining the purpose of each code cell, enhancing readability and maintainability.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for suggestions or improvements.

## License

This project does not use a specific open-source license and is intended for educational and non-commercial purposes only.

Please note that all movie-related data is sourced from The Movie Database (TMDB) and is subject to their [API Terms of Use](https://www.themoviedb.org/documentation/api/terms-of-use).

Use of TMDB data must comply with their licensing terms. TMDB data is © TMDB.

## Copyright

This product uses the TMDB API but is not endorsed or certified by TMDB.

All movie-related metadata, images, and content are © The Movie Database (TMDB).




<img width="341" alt="image" src="https://www.themoviedb.org/assets/2/v4/logos/v2/blue_long_1-8ba2ac31f354005783fab473602c34c3f4fd207150182061e425d366e4f34596.svg"/>
