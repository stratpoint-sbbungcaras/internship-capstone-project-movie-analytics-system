<p align="center">
  <a href="" rel="noopener">
 <img width=200px height=200px src="https://i.imgur.com/6wj0hh6.jpg" alt="Project logo"></a>
</p>

<h3 align="center">Project Title</h3>

<p align="center"> The pipeline output implements an Extract, Transform, Load (ETL) pipeline that was orchestrated with Apache Airflow, from extraction to data sources (TMDb API), data cleaning, transforming and loading to PostgreSQL database with a star schema model (Fact, Dim, Bridges), and lastly, interactive dashboard made with Power BI.
    <br> 
</p>

## 📝 Table of Contents

- [Core Technologies](#core)
- [Project Structure](#structure)
- [Project Architecture](#architecture)
- [Setup & Installation](#setup)
- [ETL Pipeline Tasks](#tasks)
- [Database Schema](#schema)
- [Analytical Views](views)
- [How to Run the Pipeline](#how)
- [Authors](#authors)
- [Acknowledgments](#acknowledgement)

## Core Technologies <a name = "core"></a>

  - Orchestration: Apache Airflow

  - Data Storage: PostgreSQL

  - Data Cleaning/Transformation: Pandas, PySpark

  - Programming Languages: Python, SQL

  - Data Source: The Movie Database (TMDB) API

## Project Structure <a name = "structure"></a>
```
internship-capstone-project-movie-analytics-system/
│
├── airflow/                         
│   ├── dags/                        # DAGs, SQL scripts, and utilities (functions/tasks)
│   │   ├── sql/                     # SQL scripts for analysis & modeling
│   │   │   ├── analysis1.sql
│   │   │   ├── analysis2.sql
│   │   │   ├── analysis3.sql
│   │   │   ├── analysis4.sql
│   │   │   └── modeling.sql
│   │   ├── utils/                   # Tasks scripts for ETL pipeline
│   │   │   ├── config.py            # Configuration settings
│   │   │   ├── cleaner.py           # Data cleaning logic (Pandas)
│   │   │   ├── enricher.py          # Data enrichment logic (Pandas)
│   │   │   ├── transform_load.py    # Transform & load functions (PySpark)
│   │   │   └── __init__.py
│   │   ├── capstone_pipeline_dag.py # Main Airflow pipeline DAG
│   │   └── .env                     # TMDB API KEY
│   │
│   ├── datasets/                    # Raw Datasets
│   │   ├── movies_main.csv
│   │   ├── movies_extended.csv
│   │   └── ratings.json
│   │
│   ├── jars/                        # External dependencies
│   │   └── postgresql-42.7.3.jar    # PostgreSQL JDBC driver
│   │
│   ├── logs/                        # Airflow logs
│   ├── outputs/                     # Cleaned, enriched and logs for tasks
│   |   ├── cleaned_master_data.csv
│   |   ├── enriched_master_data.csv
│   |   ├── enriched_master_data_partial.csv
│   |   ├── movie_cleaner.log
│   |   ├── movie_enrichment.log
│   |   └── movie_enrichment_progress.json
|   |
│   ├── requirements.txt              # Python dependencies (pip install)
│   └── Dockerfile                    # Container build instructions 
|
├── docker-compose.yml               # Docker services setup
└── README.md                        # Project documentation
```
## Project Architecture <a name = "architecture"></a>
The project follows a modern data engineering approach, using specialized tools for each stage of the process:

  1.  Extraction & Enrichment: A Python script (enricher.py) fetches movie data using the TMDB API and merges it with local datasets.

  2. Cleaning: A robust Python script (cleaner.py) standardizes the data. Its most critical function is to consolidate variations of production company names (e.g., mapping "20th Century Fox" and "Fox 2000 Pictures" to "Walt Disney Pictures") to ensure accurate analysis.

  3. Modeling: A SQL script (modeling.sql) runs first to define the data warehouse structure in PostgreSQL, creating a star schema with a central fact table and multiple dimension tables.

  4. Transformation & Loading: A PySpark script (transform_load.py) processes the cleaned data, creating the final dataframes for the fact and dimension tables and loading them into the PostgreSQL database.

  5. Analysis: A series of SQL scripts (analysis1.sql - analysis4.sql) are executed to create powerful, pre-aggregated views in the database. These views are designed to answer key business questions and handle complex logic, such as fairly distributing profit for co-produced movies.

  6. Orchestration: The entire process is managed by an Airflow DAG (capstone_pipeline_dag.py), which defines the sequence of tasks and their dependencies.

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See [deployment](#deployment) for notes on how to deploy the project on a live system.

## Setup & Installation <a name = "setup"></a>
  
  Follow these steps to set up the project environment.

### Prerequisites:
   
   - Dockerfiles and docker-compose.yml
   - Python 3.8 +
   - requirements.txt 
   - A TNDB Credentials
   - JDBC Credentials

### Instructions:
  1. Clone or Fork the Repository:
      ```
      git clone https://github.com/stratpoint-sbbungcaras/internship-capstone-project-movie-analytics-system.git
      cd <your-repo-name>
      ```
  2. Environment Variable:
     Create a _.env file_ in the project's root directory and add your TMDB API Key:
      ```
      TMDB_API_KEY="your_tmdb_api_key_here"
      ```
  3. requirements.txt:
      ```
      pandas
      pyspark
      requests
      numpy
      python-dotenv
      tabulate
      ```
  4. JDBC (Database Connection Details):
      Install compatible jar and put it into respective jars folder inside dags
      ```
      "pg_url": f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_dbname}",
      "pg_user": pg_conn.login,
      "pg_pass": pg_conn.password
      ```
  5. Start Containers:
      ```
      docker-compose up -d
      ```
      > This will start the Airflow webserver, scheduler, and a PostgreSQL instance to serve as the backend.

## Configure Airflow Connection Variable: <a name = "tests"></a>
  - Open the Airflow UI (usually at http://localhost:8080).
  - Go to Admin -> Connections.
  - Create a new connection with the ID postgresql (as defined in the DAG) and provide the credentials for your data warehouse PostgreSQL database.

## ETL Pipeline Tasks <a name = "tasks"></a>

The pipeline is defined in capstone_pipeline_dag.py and consists of the following key tasks:

  1. extract_and_enrich_task (enricher.py):

    - Merges local CSV files with movie data.

    - Iterates through movies, calling the TMDB API to fetch additional details like budget, revenue, and poster URLs.

    - Saves the merged and enriched data to a master CSV file.

  2. cleaning_task (cleaner.py):

    - Reads the master data file.

    - Performs critical cleaning operations: converts data types, handles missing values, and most importantly, standardizes production company names using a comprehensive mapping dictionary.

    - Saves a final, cleaned CSV file ready for transformation and loading.

  3. modeling_task (modeling.sql):

    - Connects to the PostgreSQL data warehouse.

    - Drops the existing capstone schema to ensure a fresh start.

    - Creates the entire star schema, including the fact_movies table, dimension tables (dim_genres, dim_companies, etc.), and bridge tables.

  4. transform_and_load_task (transform_load.py):

    - Initializes a Spark session.

    - Reads the cleaned CSV into a Spark DataFrame.

    - Transforms the data to create separate DataFrames for each dimension and the fact table.

    - Loads these DataFrames into the corresponding PostgreSQL tables using JDBC.

  5. analysis_tasks (analysis*.sql):

    - After the data is loaded, a series of parallel tasks are triggered.

    - Each task executes a SQL script to create a permanent analytical VIEW in the database. These views contain pre-calculated metrics and rankings.

## Database Schema <a name = "schema"></a>
   The project uses a Star Schema to optimize for analytical queries.

  - Fact Table: capstone.fact_movies contains the primary metrics of the movie (budget, revenue, runtime, etc.) and foreign keys to the dimension tables.

  - Dimension Tables: capstone.dim_genres, capstone.dim_companies, capstone.dim_countries, capstone.dim_languages, and capstone.dim_date provide descriptive, categorical context for the movies.

  - Bridge Tables: capstone.bridge_* tables manage the many-to-many relationships between movies and dimensions like genres and companies.

## Analytical Views <a name = "views"></a>
   The pipeline creates several pre-built views to make complex analysis simple:

  - capstone.company_yearly_profit_rank: Ranks production companies by their total yearly profit. It correctly handles co-productions by dividing a movie's profit equally among its producers, ensuring fair and accurate financial analysis.

  - capstone.company_yearly_profit_sequential: Shows the year-over-year profit change for each company using the LAG window function, allowing for trend analysis.

  - capstone.genre_yearly_profit_rank: Ranks genres by their total profit each year.

## How to Run the Pipeline <a name = "how"></a>

  1. Access the Airflow UI: Open http://localhost:8080 (default).

  2. Enable the DAG: Find the capstone_pipeline_dag in the DAGs list and toggle it on.

  3. Trigger the Pipeline: Press the "play" button to manually trigger a new DAG run. You can monitor the progress of each task in the "Grid" view.

## Authors <a name = "authors"></a>

- [GitHub](https://github.com/stratpoint-sbbungcaras) 

- [@LinkedIn](https://www.linkedin.com/in/shamley-bungcaras-4328b4377/) 

## Acknowledgements <a name = "acknowledgement"></a>
   
   Thank you to Stratpoint Technologies for an insightful data engineering/data analytics bootcamp. It was a wonderful experience and it helped me so much to have a solid foundation in data engineering. 
