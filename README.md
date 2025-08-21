# Movie Analytics Project for Data Engineering

This repository contains a comprehensive data engineering project designed to build a movie analytics system. The project will test and demonstrate your skills in SQL, Python, PySpark, and Power BI by processing and analyzing a real-world movie dataset. 

**Project Duration:** 3 Weeks

-----

## 📋 Project Overview

The core objective of this project is to build an end-to-end movie analytics system. You will process raw movie data through several stages, tackling common data challenges like duplicate entries, inconsistent formats, and nested data structures. The final output will be an interactive dashboard visualizing key movie performance metrics.

-----

## 💾 Dataset

The project uses a multi-file movies dataset with various data quality issues that you will need to address.

  * **Source:** [https://minio-s3.rcdc.me/public/dataset/project\_data.zip](https://www.google.com/search?q=https://minio-s3.rcdc.me/public/dataset/project_data.zip)

### Dataset Files:

1.  **`Movies_main.csv`**

      * [cite\_start]Contains duplicate entries. [cite: 10]
      * [cite\_start]Features mixed date formats. [cite: 10]
      * [cite\_start]Has missing budget data. [cite: 11]

2.  **`ratings.json`**

      * [cite\_start]Includes nested rating summaries. [cite: 13]
      * [cite\_start]Contains multiple performance metrics per movie. [cite: 13]

3.  **`movie_extended.csv`**

      * [cite\_start]Lists multiple genres per movie. [cite: 15]
      * [cite\_start]Contains production company information. [cite: 15]
      * [cite\_start]Includes country data. [cite: 16]

-----

## 🚀 Project Activities

The project is divided into four main activities, each focusing on a key technology in the data engineering stack.

### Activity 1: SQL Implementation

[cite\_start]This activity focuses on database design and advanced SQL querying. [cite: 19]

  * **Database Design & Implementation:**
      * [cite\_start]Design and create an efficient, normalized database schema to store the movie data. [cite: 21, 78] An example starting table is:
        ```sql
        CREATE TABLE movies (
            movie_id INT PRIMARY KEY,
            title VARCHAR(255),
            release_date DATE,
            budget DECIMAL(15,2),
            revenue DECIMAL(15,2)
        );
        ```
  * **Advanced Queries:**
      * [cite\_start]**LAG/LEAD:** Write queries to analyze monthly revenue changes, sequential movie performance, and year-over-year comparisons. [cite: 33]
      * [cite\_start]**ROW\_NUMBER:** Implement ranking for movies by genre, identify top performers by year, and analyze director success rates. [cite: 34]
      * [cite\_start]Perform complex aggregations and joins to derive insights. [cite: 35]

### Activity 2: Python & Pandas Processing

[cite\_start]This section involves using Python and the Pandas library for data cleaning and transformation. [cite: 36]

  * **Python Implementation:**
      * [cite\_start]Use Python **Classes/Objects** to create `Movie` and `Rating` classes. [cite: 39]
      * [cite\_start]Utilize **Dictionaries** for data processing. [cite: 40]
      * [cite\_start]Standardize dates with the **Datetime** module. [cite: 42]
      * [cite\_start]Handle data types using **Casting**. [cite: 43]
      * [cite\_start]Process data in batches using **For Loops**. [cite: 44]
      * [cite\_start]Apply **Lambda functions** for on-the-fly transformations. [cite: 46]
      * [cite\_start]Parse and handle nested **JSON** data structures. [cite: 46]
      * [cite\_start]Implement proper **File Handling** techniques. [cite: 47]
  * **Pandas Operations:**
      * [cite\_start]Read and process data from CSV and JSON files into Pandas DataFrames. [cite: 50, 51]
      * [cite\_start]Analyze DataFrames using built-in functions. [cite: 52]
      * [cite\_start]Remove duplicate records. [cite: 53]
      * [cite\_start]Perform data cleaning to fix incorrect data. [cite: 54]

### Activity 3: PySpark Data Processing

[cite\_start]Use PySpark to perform large-scale data processing and analysis. [cite: 55]

  * **PySpark Fundamentals:**
      * [cite\_start]Create and manipulate DataFrames. [cite: 58]
      * [cite\_start]Implement SQL functions and work with various datasources. [cite: 59, 60]
      * [cite\_start]Use built-in functions for analysis. [cite: 60]
  * **Analysis Tasks:**
      * [cite\_start]Load and transform the movie dataset. [cite: 62]
      * [cite\_start]Perform complex aggregations and generate statistical summaries. [cite: 63, 66]
      * [cite\_start]Create analytical views for reporting. [cite: 64]

### Activity 4: Power BI Dashboard

[cite\_start]The final activity is to visualize the processed data using Power BI. [cite: 67]

  * **Data Preparation:**
      * [cite\_start]Import and transform data in the Power Query Editor. [cite: 70]
      * [cite\_start]Create an efficient data model with proper relationships. [cite: 71]
      * [cite\_start]Configure scheduled data refresh. [cite: 72]
  * **Report Development:**
      * [cite\_start]Build a movie performance dashboard. [cite: 74]
      * [cite\_start]Implement DAX for aggregations and time intelligence functions. [cite: 74, 75]
      * [cite\_start]Design interactive filters to allow for dynamic data exploration. [cite: 75]

-----

## 💯 Evaluation Criteria

The project will be evaluated based on your proficiency in each of the four activities. [cite\_start]The passing grade is **70%**. [cite: 110]

### SQL (25%)

  * [cite\_start]**Database Schema Design (5%):** Ability to design an efficient and normalized database schema. [cite: 78]
  * [cite\_start]**Table Relationships (5%):** Understanding of how to model one-to-one, one-to-many, and many-to-many relationships. [cite: 80, 81, 82]
  * [cite\_start]**Advanced SQL Queries (10%):** Proficiency with window functions like `LAG()`, `LEAD()`, and `ROW_NUMBER()` for analytical tasks. [cite: 83]
  * [cite\_start]**Query Optimization (5%):** Ability to optimize slow queries using indexing, CTEs, and other techniques. [cite: 85]

### Python/Pandas (25%)

  * [cite\_start]**Python Classes Implementation (5%):** Demonstrates understanding of object-oriented programming principles. [cite: 87]
  * [cite\_start]**Data Processing Functions (5%):** Ability to write clean, reusable functions for data transformation. [cite: 89]
  * [cite\_start]**Pandas Operations (10%):** Skill in using Pandas for data filtering, grouping, merging, and reshaping. [cite: 92]
  * [cite\_start]**Error Handling (5%):** Implementation of robust error handling using `try/except` blocks and validation. [cite: 94]

### PySpark (25%)

  * [cite\_start]**DataFrame Operations (10%):** Proficiency in core PySpark DataFrame operations at scale. [cite: 96]
  * [cite\_start]**SQL Function Implementation (5%):** Ability to use SQL-like functions and Spark SQL. [cite: 97]
  * [cite\_start]**Data Transformation Jobs (5%):** Designing and building scalable data transformation pipelines. [cite: 99]
  * [cite\_start]**Performance Optimization (5%):** Knowledge of optimization techniques like partitioning, caching, and broadcasting. [cite: 102]

### Power BI (25%)

  * [cite\_start]**Data Model Design (5%):** Ability to create a logical and efficient data model (e.g., star/snowflake schema). [cite: 105]
  * [cite\_start]**Calculations and Measures (10%):** Skill in writing DAX formulas for calculated columns and measures. [cite: 106]
  * [cite\_start]**Data Storytelling (10%):** Proficiency in designing insightful and visually compelling dashboards that tell a clear story. [cite: 108]
