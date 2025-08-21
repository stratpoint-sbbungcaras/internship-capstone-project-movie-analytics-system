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

      * Contains duplicate entries. 
      * Features mixed date formats.
      * Has missing budget data. 

2.  **`ratings.json`**

      * Includes nested rating summaries.
      * Contains multiple performance metrics per movie. 

3.  **`movie_extended.csv`**

      * Lists multiple genres per movie. 
      * Contains production company information.
      * Includes country data. 

-----

## 🚀 Project Activities

The project is divided into four main activities, each focusing on a key technology in the data engineering stack.

### Activity 1: SQL Implementation

This activity focuses on database design and advanced SQL querying. 
  * **Database Design & Implementation:**
      *Design and create an efficient, normalized database schema to store the movie data. An example starting table is:
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
      * **LAG/LEAD:** Write queries to analyze monthly revenue changes, sequential movie performance, and year-over-year comparisons. 
      * **ROW\_NUMBER:** Implement ranking for movies by genre, identify top performers by year, and analyze director success rates. 
      * Perform complex aggregations and joins to derive insights. 

### Activity 2: Python & Pandas Processing

This section involves using Python and the Pandas library for data cleaning and transformation. 

  * **Python Implementation:**
      * Use Python **Classes/Objects** to create `Movie` and `Rating` classes [cite: 39]
      * Utilize **Dictionaries** for data processing.
      * Standardize dates with the **Datetime** module. 
      * Handle data types using **Casting**. 
      * Process data in batches using **For Loops**.
      * Apply **Lambda functions** for on-the-fly transformations.
      * Parse and handle nested **JSON** data structures. 
      * Implement proper **File Handling** techniques. 
  * **Pandas Operations:**
      * Read and process data from CSV and JSON files into Pandas DataFrames. 
      * Analyze DataFrames using built-in functions. [cite: 52]
      * Remove duplicate records. [cite: 53]
      * Perform data cleaning to fix incorrect data. 

### Activity 3: PySpark Data Processing

Use PySpark to perform large-scale data processing and analysis. 

  * **PySpark Fundamentals:**
      * Create and manipulate DataFrames. 
      * Implement SQL functions and work with various datasources. 
      * Use built-in functions for analysis.
  * **Analysis Tasks:**
      * Load and transform the movie dataset. 
      * Perform complex aggregations and generate statistical summaries. 
      * Create analytical views for reporting. 

### Activity 4: Power BI Dashboard

The final activity is to visualize the processed data using Power BI.

  * **Data Preparation:**
      * Import and transform data in the Power Query Editor. 
      * Create an efficient data model with proper relationships. 
      * Configure scheduled data refresh. 
  * **Report Development:**
      * Build a movie performance dashboard.
      * Implement DAX for aggregations and time intelligence functions. 
      * Design interactive filters to allow for dynamic data exploration.

-----

## 💯 Evaluation Criteria

The project will be evaluated based on your proficiency in each of the four activities. The passing grade is **70%**. 

### SQL (25%)

  * **Database Schema Design (5%):** Ability to design an efficient and normalized database schema.
  * **Table Relationships (5%):** Understanding of how to model one-to-one, one-to-many, and many-to-many relationships. 
  * **Advanced SQL Queries (10%):** Proficiency with window functions like `LAG()`, `LEAD()`, and `ROW_NUMBER()` for analytical tasks.
  * **Query Optimization (5%):** Ability to optimize slow queries using indexing, CTEs, and other techniques. 

### Python/Pandas (25%)

  * **Python Classes Implementation (5%):** Demonstrates understanding of object-oriented programming principles. 
  * **Data Processing Functions (5%):** Ability to write clean, reusable functions for data transformation. 
  * **Pandas Operations (10%):** Skill in using Pandas for data filtering, grouping, merging, and reshaping. 
  * **Error Handling (5%):** Implementation of robust error handling using `try/except` blocks and validation. 

### PySpark (25%)

  * **DataFrame Operations (10%):** Proficiency in core PySpark DataFrame operations at scale. 
  * **SQL Function Implementation (5%):** Ability to use SQL-like functions and Spark SQL. 
  * **Data Transformation Jobs (5%):** Designing and building scalable data transformation pipelines. 
  * **Performance Optimization (5%):** Knowledge of optimization techniques like partitioning, caching, and broadcasting. 

### Power BI (25%)

  * **Data Model Design (5%):** Ability to create a logical and efficient data model (e.g., star/snowflake schema).
  * **Calculations and Measures (10%):** Skill in writing DAX formulas for calculated columns and measures. 
  * **Data Storytelling (10%):** Proficiency in designing insightful and visually compelling dashboards that tell a clear story. 
