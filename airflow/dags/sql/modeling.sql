-- START FRESH EVERY RUN
DROP SCHEMA IF EXISTS capstone CASCADE;
CREATE SCHEMA IF NOT EXISTS capstone;

-- DIMENSION TABLES

-- DIM GENRES
CREATE TABLE IF NOT EXISTS capstone.dim_genres (
genre_id     BIGINT PRIMARY KEY,
genre_name   VARCHAR(255) UNIQUE NOT NULL
);

-- DIM COMPANIES
CREATE TABLE IF NOT EXISTS capstone.dim_companies (
company_id   BIGINT PRIMARY KEY,
company_name VARCHAR(255) UNIQUE NOT NULL
);

-- DIM COUNTRIES
CREATE TABLE IF NOT EXISTS capstone.dim_countries (
country_id   BIGINT PRIMARY KEY,
country_name VARCHAR(255) UNIQUE NOT NULL
);

-- DIM LANGUAGES
CREATE TABLE IF NOT EXISTS capstone.dim_languages (
language_id  BIGINT PRIMARY KEY,
language_name VARCHAR(255) UNIQUE NOT NULL
);

-- DIM DATE
CREATE TABLE IF NOT EXISTS capstone.dim_date (
date_id      INTEGER PRIMARY KEY,
full_date    DATE NOT NULL,
year         INTEGER NOT NULL,
quarter      INTEGER NOT NULL,
month        INTEGER NOT NULL,
month_name   VARCHAR(20) NOT NULL,
day          INTEGER NOT NULL,
day_of_week  VARCHAR(20) NOT NULL
);

-- FACT TABLE
CREATE TABLE IF NOT EXISTS capstone.fact_movies (
movie_id        INTEGER PRIMARY KEY,
title           VARCHAR(255) NOT NULL,
release_date_id INTEGER REFERENCES capstone.dim_date(date_id),
budget          BIGINT,
revenue         BIGINT,
runtime         INTEGER,
popularity      DECIMAL(15, 4),
vote_average    DECIMAL(10, 2),
vote_count      INTEGER,
poster_url      TEXT
);

-- BRIDGE TABLES (for many-to-many relationships)
CREATE TABLE IF NOT EXISTS capstone.bridge_genres (
movie_id INTEGER REFERENCES capstone.fact_movies(movie_id) ON DELETE CASCADE,
genre_id BIGINT REFERENCES capstone.dim_genres(genre_id) ON DELETE CASCADE,
PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS capstone.bridge_companies (
movie_id   INTEGER REFERENCES capstone.fact_movies(movie_id) ON DELETE CASCADE,
company_id BIGINT REFERENCES capstone.dim_companies(company_id) ON DELETE CASCADE,
PRIMARY KEY (movie_id, company_id)
);

CREATE TABLE IF NOT EXISTS capstone.bridge_countries (
movie_id   INTEGER REFERENCES capstone.fact_movies(movie_id) ON DELETE CASCADE,
country_id BIGINT REFERENCES capstone.dim_countries(country_id) ON DELETE CASCADE,
PRIMARY KEY (movie_id, country_id)
);

CREATE TABLE IF NOT EXISTS capstone.bridge_languages (
movie_id    INTEGER REFERENCES capstone.fact_movies(movie_id) ON DELETE CASCADE,
language_id BIGINT REFERENCES capstone.dim_languages(language_id) ON DELETE CASCADE,
PRIMARY KEY (movie_id, language_id)
);

-- ONE-TIME POPULATION SCRIPT FOR DIM_DATE
INSERT INTO capstone.dim_date (date_id, full_date, year, quarter, month, month_name, day, day_of_week)
SELECT
TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_id,
d AS full_date,
EXTRACT(YEAR FROM d) AS year,
EXTRACT(QUARTER FROM d) AS quarter,
EXTRACT(MONTH FROM d) AS month,
TO_CHAR(d, 'Month') AS month_name,
EXTRACT(DAY FROM d) AS day,
TO_CHAR(d, 'Day') AS day_of_week
FROM
GENERATE_SERIES('1800-01-01'::DATE, '2040-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_id) DO NOTHING;