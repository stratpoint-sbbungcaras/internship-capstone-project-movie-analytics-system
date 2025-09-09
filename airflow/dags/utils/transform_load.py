import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, LongType
from .config import FINAL_CLEANED_MASTER_PATH, JAR_PATH, PG_DRIVER

#LOGGER
def log_info(message):
    print(f"INFO: {message}")

def log_error(message):
    print(f"ERROR: {message}")

def create_spark_session(jar_path: str):
    """Create and configure Spark session."""
    try:
        log_info("Initializing Spark session for Transform and Loading Process...")
        spark = SparkSession.builder \
            .appName("TransformLoad") \
            .config("spark.jars", jar_path) \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        log_info("Spark session created successfully.")
        return spark
    except Exception as e:
        log_error(f"Failed to create Spark session: {e}")
        raise

def load_cleaned_data(spark, input_csv):
    """Load the cleaned movie CSV data with a defined schema."""
    try:
        log_info(f"Loading data from {input_csv}...")
        schema = StructType([
            StructField("movie_id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("budget", DoubleType(), True),
            StructField("revenue", DoubleType(), True),
            StructField("genres", StringType(), True),
            StructField("production_companies", StringType(), True),
            StructField("production_countries", StringType(), True),
            StructField("languages", StringType(), True),
            StructField("popularity", DoubleType(), True),
            StructField("runtime", IntegerType(), True),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("poster_url", StringType(), True)
        ])
        
        df = spark.read.csv(input_csv, header=True, schema=schema, quote='"', escape='"')
        df = df.withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd"))
        log_info(f"Successfully loaded {df.count()} rows.")
        return df
    except Exception as e:
        log_error(f"Failed to load data: {e}")
        raise

def load_to_db(df, table_name, pg_url, pg_user, pg_pass, PG_DRIVER, mode="append"):
    """Write Cleaned Data to a PostgreSQL table."""
    try:
        log_info(f"Writing {df.count()} rows to table {table_name}...")
        properties = {"user": pg_user, "password": pg_pass, "driver": PG_DRIVER}
        df.write.jdbc(url=pg_url, table=f"capstone.{table_name}", mode=mode, properties=properties)
        log_info(f"Successfully loaded data to {table_name}.")
    except Exception as e:
        log_error(f"Failed to load to table {table_name}: {e}")
        raise

def create_dimension_table(df, col_name, dim_name):
    """Create a dimension table from a comma-separated string column with sequential IDs."""
    log_info(f"Creating dimension table for {dim_name}...")
    try:
        window_spec = Window.orderBy(dim_name)
        dim_df = df.select(F.explode(F.split(F.col(col_name), ",")).alias(dim_name)) \
            .withColumn(dim_name, F.trim(F.col(dim_name))) \
            .withColumn(dim_name, F.regexp_replace(F.col(dim_name), '^"|"$', '')) \
            .filter(F.col(dim_name) != '') \
            .distinct() \
            .withColumn(f"{dim_name.replace('_name', '')}_id", F.row_number().over(window_spec))
        return dim_df
    except Exception as e:
        log_error(f"Failed to create dimension table for {col_name}: {e}")
        raise

def create_bridge_table(main_df, dim_df, main_col, dim_name_col, dim_id_col):
    """Creates a bridge table to link the main fact table with a dimension table."""
    # We also add the trim function here to ensure joins are clean
    return main_df.select("movie_id", F.explode(F.split(F.col(main_col), ",")).alias(dim_name_col)) \
        .withColumn(dim_name_col, F.trim(F.col(dim_name_col))) \
        .join(dim_df, on=dim_name_col, how="inner") \
        .select("movie_id", dim_id_col) \
        .distinct()

def transform_and_load(
    pg_url,
    pg_user,
    pg_pass
):
    """ This function is designed to be imported by DAG. """
    spark = None
    try:
        log_info("Starting Transform and Load process...")
        spark = create_spark_session(JAR_PATH)
        
        # LOAD CLEANED DATA
        df = load_cleaned_data(spark, FINAL_CLEANED_MASTER_PATH)
        df.cache()

        # CREATE AND LOG DIM TABLES
        log_info("Creating and loading dimension tables...")

        dim_genres = create_dimension_table(df, "genres", "genre_name")
        load_to_db(dim_genres, "dim_genres", pg_url, pg_user, pg_pass, PG_DRIVER)

        dim_companies = create_dimension_table(df, "production_companies", "company_name")
        load_to_db(dim_companies, "dim_companies", pg_url, pg_user, pg_pass, PG_DRIVER)
        
        dim_countries = create_dimension_table(df, "production_countries", "country_name")
        load_to_db(dim_countries, "dim_countries", pg_url, pg_user, pg_pass, PG_DRIVER)

        dim_languages = create_dimension_table(df, "languages", "language_name")
        load_to_db(dim_languages, "dim_languages", pg_url, pg_user, pg_pass, PG_DRIVER)

        # CREATE AND LOAD FACT TABLES
        log_info("Creating and loading fact_movies table...")
        
        fact_movies = df.withColumn("release_date_id", F.date_format(F.col("release_date"), "yyyyMMdd").cast("integer")) \
                         .select(
                             "movie_id",
                             "title",
                             "release_date_id",
                             "budget",
                             "revenue",
                             "runtime",
                             "popularity",
                             "vote_average",
                             "vote_count",
                             "poster_url"
                         )
        load_to_db(fact_movies, "fact_movies", pg_url, pg_user, pg_pass, PG_DRIVER)
        
        # CREATE AND LOAD BRIDGE TABLES
        log_info("Creating and loading bridge tables...")
        
        bridge_genres = create_bridge_table(df, dim_genres, "genres", "genre_name", "genre_id")
        load_to_db(bridge_genres, "bridge_genres", pg_url, pg_user, pg_pass, PG_DRIVER)

        bridge_companies = create_bridge_table(df, dim_companies, "production_companies", "company_name", "company_id")
        load_to_db(bridge_companies, "bridge_companies", pg_url, pg_user, pg_pass, PG_DRIVER)

        bridge_countries = create_bridge_table(df, dim_countries, "production_countries", "country_name", "country_id")
        load_to_db(bridge_countries, "bridge_countries", pg_url, pg_user, pg_pass, PG_DRIVER)
        
        bridge_languages = create_bridge_table(df, dim_languages, "languages", "language_name", "language_id")
        load_to_db(bridge_languages, "bridge_languages", pg_url, pg_user, pg_pass, PG_DRIVER)

        log_info("Transform and Load process completed successfully!")

    except Exception as e:
        log_error(f"Transform and Load process process failed: {e}")
        raise
    finally:
        if spark:
            log_info("Stopping Spark session.")
            spark.stop()
