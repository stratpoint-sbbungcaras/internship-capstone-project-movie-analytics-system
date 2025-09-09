import os
from dotenv import load_dotenv

load_dotenv()

#TMDb API Settings
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise ValueError("FATAL ERROR: TMDB_API_KEY not found in .env file.")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 20 # in seconds
#TMDB_ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiNjdhZmE3YmY0OTc3N2IwNDQ2Y2VjMjQ0OTM3NDE3OCIsIm5iZiI6MTc1NjM2NzcwNy41OTYsInN1YiI6IjY4YjAwYjViZjRmOTBjY2Y0ZDYyNGY3ZSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.z0CGOgz6Wui1WNPETLImyk-QFbi-qgKgeK8qtl9vTYE"

#File Paths
CONFIG_FILE_PATH = os.path.abspath(__file__)

# Navigate UP the directory tree to find the correct base folder ("airflow").
# 1. .../airflow/dags/utils
# 2. .../airflow/dags
# 3. .../airflow  <-- This is our correct base path.

#DIRECTORIES
AIRFLOW_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CONFIG_FILE_PATH)))
JAR_DIR = os.path.join(AIRFLOW_ROOT, 'jars')
DATASET_DIR = os.path.join(AIRFLOW_ROOT, 'datasets')
OUTPUT_DIR = os.path.join(AIRFLOW_ROOT, 'outputs') 

#PATHSd
MAIN_CSV_PATH = os.path.join(DATASET_DIR, 'movies_main.csv')
EXTENDED_CSV_PATH = os.path.join(DATASET_DIR, 'movies_extended.csv')
RATINGS_JSON_PATH = os.path.join(DATASET_DIR, 'ratings.json')
FINAL_MASTER_PATH = os.path.join(OUTPUT_DIR, 'enriched_master_data.csv')
FINAL_CLEANED_MASTER_PATH = os.path.join(OUTPUT_DIR, 'cleaned_master_data.csv')
PARTIAL_ENRICHED_PATH = os.path.join(OUTPUT_DIR, 'enriched_master_data_partial.csv')
PROGRESS_LOG_PATH = os.path.join(OUTPUT_DIR, 'movie_enrichment_progress.jsonl')

# CONFIGURE DATABASE CONNECTION
JAR_PATH = os.path.join(JAR_DIR, 'postgresql-42.7.3.jar')
PG_DRIVER = "org.postgresql.Driver"

# Auto-create outputs directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

