import pandas as pd
import os
import io
import ast
import logging
import requests
import time
import json
from tabulate import tabulate
from typing import Tuple, List, Dict, Any
from .config import (
    MAIN_CSV_PATH, EXTENDED_CSV_PATH, RATINGS_JSON_PATH, PARTIAL_ENRICHED_PATH,
    FINAL_MASTER_PATH, PROGRESS_LOG_PATH, TMDB_API_KEY, TMDB_BASE_URL,
    TMDB_IMAGE_BASE_URL, MAX_RETRIES, REQUEST_TIMEOUT
) 

# LOGGER CONFIG
log_path = os.path.join(os.path.dirname(FINAL_MASTER_PATH), 'movie_enrichment.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_path, mode='a'),
        logging.StreamHandler()
    ]
)

# FUNCTIONS
def display_section(title: str, content: Any):
    """ Clean formats for outputs """
    separator = "=" * 80
    print(f"\n{separator}\n--- {title.upper()} ---\n{separator}")
    if isinstance(content, pd.DataFrame):
        print(tabulate(content, headers='keys', tablefmt='psql', maxcolwidths=40))
    elif isinstance(content, pd.Series):
        print(tabulate(content.to_frame(name='Value'), headers='keys', tablefmt='psql', maxcolwidths=40))
    else:
        print(content)
    print(f"{separator}\n")

def _clean_movie_id_column(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """ Pre-cleaning and standardization of id column """
    df = df.copy()
    if 'id' in df.columns:
        df = df.rename(columns={'id': 'movie_id'})
    
    if 'movie_id' in df.columns:
        df['movie_id'] = pd.to_numeric(df['movie_id'], errors='coerce')
        df.dropna(subset=['movie_id'], inplace=True)
        df['movie_id'] = df['movie_id'].astype(int)
        df = df.drop_duplicates(subset=['movie_id'], keep='first')
    else:
        logging.warning(f"'movie_id' column not found or created for '{df_name}'.")
    return df

def extract_names_from_list_of_dicts(column: pd.Series) -> pd.Series:
    """ Convert stringified list of dicts to a list of names. """
    def parse(val):
        if pd.isnull(val): return []
        try:
            items = ast.literal_eval(str(val))
            return [item.get('name', '') for item in items if isinstance(item, dict)]
        except (ValueError, SyntaxError):
            return []
    return column.apply(parse)

def _parse_api_list_of_dicts(api_list: List[Dict]) -> List[str]:
    """ Parses a direct list of dicts from the API to extract just the names. """
    if not isinstance(api_list, list):
        return []
    return [item.get('name', '') for item in api_list if isinstance(item, dict)]

def _construct_image_url(path: str, size: str = 'w500') -> str:
    """ Constructs the full image URL from a path and size. """
    if not isinstance(path, str) or not path:
        return None
    return f"{TMDB_IMAGE_BASE_URL}{size}{path}"


# MOVIE DATA ENRICHER CLASS
class MovieDataEnricher:
    """ Orchestrates the entire data extraction, cleaning, merging, and data enrichment process. """
    def __init__(self, main_path, extended_path, ratings_path, api_key, base_url, retries, timeout):
        self.main_path = main_path
        self.extended_path = extended_path
        self.ratings_path = ratings_path
        self.api_key = api_key
        self.base_url = base_url
        self.max_retries = retries
        self.timeout = timeout
        self.master_df = None

        # Define all columns we expect to get from the API
        self.api_columns = [
            'title', 'release_date', 'budget', 'revenue', 'overview', 'tagline',
            'popularity', 'runtime', 'vote_average', 'vote_count', 'status',
            'genres', 'production_companies', 'production_countries', 'spoken_languages',
            'poster_url', 'backdrop_url'
        ]

    def run(self) -> pd.DataFrame:
        """ Executes the enrichment process, resuming from the partial file if it exists. """
        logging.info("Starting movie data enrichment process...")
        
        if os.path.exists(PARTIAL_ENRICHED_PATH):
            logging.info(f"Found existing partial file at {PARTIAL_ENRICHED_PATH}. Resuming enrichment.")
            self.master_df = pd.read_csv(PARTIAL_ENRICHED_PATH)
            for col in ['genres', 'production_companies', 'production_countries', 'spoken_languages']:
                 if col in self.master_df.columns:
                     self.master_df[col] = self.master_df[col].apply(
                         lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else x
                     )
        else:
            logging.info("No partial file found. Starting from scratch.")
            main_df, extended_df, ratings_df = self._extract_and_clean()
            self._merge_data(main_df, extended_df, ratings_df)
        
        self._backfill_from_api()
        
        logging.info("Data enrichment pipeline completed successfully.")
        return self.master_df

    def _extract_and_clean(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logging.info("--- STAGE 1: EXTRACTING & CLEANING DATA ---")
        main_df = self._extraction(self.main_path, "Movie Main")
        cleaned_main = self._wrangle_main_df(main_df)
        extended_df = self._extraction(self.extended_path, "Movie Extended")
        cleaned_extended = self._wrangle_extended_df(extended_df)
        ratings_df = self._extraction(self.ratings_path, "Movie Ratings")
        ratings_df = self._dict_flatten(ratings_df, "Movie Ratings")
        cleaned_ratings = self._wrangle_ratings_df(ratings_df)
        logging.info("--- STAGE 1 COMPLETED ---")
        return cleaned_main, cleaned_extended, cleaned_ratings

    def _merge_data(self, main_df, extended_df, ratings_df):
        logging.info("--- STAGE 2: MERGING DATA ---")
        self.master_df = pd.merge(main_df, extended_df, on='movie_id', how='outer')
        self.master_df = pd.merge(self.master_df, ratings_df, on='movie_id', how='outer')
        # FIX: Reset the index after merging to prevent ambiguity errors.
        self.master_df.reset_index(drop=True, inplace=True)
        # Ensure all expected API columns exist before backfilling
        for col in self.api_columns:
            if col not in self.master_df.columns:
                self.master_df[col] = None
        logging.info(f"Merging complete. Master DataFrame has {len(self.master_df)} rows.")
        logging.info("--- STAGE 2 COMPLETED ---")

    def _backfill_from_api(self):
        if self.master_df is None:
            logging.error("Master DataFrame not available for backfilling. Aborting.")
            return

        logging.info("--- STAGE 3: RE-FETCHING AND OVERWRITING DATA FROM TMDB API ---")
        
        # --- MODIFICATION: Determine which movies to process based on the progress log ---
        processed_movie_ids = set()
        if os.path.exists(PROGRESS_LOG_PATH):
            with open(PROGRESS_LOG_PATH, 'r') as f:
                for line in f:
                    try:
                        log_entry = json.loads(line)
                        processed_movie_ids.add(log_entry['movie_id'])
                    except (json.JSONDecodeError, KeyError):
                        continue # Ignore malformed lines
            logging.info(f"Found {len(processed_movie_ids)} already processed movies in the progress log. They will be skipped.")

        # Determine the indices of rows to process by finding which movie_ids are NOT in our processed set
        unprocessed_mask = ~self.master_df['movie_id'].isin(processed_movie_ids)
        indices_to_process = self.master_df[unprocessed_mask].index.tolist()
        
        total = len(indices_to_process)
        logging.info(f"Found {total} movies to process/overwrite.")
        
        for i, idx in enumerate(indices_to_process):
            movie_id = self.master_df.at[idx, 'movie_id']
            # Using .get() for title in case it's one of the columns to be filled
            current_title = self.master_df.get('title', {}).get(idx) 
            progress_entry = {'movie_id': int(movie_id), 'title': current_title, 'status': 'processing'}
            
            tmdb_data = self._fetch_tmdb_metadata(movie_id)

            if tmdb_data:
                updated_fields = self._update_row_with_tmdb_data(idx, tmdb_data)
                progress_entry.update({'status': 'success', 'updated_fields': updated_fields})
                logging.info(f"Successfully overwrote {len(updated_fields)} fields for movie_id {movie_id} ({i+1}/{total})")
            else:
                progress_entry['status'] = 'failed_api_fetch'
                logging.warning(f"Failed to fetch data for movie_id {movie_id} ({i+1}/{total})")
            
            self._log_progress(progress_entry)
            
            if (i + 1) % 20 == 0 or (i + 1) == total:
                self._save_partial_data()
                logging.info(f"Progress saved to partial file. {i+1}/{total} movies processed in this session.")

        logging.info("--- STAGE 3 COMPLETED ---")

    def _save_partial_data(self):
        logging.debug(f"Saving current state to {PARTIAL_ENRICHED_PATH}...")
        try:
            self.master_df.to_csv(PARTIAL_ENRICHED_PATH, index=False)
        except Exception as e:
            logging.error(f"Failed to save progress to partial CSV: {e}")

    def _update_row_with_tmdb_data(self, row_idx: int, tmdb_data: Dict) -> List[str]:
        updated_cols = []
        # This map defines how to process each field from the API
        field_map = {
            'title': None, 'release_date': pd.to_datetime, 'budget': None, 'revenue': None,
            'overview': None, 'tagline': None, 'popularity': None, 'runtime': None,
            'vote_average': None, 'vote_count': None, 'status': None,
            'genres': _parse_api_list_of_dicts, 
            'production_companies': _parse_api_list_of_dicts, 
            'production_countries': _parse_api_list_of_dicts, 
            'spoken_languages': _parse_api_list_of_dicts,
            # Handle image paths by turning them into full URLs
            'poster_path': lambda path: _construct_image_url(path, size='w500'),
            'backdrop_path': lambda path: _construct_image_url(path, size='w1280')
        }

        for api_key, parser_func in field_map.items():
            # The destination column name might be different (e.g., poster_path -> poster_url)
            dest_col = api_key.replace('_path', '_url') if '_path' in api_key else api_key
    
            if api_key in tmdb_data:
                new_val = tmdb_data[api_key]
                if parser_func:
                    new_val = parser_func(new_val)
                
                # Overwrite the value in the DataFrame
                self.master_df.at[row_idx, dest_col] = new_val
                updated_cols.append(dest_col)
                
        return updated_cols

    def _fetch_tmdb_metadata(self, movie_id: int) -> Dict:
        url = f"{self.base_url}/movie/{movie_id}?api_key={self.api_key}"
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=self.timeout)
                if response.status_code == 200: return response.json()
                elif response.status_code == 404:
                    logging.warning(f"Movie ID {movie_id} not found in TMDb (404). Aborting for this ID.")
                    return None
                elif response.status_code == 401:
                    logging.error(f"Authentication failed (401). Check your TMDB_API_KEY. Aborting for this ID.")
                    return None
                elif response.status_code == 429:
                    logging.warning(f"Rate limit exceeded (429). Waiting before retry...")
                    time.sleep(2 ** (attempt + 1))
                else:
                    logging.warning(f"TMDb API returned status {response.status_code} for movie_id {movie_id}. Retrying...")
                    time.sleep(2 ** attempt)
            except requests.RequestException as e:
                logging.error(f"Network error for movie_id {movie_id} on attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        logging.error(f"All {self.max_retries} retries failed for movie_id {movie_id}.")
        return None

    def _log_progress(self, progress_entry: Dict):
        with open(PROGRESS_LOG_PATH, 'a') as f:
            f.write(json.dumps(progress_entry) + '\n')

    def _extraction(self, path: str, df_name: str) -> pd.DataFrame:
        logging.info(f"Extracting '{df_name}' from: {path}")
        try:
            if path.lower().endswith('.csv'): return pd.read_csv(path)
            elif path.lower().endswith('.json'): return pd.read_json(path)
            else: raise ValueError("Unsupported file type.")
        except FileNotFoundError:
            logging.error(f"Extraction failed. File not found at path: {path}")
        return pd.DataFrame()

    def _dict_flatten(self, df: pd.DataFrame, df_name: str) -> pd.DataFrame:
        df_to_flat = df.copy()
        for col in df_to_flat.columns:
            if not df_to_flat[col].dropna().empty and isinstance(df_to_flat[col].dropna().iloc[0], dict):
                flattened_data = pd.json_normalize(df_to_flat[col])
                df_to_flat = df_to_flat.drop(columns=[col]).join(flattened_data)
        return df_to_flat

    def _wrangle_main_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = _clean_movie_id_column(df, "Movie Main")
        df['release_date'] = pd.to_datetime(df.get('release_date'), errors='coerce').dt.date
        df['budget'] = pd.to_numeric(df.get('budget'), errors='coerce')
        df['revenue'] = pd.to_numeric(df.get('revenue'), errors='coerce')
        return df

    def _wrangle_extended_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = _clean_movie_id_column(df, "Movie Extended")
        for col in ['production_countries', 'spoken_languages']:
            if col in df.columns:
                df[f'{col}_names'] = extract_names_from_list_of_dicts(df[col]).apply(lambda x: ', '.join(x) if x else None)
        return df

    def _wrangle_ratings_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = _clean_movie_id_column(df, "Movie Ratings")
        if 'last_rated' in df.columns:
            df['last_rated_date'] = pd.to_datetime(df['last_rated'], unit='s', errors='coerce').dt.date
        return df
    
# IMPORT THIS TO AIRFLOW DAG TO RUN THIS SCRIPT 
def extract_and_enrich():
    logging.info("="*50)
    logging.info("--- MOVIE DATA ENRICHMENT STARTED ---")
    logging.info("="*50)

    enricher = MovieDataEnricher(
        main_path=MAIN_CSV_PATH, extended_path=EXTENDED_CSV_PATH, ratings_path=RATINGS_JSON_PATH,
        api_key=TMDB_API_KEY, base_url=TMDB_BASE_URL, retries=MAX_RETRIES, timeout=REQUEST_TIMEOUT
    )
    
    final_df = enricher.run()

    if final_df is not None and not final_df.empty:
        logging.info("--- STAGE 4: SAVING FINAL ENRICHED MASTER DATA ---")
        try:
            final_df.to_csv(FINAL_MASTER_PATH, index=False)
            logging.info(f"Final master data saved successfully to: {FINAL_MASTER_PATH}")
    
        except Exception as e:
            logging.error(f"Failed to save final master data: {e}")
        
        logging.info("--- STAGE 5: FINAL EXPLORATORY DATA ANALYSIS (EDA) OF ENRICHED MASTER DATA ---")
        buffer = io.StringIO()
        final_df.info(buf=buffer)
        display_section("FINAL ENRICHED MASTER DATA INFO", buffer.getvalue())
        display_section("FINAL ENRICHED MASTER DATA VALUES", final_df.isnull().sum())
        
    else:
        logging.error("Data Enrichment finished but the final DataFrame is empty. No file was saved.")

    logging.info("="*50)
    logging.info("--- MOVIE DATA ENRICHMENT FINISHED ---")
    logging.info("="*50)

# MAIN EXECUTION BLOCK
if __name__ == "__main__":
    extract_and_enrich()