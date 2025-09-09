import pandas as pd 
import numpy as np
import os
import re
import ast
import logging
from .config import (FINAL_MASTER_PATH, FINAL_CLEANED_MASTER_PATH)

# LOGGER CONFIG
log_path = os.path.join(os.path.dirname(FINAL_CLEANED_MASTER_PATH), 'movie_cleaner.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_path, mode='a'),
        logging.StreamHandler()
    ]
)

def extract_names_from_list_of_dicts(column: pd.Series) -> pd.Series:
    """ Handles formats like "[{'name': 'Action'}]" and "['Action', 'Adventure']". """
    def parse(val):
        if pd.isnull(val): return []
        try:
            items = ast.literal_eval(str(val))
            if isinstance(items, list):
                if not items: return [] 

                # Check the type of the first element to decide parsing strategy
                if isinstance(items[0], dict):
                    return [item.get('name', '') for item in items if isinstance(item, dict)]
                elif isinstance(items[0], str):
                    return items 
                else:
                    return [] 
            else:
                return [str(items)] 
        except (ValueError, SyntaxError):

            # Fallback for non-standard formats like "name1, name2"
            if isinstance(val, str):
                items = re.split(r',(?=\s*[^"]*(?:"[^"]*"[^"]*)*$)', val)
                return [item.strip().strip('"') for item in items]
            return []
    return column.apply(parse)

class MasterDataCleaner:
    def __init__(self, input_path=FINAL_MASTER_PATH):
        """ Initializes the cleaner and loads the data """
        logging.info(f"Loading data from {input_path}")
        try:
            self.df = pd.read_csv(input_path, delimiter =',', dtype=str)
            logging.info(f"Successfully Loaded {len(self.df)} rows.")
        except FileNotFoundError:
            logging.error(f"File not found at {input_path}")
            raise
        except Exception as e:
            logging.error(f"Failed to load CSV: {e}")
            raise

        self.ESSENTIAL_COLS = [
            'title', 
            'movie_id', 
            'budget', 
            'revenue', 
            'release_date'
        ]

        self.COLS_TO_DROP = [
            'production_countries',
            'spoken_languages',
            'last_rated',
            'avg_rating',
            'total_ratings',
            'std_dev',
            'last_rated_date',
            'overview',
            'tagline',
            'status',
            'backdrop_url'
        ]

        self.MULTI_VALUE_COLS = [
            'genres', 
            'production_companies', 
            'production_countries_names',
            'spoken_languages_names'
        ]

        self.COLS_TO_RENAME = {
            'production_countries_names' : 'production_countries',
            'spoken_languages_names' : 'languages'
        }

        self.COLS_TO_FILL_MAP = {
            'spoken_languages_names' : 'No Language',
            'production_countries_names' : 'No Country',
            'genres': 'Uncategorized', 
            'production_companies': 'Unknown' 
        }

        self.COLS_TO_EXPLODE = [
            'genres', 
            'production_companies', 
            'production_countries', 
            'spoken_languages'
        ]

        self.JUNK_WORDS = {'and'}

    def _convert_datatypes(self):
        """ Converts columns to their appropriate data types. """
        self.df['release_date'] = pd.to_datetime(self.df['release_date'], errors='coerce')
            
        # Correctly handle numeric columns to avoid casting errors
        for col in ['movie_id', 'vote_count', 'runtime']:
            if col in self.df.columns:
                # First, convert to a numeric series, coercing errors to NaN
                numeric_series = pd.to_numeric(self.df[col], errors='coerce')
                self.df[col] = numeric_series.astype('Int64')
        
        for col in ['budget', 'revenue']:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype('float')

        for col in ['popularity', 'vote_average']:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                    
        logging.info("Converted data types for date, budget, and revenue.")

    def _drop_unwanted_columns(self):
        """ Drops unwanted columns. """
        existing_cols_to_drop = [col for col in self.COLS_TO_DROP if col in self.df.columns]
        if existing_cols_to_drop:
            self.df.drop(columns=existing_cols_to_drop, inplace=True)
            logging.info(f"Dropped unwanted columns: {', '.join(existing_cols_to_drop)}")
        else:
            logging.info("No columns to drop, or they were already removed.")
    
    def _drop_duplicate_rows(self):
        """ Removes duplicate rows based on movie_id. """
        initial_rows = len(self.df)
        self.df.drop_duplicates(subset=['movie_id'], keep='first', inplace=True)
        if initial_rows > len(self.df):
            logging.info(f"Removed {initial_rows - len(self.df)} duplicate movie_id rows.")

    def _handle_essential_missing_values(self):
        """ Drops rows with missing essential data. """
        self.df.dropna(subset=self.ESSENTIAL_COLS, inplace=True)
        logging.info(f"Dropped rows with missing {self.ESSENTIAL_COLS}. Rows remaining: {len(self.df)}")

        # Drop 0 budget or revenue
        initial_rows = len(self.df)
        self.df = self.df[(self.df['budget']> 0) & (self.df['revenue'] > 0)]
        if initial_rows > len(self.df):
            logging.info(f"Removed {initial_rows - len(self.df)} rows with 0 budget or revenue.")

    def _process_multivalue_columns(self):
        """ Takes raw multi-value columns and standardizes them into a clean, comma-separated string, filling empty values with placeholders. """
        for col in self.MULTI_VALUE_COLS:
            if col in self.df.columns:
                if col == 'production_companies':
                    processed_series = extract_names_from_list_of_dicts(self.df[col]).apply(lambda name_list:
                        ', '.join([item.strip() for item in name_list
                                   if item.strip() and not all(c == '?' for c in item.strip())])
                    )
                else:
                    processed_series = extract_names_from_list_of_dicts(self.df[col]).apply(lambda name_list:
                        ', '.join([self._smart_title_case(item.strip()) for item in name_list
                                   if item.strip() and
                                   not all(c == '?' for c in item.strip()) and
                                   item.strip().lower() not in self.JUNK_WORDS])
                    )
                
                self.df[col] = processed_series

                if col in self.COLS_TO_FILL_MAP:
                    placeholder = self.COLS_TO_FILL_MAP[col]
                    fill_mask = self.df[col].isna() | (self.df[col] == '')
                    fill_count = fill_mask.sum()
                    if fill_count > 0:
                        self.df.loc[fill_mask, col] = placeholder
                        logging.info(f"Filled {fill_count} empty/null values in '{col}' with '{placeholder}'.")
        logging.info("Finished standardizing multi-value columns.")
        
    def _rename_columns(self):
        """ Rename extracted names column. """
        self.df.rename(columns=self.COLS_TO_RENAME, inplace=True)
        logging.info("Renamed extracted names columns")

    def _smart_title_case(self, text):
        """ Applies a smart title case, handling prepositions and apostrophes correctly. """
        if not isinstance(text, str):
            return text

        # Handle prepositions
        prepositions = {'a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'of', 'in', 'with'}
        words = text.split(' ')
        cased_words = [words[0].capitalize()]
        for word in words[1:]:
            cased_words.append(word.lower() if word.lower() in prepositions else word.capitalize())
        titled_text = ' '.join(cased_words)

        # Handle apostrophes
        def fix_apostrophe(match):
            return match.group(1) + match.group(2).lower()
        corrected_text = re.sub(r"(\w')([A-Z])", fix_apostrophe, titled_text)
        return corrected_text

    def save_cleaned_data(self, output_path=FINAL_CLEANED_MASTER_PATH):
        """ Saves the cleaned DataFrame to a CSV file. """
        try:
            self.df.to_csv(output_path, index=False)
            logging.info(f"Cleaned data successfully saved to {output_path}")
        except Exception as e:
            logging.error(f"Failed to save cleaned data: {e}")
            raise    

    def run(self):
        logging.info("Starting movie data cleaning process.")
        try:
            # Initial Conversion
            self._convert_datatypes()

            # Dropping rows/columns
            self._drop_unwanted_columns()
            self._drop_duplicate_rows()
            self._handle_essential_missing_values()

            # Checkpoint after dropping
            self.df = self.df.copy()

            # Standardize, Modify and Convert values
            self._process_multivalue_columns()
            self.df['title'] = self.df['title'].apply(self._smart_title_case)
            self._rename_columns()
            
            logging.info(f"Cleaning process completed. Final row count: {len(self.df)}")
            return self.df
        except Exception as e:
            logging.error(f"Error during cleaning: {e}")
            raise
    
def cleaner():
    logging.info("="*50)
    logging.info("--- MOVIE DATA CLEANING STARTED ---")
    logging.info("="*50)
    try:
        cleaner = MasterDataCleaner()
        cleaner.run()
        cleaner.save_cleaned_data()
    except Exception as e:
        logging.critical(f"Script failed to run. Reason: {e}")
    logging.info("="*50)
    logging.info("--- MOVIE DATA CLEANING FINISHED ---")
    logging.info("="*50)

if __name__ == "__main__":
    cleaner()


