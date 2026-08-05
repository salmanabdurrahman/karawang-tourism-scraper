"""
Prepare Content-Based Dataset

This script builds the corpus for the content-based recommender by loading V1
review JSON files, combining place metadata and review texts, and running an
Indonesian NLP pipeline (case folding, tokenizing, stopword removal, stemming
with Sastrawi) to produce a cleaned tags corpus per place.

Features:
    - Loads and flattens V1 JSON review files
    - Combines category, attributes, description, and reviews into one corpus
    - Runs a 4-stage Indonesian NLP pipeline (Sastrawi)
    - Exports a tags corpus per place

Output:
    - CSV file in: data/processed/karawang_places_content_based.csv

Dependencies:
    - pandas, nltk, Sastrawi

Input:
    - JSON files in: data/reviews_json/V1/*.json

Author: Salman Abdurrahman
Date: 2025
"""

import glob
import json
import os

import nltk
import pandas as pd

from config import (
    CONTENT_BASED_FILE,
    PROCESSED_DIR,
    REVIEWS_JSON_V1_DIR,
    ensure_dir,
    require_dir,
)
from utils import (
    case_folding,
    remove_stopwords,
    stemming,
    tokenizing,
)

# ===========================
# CONFIG (paths & values from config.py)
# ===========================
INPUT_DIR = REVIEWS_JSON_V1_DIR  # Read from the V1 JSON folder
OUTPUT_DIR = PROCESSED_DIR
OUTPUT_FILE = CONTENT_BASED_FILE

# Frozen output schema & column order (docs/baseline.md)
CONTENT_BASED_COLUMNS = [
    'place_name', 'place_category', 'place_address', 'place_avg_rating',
    'total_reviews_scraped', 'tags_corpus'
]


# NLP Resources (checked at processing time, not at import time)
def ensure_nltk_resources():
    """
    Checks that NLTK punkt resources exist and downloads them if missing.

    Runs at the start of processing so importing this module has no side
    effects (no downloads, no filesystem writes).
    """
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        print("Downloading NLTK resources...")
        nltk.download('punkt')
        nltk.download('punkt_tab')


# File Loading
def load_place_file(filepath):
    """
    Loads one place review JSON file.

    Args:
        filepath (str): Path to the JSON file.

    Returns:
        dict: Parsed JSON content, or None if the file could not be read.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"   Failed to LOAD {os.path.basename(filepath)}: {e}")
        return None


# Transformation
def extract_place_record(data, filepath):
    """
    Extracts place metadata and combined review text from one JSON file.

    Args:
        data (dict): Parsed JSON content of one place file.
        filepath (str): Path of the source file (used in error messages).

    Returns:
        dict: Raw place record, or None if transformation failed.
    """
    try:
        p_info = data.get('place_info', {})
        reviews = data.get('reviews', [])
        
        # Grab Place Metadata
        p_name = p_info.get('name', '')
        p_cat = p_info.get('category', '')
        p_desc = p_info.get('description', '')
        p_attr = p_info.get('attributes', '')
        p_addr = p_info.get('address', '')
        
        try:
            p_rating = float(p_info.get('avg_rating', '0').replace(',', '.'))
        except (ValueError, TypeError):
            p_rating = 0.0

        # Combine ALL review texts into one long string
        all_review_text = " ".join([r.get('text', '') for r in reviews if r.get('text')])
        
        return {
            'place_name': p_name,
            'place_category': p_cat,
            'place_address': p_addr,
            'place_avg_rating': p_rating,
            'raw_description': p_desc,
            'raw_attributes': p_attr,
            'raw_reviews_combined': all_review_text,
            'total_reviews_scraped': len(reviews)
        }

    except Exception as e:
        print(f"   Failed to TRANSFORM {os.path.basename(filepath)}: {e}")
        return None


def build_corpus(df):
    """
    Combines category, attributes, description, and reviews into one raw column.

    Args:
        df (pandas.DataFrame): Raw place records.

    Returns:
        pandas.DataFrame: Records with the combined_text_raw column added.
    """
    print("\nCombining all texts (Metadata + Reviews)...")
    
    # Combine Category + Attributes + Description + Reviews into one raw column
    df = df.copy()
    df['combined_text_raw'] = (
        df['place_category'].fillna('') + " " + 
        df['raw_attributes'].str.replace('|', ' ').fillna('') + " " + 
        df['raw_description'].fillna('') + " " + 
        df['raw_reviews_combined'].fillna('')
    )

    return df


def apply_nlp_pipeline(df):
    """
    Runs the 4-stage NLP pipeline (case folding, tokenizing, stopword removal,
    stemming) and builds the final tags corpus.

    Args:
        df (pandas.DataFrame): Records with combined_text_raw.

    Returns:
        pandas.DataFrame: Records with tags_corpus added.
    """
    print("\nRunning NLP Pipeline (Sastrawi)...")
    
    # 1. Case Folding
    print("   1. Case Folding...")
    df['step1'] = df['combined_text_raw'].apply(case_folding)
    
    # 2. Tokenizing
    print("   2. Tokenizing...")
    df['step2'] = df['step1'].apply(tokenizing)
    
    # 3. Stopword Removal
    print("   3. Stopword Removal...")
    df['step3'] = df['step2'].apply(remove_stopwords)
    
    # 4. Stemming (heaviest step, be patient)
    print("   4. Stemming (be patient, this is the heaviest step)...")
    
    total = len(df)
    stemmed_results = []
    
    for i, tokens in enumerate(df['step3']):
        # Simple progress bar
        percent = int(((i+1) / total) * 100)
        if (i+1) % 5 == 0 or i == 0 or i == total-1:
            print(f"\r      Stemming progress: [{percent}%] ({i+1}/{total} places)", end="", flush=True)
        
        stemmed_tokens = stemming(tokens)
        stemmed_results.append(stemmed_tokens)
        
    print("\n      Done!")
    
    df['step4'] = stemmed_results
    
    # Join back into the final string (Corpus)
    df['tags_corpus'] = df['step4'].apply(lambda x: ' '.join(x))

    return df


# CSV Writing
def write_output(df):
    """
    Selects the frozen output columns and writes the content-based CSV.

    Args:
        df (pandas.DataFrame): Records with tags_corpus.
    """
    df_final = df[CONTENT_BASED_COLUMNS]
    
    ensure_dir(OUTPUT_DIR)
    df_final.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "="*50)
    print("DATA PREPARATION DONE!")
    print(f"Output: {OUTPUT_FILE}")
    print("-" * 30)
    if not df_final.empty:
        print("Sample 'tags_corpus' (Final Output):")
        print(str(df_final['tags_corpus'].iloc[0])[:150] + "...") 
    print("="*50)


# Orchestration
def process_data():
    """
    Main processing function that orchestrates the whole preparation flow:
    NLTK resource check, file loading, record extraction, corpus building,
    NLP pipeline, and CSV export.
    """
    print("STARTING CONTENT-BASED DATA PREPARATION (JSON SOURCE)...")

    # NLTK resources are checked here, at processing time (not at import time)
    ensure_nltk_resources()
    
    # --- A. LOAD JSON DATA & FLATTEN ---
    if not require_dir(INPUT_DIR):
        print(f"Input folder not found: {INPUT_DIR}")
        return

    all_files = glob.glob(os.path.join(INPUT_DIR, "*.json"))
    
    if not all_files:
        print(f"No JSON files found in {INPUT_DIR}")
        return

    print(f"Processing {len(all_files)} JSON files...")
    
    places_data = []

    for filename in all_files:
        data = load_place_file(filename)
        if data is None:
            continue
        record = extract_place_record(data, filename)
        if record is not None:
            places_data.append(record)

    df = pd.DataFrame(places_data)
    print(f"Total places loaded: {len(df)}")

    # --- B. PREPARE RAW CORPUS ---
    df = build_corpus(df)

    # --- C. NLP PIPELINE (4 STAGES) ---
    df = apply_nlp_pipeline(df)

    # --- D. SAVE RESULTS ---
    write_output(df)


if __name__ == "__main__":
    process_data()
