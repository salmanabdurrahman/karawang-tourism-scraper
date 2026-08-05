"""
Prepare Content-Based Dataset

This script builds the corpus for the content-based recommender by loading V1
review JSON files, combining place metadata and review texts, and running an
Indonesian NLP pipeline (case folding, tokenizing, normalization, stopword
removal, stemming with Sastrawi) to produce a cleaned tags corpus per place.

Features:
    - Loads and flattens V1 JSON review files
    - Deduplicates and caps review text used by each place corpus
    - Combines place name, category, attributes, description, and reviews
    - Runs the five-stage Indonesian NLP pipeline described in the paper
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
    MAX_SAMPLE_REVIEWS_PER_PLACE,
    PROCESSED_DIR,
    REVIEWS_JSON_V1_DIR,
    ensure_dir,
    require_dir,
)
from utils import (
    case_folding,
    clean_text,
    normalize_tokens,
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
    "place_name",
    "place_category",
    "place_address",
    "place_avg_rating",
    "total_reviews_scraped",
    "tags_corpus",
]


# NLP Resources (checked at processing time, not at import time)
def ensure_nltk_resources():
    """
    Checks both tokenizer resources required by the installed NLTK version.

    Runs during processing so importing this module has no side effects. Each
    resource is checked independently because ``punkt`` may already exist while
    ``punkt_tab`` is missing on newer NLTK versions.
    """
    resources = {
        "tokenizers/punkt": "punkt",
        "tokenizers/punkt_tab": "punkt_tab",
    }

    for resource_path, package_name in resources.items():
        try:
            nltk.data.find(resource_path)
        except LookupError:
            print(f"Downloading NLTK resource: {package_name}...")
            if not nltk.download(package_name):
                raise RuntimeError(f"Unable to download required NLTK resource: {package_name}")


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
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"   Failed to LOAD {os.path.basename(filepath)}: {e}")
        return None


# Transformation
def select_corpus_reviews(raw_reviews, max_count=MAX_SAMPLE_REVIEWS_PER_PLACE):
    """
    Cleans, deduplicates, and deterministically caps reviews for one corpus.

    The recommendation corpus must not let places with more scraped reviews
    dominate the vector space. The selection keeps up to the same 150-review
    per-place cap used by the main processing pipeline, while remaining
    deterministic for reproducible model evaluation. When a place has more
    reviews than the cap, the first target from each rating bucket is kept,
    then remaining slots are filled in source order.

    Args:
        raw_reviews (list): Raw review dictionaries from one JSON file.
        max_count (int): Maximum number of review texts to retain.

    Returns:
        list of str: Cleaned, unique review texts selected for the corpus.
    """
    if max_count <= 0:
        return []

    unique_reviews = []
    seen_signatures = set()
    buckets = {1: [], 2: [], 3: [], 4: [], 5: [], 0: []}

    for review in raw_reviews or []:
        if not isinstance(review, dict):
            continue

        text = clean_text(review.get("text", ""))
        if not text:
            continue

        user_name = clean_text(review.get("user_name", "")).strip().casefold()
        signature = (user_name, text.casefold())
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        try:
            rating = int(review.get("rating", 0))
        except (TypeError, ValueError):
            rating = 0
        if rating not in buckets:
            rating = 0

        record_index = len(unique_reviews)
        unique_reviews.append(text)
        buckets[rating].append(record_index)

    if len(unique_reviews) <= max_count:
        return unique_reviews

    target_per_rating = max_count // 5
    selected_indices = []
    for rating in range(1, 6):
        selected_indices.extend(buckets[rating][:target_per_rating])

    selected_set = set(selected_indices)
    remaining_indices = [i for i in range(len(unique_reviews)) if i not in selected_set]
    selected_indices.extend(remaining_indices[: max_count - len(selected_indices)])

    return [unique_reviews[i] for i in selected_indices]


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
        if not isinstance(data, dict):
            return None

        p_info = data.get("place_info", {})
        if not isinstance(p_info, dict):
            return None

        p_name = p_info.get("name", "")
        if not isinstance(p_name, str) or not p_name.strip():
            print(f"   Skipping place without a valid name: {os.path.basename(filepath)}")
            return None

        reviews = data.get("reviews", [])

        # Grab Place Metadata
        p_cat = p_info.get("category", "")
        p_desc = p_info.get("description", "")
        p_attr = p_info.get("attributes", "")
        p_addr = p_info.get("address", "")

        try:
            p_rating = float(p_info.get("avg_rating", "0").replace(",", "."))
        except (ValueError, TypeError):
            p_rating = 0.0

        # Use cleaned, deduplicated, capped reviews so review volume does not
        # dominate the destination's metadata in TF-IDF.
        selected_review_texts = select_corpus_reviews(reviews)
        all_review_text = " ".join(selected_review_texts)

        return {
            "place_name": p_name,
            "place_category": p_cat,
            "place_address": p_addr,
            "place_avg_rating": p_rating,
            "raw_description": p_desc,
            "raw_attributes": p_attr,
            "raw_reviews_combined": all_review_text,
            "total_reviews_scraped": len(reviews),
        }

    except Exception as e:
        print(f"   Failed to TRANSFORM {os.path.basename(filepath)}: {e}")
        return None


def build_corpus(df):
    """
    Combines the paper's text fields into one raw column.

    Args:
        df (pandas.DataFrame): Raw place records.

    Returns:
        pandas.DataFrame: Records with the combined_text_raw column added.
    """
    print("\nCombining all texts (Metadata + Reviews)...")

    # Combine Category + Name + Description + Attributes + Reviews into one
    # raw column, matching the fields described in the reference paper.
    df = df.copy()
    df["combined_text_raw"] = (
        df["place_category"].fillna("")
        + " "
        + df["place_name"].fillna("")
        + " "
        + df["raw_description"].fillna("")
        + " "
        + df["raw_attributes"].fillna("").str.replace("|", " ", regex=False)
        + " "
        + df["raw_reviews_combined"].fillna("")
    )

    return df


def apply_nlp_pipeline(df):
    """
    Runs the five-stage NLP pipeline (case folding, tokenizing, normalization,
    stopword removal, stemming) and builds the final tags corpus.

    Args:
        df (pandas.DataFrame): Records with combined_text_raw.

    Returns:
        pandas.DataFrame: Records with tags_corpus added.
    """
    print("\nRunning NLP Pipeline (Sastrawi)...")

    # 1. Case Folding
    print("   1. Case Folding...")
    df["step1"] = df["combined_text_raw"].apply(case_folding)

    # 2. Tokenizing
    print("   2. Tokenizing...")
    df["step2"] = df["step1"].apply(tokenizing)

    # 3. Word Normalization
    print("   3. Word Normalization...")
    df["step3"] = df["step2"].apply(normalize_tokens)

    # 4. Stopword Removal
    print("   4. Stopword Removal...")
    df["step4"] = df["step3"].apply(remove_stopwords)

    # 5. Stemming (heaviest step, be patient)
    print("   5. Stemming (be patient, this is the heaviest step)...")

    total = len(df)
    stemmed_results = []

    for i, tokens in enumerate(df["step4"]):
        # Simple progress bar
        percent = int(((i + 1) / total) * 100)
        if (i + 1) % 5 == 0 or i == 0 or i == total - 1:
            print(f"\r      Stemming progress: [{percent}%] ({i + 1}/{total} places)", end="", flush=True)

        stemmed_tokens = stemming(tokens)
        stemmed_results.append(stemmed_tokens)

    print("\n      Done!")

    df["step5"] = stemmed_results

    # Join back into the final string (Corpus)
    df["tags_corpus"] = df["step5"].apply(lambda x: " ".join(x))

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
    df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print("\n" + "=" * 50)
    print("DATA PREPARATION DONE!")
    print(f"Output: {OUTPUT_FILE}")
    print("-" * 30)
    if not df_final.empty:
        corpus_lengths = df_final["tags_corpus"].fillna("").astype(str).str.len()
        print(
            "Corpus character lengths: "
            f"min={corpus_lengths.min()}, median={corpus_lengths.median():.0f}, max={corpus_lengths.max()}"
        )
    print("=" * 50)


# Orchestration
def process_data():
    """
    Main processing function that orchestrates the whole preparation flow:
    NLTK resource check, file loading, record extraction, corpus building,
    NLP pipeline, and CSV export.
    """
    print("STARTING CONTENT-BASED DATA PREPARATION (JSON SOURCE)...")

    # --- A. LOAD JSON DATA & FLATTEN ---
    if not require_dir(INPUT_DIR):
        print(f"Input folder not found: {INPUT_DIR}")
        return

    # Sort paths so corpus row order remains reproducible across filesystems.
    all_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.json")))

    if not all_files:
        print(f"No JSON files found in {INPUT_DIR}")
        return

    # NLTK resources are checked here, after input validation and before NLP.
    ensure_nltk_resources()

    print(f"Processing {len(all_files)} JSON files...")

    places_data = []

    for filename in all_files:
        data = load_place_file(filename)
        if data is None:
            continue
        record = extract_place_record(data, filename)
        if record is not None:
            places_data.append(record)

    if not places_data:
        print("No valid place records found; content-based output was not written.")
        return

    df = pd.DataFrame(places_data)
    print(f"Total places loaded: {len(df)}")

    # --- B. PREPARE RAW CORPUS ---
    df = build_corpus(df)

    # --- C. NLP PIPELINE (5 STAGES) ---
    df = apply_nlp_pipeline(df)

    # --- D. SAVE RESULTS ---
    write_output(df)


if __name__ == "__main__":
    process_data()
