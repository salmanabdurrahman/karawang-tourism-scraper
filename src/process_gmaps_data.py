"""
Google Maps Reviews Data Processing Script

This script processes raw JSON review files from Google Maps scraping, performs
data cleaning, deduplication, and balanced sampling to create a final dataset
ready for analysis or machine learning tasks.

Features:
    - Loads and processes multiple JSON review files
    - Cleans text from special characters and formatting issues
    - Anonymizes user information using MD5 hashing
    - Converts relative timestamps to ISO dates
    - Removes duplicate reviews
    - Performs stratified sampling by rating (balanced distribution)
    - Exports to clean CSV format

Processing Steps:
    1. Load JSON files and extract place metadata + reviews
    2. Clean and normalize text data
    3. Deduplicate reviews within each place
    4. Apply stratified sampling (balanced star ratings)
    5. Flatten nested structure to tabular format
    6. Export to final CSV file

Output:
    - Final dataset: data/processed/karawang_tourism_final.csv

Dependencies:
    - pandas

Input:
    - JSON files in: data/reviews_json/*.json (from gmaps_reviews_scraper.py)

Author: Salman Abdurrahman
Date: 2025
"""

import glob
import json
import os
import random

import pandas as pd

from config import (
    MAX_SAMPLE_REVIEWS_PER_PLACE,
    PROCESSED_DIR,
    REVIEWS_JSON_DIR,
    TOURISM_FINAL_FILE,
    ensure_dir,
    require_dir,
)
from utils import (
    anonymize_user,
    clean_attributes,
    clean_text,
    convert_relative_time,
    parse_int_from_text,
)

# Configuration (paths & values centralized in config.py)
INPUT_DIR = REVIEWS_JSON_DIR
OUTPUT_DIR = PROCESSED_DIR
OUTPUT_FILE = TOURISM_FINAL_FILE

# Limit reviews per place for balanced dataset
MAX_REVIEWS_PER_PLACE = MAX_SAMPLE_REVIEWS_PER_PLACE

# Frozen output schema & column order (docs/baseline.md)
FINAL_COLUMNS = [
    "user_id",
    "user_rating",
    "review_text",
    "review_time",
    "place_name",
    "place_description",
    "place_category",
    "place_attributes",
    "place_address",
    "place_total_reviews_gmaps",
    "place_avg_rating",
]


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
        print(f"   Warning: failed to LOAD {os.path.basename(filepath)}: {e}")
        return None


# Review Processing Functions
def deduplicate_reviews(raw_reviews):
    """
    Removes duplicate reviews based on user name and review text.

    Also cleans and enriches review data during deduplication.

    Args:
        raw_reviews (list): List of raw review dictionaries

    Returns:
        list: List of unique, cleaned review dictionaries
    """
    unique_reviews = []
    seen_signatures = set()

    for review in raw_reviews:
        # Clean user name and review text
        user_name = clean_text(review.get("user_name", ""))
        review_text = clean_text(review.get("text", ""))

        # Skip reviews without text
        if not review_text:
            continue

        # Create signature for duplicate detection
        signature = (user_name, review_text)

        if signature not in seen_signatures:
            seen_signatures.add(signature)

            # Enrich review data
            review["clean_user_id"] = anonymize_user(user_name)
            review["clean_text"] = review_text
            review["clean_time_iso"] = convert_relative_time(clean_text(review.get("time", "")))

            unique_reviews.append(review)

    return unique_reviews


def stratified_sample_reviews(reviews, max_count):
    """
    Performs stratified sampling to balance rating distribution.

    Ensures diverse representation of all rating levels (1-5 stars) in the sample.
    Strategy:
    1. Group reviews into rating buckets (1-5 stars)
    2. Calculate target per rating (max_count / 5)
    3. Sample from each bucket to meet target
    4. Fill remaining slots from overflow pool
    5. Shuffle final results

    Args:
        reviews (list): List of review dictionaries
        max_count (int): Maximum number of reviews to return

    Returns:
        list: Balanced sample of reviews
    """
    if len(reviews) <= max_count:
        return reviews

    # Group reviews into rating buckets
    buckets = {1: [], 2: [], 3: [], 4: [], 5: [], 0: []}

    for review in reviews:
        try:
            rating = int(review.get("rating", 0))
        except (ValueError, TypeError):
            rating = 0

        if rating not in buckets:
            rating = 0

        buckets[rating].append(review)

    # Calculate target per star rating
    target_per_star = max_count // 5

    sampled_reviews = []
    overflow_pool = []

    # Sample from each rating bucket
    for star in range(1, 6):
        reviews_in_bucket = buckets[star]
        random.shuffle(reviews_in_bucket)

        # Take up to target, or all if less
        taken = reviews_in_bucket[:target_per_star]
        sampled_reviews.extend(taken)

        # Add overflow to pool
        overflow_pool.extend(reviews_in_bucket[target_per_star:])

    # Add rating 0 (no rating) to overflow pool
    overflow_pool.extend(buckets[0])

    # Fill remaining slots from overflow pool
    shortage = max_count - len(sampled_reviews)
    if shortage > 0 and overflow_pool:
        random.shuffle(overflow_pool)
        sampled_reviews.extend(overflow_pool[:shortage])

    # Shuffle to avoid grouping by rating
    random.shuffle(sampled_reviews)

    return sampled_reviews


# Transformation
def transform_place_file(data, filepath):
    """
    Cleans, deduplicates, samples, and flattens one place's data.

    Args:
        data (dict): Parsed JSON content of one place file.
        filepath (str): Path of the source file (used in error messages).

    Returns:
        list: Flattened review records, empty list if transformation failed.
    """
    try:
        place_info = data.get("place_info", {})
        raw_reviews = data.get("reviews", [])

        # Extract and clean place metadata
        place_name = clean_text(place_info.get("name", ""))
        place_category = clean_text(place_info.get("category", ""))
        place_address = clean_text(place_info.get("address", ""))
        place_description = clean_text(place_info.get("description", ""))
        place_attributes = clean_attributes(place_info.get("attributes", ""))

        # Parse numeric fields
        try:
            place_avg_rating = float(str(place_info.get("avg_rating", "0")).replace(",", "."))
        except (ValueError, TypeError):
            place_avg_rating = 0.0

        place_total_reviews = parse_int_from_text(place_info.get("total_reviews_text", ""))

        # Process reviews
        unique_reviews = deduplicate_reviews(raw_reviews)
        sampled_reviews = stratified_sample_reviews(unique_reviews, MAX_REVIEWS_PER_PLACE)

        # Flatten to table format
        flattened_records = []
        for review in sampled_reviews:
            flattened_records.append(
                {
                    "user_id": review["clean_user_id"],
                    "user_rating": review.get("rating", 0),
                    "review_text": review["clean_text"],
                    "review_time": review["clean_time_iso"],
                    "place_name": place_name,
                    "place_description": place_description,
                    "place_category": place_category,
                    "place_attributes": place_attributes,
                    "place_address": place_address,
                    "place_total_reviews_gmaps": place_total_reviews,
                    "place_avg_rating": place_avg_rating,
                }
            )

        return flattened_records

    except Exception as e:
        print(f"   Warning: failed to TRANSFORM {os.path.basename(filepath)}: {e}")
        return []


# CSV Writing
def write_output(records):
    """
    Builds the final DataFrame and writes the CSV output (utf-8-sig).

    Args:
        records (list): Flattened review records from all processed places.
    """
    df = pd.DataFrame(records)

    # Reorder columns to the frozen schema
    available_columns = [col for col in FINAL_COLUMNS if col in df.columns]
    df = df[available_columns]

    # Save to CSV
    ensure_dir(OUTPUT_DIR)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    # Print summary
    print("\n" + "=" * 50)
    print("DATA PROCESSING COMPLETED!")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Total reviews: {len(df)}")
    print("-" * 30)
    print("Rating Distribution:")
    print(df["user_rating"].value_counts().sort_index())
    print("=" * 50)


# Orchestration
def process_all_files():
    """
    Main processing function that orchestrates the entire pipeline.

    Loads all JSON files, processes each place, and exports final dataset.
    """
    print("Starting data processing with balanced sampling...")

    # Validate input folder before processing
    if not require_dir(INPUT_DIR):
        print(f"Error: Reviews JSON folder not found: {INPUT_DIR}")
        return

    # Find all JSON files
    all_files = glob.glob(os.path.join(INPUT_DIR, "*.json"))

    if not all_files:
        print("Error: No JSON files found.")
        return

    print(f"Found {len(all_files)} place files.")

    # Process all files
    all_records = []

    for filepath in all_files:
        data = load_place_file(filepath)
        if data is None:
            continue
        all_records.extend(transform_place_file(data, filepath))

    # Export to CSV
    if all_records:
        write_output(all_records)
    else:
        print("Error: No data was successfully processed.")


if __name__ == "__main__":
    process_all_files()
