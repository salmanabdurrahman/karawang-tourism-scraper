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
    - JSON files in: data/reviews_json/*.json (from gmaps_reviews_json_scraper.py)

Author: Salman Abdurrahman
Date: 2025
"""

import pandas as pd
import glob
import os
import json
import re
import hashlib
import random
from datetime import datetime, timedelta


# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, "data", "reviews_json")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "karawang_tourism_final.csv")

# Limit reviews per place for balanced dataset
MAX_REVIEWS_PER_PLACE = 150

os.makedirs(OUTPUT_DIR, exist_ok=True)


# Text Cleaning Functions
def clean_text(text):
    """
    Cleans text from special characters and Google Maps artifacts.
    
    Removes common encoding issues and normalizes whitespace.
    
    Args:
        text (str): Raw text to clean
        
    Returns:
        str: Cleaned text with normalized whitespace
    """
    if not isinstance(text, str):
        return ""
    
    # Remove Google Maps specific artifacts
    artifacts = ["Óóä", "¬†", "", "", ""]
    for artifact in artifacts:
        text = text.replace(artifact, "")
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


def clean_attributes(text):
    """
    Cleans and formats place attributes text.
    
    Removes leading special characters and formats as comma-separated list.
    
    Args:
        text (str): Raw attributes text with pipe separators
        
    Returns:
        str: Comma-separated cleaned attributes
    """
    if not isinstance(text, str):
        return ""
    
    text = clean_text(text)
    items = text.split('|')
    
    clean_items = []
    for item in items:
        # Remove leading non-alphanumeric characters
        cleaned = re.sub(r'^[^a-zA-Z0-9]+', '', item).strip()
        if cleaned:
            clean_items.append(cleaned)
    
    return ", ".join(clean_items)


# User Anonymization
def anonymize_user(user_name):
    """
    Anonymizes user name using MD5 hashing.
    
    Args:
        user_name (str): Original user name
        
    Returns:
        str: First 10 characters of MD5 hash, or "anonymous" if empty
    """
    if not isinstance(user_name, str) or not user_name:
        return "anonymous"
    
    user_name = user_name.strip().lower()
    hash_object = hashlib.md5(user_name.encode('utf-8'))
    
    return hash_object.hexdigest()[:10]


# Timestamp Conversion
def convert_relative_time(text):
    """
    Converts relative time text to ISO date format.
    
    Handles various Indonesian time expressions like:
    - "2 jam yang lalu" -> date 2 hours ago
    - "3 hari yang lalu" -> date 3 days ago
    - "1 minggu yang lalu" -> date 1 week ago
    - "2 bulan yang lalu" -> date 2 months ago
    - "1 tahun yang lalu" -> date 1 year ago
    
    Args:
        text (str): Relative time text in Indonesian
        
    Returns:
        str: ISO date string (YYYY-MM-DD), empty string if parsing fails
    """
    if not isinstance(text, str) or not text:
        return ""
    
    text = text.lower().replace("diedit", "").strip()
    current_time = datetime.now()
    delta = timedelta(0)
    
    try:
        # Recent times (minutes, seconds, just now)
        if any(word in text for word in ["menit", "detik", "baru saja"]):
            delta = timedelta(days=0)
        
        # Hours
        elif "jam" in text:
            match = re.search(r'(\d+)', text)
            hours = int(match.group(1)) if match else 1
            delta = timedelta(hours=hours)
        
        # Days
        elif "hari" in text:
            match = re.search(r'(\d+)', text)
            days = int(match.group(1)) if match else 1
            delta = timedelta(days=days)
        
        # Weeks
        elif "minggu" in text:
            match = re.search(r'(\d+)', text)
            weeks = int(match.group(1)) if match else 1
            delta = timedelta(weeks=weeks)
        
        # Months (approximate: 30 days per month)
        elif "bulan" in text:
            match = re.search(r'(\d+)', text)
            months = int(match.group(1)) if match else 1
            delta = timedelta(days=months * 30)
        
        # Years (approximate: 365 days per year)
        elif "tahun" in text:
            match = re.search(r'(\d+)', text)
            years = int(match.group(1)) if match else 1
            delta = timedelta(days=years * 365)
        
        past_date = current_time - delta
        return past_date.strftime("%Y-%m-%d")
    
    except Exception:
        return ""


def parse_int_from_text(text):
    """
    Extracts integer from text by removing all non-digit characters.
    
    Args:
        text (str): Text containing numbers
        
    Returns:
        int: Extracted integer, 0 if no digits found
    """
    if not isinstance(text, str):
        return 0
    
    nums = re.sub(r'\D', '', text)
    return int(nums) if nums else 0


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
        user_name = clean_text(review.get('user_name', ''))
        review_text = clean_text(review.get('text', ''))
        
        # Skip reviews without text
        if not review_text:
            continue
        
        # Create signature for duplicate detection
        signature = (user_name, review_text)
        
        if signature not in seen_signatures:
            seen_signatures.add(signature)
            
            # Enrich review data
            review['clean_user_id'] = anonymize_user(user_name)
            review['clean_text'] = review_text
            review['clean_time_iso'] = convert_relative_time(
                clean_text(review.get('time', ''))
            )
            
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
            rating = int(review.get('rating', 0))
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


def process_place_file(filepath):
    """
    Processes a single place JSON file.
    
    Args:
        filepath (str): Path to JSON file
        
    Returns:
        list: List of flattened review records, empty list if error
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        place_info = data.get('place_info', {})
        raw_reviews = data.get('reviews', [])
        
        # Extract and clean place metadata
        place_name = clean_text(place_info.get('name', ''))
        place_category = clean_text(place_info.get('category', ''))
        place_address = clean_text(place_info.get('address', ''))
        place_description = clean_text(place_info.get('description', ''))
        place_attributes = clean_attributes(place_info.get('attributes', ''))
        
        # Parse numeric fields
        try:
            place_avg_rating = float(
                str(place_info.get('avg_rating', '0')).replace(',', '.')
            )
        except (ValueError, TypeError):
            place_avg_rating = 0.0
        
        place_total_reviews = parse_int_from_text(
            place_info.get('total_reviews_text', '')
        )
        
        # Process reviews
        unique_reviews = deduplicate_reviews(raw_reviews)
        sampled_reviews = stratified_sample_reviews(
            unique_reviews, 
            MAX_REVIEWS_PER_PLACE
        )
        
        # Flatten to table format
        flattened_records = []
        for review in sampled_reviews:
            flattened_records.append({
                'user_id': review['clean_user_id'],
                'user_rating': review.get('rating', 0),
                'review_text': review['clean_text'],
                'review_time': review['clean_time_iso'],
                'place_name': place_name,
                'place_description': place_description,
                'place_category': place_category,
                'place_attributes': place_attributes,
                'place_address': place_address,
                'place_total_reviews_gmaps': place_total_reviews,
                'place_avg_rating': place_avg_rating
            })
        
        return flattened_records
    
    except Exception as e:
        print(f"   Warning: Error reading {os.path.basename(filepath)}: {e}")
        return []


def process_all_files():
    """
    Main processing function that orchestrates the entire pipeline.
    
    Loads all JSON files, processes each place, and exports final dataset.
    """
    print("Starting data processing with balanced sampling...")
    
    # Find all JSON files
    all_files = glob.glob(os.path.join(INPUT_DIR, "*.json"))
    
    if not all_files:
        print("Error: No JSON files found.")
        return
    
    print(f"Found {len(all_files)} place files.")
    
    # Process all files
    all_records = []
    
    for filepath in all_files:
        records = process_place_file(filepath)
        all_records.extend(records)
    
    # Export to CSV
    if all_records:
        df = pd.DataFrame(all_records)
        
        # Reorder columns for consistency
        column_order = [
            'user_id',
            'user_rating',
            'review_text',
            'review_time',
            'place_name',
            'place_description',
            'place_category',
            'place_attributes',
            'place_address',
            'place_total_reviews_gmaps',
            'place_avg_rating'
        ]
        
        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]
        
        # Save to CSV
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
        # Print summary
        print("\n" + "=" * 50)
        print("DATA PROCESSING COMPLETED!")
        print(f"Output file: {OUTPUT_FILE}")
        print(f"Total reviews: {len(df)}")
        print("-" * 30)
        print("Rating Distribution:")
        print(df['user_rating'].value_counts().sort_index())
        print("=" * 50)
    
    else:
        print("Error: No data was successfully processed.")


if __name__ == "__main__":
    process_all_files()