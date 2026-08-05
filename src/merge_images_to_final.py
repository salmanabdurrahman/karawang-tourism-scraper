"""
Merge Images into Final Dataset

This script merges the scraped place images (from gmaps_image_scraper.py) into
the main tourism review dataset, producing a final CSV with an image_url column.
Place names are normalized so minor spacing differences still match.

Features:
    - Normalizes place names for robust join keys
    - Left-joins image URLs onto the review dataset
    - Fills missing images with a placeholder URL
    - Reorders columns for readability

Output:
    - Final dataset with images: data/processed/karawang_tourism_final_with_images.csv

Dependencies:
    - pandas

Input:
    - Main dataset: data/processed/karawang_tourism_final.csv
    - Images dataset: data/processed/karawang_place_images.csv

Author: Salman Abdurrahman
Date: 2025
"""

import os

import pandas as pd

from config import (
    PLACE_IMAGES_FILE,
    PLACEHOLDER_IMAGE_URL,
    PROCESSED_DIR,
    TOURISM_FINAL_FILE,
    TOURISM_FINAL_WITH_IMAGES_FILE,
    ensure_dir,
    require_file,
)
from utils import clean_name_key

# ===========================
# CONFIG (paths & values from config.py)
# ===========================
# Main Dataset File (Reviews)
MAIN_DATASET_FILE = TOURISM_FINAL_FILE
# Images File (Output of the Image Scraper)
IMAGES_FILE = PLACE_IMAGES_FILE
# Final Output
OUTPUT_FILE = TOURISM_FINAL_WITH_IMAGES_FILE

# Frozen output schema & column order (docs/baseline.md)
FINAL_COLUMNS = [
    "user_id",
    "user_rating",
    "review_text",
    "review_time",
    "place_name",
    "image_url",
    "place_description",
    "place_category",
    "place_attributes",
    "place_address",
    "place_total_reviews_gmaps",
    "place_avg_rating",
]


# File Loading
def load_dataset(path, label):
    """
    Loads one input CSV dataset.

    Args:
        path (str): Path to the CSV file.
        label (str): Human-readable dataset label for error messages.

    Returns:
        pandas.DataFrame: Loaded dataset, or None if loading failed.
    """
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"Error: failed to LOAD {label} ({os.path.basename(path)}): {e}")
        return None


# Transformation
def merge_images(df_main, df_img):
    """
    Left-joins image URLs onto the review dataset using normalized place names.

    Args:
        df_main (pandas.DataFrame): Main review dataset.
        df_img (pandas.DataFrame): Place images dataset.

    Returns:
        pandas.DataFrame: Merged dataset with missing image URLs filled by the
        frozen placeholder.
    """
    # 2. Build Join Key (Normalized Name)
    # Trick: matches even with minor spacing differences
    df_main = df_main.copy()
    df_img = df_img.copy()
    df_main["join_key"] = df_main["place_name"].apply(clean_name_key)
    df_img["join_key"] = df_img["place_name"].apply(clean_name_key)

    # Drop the original name column in df_img to avoid duplication on merge
    df_img_clean = df_img[["join_key", "image_url"]].drop_duplicates(subset=["join_key"])

    # 3. Merge (Left Join)
    # Attach image_url to the review dataset based on join_key
    df_final = pd.merge(df_main, df_img_clean, on="join_key", how="left")

    # Fill missing values with placeholder
    df_final["image_url"] = df_final["image_url"].fillna(PLACEHOLDER_IMAGE_URL)

    # 4. Clean Up Columns
    if "join_key" in df_final.columns:
        df_final = df_final.drop(columns=["join_key"])

    return df_final


# CSV Writing
def write_output(df):
    """
    Applies the frozen column order and writes the final CSV (utf-8-sig).

    Args:
        df (pandas.DataFrame): Merged dataset.
    """
    # Reorder columns to the frozen schema (image_url next to place_name)
    available_columns = [col for col in FINAL_COLUMNS if col in df.columns]
    df = df[available_columns]

    # 5. Save
    ensure_dir(PROCESSED_DIR)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 50)
    print("DONE! Final dataset + images saved.")
    print(f"Output: {OUTPUT_FILE}")
    print("-" * 30)
    print(df[["place_name", "image_url"]].head(3))
    print("=" * 50)


# Orchestration
def merge_data():
    """
    Main merge function: validates inputs, loads both datasets, merges images,
    and writes the final dataset with images.
    """
    print("MERGING IMAGES INTO THE MAIN DATASET...")

    if not require_file(MAIN_DATASET_FILE):
        print(f"Main dataset file not found: {MAIN_DATASET_FILE}")
        return
    if not require_file(IMAGES_FILE):
        print(f"Images file not found: {IMAGES_FILE}")
        print("   Run 'src/gmaps_image_scraper.py' first!")
        return

    # 1. Load Data
    df_main = load_dataset(MAIN_DATASET_FILE, "main dataset")
    if df_main is None:
        return
    df_img = load_dataset(IMAGES_FILE, "images dataset")
    if df_img is None:
        return
    print(f"Review dataset: {len(df_main)} rows")
    print(f"Images dataset: {len(df_img)} places")

    # 2-4. Merge + placeholder fill
    df_final = merge_images(df_main, df_img)

    # 5. Save
    write_output(df_final)


if __name__ == "__main__":
    merge_data()
