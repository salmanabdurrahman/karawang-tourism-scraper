"""
Content-Based Recommender Engine (TF-IDF + Cosine Similarity)

This script builds a content-based recommendation system from the prepared
tags corpus: TF-IDF vectorization with tuned parameters, a cosine similarity
matrix, and a recommendation function that returns the top 10 similar places.
It also prints sample TF-IDF and similarity matrices for documentation.

The whole workflow (load → clean → TF-IDF → similarity → demo) runs from
`main()`, so importing this module has no side effects (no file reads, no
model computation). Call the entry point directly:

    python src/recommender_engine.py

Features:
    - Cleans and normalizes place display names
    - Builds a tuned TF-IDF matrix (1-2 ngrams, sublinear TF)
    - Computes the cosine similarity matrix
    - Recommends the top 10 similar places for a given place

Dependencies:
    - pandas, numpy, scikit-learn

Input:
    - CSV file in: data/processed/karawang_places_content_based.csv

Author: Salman Abdurrahman
Date: 2025
"""

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

from config import CONTENT_BASED_FILE, require_file
from utils import clean_display_name

# ===========================
# CONFIG (paths & values from config.py)
# ===========================
INPUT_FILE = CONTENT_BASED_FILE

# Frozen TF-IDF parameters (docs/baseline.md)
TFIDF_PARAMS = {
    "analyzer": "word",
    "ngram_range": (1, 2),
    "min_df": 2,
    "max_df": 0.85,
    "max_features": 10000,
    "sublinear_tf": True,
}

# Recommendation count: top 10 similar places (docs/baseline.md)
TOP_N = 10

# Sample places & keywords for the Table 4.6 / 4.7 console tables
SAMPLE_PLACES = ["Curug Cigentis", "Hutan Kertas", "Pantai Samudera Baru", "Goa Dayeuh", "Green Canyon"]

DISPLAY_KEYWORDS = ["air", "alam", "pantai", "sejarah", "curug", "pasir", "wahana", "kolam", "sejuk", "foto"]


# ===========================
# Dataset Loading & Cleaning
# ===========================
def load_dataset(input_file=INPUT_FILE):
    """
    Loads the prepared content-based CSV and validates it before vectorization.

    Args:
        input_file (str): Path to the content-based CSV.

    Returns:
        pandas.DataFrame: Loaded dataset, or None if the file is missing or
        the dataset is empty.
    """
    if not require_file(input_file):
        print(f"File not found: {input_file}! Run the data preparation script first.")
        return None

    print("Loading dataset...")
    df = pd.read_csv(input_file)

    if df.empty:
        print("Dataset is empty: no tourist destinations to process.")
        return None

    return df


def clean_place_names(df):
    """
    Cleans and normalizes place display names and fills null corpus values.

    Args:
        df (pandas.DataFrame): Loaded dataset.

    Returns:
        pandas.DataFrame: Dataset with cleaned place_name and tags_corpus
        without null values.
    """
    print("Cleaning up place names...")

    df["place_name"] = df["place_name"].apply(clean_display_name)

    # Make sure there are no null values in the corpus
    df["tags_corpus"] = df["tags_corpus"].fillna("")

    return df


# ===========================
# Model Building
# ===========================
def build_tfidf(df):
    """
    Builds the tuned TF-IDF matrix from the tags corpus.

    Args:
        df (pandas.DataFrame): Cleaned dataset.

    Returns:
        tuple: (tfidf_matrix, feature_names) where tfidf_matrix is the sparse
        TF-IDF matrix and feature_names its vocabulary.
    """
    print("\nComputing TF-IDF with Optimized Parameters...")

    # Initialize the TF-IDF Vectorizer with tuning
    tf = TfidfVectorizer(**TFIDF_PARAMS)

    # Fit and Transform
    tfidf_matrix = tf.fit_transform(df["tags_corpus"])
    feature_names = tf.get_feature_names_out()

    print(f"TF-IDF matrix built. Shape: {tfidf_matrix.shape}")
    print(f"   (Compressed from 60k+ words to {tfidf_matrix.shape[1]} important words)")

    return tfidf_matrix, feature_names


def build_similarity(tfidf_matrix):
    """
    Computes the dense cosine similarity matrix from the TF-IDF matrix.

    Args:
        tfidf_matrix (scipy.sparse matrix): TF-IDF matrix.

    Returns:
        numpy.ndarray: Dense cosine similarity matrix.
    """
    print("\nComputing Cosine Similarity...")

    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

    print("Similarity matrix computed.")

    return cosine_sim


def build_indices(df):
    """
    Builds the place_name → row index lookup used by the recommendation query.

    Args:
        df (pandas.DataFrame): Cleaned dataset.

    Returns:
        pandas.Series: Series with place_name as index and row index as value,
        with duplicate place names dropped.
    """
    return pd.Series(df.index, index=df["place_name"]).drop_duplicates()


# ===========================
# Recommendation Query
# ===========================
def get_recommendations(title, cosine_sim, df, indices):
    """
    Returns the top 10 similar places for a given place.

    Args:
        title (str): Place name (display or search keyword).
        cosine_sim (numpy.ndarray): Cosine similarity matrix.
        df (pandas.DataFrame): Cleaned dataset.
        indices (pandas.Series): place_name → row index lookup.

    Returns:
        pandas.DataFrame: Top 10 similar places with columns place_name,
        place_category, place_avg_rating, similarity_score, sorted by
        similarity descending, or None if the place is not found.
    """
    # Clean the user input too so it matches
    title_clean = clean_display_name(title)

    # Search logic
    if title_clean not in indices:
        mask = df["place_name"].str.contains(title_clean, case=False, na=False)
        if mask.any():
            title_clean = df[mask].iloc[0]["place_name"]
            print(f"   (Using search result: {title_clean})")
        else:
            return None

    idx = indices[title_clean]

    # Grab the similarity scores
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort in descending order
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    # Take the top 10 (skip index 0)
    sim_scores = sim_scores[1 : TOP_N + 1]

    place_indices = [i[0] for i in sim_scores]
    place_scores = [i[1] for i in sim_scores]

    # Return the result
    result = df.iloc[place_indices][["place_name", "place_category", "place_avg_rating"]].copy()
    result["similarity_score"] = place_scores
    return result


# ===========================
# Console Tables & Demo (existing output)
# ===========================
def select_sample_indices(df):
    """
    Finds up to 5 sample places for the TF-IDF/similarity console tables.

    Args:
        df (pandas.DataFrame): Cleaned dataset.

    Returns:
        list: Row indices of the sample places (exact/partial matches or a
        fallback of the first places).
    """
    print("   Looking for sample places...")

    sample_indices = []

    for p in SAMPLE_PLACES:
        # Look for exact match or a strong partial match
        mask = df["place_name"].str.contains(p, case=False, na=False)
        if mask.any():
            sample_indices.append(df[mask].index[0])

    # Fallback if the sample is incomplete
    if len(sample_indices) < 2:
        sample_indices = list(range(min(5, len(df))))

    return sample_indices


def print_tfidf_sample(df, tfidf_matrix, feature_names):
    """
    Prints the Table 4.6 sample TF-IDF matrix (target keywords only).

    Args:
        df (pandas.DataFrame): Cleaned dataset.
        tfidf_matrix (scipy.sparse matrix): TF-IDF matrix.
        feature_names (numpy.ndarray): TF-IDF vocabulary.

    Returns:
        list: Row indices of the sample places used (reused by the
        similarity table).
    """
    print("\nTable 4.6 Sample TF-IDF Implementation Matrix:")

    sample_indices = select_sample_indices(df)

    valid_keywords = [k for k in DISPLAY_KEYWORDS if k in feature_names]

    # Build the TF-IDF Display DataFrame
    tfidf_display = pd.DataFrame(
        tfidf_matrix[sample_indices].toarray(), index=df.iloc[sample_indices]["place_name"], columns=feature_names
    )

    # Show only the target keyword columns
    print(tfidf_display[valid_keywords].round(3))
    print("-" * 50)

    return sample_indices


def print_similarity_sample(df, cosine_sim, sample_indices):
    """
    Prints the Table 4.7 sample cosine similarity matrix.

    Args:
        df (pandas.DataFrame): Cleaned dataset.
        cosine_sim (numpy.ndarray): Cosine similarity matrix.
        sample_indices (list): Row indices of the sample places.
    """
    print("\nTable 4.7 Sample Cosine Similarity Matrix:")

    sim_display = pd.DataFrame(
        cosine_sim[np.ix_(sample_indices, sample_indices)],
        index=df.iloc[sample_indices]["place_name"],
        columns=df.iloc[sample_indices]["place_name"],
    )

    print(sim_display.round(3))
    print("-" * 50)


def run_demo(df, cosine_sim, indices, sample_indices):
    """
    Prints the recommendation system demo using the first sample place.

    Args:
        df (pandas.DataFrame): Cleaned dataset.
        cosine_sim (numpy.ndarray): Cosine similarity matrix.
        indices (pandas.Series): place_name → row index lookup.
        sample_indices (list): Row indices of the sample places.
    """
    print("\nRECOMMENDATION SYSTEM DEMO:")

    try:
        # Pick one place from the sample as the query
        query_place = df.iloc[sample_indices[0]]["place_name"]

        print(f"If a user views: '{query_place}', the system recommends:")
        recs = get_recommendations(query_place, cosine_sim, df, indices)

        if recs is not None:
            # Format output as a neat table
            print(recs.to_string(index=False))
        else:
            print("Place not found.")
    except Exception:
        print("Demo failed (data may be empty).")


# ===========================
# Orchestration
# ===========================
def main():
    """
    Main processing function that orchestrates the whole recommendation flow:
    dataset loading, name cleaning, TF-IDF build, similarity build, sample
    tables, and the recommendation demo.
    """
    df = load_dataset()
    if df is None:
        return

    df = clean_place_names(df)

    print(f"Data loaded: {len(df)} tourist destinations.")
    print("-" * 50)

    tfidf_matrix, feature_names = build_tfidf(df)
    sample_indices = print_tfidf_sample(df, tfidf_matrix, feature_names)

    cosine_sim = build_similarity(tfidf_matrix)
    print_similarity_sample(df, cosine_sim, sample_indices)

    indices = build_indices(df)
    run_demo(df, cosine_sim, indices, sample_indices)


if __name__ == "__main__":
    main()
