"""
Content-Based Recommender Engine (TF-IDF + Cosine Similarity)

This script builds a content-based recommendation system from the prepared
tags corpus: TF-IDF vectorization with tuned parameters, cosine similarity,
and two query modes. A place query returns similar destinations; a free-text
keyword query is transformed with the fitted vectorizer and ranked against all
places. It also prints sample TF-IDF and similarity matrices for documentation.

The whole workflow (load → clean → TF-IDF → similarity → demo) runs from
`main()`, so importing this module has no side effects (no file reads, no
model computation). Call the entry point directly:

    python src/recommender_engine.py

Features:
    - Cleans and normalizes place display names
    - Builds the paper-aligned TF-IDF model (1-2 ngrams, linear TF)
    - Computes the cosine similarity matrix for similar-place queries
    - Supports free-text keyword-to-place recommendations
    - Recommends the top 10 places by default

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
from utils import clean_display_name, preprocess_text

# ===========================
# CONFIG (paths & values from config.py)
# ===========================
INPUT_FILE = CONTENT_BASED_FILE

# Paper-aligned TF-IDF parameters (docs/baseline.md)
TFIDF_PARAMS = {
    "analyzer": "word",
    "ngram_range": (1, 2),
    "min_df": 2,
    "max_df": 0.85,
    "max_features": 10000,
    "sublinear_tf": False,
}

# Recommendation count: top 10 similar places (docs/baseline.md)
TOP_N = 10

# Sample places & keywords for the Table 4.6 / 4.7 console tables
SAMPLE_PLACES = ["Curug Cigentis", "Hutan Kertas", "Pantai Samudera Baru", "Goa Dayeuh", "Green Canyon"]
PAPER_SAMPLE_QUERIES = ["kolam renang", "curug", "taman"]

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

    required_columns = {"place_name", "place_category", "place_avg_rating", "tags_corpus"}
    missing_columns = sorted(required_columns.difference(df.columns))
    if missing_columns:
        print(f"Dataset is missing required columns: {', '.join(missing_columns)}")
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
def fit_tfidf(df):
    """
    Fits the tuned TF-IDF model and transforms all place corpora.

    The fitted vectorizer is returned so the same vocabulary, preprocessing
    rules, and IDF weights can transform a free-text user query.

    Args:
        df (pandas.DataFrame): Cleaned dataset.

    Returns:
        tuple: (vectorizer, tfidf_matrix, feature_names).

    Raises:
        ValueError: If every place corpus is empty after cleaning.
    """
    print("\nComputing TF-IDF with Optimized Parameters...")

    corpora = df["tags_corpus"].fillna("").astype(str)
    if not corpora.str.strip().any():
        raise ValueError("Cannot build TF-IDF: every place corpus is empty.")

    vectorizer = TfidfVectorizer(**TFIDF_PARAMS)
    tfidf_matrix = vectorizer.fit_transform(corpora)
    feature_names = vectorizer.get_feature_names_out()

    print(f"TF-IDF matrix built. Shape: {tfidf_matrix.shape}")
    print(f"   Vocabulary features retained: {tfidf_matrix.shape[1]}")

    return vectorizer, tfidf_matrix, feature_names


def build_tfidf(df):
    """
    Builds the tuned TF-IDF matrix from the tags corpus.

    This compatibility wrapper keeps the existing return shape for callers
    that only need the matrix and feature names. Use ``fit_tfidf`` when query
    transformation is required.

    Args:
        df (pandas.DataFrame): Cleaned dataset.

    Returns:
        tuple: (tfidf_matrix, feature_names).
    """
    _, tfidf_matrix, feature_names = fit_tfidf(df)
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
    row_positions = pd.Series(np.arange(len(df)), index=df["place_name"])
    return row_positions[~row_positions.index.duplicated(keep="first")]


def _select_unique_scored_rows(scored_rows, df, top_n, excluded_names=None):
    """Selects highest-scoring rows without duplicate display names."""
    selected = []
    seen_names = set(excluded_names or ())

    for position, score in scored_rows:
        name = str(df.iloc[position]["place_name"])
        if not name or name in seen_names:
            continue

        seen_names.add(name)
        selected.append((position, float(score)))
        if len(selected) >= top_n:
            break

    return selected


# ===========================
# Recommendation Query
# ===========================
def get_recommendations(title, cosine_sim, df, indices):
    """
    Returns the top 10 similar places for a given place.

    This is the item-to-item mode used by a destination detail page. For a
    free-text search, use ``get_keyword_recommendations`` instead.

    Args:
        title (str): Place name.
        cosine_sim (numpy.ndarray): Cosine similarity matrix.
        df (pandas.DataFrame): Cleaned dataset.
        indices (pandas.Series): place_name → positional row index lookup.

    Returns:
        pandas.DataFrame: Top 10 similar places with columns place_name,
        place_category, place_avg_rating, similarity_score, sorted by
        similarity descending, or None if the place is not found.
    """
    title_clean = clean_display_name(title)
    if not title_clean:
        return None

    if title_clean not in indices:
        mask = df["place_name"].str.contains(title_clean, case=False, na=False, regex=False)
        if mask.any():
            title_clean = df[mask].iloc[0]["place_name"]
            print(f"   (Using search result: {title_clean})")
        else:
            return None

    idx = int(indices[title_clean])
    similarity_row = np.asarray(cosine_sim[idx])
    if np.any(similarity_row > 0):
        sim_scores = [(position, float(score)) for position, score in enumerate(similarity_row) if position != idx]
    else:
        # Empty place vectors should not return arbitrary zero-score neighbors.
        sim_scores = []

    sim_scores.sort(key=lambda item: item[1], reverse=True)
    sim_scores = _select_unique_scored_rows(sim_scores, df, TOP_N, excluded_names={title_clean})

    place_indices = [position for position, _ in sim_scores]
    place_scores = [score for _, score in sim_scores]

    result = df.iloc[place_indices][["place_name", "place_category", "place_avg_rating"]].copy()
    result["similarity_score"] = place_scores
    return result


def get_keyword_recommendations(query, vectorizer, tfidf_matrix, df, top_n=TOP_N):
    """
    Ranks destinations against a free-text user query.

    The query goes through the same Indonesian NLP pipeline as the prepared
    corpus, then uses the fitted TF-IDF vocabulary and IDF weights. Zero-score
    destinations are omitted so results represent actual keyword overlap.

    Args:
        query (str): User keyword or preference text.
        vectorizer (TfidfVectorizer): Fitted corpus vectorizer.
        tfidf_matrix (scipy.sparse matrix): Place TF-IDF matrix.
        df (pandas.DataFrame): Cleaned dataset.
        top_n (int): Maximum number of results.

    Returns:
        pandas.DataFrame: Ranked matching destinations with the standard result
        columns, or None when query has no usable vocabulary/matches.
    """
    if not isinstance(query, str) or not query.strip() or not isinstance(top_n, (int, np.integer)) or top_n <= 0:
        return None

    try:
        normalized_query = preprocess_text(query)
    except (ImportError, LookupError) as exc:
        raise RuntimeError(
            "NLTK tokenizer is unavailable. Set NLTK_DISABLE_IMPORT_SECURITY=1 and run prepare_content_based.py first."
        ) from exc

    if not normalized_query:
        return None

    query_vector = vectorizer.transform([normalized_query])
    if query_vector.nnz == 0:
        return None

    scores = linear_kernel(query_vector, tfidf_matrix).ravel()
    candidate_positions = np.flatnonzero(scores > 0)
    if len(candidate_positions) == 0:
        return None

    order = np.argsort(-scores[candidate_positions], kind="stable")
    scored_rows = [
        (int(candidate_positions[position]), scores[int(candidate_positions[position])]) for position in order
    ]
    selected_rows = _select_unique_scored_rows(scored_rows, df, top_n)
    place_indices = [position for position, _ in selected_rows]
    place_scores = [score for _, score in selected_rows]

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
        # Look for exact match or a strong partial match. Store positional
        # indices because TF-IDF rows follow DataFrame row order.
        mask = df["place_name"].str.contains(p, case=False, na=False, regex=False)
        matching_positions = np.flatnonzero(mask.to_numpy())
        if len(matching_positions) > 0:
            sample_indices.append(int(matching_positions[0]))

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
    Prints the place-to-place recommendation demo using the first sample place.

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
        print("Similar-place demo failed (data may be empty).")


def run_keyword_demo(vectorizer, tfidf_matrix, df):
    """
    Prints keyword-query examples used by the reference paper.

    Args:
        vectorizer (TfidfVectorizer): Fitted corpus vectorizer.
        tfidf_matrix (scipy.sparse matrix): Place TF-IDF matrix.
        df (pandas.DataFrame): Cleaned dataset.
    """
    print("\nKEYWORD QUERY DEMO:")
    for query in PAPER_SAMPLE_QUERIES:
        print(f'Query: "{query}"')
        try:
            recs = get_keyword_recommendations(query, vectorizer, tfidf_matrix, df)
        except RuntimeError:
            print("  Query preprocessing resources unavailable.")
            continue

        if recs is None:
            print("  No matching destinations.")
        else:
            print(recs.to_string(index=False))


# ===========================
# Orchestration
# ===========================
def main():
    """
    Main processing function that orchestrates the whole recommendation flow:
    dataset loading, name cleaning, TF-IDF build, similarity build, sample
    tables, similar-place demo, and paper keyword-query demos.
    """
    df = load_dataset()
    if df is None:
        return

    df = clean_place_names(df)

    print(f"Data loaded: {len(df)} tourist destinations.")
    print("-" * 50)

    try:
        vectorizer, tfidf_matrix, feature_names = fit_tfidf(df)
    except ValueError as exc:
        print(f"Unable to build recommendation model: {exc}")
        return

    sample_indices = print_tfidf_sample(df, tfidf_matrix, feature_names)

    cosine_sim = build_similarity(tfidf_matrix)
    print_similarity_sample(df, cosine_sim, sample_indices)

    indices = build_indices(df)
    run_demo(df, cosine_sim, indices, sample_indices)
    run_keyword_demo(vectorizer, tfidf_matrix, df)


if __name__ == "__main__":
    main()
