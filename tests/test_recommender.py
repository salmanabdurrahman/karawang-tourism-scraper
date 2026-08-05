"""
Tests for the recommendation engine (src/recommender_engine.py).

Builds the real TF-IDF/similarity model on the small content-based fixture,
then verifies the frozen get_recommendations() result format and the empty/
missing input guards. No network, no real datasets.
"""

import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
del _HERE, _path

import recommender_engine

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
FIXTURE_FILE = os.path.join(FIXTURES_DIR, "content_based.csv")

RESULT_COLUMNS = ["place_name", "place_category", "place_avg_rating", "similarity_score"]


def build_model(df=None):
    if df is None:
        df = recommender_engine.load_dataset(FIXTURE_FILE)
    df = recommender_engine.clean_place_names(df)
    tfidf_matrix, _ = recommender_engine.build_tfidf(df)
    cosine_sim = recommender_engine.build_similarity(tfidf_matrix)
    indices = recommender_engine.build_indices(df)
    return df, cosine_sim, indices


class GetRecommendationsTest(unittest.TestCase):
    def test_result_format_and_sorting(self):
        df, cosine_sim, indices = build_model()
        recs = recommender_engine.get_recommendations("Curug Cigentis", cosine_sim, df, indices)

        self.assertIsNotNone(recs)
        self.assertEqual(list(recs.columns), RESULT_COLUMNS)
        # self is excluded; all other 5 places returned
        self.assertEqual(len(recs), len(df) - 1)
        self.assertNotIn("Curug Cigentis", recs["place_name"].tolist())
        # sorted by similarity descending
        self.assertTrue(recs["similarity_score"].is_monotonic_decreasing)
        # most similar place shares the most corpus terms
        self.assertEqual(recs.iloc[0]["place_name"], "Green Canyon")

    def test_top_n_cap(self):
        # max_df=0.85 prunes terms in >12 of 15 docs, so shared terms must
        # appear in at most 12 docs: each doc shares "umum{j}" with one other
        # doc (i and i+12) plus a unique "khusus{i}" token.
        rows = [
            {
                "place_name": f"Tempat {i}",
                "place_category": "Taman",
                "place_address": "Karawang",
                "place_avg_rating": 4.0,
                "total_reviews_scraped": 5,
                "tags_corpus": f"umum{i % 12} khusus{i}",
            }
            for i in range(15)
        ]
        df = pd.DataFrame(rows)
        df, cosine_sim, indices = build_model(df)
        recs = recommender_engine.get_recommendations("Tempat 0", cosine_sim, df, indices)
        self.assertEqual(len(recs), 10)  # TOP_N = 10

    def test_not_found_returns_none(self):
        df, cosine_sim, indices = build_model()
        self.assertIsNone(
            recommender_engine.get_recommendations("Tidak Ada Tempat Ini", cosine_sim, df, indices)
        )

    def test_partial_match_search_fallback(self):
        df, cosine_sim, indices = build_model()
        recs = recommender_engine.get_recommendations("cigentis", cosine_sim, df, indices)
        self.assertIsNotNone(recs)
        self.assertEqual(len(recs), len(df) - 1)


class ModelHelpersTest(unittest.TestCase):
    def test_build_indices_keeps_duplicate_names(self):
        # Known behavior: drop_duplicates() dedups the Series values (row
        # indices), not the index labels, so duplicate place names are kept.
        # Locking the actual behavior here; changing it is out of scope.
        df = pd.DataFrame({"place_name": ["A", "A", "B"], "tags_corpus": ["x", "y", "z"]})
        indices = recommender_engine.build_indices(df)
        self.assertEqual(list(indices.index), ["A", "A", "B"])
        self.assertEqual(indices["B"], 2)

    def test_clean_place_names_fills_null_corpus(self):
        df = pd.DataFrame({"place_name": ["Wisata Curug Cigentis Karawang"], "tags_corpus": [None]})
        df = recommender_engine.clean_place_names(df)
        self.assertEqual(df.iloc[0]["place_name"], "Curug Cigentis")
        self.assertEqual(df.iloc[0]["tags_corpus"], "")

    def test_load_dataset_missing_file_returns_none(self):
        self.assertIsNone(recommender_engine.load_dataset("/no/such/file.csv"))

    def test_load_dataset_empty_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            empty_file = os.path.join(tmp, "empty.csv")
            pd.DataFrame(columns=RESULT_COLUMNS).to_csv(empty_file, index=False)
            self.assertIsNone(recommender_engine.load_dataset(empty_file))


class MainSmokeTest(unittest.TestCase):
    def test_main_runs_demo_on_fixture(self):
        def fake_load():
            return pd.read_csv(FIXTURE_FILE)

        with mock.patch.object(recommender_engine, "load_dataset", fake_load), \
             redirect_stdout(io.StringIO()) as buf:
            recommender_engine.main()

        output = buf.getvalue()
        self.assertIn("RECOMMENDATION SYSTEM DEMO", output)
        self.assertIn("Table 4.6", output)
        self.assertIn("Table 4.7", output)

    def test_main_returns_early_when_dataset_unavailable(self):
        with mock.patch.object(recommender_engine, "load_dataset", return_value=None), \
             redirect_stdout(io.StringIO()):
            recommender_engine.main()  # must not raise


if __name__ == "__main__":
    unittest.main()
