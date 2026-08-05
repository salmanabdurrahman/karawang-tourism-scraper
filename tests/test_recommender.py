"""
Tests for the recommendation engine (src/recommender_engine.py).

Builds the real TF-IDF/similarity model on the small content-based fixture,
then verifies both place-to-place and keyword-to-place recommendation modes,
plus empty/missing input guards. No network, no real datasets.
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


def build_keyword_model(df):
    df = recommender_engine.clean_place_names(df)
    vectorizer, tfidf_matrix, _ = recommender_engine.fit_tfidf(df)
    return df, vectorizer, tfidf_matrix


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
        self.assertIsNone(recommender_engine.get_recommendations("Tidak Ada Tempat Ini", cosine_sim, df, indices))

    def test_partial_match_search_fallback(self):
        df, cosine_sim, indices = build_model()
        recs = recommender_engine.get_recommendations("cigentis", cosine_sim, df, indices)
        self.assertIsNotNone(recs)
        self.assertEqual(len(recs), len(df) - 1)


class KeywordRecommendationsTest(unittest.TestCase):
    def _keyword_df(self):
        return pd.DataFrame(
            [
                {
                    "place_name": "Kolam A",
                    "place_category": "Taman",
                    "place_avg_rating": 4.5,
                    "tags_corpus": "kolam renang wahana",
                },
                {
                    "place_name": "Kolam B",
                    "place_category": "Taman",
                    "place_avg_rating": 4.0,
                    "tags_corpus": "kolam renang air",
                },
                {
                    "place_name": "Hutan C",
                    "place_category": "Alam",
                    "place_avg_rating": 4.2,
                    "tags_corpus": "hutan pohon sejuk",
                },
            ]
        )

    def test_keyword_query_ranks_matching_corpus(self):
        df, vectorizer, tfidf_matrix = build_keyword_model(self._keyword_df())
        recs = recommender_engine.get_keyword_recommendations("kolam renang", vectorizer, tfidf_matrix, df)

        self.assertIsNotNone(recs)
        self.assertEqual(list(recs.columns), RESULT_COLUMNS)
        self.assertEqual(recs["place_name"].tolist(), ["Kolam A", "Kolam B"])
        self.assertTrue(recs["similarity_score"].is_monotonic_decreasing)

    def test_keyword_query_omits_zero_score_places_and_honors_limit(self):
        df, vectorizer, tfidf_matrix = build_keyword_model(self._keyword_df())
        recs = recommender_engine.get_keyword_recommendations("kolam renang", vectorizer, tfidf_matrix, df, top_n=1)

        self.assertEqual(len(recs), 1)
        self.assertNotIn("Hutan C", recs["place_name"].tolist())

    def test_keyword_query_deduplicates_display_names(self):
        df = self._keyword_df()
        df.loc[1, "place_name"] = "Kolam A"
        df, vectorizer, tfidf_matrix = build_keyword_model(df)

        recs = recommender_engine.get_keyword_recommendations("kolam renang", vectorizer, tfidf_matrix, df)

        self.assertEqual(recs["place_name"].tolist(), ["Kolam A"])

    def test_keyword_query_returns_none_for_out_of_vocabulary_text(self):
        df, vectorizer, tfidf_matrix = build_keyword_model(self._keyword_df())
        self.assertIsNone(
            recommender_engine.get_keyword_recommendations("qwerty tidak ada", vectorizer, tfidf_matrix, df)
        )

    def test_keyword_query_rejects_blank_input(self):
        df, vectorizer, tfidf_matrix = build_keyword_model(self._keyword_df())
        self.assertIsNone(recommender_engine.get_keyword_recommendations("   ", vectorizer, tfidf_matrix, df))
        self.assertIsNone(recommender_engine.get_keyword_recommendations(None, vectorizer, tfidf_matrix, df))
        self.assertIsNone(
            recommender_engine.get_keyword_recommendations("kolam", vectorizer, tfidf_matrix, df, top_n=0)
        )

    def test_keyword_query_uses_shared_normalization(self):
        df = pd.DataFrame(
            [
                {"place_name": "Tempat A", "place_category": "Taman", "place_avg_rating": 4.0, "tags_corpus": "tahu"},
                {"place_name": "Tempat B", "place_category": "Taman", "place_avg_rating": 4.0, "tags_corpus": "tahu"},
                {"place_name": "Tempat C", "place_category": "Alam", "place_avg_rating": 4.0, "tags_corpus": "hutan"},
            ]
        )
        df, vectorizer, tfidf_matrix = build_keyword_model(df)
        recs = recommender_engine.get_keyword_recommendations("tau", vectorizer, tfidf_matrix, df)

        self.assertEqual(recs["place_name"].tolist(), ["Tempat A", "Tempat B"])

    def test_keyword_query_reports_missing_nltk_resources(self):
        df, vectorizer, tfidf_matrix = build_keyword_model(self._keyword_df())
        with mock.patch.object(recommender_engine, "preprocess_text", side_effect=LookupError):
            with self.assertRaisesRegex(RuntimeError, "NLTK tokenizer"):
                recommender_engine.get_keyword_recommendations("kolam", vectorizer, tfidf_matrix, df)


class ModelHelpersTest(unittest.TestCase):
    def test_build_indices_uses_first_duplicate_name(self):
        df = pd.DataFrame({"place_name": ["A", "A", "B"], "tags_corpus": ["x", "y", "z"]})
        indices = recommender_engine.build_indices(df)
        self.assertEqual(list(indices.index), ["A", "B"])
        self.assertEqual(indices["A"], 0)
        self.assertEqual(indices["B"], 2)

    def test_duplicate_name_query_does_not_raise(self):
        df = pd.DataFrame(
            [
                {
                    "place_name": "Pantai Sedari",
                    "place_category": "Pantai",
                    "place_avg_rating": 4.0,
                    "tags_corpus": "pantai pasir",
                },
                {
                    "place_name": "Pantai Sedari",
                    "place_category": "Pantai",
                    "place_avg_rating": 4.0,
                    "tags_corpus": "pantai laut",
                },
                {
                    "place_name": "Hutan Kertas",
                    "place_category": "Hutan",
                    "place_avg_rating": 4.0,
                    "tags_corpus": "hutan pohon",
                },
            ]
        )
        df, cosine_sim, indices = build_model(df)
        recs = recommender_engine.get_recommendations("Pantai Sedari", cosine_sim, df, indices)
        self.assertIsNotNone(recs)
        self.assertEqual(len(recs), 1)
        self.assertNotIn("Pantai Sedari", recs["place_name"].tolist())

    def test_similar_query_omits_zero_score_rows(self):
        df = pd.DataFrame(
            [
                {"place_name": "Alam A", "place_category": "Alam", "place_avg_rating": 4.0, "tags_corpus": "alam"},
                {"place_name": "Kosong", "place_category": "Alam", "place_avg_rating": 4.0, "tags_corpus": ""},
                {"place_name": "Alam B", "place_category": "Alam", "place_avg_rating": 4.0, "tags_corpus": "alam"},
            ]
        )
        df, cosine_sim, indices = build_model(df)

        recs = recommender_engine.get_recommendations("Kosong", cosine_sim, df, indices)

        self.assertIsNotNone(recs)
        self.assertTrue(recs.empty)

    def test_clean_place_names_fills_null_corpus(self):
        df = pd.DataFrame({"place_name": ["Wisata Curug Cigentis Karawang"], "tags_corpus": [None]})
        df = recommender_engine.clean_place_names(df)
        self.assertEqual(df.iloc[0]["place_name"], "Curug Cigentis")
        self.assertEqual(df.iloc[0]["tags_corpus"], "")

    def test_load_dataset_missing_file_returns_none(self):
        self.assertIsNone(recommender_engine.load_dataset("/no/such/file.csv"))

    def test_fit_tfidf_rejects_all_empty_corpus(self):
        df = pd.DataFrame([{"place_name": "Kosong", "place_category": "", "place_avg_rating": 0.0, "tags_corpus": ""}])
        df = recommender_engine.clean_place_names(df)
        with self.assertRaisesRegex(ValueError, "every place corpus is empty"):
            recommender_engine.fit_tfidf(df)

    def test_load_dataset_empty_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            empty_file = os.path.join(tmp, "empty.csv")
            pd.DataFrame(columns=RESULT_COLUMNS).to_csv(empty_file, index=False)
            self.assertIsNone(recommender_engine.load_dataset(empty_file))

    def test_load_dataset_missing_required_column_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            invalid_file = os.path.join(tmp, "invalid.csv")
            pd.DataFrame([{"place_name": "Tempat", "tags_corpus": "alam"}]).to_csv(invalid_file, index=False)
            self.assertIsNone(recommender_engine.load_dataset(invalid_file))


class MainSmokeTest(unittest.TestCase):
    def test_main_runs_demo_on_fixture(self):
        def fake_load():
            return pd.read_csv(FIXTURE_FILE)

        with mock.patch.object(recommender_engine, "load_dataset", fake_load), redirect_stdout(io.StringIO()) as buf:
            recommender_engine.main()

        output = buf.getvalue()
        self.assertIn("RECOMMENDATION SYSTEM DEMO", output)
        self.assertIn("KEYWORD QUERY DEMO", output)
        self.assertIn("Table 4.6", output)
        self.assertIn("Table 4.7", output)

    def test_main_returns_early_when_dataset_unavailable(self):
        with mock.patch.object(recommender_engine, "load_dataset", return_value=None), redirect_stdout(io.StringIO()):
            recommender_engine.main()  # must not raise


if __name__ == "__main__":
    unittest.main()
