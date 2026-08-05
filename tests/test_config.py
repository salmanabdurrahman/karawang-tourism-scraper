"""
Tests for src/config.py path resolution and the frozen constants, plus the
raw-CSV-name compatibility mapping and the slug-generated output name wiring
in gmaps_scraper.py.

These guard the "run from any working directory" and "output names stay the
same" contracts from docs/baseline.md.
"""

import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
del _HERE, _path

import config
import gmaps_scraper
import recommender_engine

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BaseDirTest(unittest.TestCase):
    def test_base_dir_is_repo_root(self):
        self.assertEqual(config.BASE_DIR, REPO_ROOT)

    def test_folders_under_data(self):
        for folder in (config.RAW_DIR, config.REVIEWS_JSON_DIR, config.PROCESSED_DIR):
            self.assertEqual(os.path.dirname(folder), os.path.join(REPO_ROOT, "data"))
        self.assertEqual(config.REVIEWS_JSON_V1_DIR, os.path.join(config.REVIEWS_JSON_DIR, "V1"))


class OutputFileNamesTest(unittest.TestCase):
    def test_frozen_output_names(self):
        self.assertEqual(config.PLACES_LIST_FILE, os.path.join(config.RAW_DIR, "karawang_places_list.csv"))
        self.assertEqual(config.TOURISM_FINAL_FILE, os.path.join(config.PROCESSED_DIR, "karawang_tourism_final.csv"))
        self.assertEqual(config.PLACE_IMAGES_FILE, os.path.join(config.PROCESSED_DIR, "karawang_place_images.csv"))
        self.assertEqual(
            config.TOURISM_FINAL_WITH_IMAGES_FILE,
            os.path.join(config.PROCESSED_DIR, "karawang_tourism_final_with_images.csv"),
        )
        self.assertEqual(
            config.CONTENT_BASED_FILE,
            os.path.join(config.PROCESSED_DIR, "karawang_places_content_based.csv"),
        )
        self.assertEqual(
            config.PAPER_EVALUATION_DIR,
            os.path.join(config.PROCESSED_DIR, "paper_evaluation"),
        )

    def test_scraper_output_resolves_to_canonical_name(self):
        # gmaps_scraper names its output from the query slug; the compatibility
        # mapping must resolve it to the canonical file consumers expect.
        self.assertEqual(gmaps_scraper.OUTPUT_FILE, config.PLACES_LIST_FILE)


class RawCsvCompatibilityTest(unittest.TestCase):
    def test_known_slug_maps_to_canonical_name(self):
        self.assertEqual(
            config.resolve_raw_csv_name("tempat_wisata_di_karawang_places_list.csv"),
            "karawang_places_list.csv",
        )

    def test_unknown_name_passes_through(self):
        self.assertEqual(config.resolve_raw_csv_name("other_query_places_list.csv"), "other_query_places_list.csv")


class FrozenConstantsTest(unittest.TestCase):
    def test_review_limits(self):
        self.assertEqual(config.MAX_REVIEWS_PER_PLACE, 400)
        self.assertEqual(config.MAX_SAMPLE_REVIEWS_PER_PLACE, 150)

    def test_placeholder_image_url(self):
        self.assertEqual(config.PLACEHOLDER_IMAGE_URL, "https://via.placeholder.com/400x300?text=No+Image")

    def test_timeouts(self):
        self.assertEqual(config.PAGE_LOAD_TIMEOUT, 60000)
        self.assertEqual(config.SELECTOR_TIMEOUT, 15000)

    def test_tfidf_params_match_baseline(self):
        self.assertEqual(
            recommender_engine.TFIDF_PARAMS,
            {
                "analyzer": "word",
                "ngram_range": (1, 2),
                "min_df": 2,
                "max_df": 0.85,
                "max_features": 10000,
                "sublinear_tf": False,
            },
        )
        self.assertEqual(recommender_engine.TOP_N, 10)


class RequireHelpersTest(unittest.TestCase):
    def test_require_file(self):
        self.assertTrue(config.require_file(__file__))
        self.assertFalse(config.require_file(os.path.join(REPO_ROOT, "does_not_exist.csv")))

    def test_require_dir(self):
        self.assertTrue(config.require_dir(REPO_ROOT))
        self.assertFalse(config.require_dir(os.path.join(REPO_ROOT, "no_such_dir")))


if __name__ == "__main__":
    unittest.main()
