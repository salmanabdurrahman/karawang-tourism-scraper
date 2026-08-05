"""
Smoke tests for all seven entry points without network access.

Every entry point is importable and its orchestration function degrades
gracefully (missing inputs -> clean early return, no browser launched, no
crash). Real scraping flows are covered by the extraction unit tests and the
fixture-based contract tests instead.
"""

import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
del _HERE, _path

import gmaps_image_scraper
import gmaps_reviews_scraper
import gmaps_scraper
import merge_images_to_final
import prepare_content_based
import process_gmaps_data
import recommender_engine

MISSING = "/definitely/not/here"


class EntryPointSmokeTest(unittest.TestCase):
    def test_all_modules_importable(self):
        # importing must not trigger file reads, model builds, or downloads
        for module in (
            gmaps_scraper,
            gmaps_reviews_scraper,
            gmaps_image_scraper,
            process_gmaps_data,
            merge_images_to_final,
            prepare_content_based,
            recommender_engine,
        ):
            self.assertIsNotNone(module)

    def test_scrape_all_reviews_missing_input(self):
        with mock.patch.object(gmaps_reviews_scraper, "INPUT_FILE", MISSING), \
             redirect_stdout(io.StringIO()) as buf:
            result = gmaps_reviews_scraper.scrape_all_reviews()
        self.assertIsNone(result)
        self.assertIn("not found", buf.getvalue().lower())

    def test_scrape_images_only_missing_input(self):
        with mock.patch.object(gmaps_image_scraper, "INPUT_FILE", MISSING), \
             redirect_stdout(io.StringIO()) as buf:
            result = gmaps_image_scraper.scrape_images_only()
        self.assertIsNone(result)
        self.assertIn("not found", buf.getvalue().lower())

    def test_process_all_files_missing_input_dir(self):
        with mock.patch.object(process_gmaps_data, "INPUT_DIR", MISSING), \
             redirect_stdout(io.StringIO()):
            process_gmaps_data.process_all_files()  # must not raise

    def test_merge_data_missing_input(self):
        with mock.patch.object(merge_images_to_final, "MAIN_DATASET_FILE", MISSING), \
             redirect_stdout(io.StringIO()):
            merge_images_to_final.merge_data()  # must not raise

    def test_process_data_missing_input_dir(self):
        with mock.patch.object(prepare_content_based, "INPUT_DIR", MISSING), \
             redirect_stdout(io.StringIO()):
            prepare_content_based.process_data()  # must not raise

    def test_recommender_main_missing_dataset(self):
        with mock.patch.object(recommender_engine, "load_dataset", return_value=None), \
             redirect_stdout(io.StringIO()):
            recommender_engine.main()  # must not raise

    def test_gmaps_scraper_no_network_smoke(self):
        # entry point needs a live browser; smoke = constants wired correctly
        self.assertTrue(gmaps_scraper.OUTPUT_FILE.endswith("karawang_places_list.csv"))


if __name__ == "__main__":
    unittest.main()
