"""
Baseline comparison against the real datasets in data/.

docs/baseline.md section 4 freezes the row counts of every dataset; section 2.1
freezes the column orders. This module re-checks both whenever the real data
files are present, so refactoring drift is caught immediately. Tests skip
automatically when data/ is absent (it is gitignored, so fresh clones have no
datasets).
"""

import glob
import json
import os
import sys
import unittest

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
del _HERE, _path

from config import (
    CONTENT_BASED_FILE,
    PLACE_IMAGES_FILE,
    PLACES_LIST_FILE,
    REVIEWS_JSON_DIR,
    REVIEWS_JSON_V1_DIR,
    TOURISM_FINAL_FILE,
    TOURISM_FINAL_WITH_IMAGES_FILE,
)

# docs/baseline.md section 2.1 — exact frozen column orders
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
FINAL_WITH_IMAGES_COLUMNS = [
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
CONTENT_BASED_COLUMNS = [
    "place_name",
    "place_category",
    "place_address",
    "place_avg_rating",
    "total_reviews_scraped",
    "tags_corpus",
]
PLACES_LIST_COLUMNS = ["place_name", "gmaps_url"]
PLACE_IMAGES_COLUMNS = ["place_name", "image_url"]

# docs/baseline.md section 4 — snapshot row counts
BASELINE_ROWS = {
    PLACES_LIST_FILE: 57,
    PLACE_IMAGES_FILE: 57,
    CONTENT_BASED_FILE: 55,
    TOURISM_FINAL_FILE: 5202,
    TOURISM_FINAL_WITH_IMAGES_FILE: 5202,
}

HAS_DATA = all(os.path.isfile(p) for p in BASELINE_ROWS)


@unittest.skipUnless(HAS_DATA, "real data/ datasets not present (gitignored)")
class BaselineRowCountTest(unittest.TestCase):
    def test_active_dataset_row_counts(self):
        for path, expected in BASELINE_ROWS.items():
            with self.subTest(file=os.path.basename(path)):
                self.assertEqual(len(pd.read_csv(path)), expected)

    def test_review_json_counts(self):
        root_files = glob.glob(os.path.join(REVIEWS_JSON_DIR, "*.json"))
        v1_files = glob.glob(os.path.join(REVIEWS_JSON_V1_DIR, "*.json"))
        root_reviews = sum(len(json.load(open(f))["reviews"]) for f in root_files)
        v1_reviews = sum(len(json.load(open(f))["reviews"]) for f in v1_files)
        self.assertEqual(len(root_files), 2)
        self.assertEqual(root_reviews, 0)
        self.assertEqual(len(v1_files), 55)
        self.assertEqual(v1_reviews, 10268)


@unittest.skipUnless(HAS_DATA, "real data/ datasets not present (gitignored)")
class BaselineColumnOrderTest(unittest.TestCase):
    def test_places_list_columns(self):
        self.assertEqual(list(pd.read_csv(PLACES_LIST_FILE).columns), PLACES_LIST_COLUMNS)

    def test_place_images_columns(self):
        self.assertEqual(list(pd.read_csv(PLACE_IMAGES_FILE).columns), PLACE_IMAGES_COLUMNS)

    def test_content_based_columns(self):
        self.assertEqual(list(pd.read_csv(CONTENT_BASED_FILE).columns), CONTENT_BASED_COLUMNS)

    def test_tourism_final_columns(self):
        self.assertEqual(list(pd.read_csv(TOURISM_FINAL_FILE).columns), FINAL_COLUMNS)

    def test_tourism_final_with_images_columns(self):
        self.assertEqual(
            list(pd.read_csv(TOURISM_FINAL_WITH_IMAGES_FILE).columns),
            FINAL_WITH_IMAGES_COLUMNS,
        )


if __name__ == "__main__":
    unittest.main()
