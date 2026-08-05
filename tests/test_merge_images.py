"""
Unit tests for the image merge logic in src/merge_images_to_final.py:
normalized-name joining, placeholder filling, and duplicate-key handling.
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

import merge_images_to_final
from config import PLACEHOLDER_IMAGE_URL


class MergeImagesTest(unittest.TestCase):
    def _main_df(self):
        return pd.DataFrame(
            [
                {
                    "user_id": "a" * 10,
                    "user_rating": 5,
                    "review_text": "Bagus",
                    "review_time": "2025-01-01",
                    "place_name": "Az-Zahra Galuh Mas Pelatihan Manasik Haji",
                    "place_description": "",
                    "place_category": "Taman",
                    "place_attributes": "",
                    "place_address": "Karawang",
                    "place_total_reviews_gmaps": 10,
                    "place_avg_rating": 4.5,
                },
                {
                    "user_id": "b" * 10,
                    "user_rating": 4,
                    "review_text": "Cantik",
                    "review_time": "2025-01-02",
                    "place_name": "Curug Cigentis",
                    "place_description": "",
                    "place_category": "Taman",
                    "place_attributes": "",
                    "place_address": "Karawang",
                    "place_total_reviews_gmaps": 20,
                    "place_avg_rating": 4.0,
                },
                {
                    "user_id": "c" * 10,
                    "user_rating": 3,
                    "review_text": "Hijau",
                    "review_time": "2025-01-03",
                    "place_name": "Taman Wirasena Walahar",
                    "place_description": "",
                    "place_category": "Taman",
                    "place_attributes": "",
                    "place_address": "Karawang",
                    "place_total_reviews_gmaps": 5,
                    "place_avg_rating": 3.8,
                },
            ]
        )

    def _images_df(self):
        return pd.DataFrame(
            [
                {"place_name": "Az-Zahra Galuh Mas (Pelatihan Manasik Haji)", "image_url": "https://img/az-zahra.jpg"},
                {"place_name": "Curug Cigentis", "image_url": "https://img/curug.jpg"},
                {"place_name": "Curug Cigentis", "image_url": "https://img/dupe.jpg"},
            ]
        )

    def test_normalized_join_matches_parenthesized_variant(self):
        merged = merge_images_to_final.merge_images(self._main_df(), self._images_df())
        by_name = merged.set_index("place_name")["image_url"].to_dict()
        self.assertEqual(by_name["Az-Zahra Galuh Mas Pelatihan Manasik Haji"], "https://img/az-zahra.jpg")

    def test_missing_image_gets_placeholder(self):
        merged = merge_images_to_final.merge_images(self._main_df(), self._images_df())
        by_name = merged.set_index("place_name")["image_url"].to_dict()
        self.assertEqual(by_name["Taman Wirasena Walahar"], PLACEHOLDER_IMAGE_URL)

    def test_duplicate_image_keys_keep_first(self):
        merged = merge_images_to_final.merge_images(self._main_df(), self._images_df())
        by_name = merged.set_index("place_name")["image_url"].to_dict()
        self.assertEqual(by_name["Curug Cigentis"], "https://img/curug.jpg")

    def test_original_columns_preserved_and_join_key_removed(self):
        merged = merge_images_to_final.merge_images(self._main_df(), self._images_df())
        self.assertNotIn("join_key", merged.columns)
        self.assertIn("user_id", merged.columns)
        self.assertEqual(len(merged), 3)

    def test_write_output_column_order_and_bom(self):
        merged = merge_images_to_final.merge_images(self._main_df(), self._images_df())
        with tempfile.TemporaryDirectory() as tmp:
            out_file = os.path.join(tmp, "out.csv")
            with mock.patch.object(merge_images_to_final, "OUTPUT_FILE", out_file), mock.patch.object(
                merge_images_to_final, "PROCESSED_DIR", tmp
            ), redirect_stdout(io.StringIO()):
                merge_images_to_final.write_output(merged)
            with open(out_file, "r", encoding="utf-8") as f:
                self.assertEqual(f.read(1), "\ufeff")
            df = pd.read_csv(out_file, encoding="utf-8-sig")
            self.assertEqual(list(df.columns), merge_images_to_final.FINAL_COLUMNS)


if __name__ == "__main__":
    unittest.main()
