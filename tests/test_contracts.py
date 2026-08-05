"""
Contract tests: frozen schemas, column order, encodings, and pipeline behavior.

Runs the non-network entry points end-to-end on fixture data inside temporary
directories (module globals are patched so nothing touches the real data/).
Expected schemas/orders/encodings come from docs/baseline.md and are hardcoded
here so that any drift in the source constants is caught too.
"""

import io
import os
import shutil
import sys
import tempfile
import unittest
from collections import Counter
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
import prepare_content_based
import process_gmaps_data

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
REVIEWS_FIXTURES_DIR = os.path.join(FIXTURES_DIR, "reviews")

# docs/baseline.md section 2.1: exact column orders
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

PLACEHOLDER = "https://via.placeholder.com/400x300?text=No+Image"


def read_first_char(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read(1)


class FinalDatasetContractTest(unittest.TestCase):
    """Entry point 3: process_gmaps_data.process_all_files()."""

    def _run_pipeline(self, tmp):
        src_dir = os.path.join(tmp, "in")
        out_dir = os.path.join(tmp, "out")
        os.makedirs(src_dir)
        for name in ("curug_cigentis.json", "pantai_samudera_baru.json"):
            shutil.copy(os.path.join(REVIEWS_FIXTURES_DIR, name), os.path.join(src_dir, name))
        out_file = os.path.join(out_dir, "karawang_tourism_final.csv")

        with mock.patch.object(process_gmaps_data, "INPUT_DIR", src_dir), mock.patch.object(
            process_gmaps_data, "OUTPUT_DIR", out_dir
        ), mock.patch.object(process_gmaps_data, "OUTPUT_FILE", out_file), redirect_stdout(io.StringIO()):
            process_gmaps_data.process_all_files()

        return out_file

    def test_schema_column_order_and_encoding(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = self._run_pipeline(tmp)
            self.assertTrue(os.path.isfile(out_file))
            # utf-8-sig BOM
            self.assertEqual(read_first_char(out_file), "\ufeff")

            df = pd.read_csv(out_file, encoding="utf-8-sig")
            self.assertEqual(list(df.columns), FINAL_COLUMNS)
            self.assertEqual(list(df.columns), process_gmaps_data.FINAL_COLUMNS)

    def test_processing_rules(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = self._run_pipeline(tmp)
            df = pd.read_csv(out_file, encoding="utf-8-sig")

            # 7 reviews survive dedup/filtering in place 1 + 3 in place 2
            self.assertEqual(len(df), 10)

            # empty-text review filtered, exact duplicate removed
            self.assertNotIn("", df["review_text"].tolist())
            self.assertEqual(len(df[df["place_name"] == "Curug Cigentis"]), 7)

            # anonymized user_id format (md5 hex[:10]) or "anonymous"
            self.assertTrue(df["user_id"].str.fullmatch(r"[0-9a-f]{10}|anonymous").all())
            self.assertIn("anonymous", df["user_id"].tolist())

            # duplicate (user_id, review_text) pairs must not exist
            self.assertEqual(len(df[["user_id", "review_text"]].drop_duplicates()), len(df))

            # review_time converted to ISO date
            self.assertTrue(df["review_time"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all())

            # place metadata parsed from place_info
            curug = df[df["place_name"] == "Curug Cigentis"]
            self.assertEqual(curug.iloc[0]["place_avg_rating"], 4.5)
            self.assertEqual(curug.iloc[0]["place_total_reviews_gmaps"], 1234)
            self.assertEqual(
                curug.iloc[0]["place_attributes"],
                "Parkir: Ada, Toilet: Ada, Tiket: Murah",
            )

            # all rating buckets present for place 1 (1..5 plus unrated 0)
            self.assertEqual(set(curug["user_rating"]), {0, 1, 2, 3, 4, 5})

    def test_sampling_cap_and_stratification(self):
        # 200 unique reviews (40 per star) -> capped at 150, 30 per star
        reviews = []
        for star in range(1, 6):
            for i in range(40):
                reviews.append(
                    {
                        "user_name": f"User {star}-{i}",
                        "rating": star,
                        "text": f"Review teks unik {star}-{i}",
                        "time": "1 hari yang lalu",
                    }
                )
        data = {
            "place_info": {
                "name": "Tempat Padat",
                "category": "Taman",
                "avg_rating": "4.0",
                "total_reviews_text": "500 ulasan",
                "address": "",
                "description": "",
                "attributes": "",
            },
            "reviews": reviews,
        }
        records = process_gmaps_data.transform_place_file(data, "tempat_padat.json")
        self.assertEqual(len(records), 150)
        self.assertEqual(Counter(r["user_rating"] for r in records), {1: 30, 2: 30, 3: 30, 4: 30, 5: 30})


class MergeImagesContractTest(unittest.TestCase):
    """Entry point 5: merge_images_to_final.merge_data()."""

    def _run_merge(self, tmp):
        main_file = os.path.join(tmp, "final.csv")
        images_file = os.path.join(tmp, "images.csv")
        out_file = os.path.join(tmp, "final_with_images.csv")

        main_df = pd.DataFrame(
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
                {
                    "user_id": "d" * 10,
                    "user_rating": 5,
                    "review_text": "Pasir",
                    "review_time": "2025-01-04",
                    "place_name": "Pantai Samudera Baru",
                    "place_description": "",
                    "place_category": "Pantai",
                    "place_attributes": "",
                    "place_address": "Karawang",
                    "place_total_reviews_gmaps": 8,
                    "place_avg_rating": 4.2,
                },
            ]
        )
        main_df.to_csv(main_file, index=False, encoding="utf-8-sig")

        images_df = pd.DataFrame(
            [
                {"place_name": "Az-Zahra Galuh Mas (Pelatihan Manasik Haji)", "image_url": "https://img/az-zahra.jpg"},
                {"place_name": "Curug Cigentis", "image_url": "https://img/curug.jpg"},
                {"place_name": "Curug Cigentis", "image_url": "https://img/curug-dupe.jpg"},
            ]
        )
        images_df.to_csv(images_file, index=False)

        with mock.patch.object(merge_images_to_final, "MAIN_DATASET_FILE", main_file), mock.patch.object(
            merge_images_to_final, "IMAGES_FILE", images_file
        ), mock.patch.object(merge_images_to_final, "OUTPUT_FILE", out_file), mock.patch.object(
            merge_images_to_final, "PROCESSED_DIR", tmp
        ), redirect_stdout(io.StringIO()):
            merge_images_to_final.merge_data()

        return out_file

    def test_schema_column_order_and_encoding(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = self._run_merge(tmp)
            self.assertTrue(os.path.isfile(out_file))
            self.assertEqual(read_first_char(out_file), "\ufeff")

            df = pd.read_csv(out_file, encoding="utf-8-sig")
            self.assertEqual(list(df.columns), FINAL_WITH_IMAGES_COLUMNS)
            self.assertEqual(list(df.columns), merge_images_to_final.FINAL_COLUMNS)

    def test_join_placeholder_and_dedup(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = self._run_merge(tmp)
            df = pd.read_csv(out_file, encoding="utf-8-sig")
            self.assertEqual(len(df), 4)

            by_name = df.set_index("place_name")["image_url"].to_dict()
            # normalized join matches the parenthesized variant
            self.assertEqual(by_name["Az-Zahra Galuh Mas Pelatihan Manasik Haji"], "https://img/az-zahra.jpg")
            # first image wins on duplicate keys
            self.assertEqual(by_name["Curug Cigentis"], "https://img/curug.jpg")
            # missing image gets the frozen placeholder
            self.assertEqual(by_name["Taman Wirasena Walahar"], PLACEHOLDER)
            self.assertEqual(by_name["Pantai Samudera Baru"], PLACEHOLDER)


class ContentBasedContractTest(unittest.TestCase):
    """Entry point 6: prepare_content_based.process_data()."""

    def _run_pipeline(self, tmp):
        src_dir = os.path.join(tmp, "in")
        out_dir = os.path.join(tmp, "out")
        os.makedirs(src_dir)
        for name in ("curug_cigentis.json", "pantai_samudera_baru.json"):
            shutil.copy(os.path.join(REVIEWS_FIXTURES_DIR, name), os.path.join(src_dir, name))
        out_file = os.path.join(out_dir, "karawang_places_content_based.csv")

        with mock.patch.object(prepare_content_based, "INPUT_DIR", src_dir), mock.patch.object(
            prepare_content_based, "OUTPUT_DIR", out_dir
        ), mock.patch.object(prepare_content_based, "OUTPUT_FILE", out_file), redirect_stdout(io.StringIO()):
            prepare_content_based.process_data()

        return out_file

    def test_schema_column_order_and_encoding(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = self._run_pipeline(tmp)
            self.assertTrue(os.path.isfile(out_file))
            # plain utf-8, no BOM
            self.assertNotEqual(read_first_char(out_file), "\ufeff")

            df = pd.read_csv(out_file)
            self.assertEqual(list(df.columns), CONTENT_BASED_COLUMNS)
            self.assertEqual(list(df.columns), prepare_content_based.CONTENT_BASED_COLUMNS)

    def test_records_and_corpus(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = self._run_pipeline(tmp)
            df = pd.read_csv(out_file)
            self.assertEqual(len(df), 2)

            curug = df[df["place_name"] == "Curug Cigentis"].iloc[0]
            # raw review count includes empty/duplicate entries (pre-filter)
            self.assertEqual(curug["total_reviews_scraped"], 10)
            self.assertEqual(curug["place_avg_rating"], 4.5)
            self.assertTrue(isinstance(curug["tags_corpus"], str) and curug["tags_corpus"])


class RawAndImagesSchemaTest(unittest.TestCase):
    """Schema of the two scraper-produced CSVs, verified from fixtures."""

    def test_raw_places_fixture_columns(self):
        df = pd.read_csv(os.path.join(FIXTURES_DIR, "karawang_places_list.csv"))
        self.assertEqual(list(df.columns), PLACES_LIST_COLUMNS)
        self.assertEqual(len(df), 4)

    def test_place_images_fixture_columns(self):
        # matches the schema gmaps_image_scraper writes (place_name, image_url)
        df = pd.DataFrame([{"place_name": "Curug Cigentis", "image_url": "https://img/curug.jpg"}])
        self.assertEqual(list(df.columns), PLACE_IMAGES_COLUMNS)


if __name__ == "__main__":
    unittest.main()
