"""
Unit tests for the pure utility functions in src/utils.py.

These functions have no browser/filesystem/network access, so every test here
is hermetic. Expected values follow the contracts frozen in docs/baseline.md
(anonymization format, relative-time conversion, name normalization, NLP steps).
"""

import hashlib
import os
import sys
import unittest
from datetime import datetime, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
del _HERE, _path

from utils import (
    anonymize_user,
    case_folding,
    clean_attributes,
    clean_display_name,
    clean_name_key,
    clean_text,
    convert_relative_time,
    parse_int_from_text,
    remove_stopwords,
    sanitize_filename,
    stemming,
    tokenizing,
)


class CleanTextTest(unittest.TestCase):
    def test_normalizes_whitespace(self):
        self.assertEqual(clean_text("  Airnya   sejuk \n banget\t"), "Airnya sejuk banget")

    def test_removes_maps_artifacts(self):
        self.assertEqual(clean_text("ÓóäBagus¬†"), "Bagus")

    def test_non_string_returns_empty(self):
        self.assertEqual(clean_text(None), "")
        self.assertEqual(clean_text(123), "")


class CleanAttributesTest(unittest.TestCase):
    def test_pipe_separated_to_comma_list(self):
        self.assertEqual(
            clean_attributes("Parkir: Ada | Toilet: Ada | Tiket: Murah"),
            "Parkir: Ada, Toilet: Ada, Tiket: Murah",
        )

    def test_strips_leading_junk_and_empty_items(self):
        self.assertEqual(clean_attributes("|Parkir: Ada||| Toilet: Ada |"), "Parkir: Ada, Toilet: Ada")

    def test_empty_and_non_string(self):
        self.assertEqual(clean_attributes(""), "")
        self.assertEqual(clean_attributes(None), "")


class AnonymizeUserTest(unittest.TestCase):
    def test_md5_first_10_hex_chars(self):
        user_id = anonymize_user("Budi Santoso")
        self.assertEqual(user_id, hashlib.md5("budi santoso".encode("utf-8")).hexdigest()[:10])
        self.assertRegex(user_id, r"^[0-9a-f]{10}$")

    def test_case_insensitive_and_stripped(self):
        self.assertEqual(anonymize_user("Budi Santoso"), anonymize_user("  budi santoso "))

    def test_empty_returns_anonymous(self):
        self.assertEqual(anonymize_user(""), "anonymous")
        self.assertEqual(anonymize_user(None), "anonymous")


class ConvertRelativeTimeTest(unittest.TestCase):
    def assert_iso_date(self, value):
        self.assertRegex(value, r"^\d{4}-\d{2}-\d{2}$")

    def test_hours_ago(self):
        expected = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d")
        self.assertEqual(convert_relative_time("2 jam yang lalu"), expected)

    def test_days_ago(self):
        expected = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        self.assertEqual(convert_relative_time("3 hari yang lalu"), expected)

    def test_diedit_prefix_ignored(self):
        expected = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        self.assertEqual(convert_relative_time("diedit 5 hari yang lalu"), expected)

    def test_weeks_months_years(self):
        self.assertEqual(
            convert_relative_time("1 minggu yang lalu"),
            (datetime.now() - timedelta(weeks=1)).strftime("%Y-%m-%d"),
        )
        self.assertEqual(
            convert_relative_time("2 bulan yang lalu"),
            (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d"),
        )
        self.assertEqual(
            convert_relative_time("1 tahun yang lalu"),
            (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        )

    def test_recent_times_map_to_today(self):
        self.assertEqual(convert_relative_time("baru saja"), datetime.now().strftime("%Y-%m-%d"))
        self.assertEqual(convert_relative_time("5 menit yang lalu"), datetime.now().strftime("%Y-%m-%d"))

    def test_unparseable_falls_through_to_today(self):
        # Known behavior: text matching no time branch keeps delta=0, so the
        # result is today's date. Locking the actual behavior here.
        self.assertEqual(convert_relative_time("lorem ipsum"), datetime.now().strftime("%Y-%m-%d"))
        self.assertEqual(convert_relative_time(""), "")
        self.assertEqual(convert_relative_time(None), "")


class ParseIntFromTextTest(unittest.TestCase):
    def test_extracts_digits(self):
        self.assertEqual(parse_int_from_text("2.035 ulasan"), 2035)

    def test_no_digits_returns_zero(self):
        self.assertEqual(parse_int_from_text("tidak ada"), 0)

    def test_non_string_returns_zero(self):
        self.assertEqual(parse_int_from_text(None), 0)


class NameNormalizationTest(unittest.TestCase):
    def test_sanitize_filename(self):
        self.assertEqual(
            sanitize_filename("Az-Zahra Galuh Mas (Pelatihan Manasik Haji)"),
            "Az-Zahra Galuh Mas Pelatihan Manasik Haji",
        )
        self.assertEqual(sanitize_filename("Wisata/Curug: Cigentis!"), "WisataCurug Cigentis")

    def test_clean_name_key(self):
        self.assertEqual(
            clean_name_key("Az-Zahra Galuh Mas (Pelatihan Manasik Haji)"),
            "azzahragaluhmaspelatihanmanasikhaji",
        )
        self.assertEqual(clean_name_key(None), "")

    def test_clean_display_name(self):
        self.assertEqual(clean_display_name("Wisata Curug Cigentis Karawang"), "Curug Cigentis")
        self.assertEqual(clean_display_name("Pantai Samudera Baru"), "Pantai Samudera Baru")
        self.assertEqual(clean_display_name(None), "")


class NlpStepsTest(unittest.TestCase):
    def test_case_folding(self):
        self.assertEqual(case_folding("Air Terjun 123! Sejuk 😊"), "air terjun sejuk")
        self.assertEqual(case_folding(None), "")

    def test_tokenizing(self):
        self.assertEqual(tokenizing("air terjun sejuk"), ["air", "terjun", "sejuk"])
        self.assertEqual(tokenizing(""), [])
        self.assertEqual(tokenizing(None), [])

    def test_remove_stopwords(self):
        self.assertEqual(
            remove_stopwords(["tempat", "wisata", "air", "sejuk"]),
            ["air", "sejuk"],
        )
        # Custom tourism stopwords are part of the set
        self.assertNotIn("yg", remove_stopwords(["yg"]))
        self.assertNotIn("tempat", remove_stopwords(["tempat"]))
        self.assertEqual(remove_stopwords(["air"]), ["air"])

    def test_stemming(self):
        self.assertEqual(stemming(["bermain"]), ["main"])


if __name__ == "__main__":
    unittest.main()
