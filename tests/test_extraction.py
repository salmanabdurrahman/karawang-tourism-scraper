"""
Extraction tests using a fake Playwright page (tests/fakes.py).

Covers extract_place_metadata, extract_about_info, extract_reviews_with_js
(reviews scraper) and extract_place_data (places scraper) without a browser
or network. Selector strings come from config; behavior mirrors the frozen
baseline extraction rules.
"""

import os
import sys
import unittest
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
del _HERE, _path

import gmaps_reviews_scraper
import gmaps_scraper
from config import (
    SELECTOR_ADDRESS,
    SELECTOR_ATTRIBUTES,
    SELECTOR_CATEGORY_BUTTON,
    SELECTOR_DESCRIPTION,
    SELECTOR_PLACE_LINK,
    SELECTOR_PLACE_NAME,
    SELECTOR_RATING_CONTAINER,
    SELECTOR_RATING_VALUE,
    SELECTOR_REVIEW_CARD,
    SELECTOR_REVIEW_FILLED_STAR,
    SELECTOR_REVIEW_STARS_CONTAINER,
    SELECTOR_REVIEW_TEXT,
    SELECTOR_REVIEW_TIME,
    SELECTOR_REVIEW_USER,
    SELECTOR_REVIEWS_COUNT_LABEL,
    SELECTOR_SEE_MORE_BUTTON,
    SELECTOR_TAB,
)
from tests.fakes import FakeElement, FakeLocator, FakePage


class ExtractPlaceMetadataTest(unittest.TestCase):
    def test_full_metadata(self):
        page = FakePage()
        page.set_locator(
            SELECTOR_PLACE_NAME,
            FakeLocator([FakeElement(inner_text="Curug Cigentis")]),
        )
        rating_container = FakeLocator(
            children={
                SELECTOR_RATING_VALUE: FakeLocator([FakeElement(inner_text="4,5")]),
                SELECTOR_REVIEWS_COUNT_LABEL: FakeLocator(
                    [
                        FakeElement(attributes={"aria-label": "1.234 ulasan"}),
                    ]
                ),
            }
        )
        page.set_locator(SELECTOR_RATING_CONTAINER, rating_container)
        page.set_locator(
            SELECTOR_CATEGORY_BUTTON,
            FakeLocator([FakeElement(inner_text="Taman")]),
        )
        page.set_locator(
            SELECTOR_ADDRESS,
            FakeLocator([FakeElement(inner_text="Kecamatan Telagasari, Karawang")]),
        )

        info = gmaps_reviews_scraper.extract_place_metadata(page)
        self.assertEqual(info["name"], "Curug Cigentis")
        self.assertEqual(info["category"], "Taman")
        self.assertEqual(info["avg_rating"], "4.5")  # comma replaced by dot
        self.assertEqual(info["total_reviews_text"], "1.234 ulasan")
        self.assertEqual(info["address"], "Kecamatan Telagasari, Karawang")

    def test_empty_page_returns_defaults(self):
        page = FakePage()
        info = gmaps_reviews_scraper.extract_place_metadata(page)
        self.assertEqual(
            info,
            {
                "name": "",
                "category": "",
                "avg_rating": "0",
                "total_reviews_text": "",
                "address": "",
                "description": "",
                "attributes": "",
            },
        )


class ExtractAboutInfoTest(unittest.TestCase):
    def test_about_tab_extracts_description_and_attributes(self):
        page = FakePage()
        page.set_locator(
            SELECTOR_TAB,
            FakeLocator(
                [
                    FakeElement(inner_text="Tentang"),
                    FakeElement(inner_text="Ulasan"),
                ]
            ),
        )
        page.set_locator(
            SELECTOR_DESCRIPTION,
            FakeLocator([FakeElement(inner_text="Air terjun dengan pemandangan alam")]),
        )
        page.set_locator(
            SELECTOR_ATTRIBUTES,
            FakeLocator(
                [
                    FakeElement(inner_text="Parkir: Ada"),
                    FakeElement(inner_text="Toilet: Ada"),
                ]
            ),
        )

        place_info = {"description": "", "attributes": ""}
        with mock.patch("time.sleep"):
            gmaps_reviews_scraper.extract_about_info(page, place_info)

        self.assertEqual(place_info["description"], "Air terjun dengan pemandangan alam")
        # newlines inside an attribute become ": ", items join with " | "
        self.assertEqual(place_info["attributes"], "Parkir: Ada | Toilet: Ada")

    def test_no_about_tab_leaves_info_unchanged(self):
        page = FakePage()
        page.set_locator(SELECTOR_TAB, FakeLocator([FakeElement(inner_text="Ulasan")]))
        place_info = {"description": "x", "attributes": "y"}
        with mock.patch("time.sleep"):
            gmaps_reviews_scraper.extract_about_info(page, place_info)
        self.assertEqual(place_info, {"description": "x", "attributes": "y"})


class ExtractReviewsWithJsTest(unittest.TestCase):
    def test_uses_centralized_selectors_and_limits_to_max(self):
        page = FakePage()
        canned = [
            {"user_name": f"User {i}", "rating": 5, "text": f"Review {i}", "time": "1 hari yang lalu"} for i in range(5)
        ]
        page.evaluate_result = canned

        result = gmaps_reviews_scraper.extract_reviews_with_js(page, max_reviews=3)

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["user_name"], "User 0")

        # exactly one evaluate call, carrying the frozen selectors
        self.assertEqual(len(page.evaluate_calls), 1)
        js, selectors = page.evaluate_calls[0]
        self.assertIn("reviewCard", js)
        self.assertEqual(selectors["reviewCard"], SELECTOR_REVIEW_CARD)
        self.assertEqual(selectors["seeMore"], SELECTOR_SEE_MORE_BUTTON)
        self.assertEqual(selectors["reviewText"], SELECTOR_REVIEW_TEXT)
        self.assertEqual(selectors["userName"], SELECTOR_REVIEW_USER)
        self.assertEqual(selectors["starsContainer"], SELECTOR_REVIEW_STARS_CONTAINER)
        self.assertEqual(selectors["filledStar"], SELECTOR_REVIEW_FILLED_STAR)
        self.assertEqual(selectors["reviewTime"], SELECTOR_REVIEW_TIME)


class ExtractPlaceDataTest(unittest.TestCase):
    def test_extracts_valid_links_only(self):
        page = FakePage()
        page.set_locator(
            SELECTOR_PLACE_LINK,
            FakeLocator(
                [
                    FakeElement(
                        attributes={
                            "href": "https://maps.google.com/?cid=101",
                            "aria-label": "Curug Cigentis",
                        }
                    ),
                    FakeElement(
                        attributes={
                            "href": "https://maps.google.com/?cid=102",
                            "aria-label": "",
                        }
                    ),
                    FakeElement(
                        attributes={
                            "href": None,
                            "aria-label": "Tanpa URL",
                        }
                    ),
                ]
            ),
        )

        result = gmaps_scraper.extract_place_data(page)

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0],
            {
                "place_name": "Curug Cigentis",
                "gmaps_url": "https://maps.google.com/?cid=101",
            },
        )


if __name__ == "__main__":
    unittest.main()
