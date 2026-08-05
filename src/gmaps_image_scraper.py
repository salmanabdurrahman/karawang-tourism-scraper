"""
Google Maps Image Scraper

This script scrapes a representative image URL for each place listed in the
places CSV, by opening its Google Maps page and reading the gallery/hero image.

Features:
    - Opens each place page in a real browser (headless=False for image loading)
    - Extracts the first photo URL from the gallery, with hero-image fallback
    - Fills missing images with a placeholder URL
    - Exports results to CSV format

Output:
    - CSV file in: data/processed/karawang_place_images.csv

Dependencies:
    - playwright (install with: playwright install chromium)
    - pandas

Input:
    - CSV file with place names and URLs from gmaps_scraper.py

Author: Salman Abdurrahman
Date: 2025
"""

import re
import time

import pandas as pd

from browser import browser_session
from config import (
    PAGE_LOAD_TIMEOUT,
    PLACE_IMAGES_FILE,
    PLACEHOLDER_IMAGE_URL,
    PLACES_LIST_FILE,
    PROCESSED_DIR,
    SELECTOR_H1,
    SELECTOR_HERO_IMAGE,
    SELECTOR_PHOTO_BUTTON,
    SELECTOR_PHOTO_DIV,
    SHORT_SELECTOR_TIMEOUT,
    ensure_dir,
    require_file,
)

# ===========================
# CONFIG (paths & values from config.py)
# ===========================
INPUT_FILE = PLACES_LIST_FILE
OUTPUT_DIR = PROCESSED_DIR
OUTPUT_FILE = PLACE_IMAGES_FILE

ensure_dir(OUTPUT_DIR)


def scrape_place_image(page, url):
    """
    Extracts a representative image URL for one place.

    Tries the photo gallery button first; falls back to the hero image on the
    front page when the gallery fails. Returns an empty string when no image
    can be read (the caller applies the placeholder URL).

    Args:
        page: Playwright page instance
        url (str): Google Maps URL of the place

    Returns:
        str: Image URL, or "" if no image was found
    """
    image_url = ""

    try:
        page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
        # Wait for the title element (page loaded signal)
        try:
            page.wait_for_selector(SELECTOR_H1, timeout=SHORT_SELECTOR_TIMEOUT)
        except Exception:
            time.sleep(2)

        time.sleep(2)  # Allow render time

        # 1. Find the Photo Gallery Button
        # Class: aoRNLd kn2E5e NMjTrf lvtCsd
        try:
            photo_btn = page.locator(SELECTOR_PHOTO_BUTTON).first

            if photo_btn.count() > 0 and photo_btn.is_visible():
                photo_btn.click()

                # Wait for the gallery to appear
                # Photo div: Uf0tqf ch8jbf
                page.wait_for_selector(SELECTOR_PHOTO_DIV, timeout=SHORT_SELECTOR_TIMEOUT)
                time.sleep(1.5)

                # Grab the first photo element
                photo_div = page.locator(SELECTOR_PHOTO_DIV).first

                # Read the style attribute: background-image: url("...")
                style = photo_div.get_attribute("style")
                if style:
                    # Regex to extract the URL
                    match = re.search(r'url\((?:&quot;|")?([^&")]+)(?:&quot;|")?\)', style)
                    if match:
                        image_url = match.group(1)

                # Press ESC to close the gallery (safe before navigating)
                page.keyboard.press("Escape")
                time.sleep(1)
            else:
                print(" (Gallery button not found)", end="")

        except Exception:
            # Fallback: try the hero image on the front page if gallery fails
            try:
                hero_img = page.locator(SELECTOR_HERO_IMAGE).first
                if hero_img.count() > 0:
                    image_url = hero_img.get_attribute("src")
            except Exception:
                pass

    except Exception as e:
        print(f"Error: {e}", end="")

    return image_url


def scrape_images_only(headless=False):
    """
    Main function that orchestrates image scraping for all places.

    Args:
        headless (bool): Run browser in headless mode. Image loading is more
            reliable with a visible browser, so the default stays False.
    """
    if not require_file(INPUT_FILE):
        print(f"File not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"Loading {len(df)} places to scrape images...")

    results = []

    # Browser lifecycle is owned by browser_session (always closed on exit)
    with browser_session(headless=headless, locale="id-ID") as page:
        for index, row in df.iterrows():
            place_name = row["place_name"]
            url = row["gmaps_url"]

            print(f"[{index + 1}/{len(df)}] {place_name}...", end=" ")

            image_url = scrape_place_image(page, url)

            # Save the result (empty or filled)
            if image_url:
                print("OK")
            else:
                print("Empty")
                image_url = PLACEHOLDER_IMAGE_URL  # Placeholder

            results.append({"place_name": place_name, "image_url": image_url})

    # Save to CSV
    df_img = pd.DataFrame(results)
    df_img.to_csv(OUTPUT_FILE, index=False)
    print(f"\nDone! Image data saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    scrape_images_only()
