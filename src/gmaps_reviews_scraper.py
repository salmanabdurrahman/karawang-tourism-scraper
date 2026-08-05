"""
Google Maps Reviews JSON Scraper

This script scrapes detailed reviews and metadata for places, saving results in
structured JSON format. Unlike the CSV version, this scraper uses JavaScript
evaluation for faster extraction and filters out empty reviews automatically.

Features:
    - Extracts comprehensive place metadata (name, category, rating, address, description)
    - Collects user reviews with ratings and timestamps
    - Filters out empty/blank reviews automatically
    - Uses JavaScript evaluation for faster data extraction
    - Saves structured data in JSON format (one file per place)
    - Skips places that have already been scraped

Output:
    - Individual JSON files per place in: data/reviews_json/<place_name>.json
    - Structure: { "place_info": {...}, "reviews": [...] }

Dependencies:
    - playwright (install with: playwright install chromium)
    - pandas

Input:
    - CSV file with place names and URLs from gmaps_scraper.py

Author: Salman Abdurrahman
Date: 2025
"""

import json
import os
import time

import pandas as pd

from browser import browser_session
from config import (
    ABOUT_TAB_PATTERN,
    FALLBACK_TIMEOUT,
    MAX_REVIEWS_PER_PLACE,
    PAGE_LOAD_TIMEOUT,
    PLACES_LIST_FILE,
    REVIEWS_JSON_DIR,
    REVIEWS_TAB_PATTERN,
    SCROLL_DELAY,
    SCROLL_EXTRA_BUFFER,
    SELECTOR_ADDRESS,
    SELECTOR_ATTRIBUTES,
    SELECTOR_CATEGORY_BUTTON,
    SELECTOR_DESCRIPTION,
    SELECTOR_H1,
    SELECTOR_MAIN_PANEL,
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
    SELECTOR_TIMEOUT,
    TAB_SWITCH_DELAY,
    ensure_dir,
    require_file,
)
from utils import sanitize_filename

# Configuration (paths & values centralized in config.py)
INPUT_FILE = PLACES_LIST_FILE
OUTPUT_DIR = REVIEWS_JSON_DIR

ensure_dir(OUTPUT_DIR)


def wait_for_page_load(page):
    """
    Waits for Google Maps page to fully load.

    Tries multiple selectors with fallback strategy.

    Args:
        page: Playwright page instance

    Returns:
        bool: True if page loaded successfully
    """
    try:
        # Try primary selector (place name)
        page.wait_for_selector(SELECTOR_PLACE_NAME, timeout=SELECTOR_TIMEOUT)
        return True
    except Exception:
        try:
            # Fallback to H1 selector
            page.wait_for_selector(SELECTOR_H1, timeout=FALLBACK_TIMEOUT)
            return True
        except Exception:
            return False


def extract_place_metadata(page):
    """
    Extracts comprehensive place metadata from the main view.

    Args:
        page: Playwright page instance

    Returns:
        dict: Place metadata dictionary
    """
    place_info = {
        "name": "",
        "category": "",
        "avg_rating": "0",
        "total_reviews_text": "",
        "address": "",
        "description": "",
        "attributes": "",
    }

    # Extract place name
    try:
        name_el = page.locator(SELECTOR_PLACE_NAME).first
        if name_el.count() > 0:
            place_info["name"] = name_el.inner_text()
    except Exception:
        pass

    # Extract rating and review count
    try:
        container = page.locator(SELECTOR_RATING_CONTAINER).first

        # Average rating
        rating_el = container.locator(SELECTOR_RATING_VALUE).first
        if rating_el.count() > 0:
            place_info["avg_rating"] = rating_el.inner_text().replace(",", ".")

        # Total reviews text
        reviews_el = container.locator(SELECTOR_REVIEWS_COUNT_LABEL).first
        if reviews_el.count() > 0:
            place_info["total_reviews_text"] = reviews_el.get_attribute("aria-label")
    except Exception:
        pass

    # Extract category
    try:
        category_btn = page.locator(SELECTOR_CATEGORY_BUTTON).first
        if category_btn.count() > 0:
            place_info["category"] = category_btn.inner_text()
    except Exception:
        pass

    # Extract address
    try:
        address_elements = page.locator(SELECTOR_ADDRESS).all_inner_texts()
        if address_elements:
            place_info["address"] = address_elements[0]
    except Exception:
        pass

    return place_info


def extract_about_info(page, place_info):
    """
    Navigates to About tab and extracts description and attributes.

    Args:
        page: Playwright page instance
        place_info (dict): Place info dictionary to update
    """
    try:
        # Find and click About/Tentang tab
        about_tab = page.locator(SELECTOR_TAB).filter(has_text=ABOUT_TAB_PATTERN).first

        if about_tab.count() > 0:
            about_tab.click()
            time.sleep(1)  # Wait for content load

            # Extract description
            try:
                desc_el = page.locator(SELECTOR_DESCRIPTION)
                if desc_el.count() > 0:
                    place_info["description"] = desc_el.first.inner_text()
            except Exception:
                pass

            # Extract attributes list
            try:
                attrs = page.locator(SELECTOR_ATTRIBUTES).all_inner_texts()
                if attrs:
                    # Format: join with pipe separator, replace newlines with colon
                    place_info["attributes"] = " | ".join([a.replace("\n", ": ") for a in attrs])
            except Exception:
                pass
    except Exception:
        pass


def scroll_reviews_panel(page, max_reviews):
    """
    Scrolls the reviews panel to load more reviews.

    Loads extra reviews beyond target to account for empty ones that will be filtered.

    Args:
        page: Playwright page instance
        max_reviews (int): Target number of reviews

    Returns:
        int: Total number of review cards loaded
    """
    print("   Scrolling reviews...")

    # Focus on reviews area
    try:
        page.hover(SELECTOR_MAIN_PANEL)
        first_card = page.locator(SELECTOR_REVIEW_CARD).first
        if first_card.count() > 0:
            first_card.click()
    except Exception:
        pass

    last_card_count = 0
    scroll_attempts = 0
    target_count = max_reviews + SCROLL_EXTRA_BUFFER

    while True:
        cards = page.locator(SELECTOR_REVIEW_CARD).all()
        current_count = len(cards)

        print(f"\r      Loaded (mixed): {current_count}...", end="")

        # Load extra to account for filtering
        if current_count >= target_count:
            break

        # Check if stuck
        if current_count == last_card_count:
            scroll_attempts += 1
            page.keyboard.press("End")
            time.sleep(2)

            # Try mouse wheel if keyboard fails
            if scroll_attempts > 3:
                page.mouse.wheel(0, 5000)
                time.sleep(2)

            # Give up after max attempts
            if scroll_attempts > 10:
                break
        else:
            scroll_attempts = 0
            last_card_count = current_count
            page.keyboard.press("End")
            time.sleep(SCROLL_DELAY)

    print("")  # New line
    return current_count


def extract_reviews_with_js(page, max_reviews):
    """
    Extracts review data using JavaScript evaluation for better performance.

    Automatically filters out empty reviews during extraction.

    Args:
        page: Playwright page instance
        max_reviews (int): Maximum number of reviews to return

    Returns:
        list: List of review dictionaries (with text only)
    """
    print("   Extracting review data (filtering empty reviews)...")

    # Selectors come from config.py; passed as an argument so the JS block
    # stays readable and the selectors stay centralized.
    selectors = {
        "reviewCard": SELECTOR_REVIEW_CARD,
        "seeMore": SELECTOR_SEE_MORE_BUTTON,
        "reviewText": SELECTOR_REVIEW_TEXT,
        "userName": SELECTOR_REVIEW_USER,
        "starsContainer": SELECTOR_REVIEW_STARS_CONTAINER,
        "filledStar": SELECTOR_REVIEW_FILLED_STAR,
        "reviewTime": SELECTOR_REVIEW_TIME,
    }

    # JavaScript code to extract reviews and filter empties
    reviews_data = page.evaluate(
        """(sel) => {
        const data = [];
        const cards = document.querySelectorAll(sel.reviewCard);
        
        cards.forEach(card => {
            // 1. Click 'See More' button if present
            const moreBtn = card.querySelector(sel.seeMore);
            if (moreBtn) {
                moreBtn.click();
            }
            
            // 2. Extract review text
            const textEl = card.querySelector(sel.reviewText);
            const text = textEl ? textEl.innerText : "";
            
            // FILTER: Skip if empty or blank
            if (!text || text.trim().length === 0) {
                return;
            }
            
            // 3. Extract other data
            const userEl = card.querySelector(sel.userName);
            const user = userEl ? userEl.innerText.split('\\n')[0] : "Anonymous";
            
            // Count filled stars for rating
            const starsContainer = card.querySelector(sel.starsContainer);
            const stars = starsContainer ? 
                starsContainer.querySelectorAll(sel.filledStar).length : 0;
            
            const timeEl = card.querySelector(sel.reviewTime);
            const time = timeEl ? timeEl.innerText : "";
            
            data.push({
                user_name: user,
                rating: stars,
                text: text,
                time: time
            });
        });
        
        return data;
    }""",
        selectors,
    )

    # Limit to target count
    return reviews_data[:max_reviews]


def scrape_place_data(page, place_name, url):
    """
    Scrapes all data for a single place.

    Args:
        page: Playwright page instance
        place_name (str): Name of the place
        url (str): Google Maps URL

    Returns:
        dict: Complete place data with reviews, or None if failed
    """
    try:
        # Navigate to place
        page.goto(url, timeout=PAGE_LOAD_TIMEOUT)

        if not wait_for_page_load(page):
            print(f"   Warning: Page load timeout for {place_name}")
            return None

        time.sleep(1.5)  # Allow full rendering

        # Extract place metadata
        place_info = extract_place_metadata(page)
        place_info["name"] = place_name  # Ensure original name is preserved

        # Extract About tab information
        extract_about_info(page, place_info)

        print(f"   {place_info['category']} | Rating: {place_info['avg_rating']}")

        # Navigate to Reviews tab
        reviews_data = []

        try:
            review_tab = page.locator(SELECTOR_TAB).filter(has_text=REVIEWS_TAB_PATTERN).first

            if review_tab.count() > 0:
                review_tab.click()
                time.sleep(TAB_SWITCH_DELAY)

                # Scroll to load reviews
                scroll_reviews_panel(page, MAX_REVIEWS_PER_PLACE)

                # Extract reviews using JavaScript
                reviews_data = extract_reviews_with_js(page, MAX_REVIEWS_PER_PLACE)

        except Exception as e:
            print(f"   Warning: Error accessing reviews: {e}")

        # Return structured data
        return {"place_info": place_info, "reviews": reviews_data}

    except Exception as e:
        print(f"   Error processing {place_name}: {e}")
        return None


def save_to_json(data, output_file):
    """
    Saves place data to JSON file.

    Args:
        data (dict): Place data dictionary
        output_file (str): Path to output JSON file

    Returns:
        bool: True if save successful
    """
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        review_count = len(data.get("reviews", []))
        print(f"   Saved: {review_count} text reviews to JSON.")
        return True

    except Exception as e:
        print(f"   Error saving JSON: {e}")
        return False


def scrape_all_reviews(headless=False):
    """
    Main function that orchestrates review scraping for all places.

    Args:
        headless (bool): Run browser in headless mode
    """
    # Load places list
    if not require_file(INPUT_FILE):
        print(f"Error: Places list file not found: {INPUT_FILE}")
        print("Please run gmaps_scraper.py first!")
        return
    places_df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(places_df)} places from {INPUT_FILE}")

    try:
        # Browser lifecycle is owned by browser_session (always closed on exit)
        with browser_session(headless=headless, locale="id-ID") as page:
            # Process each place
            for index, row in places_df.iterrows():
                place_name = row["place_name"]
                url = row["gmaps_url"]

                # Create safe filename
                safe_name = sanitize_filename(place_name)
                output_json = os.path.join(OUTPUT_DIR, f"{safe_name}.json")

                # Skip if already scraped
                if os.path.exists(output_json):
                    print(f"Skipping {place_name} (JSON already exists).")
                    continue

                print(f"\n[{index + 1}/{len(places_df)}] Processing: {place_name}")

                # Scrape place data
                place_data = scrape_place_data(page, place_name, url)

                if place_data:
                    save_to_json(place_data, output_json)

        print("\nAll places processed successfully!")

    except Exception as e:
        print(f"Error during scraping: {e}")


if __name__ == "__main__":
    # Run scraper
    # Set headless=True to run without visible browser window
    scrape_all_reviews(headless=False)
