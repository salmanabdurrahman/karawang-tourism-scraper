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

from playwright.sync_api import sync_playwright
import pandas as pd
import time
import os
import re
import json


# Configuration
INPUT_FILE = "data/raw/karawang_places_list.csv"
OUTPUT_DIR = "data/reviews_json"
MAX_REVIEWS_PER_PLACE = 400  # Target review count (with text only)

# Scraping settings
PAGE_LOAD_TIMEOUT = 60000  # Milliseconds
SELECTOR_TIMEOUT = 15000  # Milliseconds for primary selector
FALLBACK_TIMEOUT = 5000  # Milliseconds for fallback selector
TAB_SWITCH_DELAY = 2  # Seconds after switching tabs
SCROLL_DELAY = 1.5  # Seconds between scroll actions
SCROLL_EXTRA_BUFFER = 100  # Extra cards to load for filtering

os.makedirs(OUTPUT_DIR, exist_ok=True)


def sanitize_filename(filename):
    """
    Sanitizes a string to be used as a safe filename.
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename
    """
    safe_chars = [c for c in filename if c.isalnum() or c in (' ', '-', '_')]
    return "".join(safe_chars).strip()


def initialize_browser_context(headless=False):
    """
    Initializes browser context with Indonesian locale.
    
    Args:
        headless (bool): Run browser in headless mode
        
    Returns:
        tuple: (playwright, browser, context, page) instances
    """
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context(locale="id-ID")
    page = context.new_page()
    
    return playwright, browser, context, page


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
        page.wait_for_selector('.DUwDvf.lfPIob', timeout=SELECTOR_TIMEOUT)
        return True
    except Exception:
        try:
            # Fallback to H1 selector
            page.wait_for_selector('h1', timeout=FALLBACK_TIMEOUT)
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
        "attributes": ""
    }
    
    # Extract place name
    try:
        name_el = page.locator('.DUwDvf.lfPIob').first
        if name_el.count() > 0:
            place_info['name'] = name_el.inner_text()
    except Exception:
        pass
    
    # Extract rating and review count
    try:
        container = page.locator('.fontBodyMedium.dmRWX').first
        
        # Average rating
        rating_el = container.locator('span[aria-hidden="true"]').first
        if rating_el.count() > 0:
            place_info['avg_rating'] = rating_el.inner_text().replace(',', '.')
        
        # Total reviews text
        reviews_el = container.locator(
            'span[aria-label*="ulasan"], span[aria-label*="reviews"]'
        ).first
        if reviews_el.count() > 0:
            place_info['total_reviews_text'] = reviews_el.get_attribute('aria-label')
    except Exception:
        pass
    
    # Extract category
    try:
        category_btn = page.locator('button.DkEaL').first
        if category_btn.count() > 0:
            place_info['category'] = category_btn.inner_text()
    except Exception:
        pass
    
    # Extract address
    try:
        address_elements = page.locator('.Io6YTe.fontBodyMedium.kR99db.fdkmkc').all_inner_texts()
        if address_elements:
            place_info['address'] = address_elements[0]
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
        about_tab = page.locator('div.Gpq6kf.NlVald').filter(
            has_text=re.compile(r"Tentang|About")
        ).first
        
        if about_tab.count() > 0:
            about_tab.click()
            time.sleep(1)  # Wait for content load
            
            # Extract description
            try:
                desc_el = page.locator('span.HlvSq')
                if desc_el.count() > 0:
                    place_info['description'] = desc_el.first.inner_text()
            except Exception:
                pass
            
            # Extract attributes list
            try:
                attrs = page.locator('ul.ZQ6we li.hpLkke').all_inner_texts()
                if attrs:
                    # Format: join with pipe separator, replace newlines with colon
                    place_info['attributes'] = " | ".join([
                        a.replace('\n', ': ') for a in attrs
                    ])
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
        page.hover('div[role="main"]')
        first_card = page.locator('div[data-review-id]').first
        if first_card.count() > 0:
            first_card.click()
    except Exception:
        pass
    
    last_card_count = 0
    scroll_attempts = 0
    target_count = max_reviews + SCROLL_EXTRA_BUFFER
    
    while True:
        cards = page.locator('div[data-review-id]').all()
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
    
    # JavaScript code to extract reviews and filter empties
    reviews_data = page.evaluate("""() => {
        const data = [];
        const cards = document.querySelectorAll('div[data-review-id]');
        
        cards.forEach(card => {
            // 1. Click 'See More' button if present
            const moreBtn = card.querySelector('button.w8nwRe.kyuRq');
            if (moreBtn) {
                moreBtn.click();
            }
            
            // 2. Extract review text
            const textEl = card.querySelector('.wiI7pd');
            const text = textEl ? textEl.innerText : "";
            
            // FILTER: Skip if empty or blank
            if (!text || text.trim().length === 0) {
                return;
            }
            
            // 3. Extract other data
            const userEl = card.querySelector('.d4r55.fontTitleMedium');
            const user = userEl ? userEl.innerText.split('\\n')[0] : "Anonymous";
            
            // Count filled stars for rating
            const starsContainer = card.querySelector('.DU9Pgb');
            const stars = starsContainer ? 
                starsContainer.querySelectorAll('span.hCCjke').length : 0;
            
            const timeEl = card.querySelector('.rsqaWe');
            const time = timeEl ? timeEl.innerText : "";
            
            data.push({
                user_name: user,
                rating: stars,
                text: text,
                time: time
            });
        });
        
        return data;
    }""")
    
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
        place_info['name'] = place_name  # Ensure original name is preserved
        
        # Extract About tab information
        extract_about_info(page, place_info)
        
        print(f"   {place_info['category']} | Rating: {place_info['avg_rating']}")
        
        # Navigate to Reviews tab
        reviews_data = []
        
        try:
            review_tab = page.locator('div.Gpq6kf.NlVald').filter(
                has_text=re.compile(r"Ulasan|Reviews")
            ).first
            
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
        return {
            "place_info": place_info,
            "reviews": reviews_data
        }
    
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
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
        review_count = len(data.get('reviews', []))
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
    try:
        places_df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(places_df)} places from {INPUT_FILE}")
    except FileNotFoundError:
        print(f"Error: Places list file not found: {INPUT_FILE}")
        print("Please run gmaps_scraper.py first!")
        return
    
    playwright = None
    browser = None
    
    try:
        # Initialize browser
        playwright, browser, context, page = initialize_browser_context(headless)
        
        # Process each place
        for index, row in places_df.iterrows():
            place_name = row['place_name']
            url = row['gmaps_url']
            
            # Create safe filename
            safe_name = sanitize_filename(place_name)
            output_json = os.path.join(OUTPUT_DIR, f"{safe_name}.json")
            
            # Skip if already scraped
            if os.path.exists(output_json):
                print(f"Skipping {place_name} (JSON already exists).")
                continue
            
            print(f"\n[{index+1}/{len(places_df)}] Processing: {place_name}")
            
            # Scrape place data
            place_data = scrape_place_data(page, place_name, url)
            
            if place_data:
                save_to_json(place_data, output_json)
        
        print("\nAll places processed successfully!")
    
    except Exception as e:
        print(f"Error during scraping: {e}")
    
    finally:
        # Cleanup
        if browser:
            browser.close()
        if playwright:
            playwright.stop()


if __name__ == "__main__":
    # Run scraper
    # Set headless=True to run without visible browser window
    scrape_all_reviews(headless=False)