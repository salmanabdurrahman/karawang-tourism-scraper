"""
Google Maps Reviews Scraper

This script scrapes detailed reviews and metadata for places listed in a CSV file.
It visits each place's Google Maps page, extracts place information from the About
tab, and collects user reviews with ratings.

Features:
    - Extracts place metadata (name, category, rating, address, attributes)
    - Collects user reviews with ratings and timestamps
    - Handles infinite scroll to load more reviews
    - Implements human-like scrolling behavior
    - Prevents duplicate reviews
    - Saves raw data for later processing
    - Skips places that have already been scraped

Output:
    - Individual CSV files per place in: data/reviews/<place_name>.csv

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


# Configuration
INPUT_FILE = "data/raw/karawang_places_list.csv"
OUTPUT_DIR = "data/reviews"
MAX_REVIEWS_PER_PLACE = 400  # Target review count per place

# Scraping settings
PAGE_LOAD_TIMEOUT = 60000  # Milliseconds
TAB_SWITCH_DELAY = 3  # Seconds to wait after switching tabs
SCROLL_DELAY = 1.5  # Seconds between scroll actions
MAX_SCROLL_STALL_ATTEMPTS = 15  # Max attempts when no new reviews load

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
        tuple: (browser, context, page) instances
    """
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context(locale="id-ID")
    page = context.new_page()
    
    return playwright, browser, context, page


def extract_place_metadata(page):
    """
    Extracts basic place metadata from the main view.
    
    Collects raw data that will be processed later. Fields include:
    - Place name
    - Category
    - Average rating (raw text)
    - Address (raw text)
    
    Args:
        page: Playwright page instance
        
    Returns:
        dict: Place metadata dictionary
    """
    place_data = {
        'place_name': '',
        'place_category': '',
        'place_avg_rating_raw': '',
        'place_address_raw': '',
        'place_attributes_raw': ''
    }
    
    # Extract place name from H1
    try:
        place_data['place_name'] = page.locator('h1').first.inner_text()
    except Exception:
        pass
    
    # Extract raw rating info from main div
    try:
        main_text = page.locator('div[role="main"]').first.inner_text()
        place_data['place_avg_rating_raw'] = main_text[:200]  # First 200 chars
    except Exception:
        pass
    
    # Extract category
    try:
        category_btn = page.locator('button[jsaction*="category"]').first
        if category_btn.count() > 0:
            place_data['place_category'] = category_btn.inner_text()
    except Exception:
        pass
    
    # Extract address from aria-label
    try:
        address_btn = page.locator('button[data-item-id="address"]')
        if address_btn.count() > 0:
            place_data['place_address_raw'] = address_btn.get_attribute("aria-label")
    except Exception:
        pass
    
    return place_data


def extract_about_attributes(page):
    """
    Navigates to About tab and extracts place attributes.
    
    Args:
        page: Playwright page instance
        
    Returns:
        str: Raw attributes text, empty string if not found
    """
    attributes_text = ''
    
    try:
        # Find and click About/Tentang tab
        about_tab = page.locator(
            'div[role="tablist"] button, button'
        ).filter(has_text=re.compile(r"^Tentang$|^About$")).first
        
        if about_tab.count() > 0:
            about_tab.click()
            time.sleep(2)  # Wait for content to load
            
            # Extract attributes from About section
            try:
                main_content = page.locator(
                    'div[role="main"] div[aria-label^="Tentang"], '
                    'div[role="main"] div[aria-label^="About"]'
                ).first
                
                if main_content.count() > 0:
                    attributes_text = main_content.inner_text()
            except Exception:
                pass
    except Exception:
        pass
    
    return attributes_text


def scroll_reviews_panel(page, max_reviews):
    """
    Scrolls the reviews panel to load more reviews.
    
    Implements human-like scrolling behavior with keyboard and mouse.
    Stops when target review count is reached or no new reviews load.
    
    Args:
        page: Playwright page instance
        max_reviews (int): Target number of reviews to load
        
    Returns:
        int: Total number of review cards loaded
    """
    print("   Scrolling reviews...")
    
    # Focus on first review card for keyboard navigation
    try:
        first_card = page.locator('div[data-review-id]').first
        if first_card.count() > 0:
            first_card.click()
    except Exception:
        pass
    
    last_card_count = 0
    scroll_attempts = 0
    
    while True:
        cards = page.locator('div[data-review-id]').all()
        current_count = len(cards)
        
        print(f"\r      Loaded: {current_count}/{max_reviews}...", end="")
        
        # Stop if target reached
        if current_count >= max_reviews:
            break
        
        # Check if stuck (no new reviews loading)
        if current_count == last_card_count:
            scroll_attempts += 1
            
            # Human-like scroll strategy
            page.keyboard.press("End")
            time.sleep(1)
            page.keyboard.press("PageDown")
            time.sleep(1)
            
            # Try mouse wheel if keyboard scrolling stalls
            if scroll_attempts > 3:
                page.mouse.wheel(0, 5000)
                time.sleep(2)
            
            # Give up if stuck for too long
            if scroll_attempts > MAX_SCROLL_STALL_ATTEMPTS:
                break
        else:
            # New reviews loaded, reset stall counter
            scroll_attempts = 0
            last_card_count = current_count
            page.keyboard.press("End")
            time.sleep(SCROLL_DELAY)
    
    print("")  # New line after progress indicator
    return current_count


def extract_review_data(card):
    """
    Extracts review information from a single review card.
    
    Args:
        card: Playwright locator for review card element
        
    Returns:
        dict: Review data dictionary with user info, rating, text, and timestamp
    """
    # Expand "See more" button if present
    try:
        more_btn = card.locator('button').filter(
            has_text=re.compile(r"^Lihat lainnya$|^See more$")
        ).first
        if more_btn.count() > 0:
            more_btn.click()
            time.sleep(0.3)  # Brief pause for expansion
    except Exception:
        pass
    
    # Extract user name
    try:
        user_name = card.locator('div[class*="d4r55"]').first.inner_text()
    except Exception:
        user_name = "Anonymous"
    
    # Extract rating from aria-label
    try:
        rating_el = card.locator(
            'span[role="img"][aria-label*="bintang"], '
            'span[role="img"][aria-label*="stars"]'
        ).first
        rating_text = rating_el.get_attribute('aria-label')
    except Exception:
        rating_text = ""
    
    # Extract review timestamp
    try:
        review_time = card.locator('span[class*="rsqaWe"]').first.inner_text()
    except Exception:
        review_time = ""
    
    # Extract review text
    try:
        review_text = card.locator('span[class*="wiI7pd"]').first.inner_text()
    except Exception:
        review_text = ""
    
    return {
        'user_name': user_name,
        'user_rating_raw': rating_text,
        'review_text': review_text,
        'review_time': review_time
    }


def scrape_place_reviews(page, place_name, url, place_data):
    """
    Scrapes all reviews for a single place.
    
    Args:
        page: Playwright page instance
        place_name (str): Name of the place
        url (str): Google Maps URL for the place
        place_data (dict): Base place metadata
        
    Returns:
        list: List of review dictionaries
    """
    final_reviews = []
    seen_reviews = set()  # To prevent duplicates
    
    try:
        # Navigate to place page
        page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
        page.wait_for_selector('h1', timeout=20000)
        time.sleep(2)  # Wait for visual rendering
        
        # Extract place metadata
        metadata = extract_place_metadata(page)
        place_data.update(metadata)
        
        # Extract about attributes
        attributes = extract_about_attributes(page)
        place_data['place_attributes_raw'] = attributes
        
        # Navigate to Reviews tab
        try:
            review_tab = page.locator(
                'div[role="tablist"] button, button'
            ).filter(has_text=re.compile(r"^Ulasan$|^Reviews$")).first
            
            if review_tab.count() > 0:
                review_tab.click()
                time.sleep(TAB_SWITCH_DELAY)  # Wait for tab animation
                
                # Scroll to load reviews
                total_loaded = scroll_reviews_panel(page, MAX_REVIEWS_PER_PLACE)
                
                # Extract review data
                print("   Extracting review data...")
                review_cards = page.locator('div[data-review-id]').all()
                
                for card in review_cards[:MAX_REVIEWS_PER_PLACE]:
                    try:
                        review_info = extract_review_data(card)
                        
                        # Create unique signature to prevent duplicates
                        signature = f"{review_info['user_name']}_{review_info['review_text'][:20]}"
                        if signature in seen_reviews:
                            continue
                        seen_reviews.add(signature)
                        
                        # Combine place data with review data
                        review_entry = place_data.copy()
                        review_entry.update(review_info)
                        final_reviews.append(review_entry)
                    
                    except Exception:
                        continue
        
        except Exception as e:
            print(f"   Warning: Error accessing reviews tab: {e}")
    
    except Exception as e:
        print(f"   Error processing {place_name}: {e}")
    
    return final_reviews


def save_reviews_to_csv(reviews, output_file):
    """
    Saves reviews to CSV file.
    
    Args:
        reviews (list): List of review dictionaries
        output_file (str): Path to output CSV file
        
    Returns:
        bool: True if save successful
    """
    if reviews:
        df = pd.DataFrame(reviews)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"   Saved: {len(df)} reviews (raw data).")
        return True
    else:
        print("   Warning: No reviews collected.")
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
            output_csv = os.path.join(OUTPUT_DIR, f"{safe_name}.csv")
            
            # Skip if already scraped
            if os.path.exists(output_csv):
                print(f"Skipping {place_name} (already scraped).")
                continue
            
            print(f"\n[{index+1}/{len(places_df)}] Processing: {place_name}")
            
            # Initialize place data
            place_data = {'place_name': place_name}
            
            # Scrape reviews
            reviews = scrape_place_reviews(page, place_name, url, place_data)
            
            # Save results
            save_reviews_to_csv(reviews, output_csv)
        
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