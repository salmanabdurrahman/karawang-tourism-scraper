"""
Google Maps Places Scraper

This script scrapes place listings from Google Maps based on a search query.
It uses Playwright for browser automation to handle dynamic content loading
and infinite scroll pagination.

Features:
    - Automated search on Google Maps
    - Handles infinite scroll to load all results
    - Extracts place names and URLs
    - Exports data to CSV format

Output:
    - CSV file in: data/raw/<query>_places_list.csv

Dependencies:
    - playwright (install with: playwright install chromium)
    - pandas

Author: Salman Abdurrahman
Date: 2025
"""

import os
import re
import time

import pandas as pd

from browser import browser_session
from config import (
    MAX_SCROLL_ATTEMPTS,
    PAGE_LOAD_TIMEOUT,
    RAW_DIR,
    SCROLL_PAUSE_TIME,
    SEARCH_QUERY,
    SELECTOR_END_OF_LIST,
    SELECTOR_PLACE_LINK,
    SELECTOR_RESULTS_FEED,
    SELECTOR_SEARCHBOX,
    SHORT_SELECTOR_TIMEOUT,
    ensure_dir,
    resolve_raw_csv_name,
)

# Configuration (paths & values centralized in config.py)
OUTPUT_DIR = RAW_DIR
ensure_dir(OUTPUT_DIR)

# Generate output filename from query, then map it to the canonical raw CSV
# name used by the rest of the pipeline (config.RAW_CSV_NAME_COMPATIBILITY).
query_slug = re.sub(r'[^\w\s-]', '', SEARCH_QUERY.lower())
query_slug = re.sub(r'[-\s]+', '_', query_slug)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, resolve_raw_csv_name(f"{query_slug}_places_list.csv"))


def navigate_to_maps(page):
    """
    Navigates to Google Maps homepage.
    
    Args:
        page: Playwright page instance
        
    Returns:
        bool: True if navigation successful
    """
    try:
        print("Opening Google Maps...")
        page.goto("https://www.google.com/maps", timeout=PAGE_LOAD_TIMEOUT)
        page.wait_for_selector(SELECTOR_SEARCHBOX, timeout=SHORT_SELECTOR_TIMEOUT)
        return True
    except Exception as e:
        print(f"Error navigating to Google Maps: {e}")
        return False


def perform_search(page, query):
    """
    Performs search on Google Maps.
    
    Args:
        page: Playwright page instance
        query (str): Search query text
        
    Returns:
        bool: True if search successful
    """
    try:
        print(f"Searching for: '{query}'")
        page.fill(SELECTOR_SEARCHBOX, query)
        page.keyboard.press('Enter')
        
        # Wait for results panel to load
        page.wait_for_selector(SELECTOR_RESULTS_FEED, timeout=SHORT_SELECTOR_TIMEOUT)
        print("Search results loaded.")
        return True
    except Exception as e:
        print(f"Error performing search: {e}")
        return False


def scroll_results_panel(page):
    """
    Scrolls the results panel to load all places (handles infinite scroll).
    
    Google Maps uses infinite scroll, so we need to scroll until
    we reach the end of the list or no new items are loaded.
    
    Args:
        page: Playwright page instance
        
    Returns:
        int: Total number of places loaded
    """
    print("Scrolling to load all places...")
    
    last_count = 0
    scroll_attempts = 0
    
    while scroll_attempts < MAX_SCROLL_ATTEMPTS:
        # Scroll the feed panel to bottom
        page.evaluate(
            """(selector) => {
                const feed = document.querySelector(selector);
                if (feed) {
                    feed.scrollTop = feed.scrollHeight;
                }
            }""",
            SELECTOR_RESULTS_FEED,
        )
        
        time.sleep(SCROLL_PAUSE_TIME)
        scroll_attempts += 1
        
        # Count currently loaded places
        # Each place has a link with class 'hfpxzc'
        places = page.locator(SELECTOR_PLACE_LINK).all()
        current_count = len(places)
        
        print(f"   Found {current_count} places (attempt {scroll_attempts})...")
        
        # Check if we've reached the end
        end_of_list = page.locator(SELECTOR_END_OF_LIST).is_visible()
        
        # Stop if no new items loaded or end marker found
        if current_count == last_count or end_of_list:
            print("Scroll complete. All places loaded.")
            break
        
        last_count = current_count
    
    return current_count


def extract_place_data(page):
    """
    Extracts place information from loaded results.
    
    Args:
        page: Playwright page instance
        
    Returns:
        list: List of dictionaries containing place data
    """
    print("Extracting place data...")
    
    results = []
    places = page.locator(SELECTOR_PLACE_LINK).all()
    
    for idx, place in enumerate(places, 1):
        try:
            url = place.get_attribute('href')
            # Place name is stored in aria-label attribute
            name = place.get_attribute('aria-label')
            
            if name and url:
                results.append({
                    'place_name': name,
                    'gmaps_url': url
                })
        except Exception as e:
            print(f"   Warning: Failed to extract data for place {idx}: {e}")
            continue
    
    print(f"Successfully extracted {len(results)} places.")
    return results


def save_to_csv(data, output_file):
    """
    Saves scraped data to CSV file.
    
    Args:
        data (list): List of place dictionaries
        output_file (str): Path to output CSV file
        
    Returns:
        bool: True if save successful
    """
    try:
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nData saved successfully to: {output_file}")
        print(f"Total places: {len(df)}")
        return True
    except Exception as e:
        print(f"Error saving data: {e}")
        return False


def scrape_gmaps_places(query, headless=False):
    """
    Main scraping function that orchestrates the entire process.
    
    Args:
        query (str): Search query for Google Maps
        headless (bool): Run browser in headless mode
        
    Returns:
        pd.DataFrame: Scraped places data, or None if failed
    """
    print(f"Starting Google Maps scraper for: '{query}'")
    print("=" * 60)
    
    try:
        # Browser lifecycle is owned by browser_session (always closed on exit)
        with browser_session(headless=headless) as page:
            # Navigate to Google Maps
            if not navigate_to_maps(page):
                return None
            
            # Perform search
            if not perform_search(page, query):
                return None
            
            # Scroll to load all results
            total_places = scroll_results_panel(page)
            
            if total_places == 0:
                print("No places found for the given query.")
                return None
            
            # Extract place data
            results = extract_place_data(page)
            
            if not results:
                print("Failed to extract any place data.")
                return None
            
            # Save to CSV
            if save_to_csv(results, OUTPUT_FILE):
                df = pd.DataFrame(results)
                print("\nPreview of scraped data:")
                print(df.head())
                return df
            
            return None
    
    except Exception as e:
        print(f"Error during scraping: {e}")
        return None


if __name__ == "__main__":
    # Run scraper
    # Set headless=True to run without visible browser window
    result = scrape_gmaps_places(SEARCH_QUERY, headless=False)
    
    if result is not None:
        print("\nScraping completed successfully!")
    else:
        print("\nScraping failed. Please check the errors above.")