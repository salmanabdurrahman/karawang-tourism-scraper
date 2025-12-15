# Karawang Tourism Scraper

This project is a web scraping and data processing tool designed to collect, process, and analyze tourism-related data for Karawang, Indonesia. It leverages Google Maps data to gather information about tourist destinations and user reviews, and organizes the data for further analysis or application use.

## Project Structure

```
karawang-tourism-scraper/
├── data/
│   ├── processed/           # Processed and cleaned datasets (CSV)
│   ├── raw/                 # Raw data sources (CSV)
│   ├── reviews_json/        # Reviews in JSON format
├── src/
│   ├── gmaps_reviews_scraper.py   # Scraper for Google Maps reviews
│   ├── gmaps_scraper.py           # Scraper for Google Maps place data
│   ├── process_gmaps_data.py      # Data processing and cleaning scripts
├── requirements.txt         # Python dependencies
├── .gitignore              # Git ignore rules
└── README.md               # Project documentation
```

## Features
- **Automated scraping** of Google Maps for places and reviews in Karawang
- **Data processing** to clean and structure raw data
- **Versioned datasets** for tracking data changes and improvements
- **Support for both CSV and JSON formats** for reviews

## Data Folders
- `data/raw/`: Contains the initial list of places to scrape (`karawang_places_list.csv`).
- `data/processed/`: Contains processed datasets.
- `data/reviews_json/`: Contains reviews in JSON format, useful for more complex data analysis or integration.

## Source Code
- `src/gmaps_scraper.py`: Main script for scraping place information from Google Maps.
- `src/gmaps_reviews_scraper.py`: Script for scraping user reviews for each place.
- `src/process_gmaps_data.py`: Script for cleaning, merging, and processing the scraped data into final datasets.

## Installation
1. **Clone the repository:**
   ```bash
   git clone https://github.com/salmanabdurrahman/karawang-tourism-scraper.git
   cd karawang-tourism-scraper
   ```
2. **Set up a virtual environment (recommended):**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Install Playwright browsers:**
   ```bash
   playwright install
   ```

## Usage
1. **Scrape place data:**
   ```bash
   python src/gmaps_scraper.py
   ```
2. **Scrape reviews:**
   ```bash
   python src/gmaps_reviews_scraper.py
   ```
3. **Process and clean data:**
   ```bash
   python src/process_gmaps_data.py
   ```

## Notes
- The `data/` and `venv/` folders are excluded from version control via `.gitignore`.
- Make sure you have a stable internet connection for scraping Google Maps.
- Review scraping may be subject to Google Maps rate limits and anti-bot measures.

## Requirements
- Python 3.8+
- [Playwright](https://playwright.dev/python/)
- pandas

## License
This project is for educational and research purposes. Please respect the terms of service of any third-party data sources.

## Acknowledgements
- Google Maps for data source
- Playwright for browser automation
- pandas for data processing
