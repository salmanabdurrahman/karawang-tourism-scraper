# Karawang Tourism Scraper

This project is a web scraping and data processing tool designed to collect, process, and analyze tourism-related data for Karawang, Indonesia. It leverages Google Maps data to gather information about tourist destinations and user reviews, and organizes the data for further analysis or application use.

The repository contains **7 scripts** organized into **3 pipelines**: a main review pipeline, an image pipeline, and a content-based recommendation pipeline. The data contracts (schemas, column order, placeholder, anonymization format) are frozen; see [docs/baseline.md](docs/baseline.md) for the full baseline and contract reference.

## Project Structure

```
karawang-tourism-scraper/
├── data/
│   ├── processed/           # Active processed datasets (CSV)
│   │   └── percobaan/       # Experiment datasets (not used by pipeline)
│   ├── raw/                 # Raw place list (CSV)
│   ├── reviews_json/        # Scraped reviews (JSON), one file per place
│   │   └── V1/              # Archived full review set (source for recommendation prep)
│   ├── reviews/             # Legacy CSV exports (root, V1, V2) — not used by scripts
├── src/
│   ├── config.py                       # Centralized paths & constants
│   ├── utils.py                        # Pure shared text/domain helpers
│   ├── browser.py                      # Shared Playwright browser lifecycle
│   ├── gmaps_scraper.py             # Scraper for Google Maps place list
│   ├── gmaps_reviews_scraper.py     # Scraper for Google Maps reviews (JSON)
│   ├── gmaps_image_scraper.py       # Scraper for place images
│   ├── process_gmaps_data.py        # Review processing → final dataset
│   ├── merge_images_to_final.py     # Merge images into final dataset
│   ├── prepare_content_based.py     # NLP preparation for recommendations
│   └── recommender_engine.py        # TF-IDF + similarity recommendation demo
├── docs/
│   └── baseline.md          # Baseline & contract freeze reference
├── requirements.txt         # Python dependencies (version ranges)
├── requirements.lock        # Exact dependency snapshot (pip freeze)
├── requirements-dev.txt     # Dev tooling (ruff linter)
├── pyproject.toml           # Ruff linter configuration
├── .gitignore              # Git ignore rules
└── README.md               # Project documentation
```

## Pipelines

### Main Pipeline (places + reviews)

```txt
src/gmaps_scraper.py ──> data/raw/karawang_places_list.csv
src/gmaps_reviews_scraper.py ──> data/reviews_json/<place_name>.json
src/process_gmaps_data.py ──> data/processed/karawang_tourism_final.csv
```

Scrape the place list from Google Maps, scrape reviews for each place into one JSON file per place, then process all JSON files into a single cleaned, deduplicated, and stratified-sampled CSV dataset.

### Image Pipeline

```txt
src/gmaps_image_scraper.py ──> data/processed/karawang_place_images.csv
src/merge_images_to_final.py ──> data/processed/karawang_tourism_final_with_images.csv
```

Scrape one image URL per place, then merge the image URLs into the final review dataset (matched on normalized place name; missing images get the placeholder).

### Recommendation Pipeline

```txt
src/prepare_content_based.py ──> data/processed/karawang_places_content_based.csv
src/recommender_engine.py ──> console output (demo + get_recommendations())
```

Build a per-place corpus (category + attributes + description + all review texts) processed through case folding → tokenizing → stopword removal → stemming, then compute TF-IDF vectors and cosine similarity to produce top-10 place recommendations.

## Script Reference

| #   | Script                         | Input                                                      | Output                                                  | Network          | Entry point                           |
| --- | ------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------- | ---------------- | ------------------------------------- |
| 1   | `src/gmaps_scraper.py`         | search query `"Tempat Wisata di Karawang"`                 | `data/raw/karawang_places_list.csv`                     | yes (Playwright) | `scrape_gmaps_places()`               |
| 2   | `src/gmaps_reviews_scraper.py` | `data/raw/karawang_places_list.csv`                        | `data/reviews_json/<name>.json`                         | yes (Playwright) | `scrape_all_reviews()`                |
| 3   | `src/process_gmaps_data.py`    | `data/reviews_json/*.json`                                 | `data/processed/karawang_tourism_final.csv`             | no               | `process_all_files()`                 |
| 4   | `src/gmaps_image_scraper.py`   | `data/raw/karawang_places_list.csv`                        | `data/processed/karawang_place_images.csv`              | yes (Playwright) | `scrape_images_only()`                |
| 5   | `src/merge_images_to_final.py` | `karawang_tourism_final.csv` + `karawang_place_images.csv` | `data/processed/karawang_tourism_final_with_images.csv` | no               | `merge_data()`                        |
| 6   | `src/prepare_content_based.py` | `data/reviews_json/V1/*.json`                              | `data/processed/karawang_places_content_based.csv`      | no               | `process_data()`                      |
| 7   | `src/recommender_engine.py`    | `data/processed/karawang_places_content_based.csv`         | console (demo + `get_recommendations()`)                | no               | `main()` (import has no side effects) |

## Output Dependencies

- Scripts 2 and 4 consume the place list produced by script 1 (`data/raw/karawang_places_list.csv`).
- Script 5 requires the outputs of scripts 3 and 4: `karawang_tourism_final.csv` and `karawang_place_images.csv`.
- Script 6 consumes review JSON from `data/reviews_json/V1/`, not the root folder.
- Script 7 requires the output of script 6 (`karawang_places_content_based.csv`).

## Data Folders

| Folder                               | Content                                  | Used by                   |
| ------------------------------------ | ---------------------------------------- | ------------------------- |
| `data/raw/`                          | Place list (`karawang_places_list.csv`)  | scripts 2 & 4             |
| `data/reviews_json/` (root)          | JSON files written by script 2           | script 3                  |
| `data/reviews_json/V1/`              | Archived full review set (55 files)      | script 6                  |
| `data/reviews/` (root, `V1/`, `V2/`) | Legacy CSV exports from an older scraper | none (kept for reference) |
| `data/processed/`                    | Active datasets                          | scripts 5 & 7 + consumers |
| `data/processed/percobaan/`          | Experiment datasets                      | none                      |

**`reviews_json` root vs `V1`:** the reviews scraper writes to the root (`data/reviews_json/`), which is what `process_gmaps_data.py` reads. The `V1` subfolder is a manual archive of the full review set and is read only by `prepare_content_based.py`. They are **not** interchangeable inputs.

**Output naming compatibility:** script 1 names its output from the query slug, but the rest of the pipeline uses the canonical name `karawang_places_list.csv`. `src/config.py` holds a compatibility mapping (`RAW_CSV_NAME_COMPATIBILITY`) that resolves the slug-generated name to the canonical file, so script 1 writes the file the pipeline actually consumes. Add a new entry to the mapping if `SEARCH_QUERY` changes.

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
   For an exact, tested dependency snapshot instead of version ranges, use the lock file:
   ```bash
   pip install -r requirements.lock
   ```
4. **Install dev tooling (linter):**
   ```bash
   pip install -r requirements-dev.txt
   ```
5. **Install Playwright Chromium browser:**
   ```bash
   playwright install chromium
   ```
   Note: the scraping scripts use Playwright with a visible (non-headless) browser by default. Set `headless=True` in the entry-point call if you want to run without a window.

## Usage

All scripts resolve paths from the repository location (`src/config.py`, based on the module file location), so they can be run from the repository root **or from any other working directory**.

```bash
# 1. Scrape place list (Google Maps, network)
python src/gmaps_scraper.py

# 2. Scrape reviews per place (Google Maps, network; skips existing JSON)
python src/gmaps_reviews_scraper.py

# 3. Process reviews into final dataset
python src/process_gmaps_data.py

# 4. Scrape one image URL per place (Google Maps, network)
python src/gmaps_image_scraper.py

# 5. Merge images into final dataset
python src/merge_images_to_final.py

# 6. Prepare content-based dataset (NLP, downloads NLTK punkt on first run)
NLTK_DISABLE_IMPORT_SECURITY=1 python src/prepare_content_based.py

# 7. Run recommendation demo (load → TF-IDF → similarity → demo)
python src/recommender_engine.py
```

Pipeline order matters: run 1 → 2 → 3 for the main pipeline, 4 → 5 for images, 6 → 7 for recommendations.

## Testing

The test suite in `tests/` uses only the Python standard library (`unittest`),
so no extra dependencies are needed. It never hits the network: scraper entry
points are smoke-tested on their missing-input paths, extraction logic runs
against a fake Playwright page, and the processing pipelines run end-to-end on
small fixtures inside temporary directories.

```bash
# from the repository root
python -m unittest discover -s tests

# from any other working directory
python -m unittest discover -s /path/to/karawang-tourism-scraper/tests
```

Contract tests verify the frozen CSV/JSON schemas, column order, and encodings
from `docs/baseline.md`, plus the raw-CSV-name compatibility mapping. A
baseline test compares the real datasets in `data/` against the frozen row
counts when the files are present (it skips automatically when they are not).
Note: the suite sets `NLTK_DISABLE_IMPORT_SECURITY=1` at import time because
NLTK 3.10+ blocks venv imports when the virtualenv lives inside the repo root.
The same env var is needed when running `src/prepare_content_based.py` from
the repository root (step 6 in Usage).

Linting and formatting are done with [ruff](https://docs.astral.sh/ruff/) (configured in
`pyproject.toml`):

```bash
# from the repository root
ruff check src tests
ruff format --check src tests
```

To apply the formatter after editing:

```bash
ruff format src tests
```

The linter only enforces error/import-sorting rules (`E`, `F`, `I`); the
scrapers intentionally keep broad `except Exception` fallbacks and
`datetime.now()`-based relative time conversion, which stricter rule sets
would flag.

CI (GitHub Actions, `.github/workflows/ci.yml`) runs lint + compile + the full
test suite on every push to `main` and on pull requests, installing from
`requirements.lock` for reproducibility.

## Notes

- All paths and shared constants (timeouts, review limits, placeholder image URL, output folders, Google Maps selectors) live in `src/config.py`. Input files are validated before each script starts.
- The three scraper scripts share a Playwright browser lifecycle helper (`src/browser.py`): a context manager that always closes the browser/context on both success and failure. Navigation, extraction, scrolling, and persistence stay separate functions inside each scraper.
- `src/utils.py` holds the pure text/domain helpers (text cleaning, anonymization, relative-time parsing, name normalization, NLP steps). Importing it has no side effects; nltk/Sastrawi are imported lazily on first use.
- The `data/` and `venv/` folders are excluded from version control via `.gitignore`.
- `process_gmaps_data.py` samples up to 150 reviews per place with stratified sampling per rating (no random seed → output is not deterministic between runs).
- `recommender_engine.py` runs its workflow (load → TF-IDF → similarity → demo) from `main()`; importing the module has no side effects.
- `prepare_content_based.py` checks/downloads NLTK `punkt` at processing start (not at import time); Sastrawi is initialized lazily on first use.
- Make sure you have a stable internet connection for scraping Google Maps.
- Review scraping may be subject to Google Maps rate limits and anti-bot measures.
- See `docs/baseline.md` for the frozen data contracts: CSV/JSON schemas, column order, encodings, placeholder, anonymization format, and dataset row counts (snapshot).

## Requirements

- Python 3.8+
- [Playwright](https://playwright.dev/python/) (Chromium) — browser automation for the scraper scripts
- pandas — CSV/DataFrame processing
- numpy — vectorized math in the recommendation engine
- scikit-learn — TF-IDF vectorization and cosine similarity
- nltk — tokenization (`punkt`) in the NLP pipeline
- Sastrawi — Indonesian stopword removal and stemming
- ruff (dev only) — linter, see `requirements-dev.txt`

Dependency strategy: `requirements.txt` declares minimum versions capped at
the next major version (e.g. `pandas>=2.3.0,<4.0.0`), so pip resolves within
the same major line as the tested snapshot. `requirements.lock` pins the exact
tested runtime snapshot (generated with `pip freeze`, dev tooling excluded).
Regenerate the lock file only when intentionally changing dependencies — never
as part of an unrelated change.

## License

This project is for educational and research purposes. Please respect the terms of service of any third-party data sources.

## Acknowledgements

- Google Maps for data source
- Playwright for browser automation
- pandas for data processing
