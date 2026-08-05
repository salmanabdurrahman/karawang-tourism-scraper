# Baseline & Contract Freeze

This document locks current behavior, output contracts, and dataset state before refactoring. All numbers are a snapshot at the baseline date. Every refactor phase must be compared against this document.

> **Document status:** this is a **frozen snapshot**. The contracts in sections 2, 4, and 5 (schemas, column order, encodings, row counts, locked rules) are still in force and must not be changed. Behavioral observations that the cleanup refactor has since superseded are marked explicitly in sections 1 and 7; see `README.md` for the current behavior.

> **Freeze rules:** do not change the pipeline order, entry point file names in `src/`, dataset folder/output names, CSV/JSON schemas, column order, placeholder, `user_id` format, or Google Maps selectors without separate tests and approval.

## 1. Actual Pipeline

Seven scripts, three flows:

```txt
MAIN PIPELINE
  gmaps_scraper.py ──> data/raw/karawang_places_list.csv
  gmaps_reviews_scraper.py ──> data/reviews_json/*.json
  process_gmaps_data.py ──> data/processed/karawang_tourism_final.csv

IMAGE PIPELINE
  gmaps_image_scraper.py ──> data/processed/karawang_place_images.csv
  merge_images_to_final.py ──> data/processed/karawang_tourism_final_with_images.csv
  (input: karawang_tourism_final.csv + karawang_place_images.csv)

RECOMMENDATION PIPELINE
  prepare_content_based.py ──> data/processed/karawang_places_content_based.csv
  recommender_engine.py ──> console output (recommendation demo)
  (input: karawang_places_content_based.csv)
```

| #   | Script                         | Input                                                                     | Output                                                  | Network          | Entry                             |
| --- | ------------------------------ | ------------------------------------------------------------------------- | ------------------------------------------------------- | ---------------- | --------------------------------- |
| 1   | `src/gmaps_scraper.py`         | query `"Tempat Wisata di Karawang"`                                       | `data/raw/<slug>_places_list.csv`                       | yes (Playwright) | `scrape_gmaps_places()`           |
| 2   | `src/gmaps_reviews_scraper.py` | `data/raw/karawang_places_list.csv`                                       | `data/reviews_json/<name>.json`                         | yes (Playwright) | `scrape_all_reviews()`            |
| 3   | `src/process_gmaps_data.py`    | `data/reviews_json/*.json`                                                | `data/processed/karawang_tourism_final.csv`             | no               | `process_all_files()`             |
| 4   | `src/gmaps_image_scraper.py`   | `data/raw/karawang_places_list.csv`                                       | `data/processed/karawang_place_images.csv`              | yes (Playwright) | `scrape_images_only()`            |
| 5   | `src/merge_images_to_final.py` | `data/processed/karawang_tourism_final.csv` + `karawang_place_images.csv` | `data/processed/karawang_tourism_final_with_images.csv` | no               | `merge_data()`                    |
| 6   | `src/prepare_content_based.py` | `data/reviews_json/v1/*.json`                                             | `data/processed/karawang_places_content_based.csv`      | no               | `process_data()`                  |
| 7   | `src/recommender_engine.py`    | `data/processed/karawang_places_content_based.csv`                        | console (demo + `get_recommendations()`)                | no               | import-time (no `__main__` guard) |

Important notes:

- Scripts 1–2 use relative paths (`data/...`), **must be run from the project root**. Scripts 3–7 use `BASE_DIR` based on file location, safe from any working directory.
- Script 7 (`recommender_engine.py`) has no `if __name__ == "__main__"`: the whole process (data load, TF-IDF, similarity, demo) runs at import time.
- Script 6 downloads/checks NLTK `punkt` and initializes Sastrawi at import time (not inside the processing function).

**Refactor status:** the observations above describe the pre-refactor state and no longer hold. Current behavior after the cleanup (phases 2–6):

| Baseline observation                                                  | Current behavior                                                                                 |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Scripts 1–2 use relative paths; run from project root only            | All scripts resolve paths via `src/config.py` (`BASE_DIR`) — runnable from any working directory |
| Script 7 runs its whole workflow at import time                       | Workflow moved into `main()`; importing the module is side-effect free                           |
| Script 6 downloads NLTK `punkt` / initializes Sastrawi at import time | `punkt` check/download moved inside `process_data()`; Sastrawi initialized lazily on first use   |

## 2. CSV & JSON Schemas

### 2.1 Active outputs (used by the pipeline)

| File                                                    | Column order                                                                                                                                                                                                   |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data/raw/karawang_places_list.csv`                     | `place_name`, `gmaps_url`                                                                                                                                                                                      |
| `data/processed/karawang_place_images.csv`              | `place_name`, `image_url`                                                                                                                                                                                      |
| `data/processed/karawang_places_content_based.csv`      | `place_name`, `place_category`, `place_address`, `place_avg_rating`, `total_reviews_scraped`, `tags_corpus`                                                                                                    |
| `data/processed/karawang_tourism_final.csv`             | `user_id`, `user_rating`, `review_text`, `review_time`, `place_name`, `place_description`, `place_category`, `place_attributes`, `place_address`, `place_total_reviews_gmaps`, `place_avg_rating`              |
| `data/processed/karawang_tourism_final_with_images.csv` | `user_id`, `user_rating`, `review_text`, `review_time`, `place_name`, `image_url`, `place_description`, `place_category`, `place_attributes`, `place_address`, `place_total_reviews_gmaps`, `place_avg_rating` |

Encoding: `karawang_tourism_final.csv` and `karawang_tourism_final_with_images.csv` are written with `utf-8-sig` (BOM `\ufeff` in the first column). Other CSV files use `utf-8` without BOM.

### 2.2 Review JSON (main processing input)

```json
{
  "place_info": {
    "name": "string",
    "category": "string",
    "avg_rating": "string (e.g. '4.3')",
    "total_reviews_text": "string (e.g. '2.035 ulasan')",
    "address": "string",
    "description": "string",
    "attributes": "string, joined by ' | ', each item 'label: value'"
  },
  "reviews": [
    {
      "user_name": "string",
      "rating": "int 0-5",
      "text": "string",
      "time": "relative time string"
    }
  ]
}
```

JSON encoding: `utf-8`, `ensure_ascii=False`, `indent=4`. Empty reviews are filtered automatically during extraction.

### 2.3 Legacy outputs (not produced by current scripts)

`data/reviews/*.csv` are old-version review exports (from the old CSV scraper script, no longer in the repo):

| Folder                      | Column order                                                                                                                                                           |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data/reviews/*.csv` (root) | `place_name`, `place_category`, `place_avg_rating_raw`, `place_address_raw`, `place_attributes_raw`, `user_name`, `user_rating_raw`, `review_text`, `review_time`      |
| `data/reviews/V1/*.csv`     | `place_name`, `place_category`, `place_avg_rating`, `user_name`, `user_rating`, `review_text`, `review_time`                                                           |
| `data/reviews/V2/*.csv`     | `place_name`, `place_category`, `place_avg_rating`, `place_address`, `place_description`, `place_attributes`, `user_name`, `user_rating`, `review_text`, `review_time` |

## 3. Dataset Map & Folder Differences

| Folder                      | Content                                                                                          | Status                                   |
| --------------------------- | ------------------------------------------------------------------------------------------------ | ---------------------------------------- |
| `data/raw/`                 | `karawang_places_list.csv` (57 places)                                                           | used by scripts 2 & 4                    |
| `data/reviews_json/` (root) | 2 JSON files, **0 reviews** (Taruma Leisure Waterpark, Wonderland Adventure Waterpark Galuh Mas) | **stale** — leftover from the move to V1 |
| `data/reviews_json/V1/`     | 55 JSON files, 10.268 reviews total                                                              | main processing source                   |
| `data/reviews/` (root)      | 13 CSV files, 94.114 rows, `*_raw` schema                                                        | legacy, not used by scripts              |
| `data/reviews/V1/`          | 55 CSV files, 1.899 rows, schema without address/desc/attributes                                 | legacy                                   |
| `data/reviews/V2/`          | 25 CSV files, 4.043 rows, full schema                                                            | legacy; subset of V1 places              |
| `data/processed/`           | 4 active datasets                                                                                | used by the pipeline                     |
| `data/processed/percobaan/` | 5 experiments (`v1`–`v5`)                                                                        | experiments, not used                    |

Key differences:

- **JSON root vs V1**: the root has only 2 files with no reviews; V1 has 55 files with reviews. `process_gmaps_data.py` reads the root folder, so the existing `karawang_tourism_final.csv` is an **old snapshot** from before the data was moved to V1. Re-running script 3 today would produce ~0 rows.
- **V1 vs V2 (reviews CSV)**: V2 = a subset of V1 places (25 of 55) with a more complete schema (has `place_address`, `place_description`, `place_attributes`).
- **percobaan v1–v5**: `v1`–`v2` 3.898 rows with the old schema (`user_name`), `v3`–`v4` 5.202 rows with the `user_name` schema, `v5` 5.202 rows with the `user_id` schema — `v5` has the same schema as the current `karawang_tourism_final.csv`.
- **Place names are inconsistent across folders**: the raw list uses a format without parentheses, while some V1 JSON files use a format with parentheses/commas (e.g. `Az-Zahra Galuh Mas (Pelatihan Manasik Haji)` vs `Az-Zahra Galuh Mas Pelatihan Manasik Haji`). Name matching uses normalization (lowercase + remove non-alphanumeric) in script 5.

## 4. Baseline Row Count

| Dataset                                                  | Data rows                              |
| -------------------------------------------------------- | -------------------------------------- |
| `data/raw/karawang_places_list.csv`                      | 57                                     |
| `data/processed/karawang_place_images.csv`               | 57                                     |
| `data/processed/karawang_places_content_based.csv`       | 55                                     |
| `data/processed/karawang_tourism_final.csv`              | 5.202                                  |
| `data/processed/karawang_tourism_final_with_images.csv`  | 5.202                                  |
| `data/processed/percobaan/karawang_tourism_final_v1.csv` | 3.898                                  |
| `data/processed/percobaan/karawang_tourism_final_v2.csv` | 3.898                                  |
| `data/processed/percobaan/karawang_tourism_final_v3.csv` | 5.202                                  |
| `data/processed/percobaan/karawang_tourism_final_v4.csv` | 5.202                                  |
| `data/processed/percobaan/karawang_tourism_final_v5.csv` | 5.202                                  |
| `data/reviews/*.csv` (13 files)                          | 94.114 (min 351 / max 11.019 per file) |
| `data/reviews/V1/*.csv` (55 files)                       | 1.899 (min 2 / max 40 per file)        |
| `data/reviews/V2/*.csv` (25 files)                       | 4.043 (min 3 / max 200 per file)       |
| `data/reviews_json/*.json` (2 files)                     | 0 reviews                              |
| `data/reviews_json/V1/*.json` (55 files)                 | 10.268 reviews                         |

Distinct places: raw list 57; final dataset 55. Two places have no review JSON (no file in `reviews_json/V1`): `Wonderland Adventure Waterpark Galuh Mas Karawang` and `Taman wirasena walahar` (the latter still has 2 legacy CSV rows in `data/reviews/V1/`).

## 5. Locked Contracts

- Reviews scraper cap: `MAX_REVIEWS_PER_PLACE = 400` (+100 scroll buffer).
- Processing sampling: 150 reviews per place, stratified per rating (5 buckets) + fill shortage from overflow + random shuffle. **No random seed → output is not deterministic**.
- Anonymization: `md5(user_name.strip().lower())[:10]`; empty → `"anonymous"`.
- Relative time conversion → `YYYY-MM-DD` (based on `datetime.now()` at run time).
- Image placeholder: `https://via.placeholder.com/400x300?text=No+Image`.
- TF-IDF: `analyzer='word'`, `ngram_range=(1,2)`, `min_df=2`, `max_df=0.85`, `max_features=10000`, `sublinear_tf=True`; dense similarity `linear_kernel`; top-10 recommendation results.
- `get_recommendations()` result format: DataFrame with columns `place_name`, `place_category`, `place_avg_rating`, `similarity_score`, sorted by similarity descending.

## 6. Manual Entry Point Checklist

One-time prerequisites (run from the repository root, inside the project venv): `python3 -m venv venv`, `source venv/bin/activate`, `pip install -r requirements.txt` (or `-r requirements.lock`), `playwright install chromium`.

| Step | Command                                                              | Network | Expected output                                                             |
| ---- | -------------------------------------------------------------------- | ------- | --------------------------------------------------------------------------- |
| 0    | `python src/gmaps_scraper.py`                                        | yes     | `data/raw/karawang_places_list.csv` (actual file name; see notes)           |
| 1    | `python src/gmaps_reviews_scraper.py`                                | yes     | JSON per place in `data/reviews_json/`; existing places are skipped         |
| 2    | `python src/process_gmaps_data.py`                                   | no      | `data/processed/karawang_tourism_final.csv` + rating summary in console     |
| 3    | `python src/gmaps_image_scraper.py`                                  | yes     | `data/processed/karawang_place_images.csv` (57 rows)                        |
| 4    | `python src/merge_images_to_final.py`                                | no      | `data/processed/karawang_tourism_final_with_images.csv` (5.202 rows)        |
| 5    | `NLTK_DISABLE_IMPORT_SECURITY=1 python src/prepare_content_based.py` | no      | `data/processed/karawang_places_content_based.csv` (55 rows) + NLP progress |
| 6    | `python src/recommender_engine.py`                                   | no      | console: TF-IDF, similarity, recommendation demo                            |

Non-network verification (contract + quality gates):

- `python -m unittest discover -s tests` — full suite (schemas, column order, encodings, compatibility mapping, smoke tests).
- `ruff check src tests` and `ruff format --check src tests` — lint + format enforcement (dev-only dependency, see `requirements-dev.txt`).

The `NLTK_DISABLE_IMPORT_SECURITY=1` prefix is required for scripts that
import NLTK when the venv lives inside the repo root (NLTK 3.10+ import
block). The test suite sets it automatically.

## 7. Baseline Anomalies & Risks

Each anomaly keeps its original baseline wording; status notes mark observations that the cleanup refactor has resolved.

1. **Places scraper output name does not match the actual file**: `gmaps_scraper.py` builds the name from the query slug → `tempat_wisata_di_karawang_places_list.csv`, but the actual file is named `karawang_places_list.csv` (read by scripts 2 & 4). Do not rename without updating the configuration in both consuming scripts.
   - **Status: resolved by the refactor.** `src/config.py` now holds `RAW_CSV_NAME_COMPATIBILITY`, which maps the slug-generated name to the canonical `karawang_places_list.csv`. Add a new mapping entry if `SEARCH_QUERY` changes.
2. **`reviews_json` root is stale**: re-running `process_gmaps_data.py` today does not reproduce the existing `karawang_tourism_final.csv`. The 5.202 baseline row count refers to the stored file, not a fresh run.
   - **Status: still valid.** No dataset migration was done; the baseline count still refers to the stored file.
3. **Lowercase `v1` path in `prepare_content_based.py`** (`data/reviews_json/v1`) vs the actual folder `V1`. Works on macOS (case-insensitive), risky on case-sensitive filesystems (Linux/CI).
   - **Status: resolved by the refactor.** `src/config.py` defines `REVIEWS_JSON_V1_DIR` with the correct `V1` casing; scripts use the constant.
4. **`recommender_engine.py` runs its process at import time** — importing the module alone triggers file load, TF-IDF computation, and the demo.
   - **Status: resolved by the refactor.** The workflow now runs from `main()`; importing the module has no side effects.
5. **Random sampling without seed** and **`review_time` depending on the run date** → output diffs between runs are expected; baseline comparison must use columns/schema/count, not identical row contents.
   - **Status: still valid by design.** Sampling stays unseeded (frozen contract).
6. **4 scripts are not tracked by git** (`gmaps_image_scraper.py`, `merge_images_to_final.py`, `prepare_content_based.py`, `recommender_engine.py`), so `git diff` will not detect changes to those files.
   - **Status: still valid.** The refactor added more untracked files (`src/browser.py`, `src/config.py`, `src/utils.py`, `tests/`, `docs/`, ...); commit them once the repository cleanup is accepted.
7. **`data/` is gitignored** (`data`, `venv`) → this document's baseline is the only dataset reference stored in the repo.
   - **Status: still valid.** `.gitignore` also covers `__pycache__/`, `nltk_data/`, `.DS_Store`, `.env`, and `.pi/`.
