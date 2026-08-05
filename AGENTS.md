# AGENTS.md — Karawang Tourism Scraper

Project rules for AI coding agents working in this repo. These rules are strict by design:
they protect the frozen data contracts and the fragile scraping pipeline. They never weaken
global/system rules (safety, approvals, secrets, tool gates) — those always win.

## 1. What this project is

Python 3.8+ scraping & data-processing tool for Karawang (Indonesia) tourism data, sourced
from Google Maps. **7 scripts, 3 pipelines**:

| Pipeline                | Scripts                                                                   | Entry points                                                           |
| ----------------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Main (places + reviews) | `gmaps_scraper.py` → `gmaps_reviews_scraper.py` → `process_gmaps_data.py` | `scrape_gmaps_places()`, `scrape_all_reviews()`, `process_all_files()` |
| Image                   | `gmaps_image_scraper.py` → `merge_images_to_final.py`                     | `scrape_images_only()`, `merge_data()`                                 |
| Recommendation          | `prepare_content_based.py` → `recommender_engine.py`                      | `process_data()`, `get_recommendations()`                              |

Dependencies: Playwright (Chromium), pandas, numpy, scikit-learn, nltk, Sastrawi, requests.

## 2. Non-negotiable rules

1. **Data contracts are FROZEN.** Schemas, column order, encodings, placeholder,
   anonymization format, output filenames, and row counts are locked in
   `docs/baseline.md`. Never change them without an explicit user request **and** a
   corresponding update to `docs/baseline.md`. Read `docs/baseline.md` before touching any
   schema/output/format logic.
2. **`data/` is never committed.** It holds scraped datasets, review JSON, and experiment
   files. Same for `venv/`, `.pi/`, `.DS_Store`, `.env`, `nltk_data/`, `__pycache__/`.
3. **Path resolution stays in `src/config.py`.** Every script resolves paths from
   `BASE_DIR`, so it runs from any working directory. Do not reintroduce
   `os.getcwd()`-based paths.
4. **Keep the `RAW_CSV_NAME_COMPATIBILITY` contract.** `gmaps_scraper.py` names its output
   from the query slug; consumers use the canonical `karawang_places_list.csv`. If
   `SEARCH_QUERY` changes, add a new mapping entry — do not rename files ad hoc.
5. **Pipeline order is fixed:** 1 → 2 → 3 (main), 4 → 5 (images), 6 → 7 (recommendations).
   Never reorder or skip validation steps.
6. **No import-time side effects.** `recommender_engine.py` runs its whole workflow
   (load → TF-IDF → similarity → demo) from `main()`, and `prepare_content_based.py`
   checks/downloads NLTK `punkt` inside `process_data()`. Importing any module must
   stay side-effect free. Do not reintroduce import-time execution or lazy downloads
   without an explicit request.
7. **No determinism assumptions.** `process_gmaps_data.py` uses stratified sampling without
   a seed — output varies between runs by design. Never "fix" this silently.

## 3. Engineering workflow (strict)

- Follow: understand → inspect → plan → execute → verify → summarize.
- **Inspect before edit:** read the target script fully, plus `docs/baseline.md` and
  `README.md` sections relevant to the change.
- Match existing style: module docstrings, `Args:`/`Returns:` sections, constants
  centralized in `src/config.py`.
- Keep changes small, scoped, reversible. One concern per change. No unrelated cleanup.
- **Verify after change:** for non-network scripts, run the entry point on real data
  (e.g. `python src/process_gmaps_data.py`, `python src/merge_images_to_final.py`).
  For network scripts (`*_scraper.py`), confirm with the user before running — they launch
  a visible Playwright browser, hit Google Maps, and are subject to rate limits.
- No test suite exists yet. If you add logic, write it as functions that are testable
  without network access; adding a small `tests/` harness is welcome but keep it optional
  and dependency-light.
- Do not modify `docs/baseline.md` row counts or schemas from observation of newer data;
  that file is a snapshot/contract, not a live report.

## 4. Safety (strict)

- **Respect Google Maps ToS.** No aggressive retry loops, no anti-bot evasion, no
  credential reuse, no scraping beyond configured limits (`MAX_REVIEWS_PER_PLACE`,
  timeouts in `src/config.py`).
- **Personal data:** review content is anonymized per the frozen format. Never weaken or
  skip anonymization.
- Approval required (even locally): deleting `data/` content, `git reset --hard` /
  `git clean -fd`, `git push`, any change to credentials/auth, any release/deploy.
- Never hardcode or commit API keys, tokens, or personal data. Audit staged diffs before
  any commit.

## 5. Git rules

- Before committing: check `git status` — `data/`, `venv/`, `.pi/`, `nltk_data/`,
  `__pycache__/`, `.DS_Store` must not appear in the staged diff.
- Keep commits scoped to one pipeline or concern.
- No secret patterns (`api_key`, `token`, `password`, `.env` content) in diffs.

## 6. Project map

```
src/config.py                       # all paths, constants, compatibility mapping
src/gmaps_scraper.py                # [network] place list → data/raw/karawang_places_list.csv
src/gmaps_reviews_scraper.py        # [network] reviews → data/reviews_json/<place>.json
src/process_gmaps_data.py           # JSON → data/processed/karawang_tourism_final.csv
src/gmaps_image_scraper.py          # [network] images → data/processed/karawang_place_images.csv
src/merge_images_to_final.py        # final + images → ..._with_images.csv
src/prepare_content_based.py        # reviews_json/V1 → ..._content_based.csv
src/recommender_engine.py           # content-based → console demo / get_recommendations()
docs/baseline.md                    # FROZEN contracts & snapshot counts
```

Key gotchas:

- `data/reviews_json/` (root, written by scraper) vs `data/reviews_json/V1/` (archived,
  read by `prepare_content_based.py`) are **not interchangeable**.
- `data/reviews/` (root, `V1/`, `V2/`) are legacy exports — not used by scripts; do not
  wire them into the pipeline.
- `data/processed/percobaan/` = experiments — not used by pipeline; do not consume.
- Scraper scripts default to a **visible** browser; `headless=True` is opt-in at the entry
  point.

## 7. Communication

- Reply in the same language as the user (Indonesian ↔ English).
- Concise, high-signal summaries: what changed, files touched, verification done,
  remaining risks.
