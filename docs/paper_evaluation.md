# Reproducing Paper Tables 7–12

`src/generate_paper_tables.py` generates current values for Tables 7–12 from the
prepared content-based dataset. It does not scrape Google Maps and does not
rebuild the corpus; run `src/prepare_content_based.py` first when the V1 JSON
source changes.

## Current evaluation profile

The current paper profile is fixed to the 55-place snapshot:

- input: `data/processed/karawang_places_content_based.csv`;
- destination documents: 55;
- raw V1 reviews reported by preparation: 10,268;
- review texts retained after deduplication/capping: 5,202;
- TF-IDF: word features, unigram + bigram, `min_df=2`, `max_df=0.85`,
  `max_features=10000`, `sublinear_tf=False`;
- similarity: cosine similarity through `linear_kernel`;
- sample TF-IDF keywords: `air`, `alam`, `pantai`, `sejarah`, `curug`, `pasir`,
  `wahana`, `kolam`;
- keyword cutoffs: `kolam renang` K=8, `curug` K=6, `taman` K=7.

`sublinear_tf=False` follows the paper's linear TF formula. Scikit-learn IDF
smoothing and L2 normalization remain explicit implementation details of this
repository's TF-IDF profile.

## Step-by-step usage

From repository root, inside the project virtual environment:

```bash
# 1. Build/update the prepared corpus from the V1 JSON files.
NLTK_DISABLE_IMPORT_SECURITY=1 python src/prepare_content_based.py

# 2. Generate Tables 7–12.
NLTK_DISABLE_IMPORT_SECURITY=1 python src/generate_paper_tables.py
```

The evaluator validates the 55-place profile and all five frozen sample
places before calculating tables. It prints progress and writes these files to
`data/processed/paper_evaluation/`:

```text
table_7_tfidf.csv
table_8_cosine_similarity.csv
table_9_kolam_renang.csv
table_10_curug.csv
table_11_taman.csv
table_12_precision_summary.csv
paper_evaluation_metadata.json
paper_tables.md
```

`paper_tables.md` is copy-friendly for the manuscript. CSV files retain six
decimal places for audit/recalculation; round to three decimals in the paper
tables. `paper_evaluation_metadata.json` records the input SHA-256, profile,
cutoffs, and sample indices. A small set of cleaned model names is rendered
with manuscript-friendly aliases (for example, `Goa Dayeuh, Selatan` appears as
`Goa Dayeuh`); numeric calculation still uses the underlying current model
names.

## Custom input/output paths

Use a frozen copy of the prepared CSV when revising the manuscript:

```bash
NLTK_DISABLE_IMPORT_SECURITY=1 python src/generate_paper_tables.py \
  --input-file /path/to/frozen/karawang_places_content_based.csv \
  --output-dir /path/to/paper-evaluation
```

The input file must contain the prepared `tags_corpus` and place metadata. Do
not use the legacy `data/reviews/` exports or the root
`data/reviews_json/` folder as evaluator input.

## What each step calculates

1. **Load and clean names** — reads one prepared corpus document per place.
2. **Fit TF-IDF** — fits one vectorizer over all 55 place documents. The fitted
   vectorizer is reused for every keyword query so IDF weights remain fixed.
3. **Table 7** — selects the five paper sample places and prints the eight
   requested TF-IDF feature columns.
4. **Table 8** — selects the same five rows from the full cosine-similarity
   matrix.
5. **Tables 9–11** — transforms each keyword with the shared Indonesian NLP
   pipeline, ranks places, keeps the configured cutoff, and applies the frozen
   manual relevance labels.
6. **Table 12** — counts `Sesuai`/`Tidak Sesuai` per query and computes the
   unweighted macro precision.

The relevance labels are manual judgments for the current 55-place snapshot,
not an automatic classifier. The evaluator fails if the input does not contain
exactly 55 destination documents, if the five sample places are incomplete, or
if a query cannot produce its fixed cutoff. If corpus content, place set,
query, or cutoff changes, review `PAPER_QUERIES` in
`src/generate_paper_tables.py` before using new precision values.

## Expected current summary

With the current prepared 55-place snapshot, expected summary is:

```text
kolam renang  8/8  = 100.00%
curug         4/6  =  66.67%
taman         7/7  = 100.00%
macro average          88.89%
```

These values are valid only when input corpus, TF-IDF parameters, dependency
versions, query cutoffs, and manual labels match the profile above. They should
not be mixed with the original 57-place/84.13% paper snapshot.

## Verification

Run evaluator tests without network access:

```bash
NLTK_DISABLE_IMPORT_SECURITY=1 python -m unittest discover -s tests
ruff check src tests
ruff format --check src tests
python -m compileall -q src tests
```
