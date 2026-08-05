# Recommendation Model

## Scope

Recommendation pipeline uses content-based filtering with Indonesian text
processing, TF-IDF, and cosine similarity. Model output keeps existing result
columns:

```text
place_name, place_category, place_avg_rating, similarity_score
```

Current evaluation snapshot contains 55 destination documents. The V1 source
contains 10,268 raw reviews; preparation retains 5,202 review texts after
cleaning, deduplication, and per-place capping. The current paper tables use
this snapshot and the reproducible evaluation command documented in
`docs/paper_evaluation.md`.

## Preparation flow

`src/prepare_content_based.py` reads `data/reviews_json/V1/*.json` and creates
one corpus per place:

```text
place_category + place_name + place_description + place_attributes + reviews
→ case folding
→ tokenizing
→ word normalization
→ stopword removal
→ stemming
→ tags_corpus
```

Review text is cleaned, deduplicated by normalized user/text signature, and
capped at 150 reviews per place. Selection is deterministic. The cap reduces
review-volume bias and matches the main pipeline's per-place limit without
changing the CSV schema or the raw `total_reviews_scraped` field.

Word normalization includes the paper's examples (`tau → tahu`, `buat →
untuk`) and common Indonesian review abbreviations. Documents and user queries
use the same preprocessing functions.

## Recommendation modes

### Keyword-to-place

Use `get_keyword_recommendations()` for homepage/search behavior:

```python
from recommender_engine import (
    clean_place_names,
    fit_tfidf,
    get_keyword_recommendations,
    load_dataset,
)

df = clean_place_names(load_dataset())
vectorizer, tfidf_matrix, _ = fit_tfidf(df)
results = get_keyword_recommendations(
    "kolam renang",
    vectorizer,
    tfidf_matrix,
    df,
)
```

The query is transformed with the same NLP pipeline, then compared against
each place vector. Places with zero similarity are omitted. Duplicate cleaned display names are
emitted once, keeping the highest-scoring row. Default result limit is 10;
`top_n` can be supplied for a fixed evaluation cutoff. Equal scores retain
corpus row order for deterministic output.

### Place-to-place

Use `get_recommendations()` for a destination detail page. It compares the
selected place vector to all other place vectors and excludes the selected row.
This preserves the existing top-10 result contract.

## TF-IDF configuration

The configured parameters remain centralized in `src/recommender_engine.py`.
The evaluation profile uses standard linear TF (`sublinear_tf=False`) to match
the paper's TF formula. Paper flow is implemented through the shared corpus,
preprocessing, fitted vectorizer, query transformation, and cosine ranking;
scikit-learn IDF smoothing and L2 normalization remain explicit implementation
details.

- word analyzer
- unigram and bigram features
- `min_df=2`
- `max_df=0.85`
- maximum 10,000 features
- standard linear term frequency (`sublinear_tf=False`)

The fitted vectorizer must be retained for keyword queries. Re-fitting a new
vectorizer for each query would produce incompatible IDF weights. The NLTK
`punkt` and `punkt_tab` resources must be available; the preparation entry point
checks and downloads them before building the corpus, while direct query calls
report an actionable error when resources/imports are unavailable.

## Evaluation guidance

Run `src/generate_paper_tables.py` after preparing the V1 corpus. The evaluator
keeps these concerns separate:

1. Keyword search: query vector against place vectors.
2. Similar-place mode: place vector against other place vectors.
3. Paper tables: fixed sample places, query cutoffs, and manual relevance
   labels for the current 55-place snapshot.

For reproducible precision, freeze the corpus file, row order, model
parameters, dependency versions, query list, and binary relevance labels. The
current profile evaluates `kolam renang` at K=8, `curug` at K=6, and `taman` at
K=7. Its macro precision is 88.89%. This is a result for the current snapshot,
not an automatic score for every future model rebuild.

## Known data limitation

The raw list contains 57 places, while `reviews_json/V1` currently contains 55
place files. The two missing places cannot be treated as fully comparable
recommendation documents until their metadata/review source is reconciled.
Do not report paper-level precision for a dataset with a different place set
or corpus snapshot.
