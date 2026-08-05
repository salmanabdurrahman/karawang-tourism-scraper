"""
Generate reproducible values for paper Tables 7–12.

This optional evaluation script uses the prepared content-based CSV and the
same TF-IDF/cosine functions as ``recommender_engine.py``. It creates:

    - Table 7: sample TF-IDF matrix
    - Table 8: sample cosine-similarity matrix
    - Tables 9–11: keyword recommendation tables
    - Table 12: precision summary and macro average

The paper evaluation profile uses ``sublinear_tf=False``. Run the preparation
pipeline first, then execute this script from the repository root:

    NLTK_DISABLE_IMPORT_SECURITY=1 python src/generate_paper_tables.py

Outputs are written to ``data/processed/paper_evaluation/`` by default. The
folder is generated data and is not part of the frozen pipeline schemas.

Author: Salman Abdurrahman
Date: 2025
"""

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from config import CONTENT_BASED_FILE, PAPER_EVALUATION_DIR, ensure_dir
from recommender_engine import (
    TFIDF_PARAMS,
    build_similarity,
    clean_place_names,
    fit_tfidf,
    get_keyword_recommendations,
    load_dataset,
)
from utils import clean_name_key

PAPER_PLACE_COUNT = 55
PAPER_TFIDF_COLUMNS = ["air", "alam", "pantai", "sejarah", "curug", "pasir", "wahana", "kolam"]
PAPER_SAMPLE_ALIASES = (
    ("Curug Cigentis", ("Curug Cigentis",)),
    ("Hutan Kertas", ("Hutan Kertas",)),
    ("Pantai Samudera Baru", ("Pantai Samudera Baru",)),
    ("Goa Dayeuh", ("Goa Dayeuh", "Goa Dayeuh, Selatan")),
    ("Green Canyon", ("Green Canyon", "Green Canyon Karawang")),
)

# Display aliases keep generated tables readable and consistent with the
# manuscript while relevance matching continues to use current model names.
PAPER_DISPLAY_NAME_ALIASES = {
    clean_name_key("Goa Dayeuh, Selatan"): "Goa Dayeuh",
    clean_name_key("Taman Jabon Cms"): "Taman Wisata Jabon",
    clean_name_key("Taman Cibonteng"): "Taman Wisata Cibonteng",
    clean_name_key("Taman Mandar"): "Wisata Taman Mandar",
    clean_name_key("Taman Kertabumi By Agung Podomoro Land"): "Taman Kertabumi",
    clean_name_key("Curug Bandung Loji"): "Curug Bandung Loji Karawang",
    clean_name_key("Curug Cilalay -Jonggol"): "Curug Cilalay Karawang Jonggol",
    clean_name_key("Raja Camp (Glamping & Camping Di"): "Raja Camp",
}

RESULT_COLUMNS = ["No", "Nama Wisata", "Skor Kemiripan", "Tingkat Kesesuaian"]
SUMMARY_COLUMNS = ["No", "Kata Kunci", "Jumlah Sesuai", "Jumlah Tidak Sesuai", "Nilai Presisi"]


@dataclass(frozen=True)
class PaperQuery:
    """One manually labeled paper query and its evaluation cutoff."""

    query: str
    top_n: int
    relevant_names: Tuple[str, ...]


# These labels reproduce the manual relevance judgments used in the updated
# paper for the current 55-place snapshot. They are not an automatic semantic
# classifier and must be reviewed if the corpus or place set changes.
PAPER_QUERIES = (
    PaperQuery(
        query="kolam renang",
        top_n=8,
        relevant_names=(
            "Taman Hud-Hud",
            "Kolam Renang Alam Leweung Sereh",
            "Taruma Leisure Waterpark",
            "Waterboom Elmujira",
            "Cipaga Stone Park",
            "Situ Buer",
            "Kampung Turis Water & Adventure Park",
            "Taman Cibonteng",
        ),
    ),
    PaperQuery(
        query="curug",
        top_n=6,
        relevant_names=(
            "Curug Bandung Loji",
            "Curug Cilalay -Jonggol",
            "Curug Cigentis",
            "Green Canyon",
        ),
    ),
    PaperQuery(
        query="taman",
        top_n=7,
        relevant_names=(
            "Taman Jabon Cms",
            "Taman Kota Ade Irma Nasution",
            "Taman Mandar",
            "Taman Galuh Mas",
            "Taman Pelangi Pebayuran",
            "Taman Kertabumi By Agung Podomoro Land",
            "Alun-Alun Kota",
        ),
    ),
)


@dataclass
class PaperTables:
    """All generated paper tables and model metadata."""

    tfidf: pd.DataFrame
    cosine: pd.DataFrame
    keyword_tables: Mapping[str, pd.DataFrame]
    summary: pd.DataFrame
    place_count: int
    feature_count: int
    sample_indices: List[int]
    input_file: str = ""
    input_sha256: str = ""


def validate_paper_profile(df: pd.DataFrame) -> None:
    """Reject inputs that cannot reproduce the current 55-place paper profile."""
    if len(df) != PAPER_PLACE_COUNT:
        raise ValueError(f"Paper profile requires exactly {PAPER_PLACE_COUNT} destination documents; got {len(df)}.")
    if TFIDF_PARAMS.get("sublinear_tf") is not False:
        raise ValueError("Paper profile requires sublinear_tf=False.")


def _paper_display_name(name: str) -> str:
    """Return manuscript-friendly label for a current model place name."""
    return PAPER_DISPLAY_NAME_ALIASES.get(clean_name_key(name), name)


def _validate_sample_indices(df: pd.DataFrame, sample_indices: Sequence[int]) -> List[int]:
    """Validate and normalize positional sample indices."""
    indices = [int(index) for index in sample_indices]
    if not indices:
        raise ValueError("Paper sample places are empty.")
    if any(index < 0 or index >= len(df) for index in indices):
        raise ValueError("Paper sample place index is outside the dataset.")
    return indices


def build_tfidf_table(
    df: pd.DataFrame,
    tfidf_matrix,
    feature_names: Sequence[str],
    sample_indices: Sequence[int],
) -> pd.DataFrame:
    """Build Table 7 from selected places and the requested paper features."""
    indices = _validate_sample_indices(df, sample_indices)
    available = set(feature_names)
    missing = [keyword for keyword in PAPER_TFIDF_COLUMNS if keyword not in available]
    if missing:
        raise ValueError(f"Table 7 features missing from vocabulary: {', '.join(missing)}")

    matrix = tfidf_matrix[indices].toarray()
    labels = [_paper_display_name(name) for name in df.iloc[indices]["place_name"].tolist()]
    table = pd.DataFrame(matrix, index=labels, columns=feature_names)
    table = table.loc[:, PAPER_TFIDF_COLUMNS]
    table.index.name = "Nama Tempat"
    return table


def build_cosine_table(df: pd.DataFrame, cosine_similarity: np.ndarray, sample_indices: Sequence[int]) -> pd.DataFrame:
    """Build Table 8 from the selected rows of the full similarity matrix."""
    indices = _validate_sample_indices(df, sample_indices)
    if cosine_similarity.shape != (len(df), len(df)):
        raise ValueError("Cosine-similarity matrix shape does not match dataset rows.")

    labels = [_paper_display_name(name) for name in df.iloc[indices]["place_name"].tolist()]
    table = pd.DataFrame(cosine_similarity[np.ix_(indices, indices)], index=labels, columns=labels)
    table.index.name = "Nama Tempat"
    return table


def _relevance_keys(names: Iterable[str]) -> set:
    """Create stable comparison keys for manually labeled place names."""
    return {clean_name_key(name) for name in names if clean_name_key(name)}


def _validate_manual_labels(df: pd.DataFrame) -> None:
    """Ensure every frozen manual label refers to a current destination."""
    available = _relevance_keys(df["place_name"].tolist())
    for specification in PAPER_QUERIES:
        missing = [name for name in specification.relevant_names if clean_name_key(name) not in available]
        if missing:
            raise ValueError(f'Manual labels for "{specification.query}" are missing from corpus: {", ".join(missing)}')


def _select_paper_sample_indices(df: pd.DataFrame) -> List[int]:
    """Select the five frozen paper samples or fail instead of falling back."""
    indices = []
    used = set()
    for display_name, aliases in PAPER_SAMPLE_ALIASES:
        alias_keys = _relevance_keys(aliases)
        matches = [
            index
            for index, name in enumerate(df["place_name"])
            if clean_name_key(name) in alias_keys and index not in used
        ]
        if len(matches) != 1:
            raise ValueError(f'Paper sample "{display_name}" requires one matching destination; found {len(matches)}.')
        indices.append(matches[0])
        used.add(matches[0])
    return indices


def build_keyword_table(results: pd.DataFrame, relevant_names: Iterable[str]) -> pd.DataFrame:
    """Build a paper-format keyword table with manual relevance labels."""
    if results is None or results.empty:
        raise ValueError("Keyword query returned no results; cannot build paper table.")

    required = {"place_name", "similarity_score"}
    missing = required.difference(results.columns)
    if missing:
        raise ValueError(f"Keyword result is missing columns: {', '.join(sorted(missing))}")

    relevant = _relevance_keys(relevant_names)
    table = results[["place_name", "similarity_score"]].copy().reset_index(drop=True)
    table.insert(0, "No", np.arange(1, len(table) + 1))
    table.columns = ["No", "Nama Wisata", "Skor Kemiripan"]
    raw_names = table["Nama Wisata"].tolist()
    table["Tingkat Kesesuaian"] = [
        "Sesuai" if clean_name_key(name) in relevant else "Tidak Sesuai" for name in raw_names
    ]
    table["Nama Wisata"] = table["Nama Wisata"].map(_paper_display_name)
    return table[RESULT_COLUMNS]


def build_precision_summary(keyword_tables: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """Build Table 12 and append the unweighted macro-precision row."""
    rows = []
    for number, (query, table) in enumerate(keyword_tables.items(), start=1):
        relevant_count = int((table["Tingkat Kesesuaian"] == "Sesuai").sum())
        total_count = len(table)
        not_relevant_count = total_count - relevant_count
        precision = relevant_count / total_count if total_count else 0.0
        rows.append(
            {
                "No": number,
                "Kata Kunci": query,
                "Jumlah Sesuai": relevant_count,
                "Jumlah Tidak Sesuai": not_relevant_count,
                "Nilai Presisi": precision * 100,
            }
        )

    summary = pd.DataFrame(rows, columns=SUMMARY_COLUMNS)
    macro_precision = float(summary["Nilai Presisi"].mean()) if not summary.empty else 0.0
    average_row = pd.DataFrame(
        [
            {
                "No": "",
                "Kata Kunci": "Rata-rata",
                "Jumlah Sesuai": "",
                "Jumlah Tidak Sesuai": "",
                "Nilai Presisi": macro_precision,
            }
        ],
        columns=SUMMARY_COLUMNS,
    )
    return pd.concat([summary, average_row], ignore_index=True)


def _sha256_file(path: str) -> str:
    """Return content hash used to identify the evaluated corpus snapshot."""
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def generate_tables(input_file: str = CONTENT_BASED_FILE) -> PaperTables:
    """Run the paper evaluation flow without writing files."""
    df = load_dataset(input_file)
    if df is None:
        raise FileNotFoundError(f"Unable to load content-based dataset: {input_file}")

    df = clean_place_names(df)
    validate_paper_profile(df)
    _validate_manual_labels(df)
    vectorizer, tfidf_matrix, feature_names = fit_tfidf(df)
    cosine_similarity = build_similarity(tfidf_matrix)
    sample_indices = _select_paper_sample_indices(df)

    tfidf_table = build_tfidf_table(df, tfidf_matrix, feature_names, sample_indices)
    cosine_table = build_cosine_table(df, cosine_similarity, sample_indices)

    keyword_tables: Dict[str, pd.DataFrame] = {}
    for specification in PAPER_QUERIES:
        results = get_keyword_recommendations(
            specification.query,
            vectorizer,
            tfidf_matrix,
            df,
            top_n=specification.top_n,
        )
        if results is None:
            raise ValueError(f'Keyword query returned no results: "{specification.query}"')
        if len(results) != specification.top_n:
            raise ValueError(
                f'Keyword query "{specification.query}" returned {len(results)} results; '
                f"expected fixed cutoff {specification.top_n}."
            )
        keyword_tables[specification.query] = build_keyword_table(results, specification.relevant_names)

    summary = build_precision_summary(keyword_tables)
    return PaperTables(
        tfidf=tfidf_table,
        cosine=cosine_table,
        keyword_tables=keyword_tables,
        summary=summary,
        place_count=len(df),
        feature_count=len(feature_names),
        sample_indices=sample_indices,
        input_file=os.path.abspath(input_file),
        input_sha256=_sha256_file(input_file),
    )


def _format_value(value, decimals: int = 3) -> str:
    """Format table values for paper-friendly Markdown output."""
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.{decimals}f}"
    return str(value)


def dataframe_to_markdown(frame: pd.DataFrame, include_index: bool = False) -> str:
    """Render a DataFrame without requiring the optional tabulate package."""
    display = frame.reset_index() if include_index else frame.copy()
    columns = [str(column) for column in display.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---:" if index else "---" for index in range(len(columns))) + " |",
    ]
    for row in display.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(_format_value(value) for value in row) + " |")
    return "\n".join(lines)


def _summary_for_markdown(summary: pd.DataFrame) -> pd.DataFrame:
    """Format precision percentages for direct manuscript copy/paste."""
    display = summary.copy()
    display["Nilai Presisi"] = display["Nilai Presisi"].map(lambda value: "" if value == "" else f"{float(value):.2f}%")
    return display


def _paper_report(tables: PaperTables) -> str:
    """Render all generated tables and reproducibility metadata."""
    lines = [
        "# Paper Tables 7–12",
        "",
        "Generated from current content-based dataset.",
        "",
        f"- Input file: `{tables.input_file}`",
        f"- Input SHA-256: `{tables.input_sha256}`",
        f"- Destination documents: {tables.place_count}",
        f"- TF-IDF features: {tables.feature_count}",
        f"- `sublinear_tf`: {TFIDF_PARAMS['sublinear_tf']}",
        f"- TF-IDF parameters: `{TFIDF_PARAMS}`",
        "- Query cutoffs: `kolam renang` K=8, `curug` K=6, `taman` K=7",
        "- Precision labels: manual labels frozen for current 55-place snapshot",
        "",
        "## Table 7 — Sample TF-IDF Matrix",
        "",
        dataframe_to_markdown(tables.tfidf, include_index=True),
        "",
        "## Table 8 — Sample Cosine Similarity",
        "",
        dataframe_to_markdown(tables.cosine, include_index=True),
        "",
    ]

    for number, (query, table) in enumerate(tables.keyword_tables.items(), start=9):
        lines.extend([f"## Table {number} — Keyword: {query}", "", dataframe_to_markdown(table), ""])

    lines.extend(
        ["## Table 12 — Precision Summary", "", dataframe_to_markdown(_summary_for_markdown(tables.summary)), ""]
    )
    return "\n".join(lines)


def write_outputs(tables: PaperTables, output_dir: str = PAPER_EVALUATION_DIR) -> str:
    """Write CSV and Markdown artifacts for paper copy/paste."""
    ensure_dir(output_dir)
    tables.tfidf.reset_index().to_csv(os.path.join(output_dir, "table_7_tfidf.csv"), index=False, float_format="%.6f")
    tables.cosine.reset_index().to_csv(
        os.path.join(output_dir, "table_8_cosine_similarity.csv"), index=False, float_format="%.6f"
    )

    for number, (query, table) in enumerate(tables.keyword_tables.items(), start=9):
        slug = query.replace(" ", "_")
        table.to_csv(os.path.join(output_dir, f"table_{number}_{slug}.csv"), index=False, float_format="%.6f")

    tables.summary.to_csv(os.path.join(output_dir, "table_12_precision_summary.csv"), index=False, float_format="%.6f")
    metadata = {
        "input_file": tables.input_file,
        "input_sha256": tables.input_sha256,
        "place_count": tables.place_count,
        "feature_count": tables.feature_count,
        "tfidf_params": {**TFIDF_PARAMS, "ngram_range": list(TFIDF_PARAMS["ngram_range"])},
        "query_cutoffs": {specification.query: specification.top_n for specification in PAPER_QUERIES},
        "sample_indices": tables.sample_indices,
    }
    with open(os.path.join(output_dir, "paper_evaluation_metadata.json"), "w", encoding="utf-8") as metadata_file:
        json.dump(metadata, metadata_file, indent=2, ensure_ascii=False)
    report_path = os.path.join(output_dir, "paper_tables.md")
    with open(report_path, "w", encoding="utf-8") as report:
        report.write(_paper_report(tables))
    return report_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate reproducible paper Tables 7–12.")
    parser.add_argument(
        "--input-file",
        default=CONTENT_BASED_FILE,
        help="Prepared content-based CSV (default: data/processed/karawang_places_content_based.csv).",
    )
    parser.add_argument(
        "--output-dir",
        default=PAPER_EVALUATION_DIR,
        help="Directory for CSV/Markdown table outputs.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point for the paper table generation workflow."""
    args = _build_parser().parse_args(argv)
    print("[1/4] Loading dataset and fitting paper TF-IDF profile...")
    try:
        tables = generate_tables(args.input_file)
        print("[2/4] Building Tables 7 and 8...")
        print("[3/4] Running keyword evaluations for Tables 9–11...")
        report_path = write_outputs(tables, args.output_dir)
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as exc:
        print(f"Paper evaluation failed: {exc}")
        return 1

    print("[4/4] Building Table 12 precision summary...")
    print(f"Destination documents: {tables.place_count}")
    print(f"TF-IDF features: {tables.feature_count}")
    print(f"Outputs: {args.output_dir}")
    print(f"Markdown report: {report_path}")
    print(tables.summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
