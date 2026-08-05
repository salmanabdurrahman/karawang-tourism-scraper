"""Tests for reproducible paper Tables 7–12 generation."""

import os
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")
del _HERE, _path

from generate_paper_tables import (  # noqa: E402
    PAPER_QUERIES,
    PAPER_TFIDF_COLUMNS,
    PaperTables,
    build_cosine_table,
    build_keyword_table,
    build_precision_summary,
    build_tfidf_table,
    dataframe_to_markdown,
    generate_tables,
    validate_paper_profile,
    write_outputs,
)


class PaperTableHelpersTest(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"place_name": ["Tempat A", "Tempat B", "Tempat C"]})
        self.feature_names = np.array(PAPER_TFIDF_COLUMNS)
        self.tfidf = csr_matrix(np.arange(24, dtype=float).reshape(3, 8) / 100)
        self.cosine = np.array(
            [
                [1.0, 0.2, 0.3],
                [0.2, 1.0, 0.4],
                [0.3, 0.4, 1.0],
            ]
        )

    def test_validate_paper_profile_rejects_non_55_place_input(self):
        with self.assertRaisesRegex(ValueError, "exactly 55"):
            validate_paper_profile(self.df)

    def test_build_tfidf_table_preserves_paper_columns_and_rows(self):
        table = build_tfidf_table(self.df, self.tfidf, self.feature_names, [0, 1, 2])

        self.assertEqual(list(table.columns), PAPER_TFIDF_COLUMNS)
        self.assertEqual(table.index.name, "Nama Tempat")
        self.assertEqual(table.index.tolist(), ["Tempat A", "Tempat B", "Tempat C"])
        self.assertEqual(table.loc["Tempat B", "curug"], 0.12)

    def test_build_tfidf_table_rejects_missing_feature(self):
        with self.assertRaisesRegex(ValueError, "features missing"):
            build_tfidf_table(self.df, self.tfidf, self.feature_names[:-1], [0, 1, 2])

    def test_build_cosine_table_selects_symmetric_sample(self):
        table = build_cosine_table(self.df, self.cosine, [0, 2])

        self.assertEqual(table.index.name, "Nama Tempat")
        self.assertEqual(table.index.tolist(), ["Tempat A", "Tempat C"])
        self.assertEqual(table.loc["Tempat A", "Tempat C"], 0.3)
        self.assertTrue(np.allclose(table.to_numpy(), table.to_numpy().T))

    def test_build_cosine_table_rejects_wrong_shape(self):
        with self.assertRaisesRegex(ValueError, "shape does not match"):
            build_cosine_table(self.df, np.eye(2), [0, 1])

    def test_build_keyword_table_applies_manual_labels(self):
        results = pd.DataFrame(
            {
                "place_name": ["Taman A", "Hutan B", "Taman C"],
                "similarity_score": [0.8, 0.4, 0.2],
            }
        )

        table = build_keyword_table(results, ["Taman A", "Taman C"])

        self.assertEqual(list(table.columns), ["No", "Nama Wisata", "Skor Kemiripan", "Tingkat Kesesuaian"])
        self.assertEqual(table["No"].tolist(), [1, 2, 3])
        self.assertEqual(table["Tingkat Kesesuaian"].tolist(), ["Sesuai", "Tidak Sesuai", "Sesuai"])

    def test_build_precision_summary_uses_macro_average(self):
        tables = {
            "query satu": build_keyword_table(
                pd.DataFrame({"place_name": ["A", "B"], "similarity_score": [0.8, 0.2]}), ["A"]
            ),
            "query dua": build_keyword_table(
                pd.DataFrame({"place_name": ["C", "D", "E"], "similarity_score": [0.7, 0.4, 0.1]}),
                ["C", "D"],
            ),
        }

        summary = build_precision_summary(tables)

        self.assertEqual(summary.iloc[0]["Jumlah Sesuai"], 1)
        self.assertEqual(summary.iloc[0]["Jumlah Tidak Sesuai"], 1)
        self.assertAlmostEqual(summary.iloc[0]["Nilai Presisi"], 50.0)
        self.assertAlmostEqual(summary.iloc[1]["Nilai Presisi"], 66.6666666667)
        self.assertAlmostEqual(summary.iloc[2]["Nilai Presisi"], 58.3333333333)

    def test_write_outputs_creates_copyable_artifacts(self):
        keyword_table = build_keyword_table(
            pd.DataFrame({"place_name": ["Taman A"], "similarity_score": [0.8]}), ["Taman A"]
        )
        tables = PaperTables(
            tfidf=pd.DataFrame(self.tfidf.toarray(), index=self.df.place_name, columns=PAPER_TFIDF_COLUMNS),
            cosine=pd.DataFrame(self.cosine, index=self.df.place_name, columns=self.df.place_name),
            keyword_tables={"taman": keyword_table},
            summary=build_precision_summary({"taman": keyword_table}),
            place_count=3,
            feature_count=8,
            sample_indices=[0, 1, 2],
        )

        with tempfile.TemporaryDirectory() as tmp:
            report_path = write_outputs(tables, tmp)
            self.assertTrue(os.path.isfile(report_path))
            self.assertTrue(os.path.isfile(os.path.join(tmp, "table_7_tfidf.csv")))
            self.assertTrue(os.path.isfile(os.path.join(tmp, "table_8_cosine_similarity.csv")))
            self.assertTrue(os.path.isfile(os.path.join(tmp, "table_9_taman.csv")))
            self.assertTrue(os.path.isfile(os.path.join(tmp, "table_12_precision_summary.csv")))
            self.assertTrue(os.path.isfile(os.path.join(tmp, "paper_evaluation_metadata.json")))
            with open(report_path, encoding="utf-8") as report:
                content = report.read()
            self.assertIn("Paper Tables 7–12", content)
            self.assertIn("Destination documents: 3", content)
            self.assertIn("Input SHA-256:", content)

    def test_paper_query_cutoffs_match_updated_paper(self):
        self.assertEqual([spec.query for spec in PAPER_QUERIES], ["kolam renang", "curug", "taman"])
        self.assertEqual([spec.top_n for spec in PAPER_QUERIES], [8, 6, 7])

    @unittest.skipUnless(
        os.path.isfile(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..",
                "data",
                "processed",
                "karawang_places_content_based.csv",
            )
        ),
        "real prepared corpus is not available",
    )
    def test_current_55_place_profile_matches_paper_tables(self):
        input_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "data", "processed", "karawang_places_content_based.csv"
        )
        tables = generate_tables(input_file)

        self.assertEqual(tables.place_count, 55)
        self.assertEqual(tables.feature_count, 8561)
        self.assertAlmostEqual(tables.tfidf.loc["Curug Cigentis", "curug"], 0.502468, places=6)
        self.assertAlmostEqual(tables.cosine.loc["Curug Cigentis", "Green Canyon"], 0.558784, places=6)
        self.assertEqual(
            tables.keyword_tables["curug"]["Nama Wisata"].tolist()[:3],
            [
                "Curug Bandung Loji Karawang",
                "Curug Cilalay Karawang Jonggol",
                "Curug Cigentis",
            ],
        )
        self.assertEqual(tables.summary.iloc[-1]["Kata Kunci"], "Rata-rata")
        self.assertAlmostEqual(tables.summary.iloc[-1]["Nilai Presisi"], 88.8888888889, places=6)

    def test_markdown_renderer_has_header_and_rows(self):
        markdown = dataframe_to_markdown(pd.DataFrame({"Nama": ["A"], "Skor": [0.123456]}))
        self.assertIn("| Nama | Skor |", markdown)
        self.assertIn("| A | 0.123 |", markdown)


if __name__ == "__main__":
    unittest.main()
