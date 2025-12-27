import json
from pathlib import Path
from collections import defaultdict
import pandas as pd
from docling.document_converter import DocumentConverter
from docling.datamodel.document import DoclingDocument


class DoclingTableExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = Path(pdf_path)
        # Define JSON path next to the PDF
        self.json_path = self.pdf_path.with_suffix(".json")

        self._tables = []
        self.master_dataframe = pd.DataFrame()

    def extract(self):
        """
        Extracts document data. Reads from JSON if it exists,
        otherwise parses the PDF and saves the JSON.
        """
        if self.json_path.exists():
            print(f"Loading cached data from: {self.json_path.name}")
            with self.json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            # Reconstruct the DoclingDocument from saved JSON
            document = DoclingDocument.model_validate(data)
        else:
            print(f"Parsing PDF: {self.pdf_path.name}")
            converter = DocumentConverter()
            result = converter.convert(str(self.pdf_path))
            document = result.document

            # Save the JSON next to the PDF for next time
            with self.json_path.open("w", encoding="utf-8") as f:
                json.dump(document.export_to_dict(), f, indent=4)
            print(f"Saved conversion to: {self.json_path.name}")

        self._tables = document.tables
        return self._tables

    def consolidate_to_master(self):
        """
        Converts tables to DataFrames, filters for specific row counts (23 or 24),
        and appends them to the master_dataframe.
        """
        if not self._tables:
            self.extract()

        df_list = []
        for table in self._tables:
            # Native export to pandas
            df = table.export_to_dataframe()

            # --- FILTER LOGIC START ---
            # Check if the row count is exactly 23 or 24
            row_count = len(df)
            if row_count not in [23, 24]:
                continue
            # --- FILTER LOGIC END ---

            # Add metadata for tracking
            df["src_page"] = self._get_page_number(table)
            df_list.append(df)

        if df_list:
            self.master_dataframe = pd.concat(df_list, ignore_index=True)
        else:
            print("No tables found with 23 or 24 rows.")
            self.master_dataframe = pd.DataFrame()

        return self.master_dataframe

    @staticmethod
    def _get_page_number(table):
        """Extracts page number from provenance."""
        if table.prov and len(table.prov) > 0:
            return table.prov[0].page_no
        return -1

    def get_dataframes_by_page(self):
        """Returns a dictionary of DataFrames grouped by page number."""
        if not self._tables:
            self.extract()

        grouped = defaultdict(list)
        for table in self._tables:
            page = self._get_page_number(table)
            df = table.export_to_dataframe()
            grouped[page].append(df)

        return dict(grouped)


# --- Usage Example ---
# extractor = DoclingTableExtractor("report.pdf")
#
# # First run: Parses PDF, saves report.json, populates master_dataframe
# df_all = extractor.consolidate_to_master()
#
# # Second run (or new instance): Will read report.json automatically
# # extractor_2 = DoclingTableExtractor("report.pdf")
# # df_cached = extractor_2.consolidate_to_master()
