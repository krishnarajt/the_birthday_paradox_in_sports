from pathlib import Path

import pandas as pd
from docling.document_converter import DocumentConverter


class DoclingTableExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = Path(pdf_path)
        self._tables = []

    def extract(self):
        converter = DocumentConverter()
        result = converter.convert(str(self.pdf_path))
        document = result.document

        tables = []
        for section in document.sections:
            for element in section.elements:
                # Version-safe table detection
                if hasattr(element, "cells") and hasattr(element, "n_rows"):
                    tables.append(element)

        self._tables = tables
        return tables

    @staticmethod
    def table_to_grid(table):
        grid = [["" for _ in range(table.n_cols)] for _ in range(table.n_rows)]
        for cell in table.cells:
            r, c = cell.row, cell.col
            grid[r][c] = cell.text.strip() if cell.text else ""
        return grid

    @staticmethod
    def grid_to_dataframe(grid):
        """
        Assumes first row is header.
        """
        header = grid[0]
        rows = grid[1:]
        return pd.DataFrame(rows, columns=header)

    def get_dataframes(self):
        """
        Returns a list of pandas DataFrames, one per table.
        """
        if not self._tables:
            self.extract()

        dataframes = []
        for table in self._tables:
            grid = self.table_to_grid(table)
            df = self.grid_to_dataframe(grid)
            dataframes.append(df)

        return dataframes
