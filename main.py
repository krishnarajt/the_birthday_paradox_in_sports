from BirthdayParadoxAnalyzer import BirthdayParadoxAnalyzer
from utils.CricInfoProfileScraper import CricinfoProfileScraper
from utils.CricSheetJsonParser import CricSheetJsonParser
from utils.DoclingTableExtractor import DoclingTableExtractor


def main():
    print("Hello from the-birthday-paradox-in-sports!")
    from pathlib import Path

    root_path = Path().resolve()
    current_path = Path().resolve()
    print("Root path:", root_path)
    print("Current path:", current_path)
    pdf_path = root_path / "dataset" / "SquadLists-English.pdf"
    pdf_name = pdf_path.name
    print("PDF Name:", pdf_name)
    pdf_name_without_ext = pdf_path.stem
    print("PDF Name without extension:", pdf_name_without_ext)
    extractor = DoclingTableExtractor(pdf_path)
    master_df = extractor.consolidate_to_master()
    # df to csv
    master_df.to_csv(current_path / (pdf_name_without_ext + "_tables.csv"), index=False)
    print(master_df.head())

def profile_scraper():
    scraper = CricinfoProfileScraper("1365288")
    profile = scraper.get_profile()

    print(profile)


def cricsheet_parser():
    INPUT_DIR = "dataset/all_json"
    OUTPUT_FILE = "cricsheet_flat_database.csv"

    parser = CricSheetJsonParser(INPUT_DIR, OUTPUT_FILE)
    parser.run()

def birthday_paradox_analyzer_for_womens_football_23_wc():
    analyzer = BirthdayParadoxAnalyzer("SquadLists-English_tables.csv")
    analyzer.run_analysis()


if __name__ == "__main__":
    birthday_paradox_analyzer_for_womens_football_23_wc()
    # main()
    # profile_scraper()
    # cricsheet_parser()
