import ast
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PARIS_DIR = ROOT / "dataset" / "external" / "paris_2024_olympics"
ATHLETES_PATH = PARIS_DIR / "athletes.csv"
TEAMS_PATH = PARIS_DIR / "teams.csv"
OUT_PATH = PARIS_DIR / "paris_2024_team_roster_birthdays.csv"


def parse_list(value):
    if not value:
        return []
    try:
        parsed = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def birthday_md(date_text):
    parts = (date_text or "").split("-")
    if len(parts) != 3:
        return ""
    return f"{parts[1]}-{parts[2]}"


def load_athletes():
    athletes = {}
    with ATHLETES_PATH.open(encoding="utf-8-sig", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            athletes[str(row["code"])] = row
    return athletes


def main():
    athletes = load_athletes()
    rows = []

    with TEAMS_PATH.open(encoding="utf-8-sig", newline="") as csv_file:
        for team in csv.DictReader(csv_file):
            for athlete_code in parse_list(team.get("athletes_codes", "")):
                athlete = athletes.get(str(athlete_code), {})
                birth_date = athlete.get("birth_date", "")
                rows.append(
                    {
                        "tournament": "Paris 2024 Olympics",
                        "sport": team.get("discipline", ""),
                        "gender": team.get("team_gender", ""),
                        "team_code": team.get("code", ""),
                        "team_name": team.get("team", ""),
                        "country": team.get("country", ""),
                        "country_code": team.get("country_code", ""),
                        "event": team.get("events", ""),
                        "athlete_code": athlete_code,
                        "athlete_name": athlete.get("name", ""),
                        "athlete_gender": athlete.get("gender", ""),
                        "birth_date": birth_date,
                        "birthday_md": birthday_md(birth_date),
                        "birth_country": athlete.get("birth_country", ""),
                    }
                )

    fields = [
        "tournament",
        "sport",
        "gender",
        "team_code",
        "team_name",
        "country",
        "country_code",
        "event",
        "athlete_code",
        "athlete_name",
        "athlete_gender",
        "birth_date",
        "birthday_md",
        "birth_country",
    ]
    with OUT_PATH.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
