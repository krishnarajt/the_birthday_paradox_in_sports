import csv
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PDF_PATH = ROOT / "dataset" / "SquadLists-English.pdf"
OUT_DIR = ROOT / "dataset" / "external" / "fifa_wwc_2023"
OUT_PATH = OUT_DIR / "fifa_wwc_2023_squads.csv"


PLAYER_ROW_RE = re.compile(r"^\s*\d{1,2}\s+(GK|DF|MF|FW)\s+")
TEAM_RE = re.compile(r"^\s*(?P<team>.+?)\s+\((?P<code>[A-Z]{3})\)\s*$")


def birthday_md(dob):
    parts = dob.split("/")
    if len(parts) != 3:
        return ""
    return f"{parts[1]}-{parts[0]}"


def extract_text(pdf_path):
    return subprocess.check_output(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        text=True,
        encoding="utf-8",
    )


def get_team(lines):
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or PLAYER_ROW_RE.match(stripped):
            continue
        match = TEAM_RE.match(stripped)
        if match:
            return match.group("team"), match.group("code")
    return "", ""


def parse_page(page_text, page_number):
    lines = page_text.splitlines()
    team_name, team_code = get_team(lines)
    rows = []

    for line in lines:
        if not PLAYER_ROW_RE.match(line):
            continue

        cells = re.split(r"\s{2,}", line.strip())
        if len(cells) < 9:
            continue

        number, position, player_name = cells[0], cells[1], cells[2]
        first_names, last_names, name_on_shirt = cells[3], cells[4], cells[5]
        dob = cells[6]
        club = cells[7]
        height_cm = cells[8]
        caps = cells[9] if len(cells) > 9 else ""
        goals = cells[10] if len(cells) > 10 else ""

        rows.append(
            {
                "tournament": "FIFA Women's World Cup 2023",
                "sport": "Football",
                "gender": "Female",
                "team_name": team_name,
                "team_code": team_code,
                "source_page": page_number,
                "number": number,
                "position": position,
                "player_name": player_name,
                "first_names": first_names,
                "last_names": last_names,
                "name_on_shirt": name_on_shirt,
                "date_of_birth": dob,
                "birthday_md": birthday_md(dob),
                "club": club,
                "height_cm": height_cm,
                "caps": caps,
                "goals": goals,
            }
        )

    return rows


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    text = extract_text(PDF_PATH)
    rows = []

    for page_number, page_text in enumerate(text.split("\f"), start=1):
        rows.extend(parse_page(page_text, page_number))

    fields = [
        "tournament",
        "sport",
        "gender",
        "team_name",
        "team_code",
        "source_page",
        "number",
        "position",
        "player_name",
        "first_names",
        "last_names",
        "name_on_shirt",
        "date_of_birth",
        "birthday_md",
        "club",
        "height_cm",
        "caps",
        "goals",
    ]
    with OUT_PATH.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    teams = sorted({row["team_code"] for row in rows if row["team_code"]})
    print(f"Wrote {len(rows)} rows to {OUT_PATH}")
    print(f"Teams: {len(teams)}")


if __name__ == "__main__":
    main()
