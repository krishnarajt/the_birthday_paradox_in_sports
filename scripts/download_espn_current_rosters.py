import csv
import json
import time
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "dataset" / "external" / "espn_current_rosters"
OUT_PATH = OUT_DIR / "espn_current_rosters.csv"
META_PATH = OUT_DIR / "source_metadata.json"
BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"


LEAGUES = [
    {
        "sport_path": "basketball/nba",
        "sport": "Basketball",
        "league": "NBA",
        "gender": "Male",
        "popularity_bucket": "global_major",
    },
    {
        "sport_path": "basketball/wnba",
        "sport": "Basketball",
        "league": "WNBA",
        "gender": "Female",
        "popularity_bucket": "women_major",
    },
    {
        "sport_path": "soccer/usa.1",
        "sport": "Football",
        "league": "MLS",
        "gender": "Male",
        "popularity_bucket": "regional_major",
    },
    {
        "sport_path": "soccer/usa.nwsl",
        "sport": "Football",
        "league": "NWSL",
        "gender": "Female",
        "popularity_bucket": "women_major",
    },
    {
        "sport_path": "soccer/eng.1",
        "sport": "Football",
        "league": "Premier League",
        "gender": "Male",
        "popularity_bucket": "global_major",
    },
    {
        "sport_path": "soccer/esp.1",
        "sport": "Football",
        "league": "La Liga",
        "gender": "Male",
        "popularity_bucket": "global_major",
    },
    {
        "sport_path": "soccer/ger.1",
        "sport": "Football",
        "league": "Bundesliga",
        "gender": "Male",
        "popularity_bucket": "global_major",
    },
    {
        "sport_path": "soccer/ita.1",
        "sport": "Football",
        "league": "Serie A",
        "gender": "Male",
        "popularity_bucket": "global_major",
    },
    {
        "sport_path": "soccer/fra.1",
        "sport": "Football",
        "league": "Ligue 1",
        "gender": "Male",
        "popularity_bucket": "global_major",
    },
    {
        "sport_path": "football/nfl",
        "sport": "American Football",
        "league": "NFL",
        "gender": "Male",
        "popularity_bucket": "domestic_major",
    },
    {
        "sport_path": "baseball/mlb",
        "sport": "Baseball",
        "league": "MLB",
        "gender": "Male",
        "popularity_bucket": "domestic_major",
    },
    {
        "sport_path": "hockey/nhl",
        "sport": "Ice Hockey",
        "league": "NHL",
        "gender": "Male",
        "popularity_bucket": "regional_major",
    },
]


def fetch_json(url):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "birthday-paradox-sports-research/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def get_teams(sport_path):
    data = fetch_json(f"{BASE_URL}/{sport_path}/teams")
    sports = data.get("sports", [])
    leagues = sports[0].get("leagues", []) if sports else []
    teams = leagues[0].get("teams", []) if leagues else []
    return [item.get("team", {}) for item in teams]


def iter_athletes(roster_data):
    for item in roster_data.get("athletes", []):
        if "items" in item:
            position_group = item.get("position", "")
            for athlete in item.get("items", []):
                yield athlete, position_group
        else:
            yield item, ""


def date_from_espn(value):
    return (value or "").split("T", 1)[0]


def birthday_md(date_text):
    parts = (date_text or "").split("-")
    if len(parts) != 3:
        return ""
    return f"{parts[1]}-{parts[2]}"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    errors = []

    for league in LEAGUES:
        print(f"Fetching {league['league']} teams")
        try:
            teams = get_teams(league["sport_path"])
        except Exception as exc:
            errors.append({"league": league["league"], "stage": "teams", "error": str(exc)})
            continue

        for team in teams:
            team_id = team.get("id", "")
            team_name = team.get("displayName", "")
            if not team_id:
                continue

            url = f"{BASE_URL}/{league['sport_path']}/teams/{team_id}/roster"
            try:
                roster = fetch_json(url)
            except Exception as exc:
                errors.append(
                    {
                        "league": league["league"],
                        "team_id": team_id,
                        "team_name": team_name,
                        "error": str(exc),
                    }
                )
                continue

            season = roster.get("season", {})
            for athlete, position_group in iter_athletes(roster):
                birth_date = date_from_espn(athlete.get("dateOfBirth", ""))
                position = athlete.get("position") or {}
                birth_place = athlete.get("birthPlace") or {}
                rows.append(
                    {
                        "source": "ESPN Site API",
                        "sport": league["sport"],
                        "league": league["league"],
                        "gender": league["gender"],
                        "popularity_bucket": league["popularity_bucket"],
                        "season_year": season.get("year", ""),
                        "season_name": season.get("displayName", ""),
                        "team_id": team_id,
                        "team_abbrev": team.get("abbreviation", ""),
                        "team_name": team_name,
                        "athlete_id": athlete.get("id", ""),
                        "athlete_name": athlete.get("fullName", ""),
                        "position_group": position_group,
                        "position": position.get("abbreviation", ""),
                        "height": athlete.get("height", ""),
                        "weight": athlete.get("weight", ""),
                        "birth_date": birth_date,
                        "birthday_md": birthday_md(birth_date),
                        "birth_city": birth_place.get("city", ""),
                        "birth_state": birth_place.get("state", ""),
                        "birth_country": birth_place.get("country", ""),
                    }
                )
            time.sleep(0.05)

    fields = [
        "source",
        "sport",
        "league",
        "gender",
        "popularity_bucket",
        "season_year",
        "season_name",
        "team_id",
        "team_abbrev",
        "team_name",
        "athlete_id",
        "athlete_name",
        "position_group",
        "position",
        "height",
        "weight",
        "birth_date",
        "birthday_md",
        "birth_city",
        "birth_state",
        "birth_country",
    ]
    with OUT_PATH.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    META_PATH.write_text(
        json.dumps(
            {
                "source": "ESPN Site API",
                "base_url": BASE_URL,
                "leagues": LEAGUES,
                "rows": len(rows),
                "errors": errors,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Wrote {len(rows)} rows to {OUT_PATH}")
    print(f"Errors: {len(errors)}")


if __name__ == "__main__":
    main()
