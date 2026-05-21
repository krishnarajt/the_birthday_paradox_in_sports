import csv
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXTERNAL = ROOT / "dataset" / "external"
OUT_DIR = ROOT / "dataset" / "analysis"


PLAYER_FIELDS = [
    "dataset",
    "sport",
    "league",
    "competition",
    "season",
    "gender",
    "popularity_bucket",
    "country",
    "country_code",
    "team_id",
    "team_name",
    "athlete_id",
    "athlete_name",
    "birth_date",
    "birthday_md",
    "birth_country",
]


GROUP_FIELDS = [
    "dataset",
    "sport",
    "league",
    "competition",
    "season",
    "gender",
    "popularity_bucket",
    "country",
    "country_code",
    "team_id",
    "team_name",
    "eligible_min15",
    "roster_size",
    "valid_birthdays",
    "missing_birthdays",
    "has_shared_birthday",
    "shared_birthday_count",
    "max_same_birthday_count",
    "theoretical_probability",
    "observed_minus_expected",
    "shared_birthdays",
]


def theoretical_probability(group_size):
    if group_size > 365:
        return 1.0

    probability_no_match = 1.0
    for i in range(group_size):
        probability_no_match *= (365 - i) / 365
    return 1 - probability_no_match


def month_day_from_iso(date_text):
    parts = (date_text or "").split("-")
    if len(parts) != 3:
        return ""
    return f"{parts[1]}-{parts[2]}"


def normalize_gender(value):
    return {
        "M": "Male",
        "W": "Female",
        "X": "Mixed",
        "O": "Open",
    }.get(value, value or "Unknown")


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as csv_file:
        yield from csv.DictReader(csv_file)


def player_row(**kwargs):
    row = {field: "" for field in PLAYER_FIELDS}
    row.update(kwargs)
    return row


def iter_nflverse():
    path = EXTERNAL / "nflverse_rosters" / "nflverse_rosters_1920_2025.csv"
    for row in read_csv(path):
        birth_date = row.get("birth_date", "")
        yield player_row(
            dataset="nflverse_rosters_1920_2025",
            sport="American Football",
            league="NFL",
            season=row.get("season", ""),
            gender="Male",
            popularity_bucket="domestic_major",
            country="United States",
            country_code="USA",
            team_id=f"{row.get('season', '')}-{row.get('team', '')}",
            team_name=row.get("team", ""),
            athlete_id=row.get("gsis_id", "") or row.get("pfr_id", ""),
            athlete_name=row.get("full_name", ""),
            birth_date=birth_date,
            birthday_md=month_day_from_iso(birth_date),
        )


def iter_footballcsv():
    path = (
        EXTERNAL
        / "footballcsv_cache_footballsquads"
        / "footballcsv_current_squads_flat.csv"
    )
    for row in read_csv(path):
        yield player_row(
            dataset="footballcsv_squad_cache",
            sport="Football",
            league=row.get("competition", ""),
            season=row.get("season", ""),
            gender="Unknown",
            popularity_bucket="mixed",
            country=row.get("country", ""),
            team_id=row.get("source_path", ""),
            team_name=row.get("squad_title", "") or row.get("team_slug", ""),
            athlete_name=row.get("name", ""),
            birth_date=row.get("date_of_birth_raw", ""),
            birthday_md=row.get("birthday_md", ""),
            birth_country=row.get("nationality", ""),
        )


def iter_lahman():
    path = EXTERNAL / "lahman_sabr" / "lahman_team_player_birthdays.csv"
    for row in read_csv(path):
        birth_date = ""
        if row.get("birth_year") and row.get("birth_month") and row.get("birth_day"):
            birth_date = (
                f"{row['birth_year']}-"
                f"{int(row['birth_month']):02d}-"
                f"{int(row['birth_day']):02d}"
            )
        yield player_row(
            dataset="sabr_lahman_team_season_appearances",
            sport="Baseball",
            league="MLB",
            season=row.get("year_id", ""),
            gender="Male",
            popularity_bucket="domestic_major",
            country="United States / Canada",
            team_id=f"{row.get('year_id', '')}-{row.get('team_id', '')}",
            team_name=row.get("team_name", "") or row.get("team_id", ""),
            athlete_id=row.get("player_id", ""),
            athlete_name=row.get("player_name", ""),
            birth_date=birth_date,
            birthday_md=row.get("birthday_md", ""),
        )


def iter_nhl_api():
    path = EXTERNAL / "nhl_api_rosters" / "nhl_current_rosters_2025_2026.csv"
    for row in read_csv(path):
        birth_date = row.get("birth_date", "")
        yield player_row(
            dataset="nhl_api_current_rosters_2025_2026",
            sport="Ice Hockey",
            league="NHL",
            season=row.get("season_id", ""),
            gender="Male",
            popularity_bucket="regional_major",
            country="United States / Canada",
            team_id=row.get("team_abbrev", ""),
            team_name=row.get("team_abbrev", ""),
            athlete_id=row.get("player_id", ""),
            athlete_name=row.get("full_name", ""),
            birth_date=birth_date,
            birthday_md=month_day_from_iso(birth_date),
            birth_country=row.get("birth_country", ""),
        )


def iter_fifa_wwc():
    path = EXTERNAL / "fifa_wwc_2023" / "fifa_wwc_2023_squads.csv"
    for row in read_csv(path):
        yield player_row(
            dataset="fifa_wwc_2023_squads",
            sport="Football",
            league="FIFA Women's World Cup",
            competition=row.get("tournament", ""),
            season="2023",
            gender="Female",
            popularity_bucket="global_major",
            country=row.get("team_name", ""),
            country_code=row.get("team_code", ""),
            team_id=row.get("team_code", ""),
            team_name=row.get("team_name", ""),
            athlete_name=row.get("player_name", ""),
            birth_date=row.get("date_of_birth", ""),
            birthday_md=row.get("birthday_md", ""),
        )


def iter_paris_olympics():
    path = EXTERNAL / "paris_2024_olympics" / "paris_2024_team_roster_birthdays.csv"
    for row in read_csv(path):
        yield player_row(
            dataset="paris_2024_olympic_team_events",
            sport=row.get("sport", ""),
            league="Olympics",
            competition=row.get("event", ""),
            season="2024",
            gender=normalize_gender(row.get("gender", "")),
            popularity_bucket="olympic",
            country=row.get("country", ""),
            country_code=row.get("country_code", ""),
            team_id=row.get("team_code", ""),
            team_name=row.get("team_name", ""),
            athlete_id=row.get("athlete_code", ""),
            athlete_name=row.get("athlete_name", ""),
            birth_date=row.get("birth_date", ""),
            birthday_md=row.get("birthday_md", ""),
            birth_country=row.get("birth_country", ""),
        )


def iter_espn():
    path = EXTERNAL / "espn_current_rosters" / "espn_current_rosters.csv"
    for row in read_csv(path):
        yield player_row(
            dataset="espn_current_rosters",
            sport=row.get("sport", ""),
            league=row.get("league", ""),
            season=row.get("season_name", "") or row.get("season_year", ""),
            gender=row.get("gender", ""),
            popularity_bucket=row.get("popularity_bucket", ""),
            team_id=f"{row.get('league', '')}-{row.get('team_id', '')}",
            team_name=row.get("team_name", ""),
            athlete_id=row.get("athlete_id", ""),
            athlete_name=row.get("athlete_name", ""),
            birth_date=row.get("birth_date", ""),
            birthday_md=row.get("birthday_md", ""),
            birth_country=row.get("birth_country", ""),
        )


def group_key(row):
    return (
        row["dataset"],
        row["sport"],
        row["league"],
        row["competition"],
        row["season"],
        row["gender"],
        row["popularity_bucket"],
        row["country"],
        row["country_code"],
        row["team_id"],
        row["team_name"],
    )


def group_record(key, birthdays):
    (
        dataset,
        sport,
        league,
        competition,
        season,
        gender,
        popularity_bucket,
        country,
        country_code,
        team_id,
        team_name,
    ) = key
    birthday_values = [value for value in birthdays if value]
    counts = Counter(birthday_values)
    shared = {birthday: count for birthday, count in counts.items() if count > 1}
    expected = theoretical_probability(len(birthday_values))
    observed = 1 if shared else 0

    return {
        "dataset": dataset,
        "sport": sport,
        "league": league,
        "competition": competition,
        "season": season,
        "gender": gender,
        "popularity_bucket": popularity_bucket,
        "country": country,
        "country_code": country_code,
        "team_id": team_id,
        "team_name": team_name,
        "eligible_min15": 1 if len(birthday_values) >= 15 else 0,
        "roster_size": len(birthdays),
        "valid_birthdays": len(birthday_values),
        "missing_birthdays": len(birthdays) - len(birthday_values),
        "has_shared_birthday": observed,
        "shared_birthday_count": len(shared),
        "max_same_birthday_count": max(shared.values()) if shared else 1,
        "theoretical_probability": f"{expected:.6f}",
        "observed_minus_expected": f"{observed - expected:.6f}",
        "shared_birthdays": ";".join(
            f"{birthday}:{count}" for birthday, count in sorted(shared.items())
        ),
    }


def write_summary(path, groups, dimensions, min_valid_birthdays=0):
    summary = defaultdict(list)
    for row in groups:
        if int(row["valid_birthdays"]) < min_valid_birthdays:
            continue
        key = tuple(row[dimension] for dimension in dimensions)
        summary[key].append(row)

    fields = list(dimensions) + [
        "group_count",
        "player_count",
        "avg_roster_size",
        "observed_shared_pct",
        "avg_theoretical_probability",
        "observed_minus_expected",
    ]
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        for key, rows in sorted(summary.items()):
            group_count = len(rows)
            player_count = sum(int(row["valid_birthdays"]) for row in rows)
            observed = sum(int(row["has_shared_birthday"]) for row in rows) / group_count
            expected = (
                sum(float(row["theoretical_probability"]) for row in rows) / group_count
            )
            output = dict(zip(dimensions, key))
            output.update(
                {
                    "group_count": group_count,
                    "player_count": player_count,
                    "avg_roster_size": f"{player_count / group_count:.2f}",
                    "observed_shared_pct": f"{observed * 100:.2f}",
                    "avg_theoretical_probability": f"{expected * 100:.2f}",
                    "observed_minus_expected": f"{(observed - expected) * 100:.2f}",
                }
            )
            writer.writerow(output)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    players_path = OUT_DIR / "roster_players.csv"
    groups_path = OUT_DIR / "roster_groups.csv"

    iterators = [
        iter_nflverse,
        iter_footballcsv,
        iter_lahman,
        iter_nhl_api,
        iter_fifa_wwc,
        iter_paris_olympics,
        iter_espn,
    ]

    groups = defaultdict(list)
    player_count = 0
    with players_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=PLAYER_FIELDS)
        writer.writeheader()
        for iterator in iterators:
            for row in iterator():
                writer.writerow(row)
                groups[group_key(row)].append(row["birthday_md"])
                player_count += 1

    group_rows = [group_record(key, birthdays) for key, birthdays in groups.items()]
    with groups_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=GROUP_FIELDS)
        writer.writeheader()
        writer.writerows(group_rows)

    write_summary(OUT_DIR / "summary_by_dataset_sport.csv", group_rows, ["dataset", "sport"])
    write_summary(
        OUT_DIR / "summary_by_dataset_sport_min15.csv",
        group_rows,
        ["dataset", "sport"],
        min_valid_birthdays=15,
    )
    write_summary(OUT_DIR / "summary_by_sport.csv", group_rows, ["sport"])
    write_summary(
        OUT_DIR / "summary_by_sport_min15.csv",
        group_rows,
        ["sport"],
        min_valid_birthdays=15,
    )
    write_summary(OUT_DIR / "summary_by_gender.csv", group_rows, ["gender"])
    write_summary(
        OUT_DIR / "summary_by_gender_min15.csv",
        group_rows,
        ["gender"],
        min_valid_birthdays=15,
    )
    write_summary(
        OUT_DIR / "summary_by_country.csv",
        group_rows,
        ["sport", "country", "country_code"],
    )
    write_summary(
        OUT_DIR / "summary_by_country_min15.csv",
        group_rows,
        ["sport", "country", "country_code"],
        min_valid_birthdays=15,
    )
    write_summary(
        OUT_DIR / "summary_by_popularity_bucket.csv",
        group_rows,
        ["popularity_bucket"],
    )
    write_summary(
        OUT_DIR / "summary_by_popularity_bucket_min15.csv",
        group_rows,
        ["popularity_bucket"],
        min_valid_birthdays=15,
    )

    print(f"Wrote {player_count} players to {players_path}")
    print(f"Wrote {len(group_rows)} groups to {groups_path}")


if __name__ == "__main__":
    main()
