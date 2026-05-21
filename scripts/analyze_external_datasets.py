import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXTERNAL = ROOT / "dataset" / "external"


def theoretical_probability(group_size):
    if group_size > 365:
        return 1.0

    probability_no_match = 1.0
    for i in range(group_size):
        probability_no_match *= (365 - i) / 365
    return 1 - probability_no_match


def month_day_from_iso(date_text):
    if not date_text:
        return ""

    parts = date_text.split("-")
    if len(parts) != 3:
        return ""
    return f"{parts[1]}-{parts[2]}"


def summarize(label, path, group_key, birthday, min_size=15):
    groups = defaultdict(list)
    with path.open(encoding="utf-8", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            key = group_key(row)
            birthday_md = birthday(row)
            if key and birthday_md:
                groups[key].append(birthday_md)

    filtered_groups = {
        key: birthdays for key, birthdays in groups.items() if len(birthdays) >= min_size
    }
    if not filtered_groups:
        return {
            "dataset": label,
            "groups": 0,
            "rows": 0,
            "avg_size": 0,
            "min_size": 0,
            "median_size": 0,
            "max_size": 0,
            "observed_pct": 0,
            "theory_pct": 0,
        }

    group_sizes = sorted(len(birthdays) for birthdays in filtered_groups.values())
    shared_count = sum(
        1
        for birthdays in filtered_groups.values()
        if len(birthdays) != len(set(birthdays))
    )
    total_groups = len(filtered_groups)

    return {
        "dataset": label,
        "groups": total_groups,
        "rows": sum(group_sizes),
        "avg_size": sum(group_sizes) / total_groups,
        "min_size": group_sizes[0],
        "median_size": group_sizes[len(group_sizes) // 2],
        "max_size": group_sizes[-1],
        "observed_pct": shared_count / total_groups * 100,
        "theory_pct": sum(
            theoretical_probability(len(birthdays))
            for birthdays in filtered_groups.values()
        )
        / total_groups
        * 100,
    }


def main():
    summaries = [
        summarize(
            "NFL nflverse rosters 1920-2025",
            EXTERNAL / "nflverse_rosters" / "nflverse_rosters_1920_2025.csv",
            lambda row: (row.get("season"), row.get("team")),
            lambda row: month_day_from_iso(row.get("birth_date", "")),
        ),
        summarize(
            "Footballcsv squad cache",
            EXTERNAL
            / "footballcsv_cache_footballsquads"
            / "footballcsv_current_squads_flat.csv",
            lambda row: row.get("source_path"),
            lambda row: row.get("birthday_md", ""),
        ),
        summarize(
            "SABR Lahman MLB team-season appearances",
            EXTERNAL / "lahman_sabr" / "lahman_team_player_birthdays.csv",
            lambda row: (row.get("year_id"), row.get("team_id")),
            lambda row: row.get("birthday_md", ""),
        ),
        summarize(
            "NHL current rosters 2025-26 snapshot",
            EXTERNAL
            / "nhl_api_rosters"
            / "nhl_current_rosters_2025_2026.csv",
            lambda row: row.get("team_abbrev"),
            lambda row: month_day_from_iso(row.get("birth_date", "")),
        ),
    ]

    for summary in summaries:
        print(summary["dataset"])
        print(f"  groups >= 15: {summary['groups']:,}")
        print(f"  rows in groups: {summary['rows']:,}")
        print(
            "  roster size avg/min/median/max: "
            f"{summary['avg_size']:.1f}/"
            f"{summary['min_size']}/"
            f"{summary['median_size']}/"
            f"{summary['max_size']}"
        )
        print(
            "  observed groups with shared birthday: "
            f"{summary['observed_pct']:.2f}%"
        )
        print(
            "  avg theoretical probability by roster size: "
            f"{summary['theory_pct']:.2f}%"
        )
        print()


if __name__ == "__main__":
    main()
