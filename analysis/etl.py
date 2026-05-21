"""ETL: load every available dataset into a unified `rosters` table.

Run with:  python -m analysis.etl
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from analysis.models import get_engine, init_db

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "dataset"
EXTERNAL = DATA / "external"


# Olympic disciplines that are actually "team play together" events.
# Everything else from Olympics is a national delegation.
OLYMPIC_TEAM_KEYWORDS = (
    "Basketball",
    "Football",
    "Volleyball",
    "Handball",
    "Ice Hockey",
    "Water Polo",
    "Hockey",  # field hockey
    "Rugby",
    "Baseball",
    "Softball",
    "Curling",
    "Artistic Swimming",
    "Synchronized Swimming",
    "Polo",
)


def _is_olympic_team_event(discipline) -> bool:
    if not discipline:
        return False
    d = str(discipline)
    if "Beach" in d:  # 2v2 — count as delegation
        return False
    return any(kw in d for kw in OLYMPIC_TEAM_KEYWORDS)


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------


def _md(birth_date) -> str | None:
    if not isinstance(birth_date, str) or len(birth_date) < 10:
        return None
    md = birth_date[5:10]
    if re.match(r"^\d{2}-\d{2}$", md):
        return md
    return None


def _row(
    source_dataset: str,
    sport: str,
    group_id: str,
    team: str,
    country,
    gender,
    season,
    player_external_id,
    player_name,
    birth_date,
    cohort_kind: str = "team",
) -> dict:
    pid = None
    if player_external_id is not None and pd.notna(player_external_id):
        pid = str(player_external_id)
        if pid == "":
            pid = None
    return {
        "source_dataset": source_dataset,
        "sport": sport,
        "group_id": group_id,
        "team": team,
        "country": country if (country is None or pd.notna(country)) else None,
        "gender": gender,
        "season": season,
        "cohort_kind": cohort_kind,
        "player_external_id": pid,
        "player_name": str(player_name) if (player_name is not None and pd.notna(player_name)) else None,
        "birth_date": birth_date if isinstance(birth_date, str) else None,
        "birth_md": _md(birth_date),
    }


def _slug(s) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")


# ----------------------------------------------------------------------------
# Loaders
# ----------------------------------------------------------------------------


def load_olympics() -> pd.DataFrame:
    src = EXTERNAL / "olympics" / "olympics_roster_with_dob.csv"
    if not src.exists():
        print(f"  skip olympics: {src} missing")
        return pd.DataFrame()
    df = pd.read_csv(src, low_memory=False)

    raw_bios = pd.read_csv(
        EXTERNAL / "olympics" / "olympedia_bios.csv",
        usecols=["athlete_id", "Sex"],
        low_memory=False,
    ).rename(columns={"Sex": "sex"})
    df = df.merge(raw_bios, on="athlete_id", how="left")

    rows = []
    for r in df.itertuples(index=False):
        year = int(r.year)
        games = f"{year} {r.type}"
        group_id = f"oly_{year}_{r.type}_{r.noc}_{r.discipline}".replace(" ", "_")
        gender = "F" if str(r.sex) == "Female" else ("M" if str(r.sex) == "Male" else None)
        kind = "team" if _is_olympic_team_event(r.discipline) else "delegation"
        rows.append(
            _row(
                source_dataset="olympics",
                sport=f"Olympic {r.discipline}",
                group_id=group_id,
                team=f"{r.noc} {r.discipline} ({games})",
                country=r.noc,
                gender=gender,
                season=games,
                player_external_id=r.athlete_id,
                player_name=r.name,
                birth_date=r.born_date,
                cohort_kind=kind,
            )
        )
    print(f"  olympics: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_paris_2024() -> pd.DataFrame:
    rows = []

    team_src = EXTERNAL / "paris_2024_olympics" / "paris_2024_team_roster_birthdays.csv"
    if team_src.exists():
        df = pd.read_csv(team_src, low_memory=False)
        for r in df.itertuples(index=False):
            gender = "F" if str(r.athlete_gender) == "Female" else ("M" if str(r.athlete_gender) == "Male" else None)
            rows.append(
                _row(
                    source_dataset="paris2024",
                    sport=f"Olympic {r.sport}",
                    group_id=f"p2024_team_{r.team_code}",
                    team=f"{r.team_name} - {r.event}",
                    country=str(r.country_code) if pd.notna(r.country_code) else None,
                    gender=gender,
                    season="2024 Summer",
                    player_external_id=r.athlete_code,
                    player_name=r.athlete_name,
                    birth_date=str(r.birth_date)[:10] if pd.notna(r.birth_date) else None,
                    cohort_kind="team",
                )
            )

    ind_src = EXTERNAL / "paris_2024_olympics" / "athletes.csv"
    if ind_src.exists():
        df = pd.read_csv(
            ind_src,
            low_memory=False,
            usecols=["code", "name", "gender", "country_code", "country", "disciplines", "birth_date"],
        )
        df = df.dropna(subset=["birth_date", "disciplines", "country_code"]).copy()
        df["disc"] = df["disciplines"].str.extract(r"['\"]([^'\"]+)['\"]")
        df = df.dropna(subset=["disc"])
        team_disc = {r["sport"].replace("Olympic ", "") for r in rows}
        for r in df.itertuples(index=False):
            d = r.disc
            if d in team_disc:
                continue
            gender = "F" if str(r.gender) == "Female" else ("M" if str(r.gender) == "Male" else None)
            rows.append(
                _row(
                    source_dataset="paris2024",
                    sport=f"Olympic {d}",
                    group_id=f"p2024_del_{r.country_code}_{_slug(d)}",
                    team=f"{r.country_code} {d} (2024 Summer)",
                    country=r.country_code,
                    gender=gender,
                    season="2024 Summer",
                    player_external_id=r.code,
                    player_name=r.name,
                    birth_date=str(r.birth_date)[:10],
                    cohort_kind="delegation",
                )
            )

    print(f"  paris2024: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_mlb() -> pd.DataFrame:
    src = EXTERNAL / "lahman_sabr" / "lahman_team_player_birthdays.csv"
    if not src.exists():
        print(f"  skip mlb: {src} missing")
        return pd.DataFrame()
    df = pd.read_csv(src, low_memory=False)

    def _date(r):
        y, m, d = r.birth_year, r.birth_month, r.birth_day
        if pd.isna(y) or pd.isna(m) or pd.isna(d):
            return None
        try:
            return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except Exception:
            return None

    df["birth_date"] = df.apply(_date, axis=1)
    df = df.dropna(subset=["birth_date"])

    rows = [
        _row(
            source_dataset="mlb",
            sport="Baseball",
            group_id=f"mlb_{r.year_id}_{r.team_id}",
            team=str(r.team_name) if pd.notna(r.team_name) else str(r.team_id),
            country="USA",
            gender="M",
            season=str(int(r.year_id)),
            player_external_id=r.player_id,
            player_name=r.player_name,
            birth_date=r.birth_date,
            cohort_kind="team",
        )
        for r in df.itertuples(index=False)
    ]
    print(f"  mlb: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_nfl() -> pd.DataFrame:
    src = EXTERNAL / "nflverse_rosters" / "nflverse_rosters_1920_2025.csv"
    if not src.exists():
        print(f"  skip nfl: {src} missing")
        return pd.DataFrame()
    df = pd.read_csv(
        src,
        low_memory=False,
        usecols=["season", "team", "full_name", "birth_date", "gsis_id", "status"],
    )
    df = df[df["birth_date"].notna() & df["birth_date"].str.match(r"^\d{4}-\d{2}-\d{2}", na=False)]
    df = df[
        df["status"].fillna("").str.upper().isin(["ACT", "INA", "RES", "DEV", "EXE", "CUT"])
        | df["status"].isna()
    ]

    rows = [
        _row(
            source_dataset="nfl",
            sport="American Football",
            group_id=f"nfl_{int(r.season)}_{r.team}",
            team=str(r.team),
            country="USA",
            gender="M",
            season=str(int(r.season)),
            player_external_id=r.gsis_id,
            player_name=r.full_name,
            birth_date=r.birth_date,
            cohort_kind="team",
        )
        for r in df.itertuples(index=False)
    ]
    print(f"  nfl: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_nhl() -> pd.DataFrame:
    src = EXTERNAL / "nhl_api_rosters" / "nhl_current_rosters_2025_2026.csv"
    if not src.exists():
        print(f"  skip nhl: {src} missing")
        return pd.DataFrame()
    df = pd.read_csv(src, low_memory=False)
    df = df[df["birth_date"].notna()]

    rows = [
        _row(
            source_dataset="nhl",
            sport="Ice Hockey",
            group_id=f"nhl_{r.season_id}_{r.team_abbrev}",
            team=str(r.team_abbrev),
            country=str(r.birth_country) if pd.notna(r.birth_country) else None,
            gender="M",
            season="2025-2026",
            player_external_id=r.player_id,
            player_name=r.full_name,
            birth_date=str(r.birth_date)[:10],
            cohort_kind="team",
        )
        for r in df.itertuples(index=False)
    ]
    print(f"  nhl: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_footballcsv() -> pd.DataFrame:
    src = EXTERNAL / "footballcsv_cache_footballsquads" / "footballcsv_current_squads_flat.csv"
    if not src.exists():
        print(f"  skip footballcsv: {src} missing")
        return pd.DataFrame()
    df = pd.read_csv(src, low_memory=False)

    def _date(r):
        try:
            d = int(r.birth_day)
            m = int(r.birth_month)
            y = int(str(r.birth_year_raw).strip())
            if y < 100:
                y = 2000 + y if y <= 30 else 1900 + y
            return f"{y:04d}-{m:02d}-{d:02d}"
        except Exception:
            return None

    df = df.dropna(subset=["birth_day", "birth_month", "birth_year_raw"])
    df["birth_date_norm"] = df.apply(_date, axis=1)
    df = df.dropna(subset=["birth_date_norm"])

    rows = [
        _row(
            source_dataset="footballcsv",
            sport="Football (Soccer)",
            group_id=f"fc_{r.country}_{r.season}_{r.competition}_{r.team_slug}".replace(" ", "_"),
            team=str(r.squad_title)[:120] if pd.notna(r.squad_title) else str(r.team_slug),
            country=str(r.country).upper() if pd.notna(r.country) else None,
            gender="M",
            season=str(r.season),
            player_external_id=None,
            player_name=str(r.name) if pd.notna(r.name) else None,
            birth_date=r.birth_date_norm,
            cohort_kind="team",
        )
        for r in df.itertuples(index=False)
    ]
    print(f"  footballcsv: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_wwc2023() -> pd.DataFrame:
    src = EXTERNAL / "fifa_wwc_2023" / "fifa_wwc_2023_squads.csv"
    if not src.exists():
        print(f"  skip wwc2023: {src} missing")
        return pd.DataFrame()
    df = pd.read_csv(src, low_memory=False)

    def _date(s):
        if not isinstance(s, str):
            return None
        try:
            ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
            if pd.isna(ts):
                return None
            return ts.strftime("%Y-%m-%d")
        except Exception:
            return None

    df["birth_date_norm"] = df["date_of_birth"].apply(_date)
    df = df.dropna(subset=["birth_date_norm"])

    rows = [
        _row(
            source_dataset="wwc2023",
            sport="Football (Soccer)",
            group_id=f"wwc2023_{r.team_code}",
            team=f"{r.team_name} — WWC 2023",
            country=str(r.team_code) if pd.notna(r.team_code) else None,
            gender="F",
            season="2023",
            player_external_id=None,
            player_name=str(r.player_name) if pd.notna(r.player_name) else None,
            birth_date=r.birth_date_norm,
            cohort_kind="team",
        )
        for r in df.itertuples(index=False)
    ]
    print(f"  wwc2023: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_espn_current() -> pd.DataFrame:
    """ESPN current rosters: NBA, WNBA, MLS, NWSL, top-5 European football
    leagues. Each (league, team, season_year) is a proper team."""
    src = EXTERNAL / "espn_current_rosters" / "espn_current_rosters.csv"
    if not src.exists():
        print(f"  skip espn: {src} missing")
        return pd.DataFrame()
    df = pd.read_csv(src, low_memory=False)
    df = df[df["birth_date"].notna()]
    df = df[~df["league"].isin(["MLB", "NFL", "NHL"])]

    SPORT_MAP = {
        "NBA": "Basketball",
        "WNBA": "Basketball",
        "MLS": "Football (Soccer)",
        "NWSL": "Football (Soccer)",
        "Premier League": "Football (Soccer)",
        "La Liga": "Football (Soccer)",
        "Serie A": "Football (Soccer)",
        "Bundesliga": "Football (Soccer)",
        "Ligue 1": "Football (Soccer)",
    }

    rows = []
    for r in df.itertuples(index=False):
        league = r.league
        sport = SPORT_MAP.get(league, str(r.sport))
        gender = "F" if str(r.gender) == "Female" else ("M" if str(r.gender) == "Male" else None)
        season = str(r.season_name) if pd.notna(r.season_name) else str(r.season_year)
        rows.append(
            _row(
                source_dataset="espn",
                sport=sport,
                group_id=f"espn_{_slug(league)}_{r.season_year}_{r.team_abbrev}",
                team=f"{r.team_name} ({league})",
                country=str(r.birth_country)[:8] if pd.notna(r.birth_country) else None,
                gender=gender,
                season=season,
                player_external_id=r.athlete_id,
                player_name=r.athlete_name,
                birth_date=str(r.birth_date)[:10],
                cohort_kind="team",
            )
        )
    print(f"  espn: {len(rows):,} rows")
    return pd.DataFrame(rows)


def _load_cricket_dob_map() -> pd.DataFrame:
    """Build a player_id -> birth_date map for cricket players, joining
    cricsheet identifiers to ESPN Cricinfo player IDs to Wikidata DOBs.

    Returns a DataFrame with columns [player_id, birth_date].
    """
    people = DATA / "people.csv"
    wd = DATA / "wikidata_cricinfo_dob.csv"
    if not people.exists() or not wd.exists():
        return pd.DataFrame(columns=["player_id", "birth_date"])

    p = pd.read_csv(people, low_memory=False, usecols=["identifier", "key_cricinfo"])
    p = p.dropna(subset=["key_cricinfo"]).copy()
    # key_cricinfo loads as float; coerce to int-string
    p["key_cricinfo"] = p["key_cricinfo"].astype("Int64").astype(str)

    w = pd.read_csv(wd, dtype={"key_cricinfo": str})
    w["key_cricinfo"] = w["key_cricinfo"].astype(str)

    m = p.merge(w, on="key_cricinfo", how="inner")
    m = m.rename(columns={"identifier": "player_id", "wd_dob": "birth_date"})

    # Also overlay any locally-scraped DOBs (small but trusted) — they win
    up = DATA / "people_updated.csv"
    if up.exists():
        u = pd.read_csv(up, low_memory=False, usecols=["identifier", "scraped_dob"])
        u = u.dropna(subset=["scraped_dob"])
        u = u[u["scraped_dob"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}")]
        u = u.rename(columns={"identifier": "player_id", "scraped_dob": "birth_date"})
        m = pd.concat([u[["player_id", "birth_date"]], m[["player_id", "birth_date"]]])
        m = m.drop_duplicates(subset="player_id", keep="first")

    return m[["player_id", "birth_date"]]


def load_cricket_women() -> pd.DataFrame:
    """Women's cricket squads: cohort = (event_name, season, team_name).

    Joins cricsheet women's player rosters with Wikidata DOBs (via
    cricinfo player IDs).
    """
    rosters_src = EXTERNAL / "cricket_squads" / "cricsheet_female_player_rosters.csv"
    if not rosters_src.exists():
        print("  skip cricket_women: rosters missing")
        return pd.DataFrame()

    dobs = _load_cricket_dob_map()
    if dobs.empty:
        print("  cricket_women: no DOB map available")
        return pd.DataFrame()

    rosters = pd.read_csv(rosters_src, low_memory=False)
    merged = rosters.merge(dobs, on="player_id", how="inner")
    if merged.empty:
        print("  cricket_women: no DOB matches")
        return pd.DataFrame()

    rows = []
    for r in merged.itertuples(index=False):
        event = r.event_name if pd.notna(r.event_name) else "Other"
        season = str(r.season) if pd.notna(r.season) else "?"
        team = str(r.team_name)
        rows.append(
            _row(
                source_dataset="cricket_w",
                sport="Cricket",
                group_id=f"crkw_{_slug(event)}_{_slug(season)}_{_slug(team)}",
                team=f"{team} — {event} {season}",
                country=team[:8].upper() if r.team_type == "international" else None,
                gender="F",
                season=season,
                player_external_id=r.player_id,
                player_name=r.player_name,
                birth_date=str(r.birth_date)[:10],
                cohort_kind="squad",
            )
        )
    print(f"  cricket_women: {len(rows):,} rows")
    return pd.DataFrame(rows)


def load_cricket_men() -> pd.DataFrame:
    """Men's cricket squads: melt cricsheet flat DB (team1_p1 .. team2_p11)
    into long form and form per-(event, season, team) squads.
    """
    src = ROOT / "cricsheet_flat_database.csv"
    if not src.exists():
        print("  skip cricket_men: flat db missing")
        return pd.DataFrame()

    dobs = _load_cricket_dob_map()
    if dobs.empty:
        return pd.DataFrame()

    use = [
        "match_id", "season", "match_type", "event_name", "gender",
        "team_type", "team1_name", "team2_name",
    ]
    use += [f"team1_p{i}_id" for i in range(1, 12)]
    use += [f"team1_p{i}_name" for i in range(1, 12)]
    use += [f"team2_p{i}_id" for i in range(1, 12)]
    use += [f"team2_p{i}_name" for i in range(1, 12)]
    df = pd.read_csv(src, low_memory=False, usecols=lambda c: c in set(use))

    # We want only men's matches here (women's are in the dedicated file)
    if "gender" in df.columns:
        df = df[df["gender"].astype(str).str.lower() == "male"]

    records = []
    for side in (1, 2):
        team_col = f"team{side}_name"
        for i in range(1, 12):
            id_col, name_col = f"team{side}_p{i}_id", f"team{side}_p{i}_name"
            if id_col not in df.columns:
                continue
            sub = df[[
                "match_id", "season", "event_name", "team_type", team_col, id_col, name_col,
            ]].rename(columns={team_col: "team_name", id_col: "player_id", name_col: "player_name"})
            records.append(sub)
    long = pd.concat(records, ignore_index=True)
    long = long.dropna(subset=["player_id"])

    merged = long.merge(dobs, on="player_id", how="inner")
    if merged.empty:
        return pd.DataFrame()

    # Deduplicate to one row per (team_name, event, season, player_id)
    merged = merged.drop_duplicates(subset=["team_name", "event_name", "season", "player_id"], keep="first")

    rows = []
    for r in merged.itertuples(index=False):
        event = r.event_name if pd.notna(r.event_name) else "Other"
        season = str(r.season) if pd.notna(r.season) else "?"
        team = str(r.team_name)
        rows.append(
            _row(
                source_dataset="cricket_m",
                sport="Cricket",
                group_id=f"crkm_{_slug(event)}_{_slug(season)}_{_slug(team)}",
                team=f"{team} — {event} {season}",
                country=team[:8].upper() if r.team_type == "international" else None,
                gender="M",
                season=season,
                player_external_id=r.player_id,
                player_name=r.player_name,
                birth_date=str(r.birth_date)[:10],
                cohort_kind="squad",
            )
        )
    print(f"  cricket_men: {len(rows):,} rows")
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------------


def run(db_path: str = "analysis_db.sqlite") -> None:
    print(f"Initializing database at {db_path} ...")
    init_db(db_path)
    engine = get_engine(db_path)

    loaders = [
        ("olympics", load_olympics),
        ("paris2024", load_paris_2024),
        ("mlb", load_mlb),
        ("nfl", load_nfl),
        ("nhl", load_nhl),
        ("footballcsv", load_footballcsv),
        ("wwc2023", load_wwc2023),
        ("espn", load_espn_current),
        ("cricket_women", load_cricket_women),
        ("cricket_men", load_cricket_men),
    ]

    total = 0
    skipped_unknown = 0
    skipped_placeholder = 0
    for name, fn in loaders:
        print(f"Loading {name} ...")
        df = fn()
        if df.empty:
            continue
        before = len(df)
        df = df.dropna(subset=["birth_md"])
        after_null = len(df)
        skipped_unknown += before - after_null
        # Drop the "YYYY-01-01" placeholder pattern that several sources use
        # as a stand-in for "DOB unknown — only year was recorded". We lose
        # genuine Jan-1 athletes too, but the alternative is letting tens of
        # thousands of placeholder records distort the distribution.
        is_placeholder = df["birth_date"].astype(str).str.match(r"^\d{4}-01-01$") & (df["birth_md"] == "01-01")
        skipped_placeholder += int(is_placeholder.sum())
        df = df[~is_placeholder]
        df.to_sql("rosters", engine, if_exists="append", index=False, chunksize=5000)
        total += len(df)
        print(f"    -> {name}: {len(df):,} kept after DOB filter")

    print(f"\nTotal roster rows inserted: {total:,}")
    print(f"Skipped (missing DOB): {skipped_unknown:,}")
    print(f"Skipped (YYYY-01-01 placeholder): {skipped_placeholder:,}")


if __name__ == "__main__":
    run()
