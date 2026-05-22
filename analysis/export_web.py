"""Compute aggregated views from `group_stats` and write JSON files
that the static Next.js site will consume.

Outputs to: public/data/

Files produced:
  kpis.json                    -- top-level dashboard numbers
  observed_vs_theoretical.json -- per-sport observed vs theoretical
  sports_ranking.json          -- sports sorted by closeness to theory
  by_country.json              -- per-country breakdown
  by_gender.json               -- M vs F overall and by sport
  popularity.json              -- roster counts (proxy for popularity)
  sport_size_curve.json        -- observed-rate as a function of n, per sport
  sports/<slug>.json           -- per-sport detail page payload
  meta.json                    -- when generated, source counts

Run with:  python -m analysis.export_web
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from analysis.compute import theoretical_probability
from analysis.models import get_engine

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "public" / "data"


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _safe(v):
    """Convert numpy/pandas scalars to JSON-friendly Python primitives."""
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return round(v, 6)
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return v


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    def default(o):
        if isinstance(o, float):
            if math.isnan(o) or math.isinf(o):
                return None
            return o
        if isinstance(o, (pd.Timestamp, datetime)):
            return o.isoformat()
        if hasattr(o, "item"):
            try:
                v = o.item()
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    return None
                return v
            except Exception:
                pass
        # pandas NA / NaT
        try:
            if pd.isna(o):
                return None
        except (TypeError, ValueError):
            pass
        return str(o)

    def _clean(v):
        if isinstance(v, float):
            return None if math.isnan(v) or math.isinf(v) else v
        if isinstance(v, dict):
            return {k: _clean(x) for k, x in v.items()}
        if isinstance(v, list):
            return [_clean(x) for x in v]
        try:
            if pd.isna(v) and not isinstance(v, (list, tuple, dict)):
                return None
        except (TypeError, ValueError):
            pass
        return v

    with path.open("w", encoding="utf-8") as f:
        json.dump(_clean(payload), f, indent=2, ensure_ascii=False,
                  default=default, allow_nan=False)
    print(f"  wrote {path.relative_to(ROOT)}")


def run(db_path: str = "analysis_db.sqlite") -> None:
    engine = get_engine(db_path)

    stats = pd.read_sql("SELECT * FROM group_stats", engine)
    rosters_n = pd.read_sql("SELECT COUNT(*) AS n FROM rosters", engine).iloc[0]["n"]

    # ----- common filter: only cohorts with at least 5 players -----
    s = stats[stats["roster_size"] >= 5].copy()
    s["observed"] = s["has_shared_birthday"].astype(int)

    # =========================================================================
    # KPIs (dashboard headline)
    # =========================================================================
    n_cohorts = len(s)
    n_players = int(rosters_n)
    n_sports = s["sport"].nunique()
    n_countries = s["country"].dropna().nunique()
    n_with_shared = int(s["observed"].sum())
    obs_rate = float(s["observed"].mean()) if n_cohorts else 0.0
    theo_rate = float(s["theoretical_probability"].mean()) if n_cohorts else 0.0

    avg_roster = float(s["roster_size"].mean()) if n_cohorts else 0.0

    kpis = {
        "total_cohorts": n_cohorts,
        "total_players_roster_rows": n_players,
        "total_sports": int(n_sports),
        "total_countries": int(n_countries),
        "cohorts_with_shared_birthday": n_with_shared,
        "observed_shared_rate": _safe(obs_rate),
        "theoretical_average_rate": _safe(theo_rate),
        "deviation_obs_minus_theo": _safe(obs_rate - theo_rate),
        "avg_roster_size": _safe(avg_roster),
        "data_sources": int(s["source_dataset"].nunique()),
    }
    _write_json(OUT / "kpis.json", kpis)

    # =========================================================================
    # Observed vs theoretical, per sport
    # =========================================================================
    per_sport = (
        s.groupby("sport")
        .agg(
            cohorts=("group_id", "count"),
            avg_roster_size=("roster_size", "mean"),
            observed_rate=("observed", "mean"),
            theoretical_rate=("theoretical_probability", "mean"),
        )
        .reset_index()
    )
    per_sport["deviation"] = per_sport["observed_rate"] - per_sport["theoretical_rate"]
    per_sport["abs_deviation"] = per_sport["deviation"].abs()

    # We have many Olympic disciplines. Top-N + bucket the rest? Keep all
    # but mark whether they're an Olympic discipline.
    per_sport["is_olympic"] = per_sport["sport"].str.startswith("Olympic ")

    obs_vs_theo = per_sport.sort_values("cohorts", ascending=False).head(30).copy()
    obs_vs_theo_payload = [
        {
            "sport": r.sport,
            "cohorts": int(r.cohorts),
            "avg_roster_size": _safe(r.avg_roster_size),
            "observed_rate": _safe(r.observed_rate),
            "theoretical_rate": _safe(r.theoretical_rate),
            "deviation": _safe(r.deviation),
            "is_olympic": bool(r.is_olympic),
        }
        for r in obs_vs_theo.itertuples(index=False)
    ]
    _write_json(OUT / "observed_vs_theoretical.json", obs_vs_theo_payload)

    # =========================================================================
    # Sports ranked by closeness to theory (smallest |deviation|)
    # =========================================================================
    # Filter: at least 20 cohorts so the sample isn't trivially small
    ranking = per_sport[per_sport["cohorts"] >= 20].sort_values("abs_deviation")
    ranking_payload = [
        {
            "rank": i + 1,
            "sport": r.sport,
            "cohorts": int(r.cohorts),
            "avg_roster_size": _safe(r.avg_roster_size),
            "observed_rate": _safe(r.observed_rate),
            "theoretical_rate": _safe(r.theoretical_rate),
            "deviation": _safe(r.deviation),
            "abs_deviation": _safe(r.abs_deviation),
        }
        for i, r in enumerate(ranking.itertuples(index=False))
    ]
    _write_json(OUT / "sports_ranking.json", ranking_payload)

    # =========================================================================
    # By country
    # =========================================================================
    by_country = (
        s.dropna(subset=["country"])
        .groupby("country")
        .agg(
            cohorts=("group_id", "count"),
            observed_rate=("observed", "mean"),
            theoretical_rate=("theoretical_probability", "mean"),
            avg_roster_size=("roster_size", "mean"),
        )
        .reset_index()
        .sort_values("observed_rate", ascending=False)
    )
    by_country = by_country[by_country["cohorts"] >= 10]
    by_country_payload = [
        {
            "country": r.country,
            "cohorts": int(r.cohorts),
            "observed_rate": _safe(r.observed_rate),
            "theoretical_rate": _safe(r.theoretical_rate),
            "avg_roster_size": _safe(r.avg_roster_size),
        }
        for r in by_country.itertuples(index=False)
    ]
    _write_json(OUT / "by_country.json", by_country_payload)

    # =========================================================================
    # By gender (overall + per top sports)
    # =========================================================================
    by_gender = (
        s.dropna(subset=["gender"])
        .groupby("gender")
        .agg(
            cohorts=("group_id", "count"),
            observed_rate=("observed", "mean"),
            theoretical_rate=("theoretical_probability", "mean"),
            avg_roster_size=("roster_size", "mean"),
        )
        .reset_index()
    )
    by_gender_payload = {
        "overall": [
            {
                "gender": r.gender,
                "cohorts": int(r.cohorts),
                "observed_rate": _safe(r.observed_rate),
                "theoretical_rate": _safe(r.theoretical_rate),
                "avg_roster_size": _safe(r.avg_roster_size),
            }
            for r in by_gender.itertuples(index=False)
        ],
    }
    _write_json(OUT / "by_gender.json", by_gender_payload)

    # =========================================================================
    # Popularity proxy = total roster rows per sport
    # =========================================================================
    popularity = (
        s.groupby("sport")
        .agg(
            cohorts=("group_id", "count"),
            total_players=("roster_size", "sum"),
            observed_rate=("observed", "mean"),
            theoretical_rate=("theoretical_probability", "mean"),
        )
        .reset_index()
        .sort_values("total_players", ascending=False)
        .head(25)
    )
    popularity_payload = [
        {
            "sport": r.sport,
            "cohorts": int(r.cohorts),
            "total_players": int(r.total_players),
            "observed_rate": _safe(r.observed_rate),
            "theoretical_rate": _safe(r.theoretical_rate),
            "deviation": _safe(r.observed_rate - r.theoretical_rate),
        }
        for r in popularity.itertuples(index=False)
    ]
    _write_json(OUT / "popularity.json", popularity_payload)

    # =========================================================================
    # Observed-rate by roster-size bucket, per top sport
    # =========================================================================
    top_sports = per_sport.sort_values("cohorts", ascending=False).head(8)["sport"].tolist()

    curve = {}
    universal = []
    for n in range(5, 71, 1):
        slice_ = s[s["roster_size"] == n]
        if len(slice_) >= 5:
            universal.append(
                {
                    "n": n,
                    "cohorts": int(len(slice_)),
                    "observed_rate": _safe(slice_["observed"].mean()),
                    "theoretical_rate": _safe(theoretical_probability(n)),
                }
            )
    curve["__theory_curve__"] = [
        {"n": n, "p": _safe(theoretical_probability(n))} for n in range(2, 71)
    ]
    curve["__universal__"] = universal

    for sport in top_sports:
        ss = s[s["sport"] == sport]
        points = []
        for n in range(5, 71, 1):
            sl = ss[ss["roster_size"] == n]
            if len(sl) >= 3:
                points.append(
                    {
                        "n": n,
                        "cohorts": int(len(sl)),
                        "observed_rate": _safe(sl["observed"].mean()),
                    }
                )
        if points:
            curve[sport] = points

    _write_json(OUT / "sport_size_curve.json", curve)

    # =========================================================================
    # Per-sport detail pages
    # =========================================================================
    sports_index = []
    for sport, g in s.groupby("sport"):
        if len(g) < 5:
            continue
        slug = _slugify(sport)
        # By country, within this sport
        cc = (
            g.dropna(subset=["country"])
            .groupby("country")
            .agg(
                cohorts=("group_id", "count"),
                observed_rate=("observed", "mean"),
                avg_roster=("roster_size", "mean"),
            )
            .reset_index()
            .sort_values("cohorts", ascending=False)
            .head(40)
        )
        cc_payload = [
            {
                "country": r.country,
                "cohorts": int(r.cohorts),
                "observed_rate": _safe(r.observed_rate),
                "avg_roster_size": _safe(r.avg_roster),
            }
            for r in cc.itertuples(index=False)
        ]

        # By gender, within this sport
        gg = (
            g.dropna(subset=["gender"])
            .groupby("gender")
            .agg(
                cohorts=("group_id", "count"),
                observed_rate=("observed", "mean"),
                theoretical_rate=("theoretical_probability", "mean"),
                avg_roster_size=("roster_size", "mean"),
            )
            .reset_index()
        )
        gg_payload = [
            {
                "gender": r.gender,
                "cohorts": int(r.cohorts),
                "observed_rate": _safe(r.observed_rate),
                "theoretical_rate": _safe(r.theoretical_rate),
                "avg_roster_size": _safe(r.avg_roster_size),
            }
            for r in gg.itertuples(index=False)
        ]

        # Largest cohorts that have shared birthdays (interesting examples)
        examples = (
            g.sort_values(["has_shared_birthday", "roster_size"], ascending=[False, False])
            .head(10)
        )
        examples_payload = [
            {
                "team": r.team,
                "season": r.season,
                "country": r.country,
                "gender": r.gender,
                "roster_size": int(r.roster_size),
                "duplicate_pairs": int(r.duplicate_pairs),
                "has_shared_birthday": bool(r.has_shared_birthday),
                "theoretical_probability": _safe(r.theoretical_probability),
            }
            for r in examples.itertuples(index=False)
        ]

        kind_counts = g["cohort_kind"].value_counts().to_dict()
        primary_kind = max(kind_counts, key=kind_counts.get) if kind_counts else "team"
        payload = {
            "sport": sport,
            "slug": slug,
            "cohorts": int(len(g)),
            "total_players": int(g["roster_size"].sum()),
            "avg_roster_size": _safe(g["roster_size"].mean()),
            "observed_rate": _safe(g["observed"].mean()),
            "theoretical_rate": _safe(g["theoretical_probability"].mean()),
            "deviation": _safe(g["observed"].mean() - g["theoretical_probability"].mean()),
            "cohort_kind": primary_kind,
            "cohort_kind_breakdown": {k: int(v) for k, v in kind_counts.items()},
            "by_country": cc_payload,
            "by_gender": gg_payload,
            "example_cohorts": examples_payload,
        }
        _write_json(OUT / "sports" / f"{slug}.json", payload)

        sports_index.append(
            {
                "sport": sport,
                "slug": slug,
                "cohorts": int(len(g)),
                "avg_roster_size": _safe(g["roster_size"].mean()),
                "observed_rate": _safe(g["observed"].mean()),
                "theoretical_rate": _safe(g["theoretical_probability"].mean()),
                "is_olympic": sport.startswith("Olympic "),
            }
        )

    sports_index.sort(key=lambda x: x["cohorts"], reverse=True)
    _write_json(OUT / "sports_index.json", sports_index)

    # =========================================================================
    # Curiosities: birth-month distribution, relative-age effect, quirky stats
    # =========================================================================
    # Unique-player roster table (dedupe so the same person isn't counted
    # 50 times across seasons).  We dedupe within (source_dataset, sport)
    # using player_external_id when present, otherwise player_name.
    ros = pd.read_sql(
        "SELECT source_dataset, sport, group_id, team, country, gender, season, cohort_kind, "
        "player_external_id, player_name, birth_date, birth_md "
        "FROM rosters WHERE birth_md IS NOT NULL",
        engine,
    )
    ros["dedup_key"] = ros["player_external_id"].fillna("").astype(str)
    ros.loc[ros["dedup_key"] == "", "dedup_key"] = "name:" + ros["player_name"].fillna("").astype(str)
    unique = ros.drop_duplicates(subset=["source_dataset", "sport", "dedup_key"]).copy()
    unique["month"] = unique["birth_md"].str[:2].astype(int)
    unique["day"] = unique["birth_md"].str[3:5].astype(int)
    # Robust year parse: birth_date is "YYYY-MM-DD" (may have nulls)
    unique["year"] = pd.to_numeric(unique["birth_date"].str[:4], errors="coerce")

    # -------- birth-month distribution -----------------------------------------
    # Expected share for each month under a uniform distribution scaled by
    # the number of days in that month (ignoring leap-day for simplicity).
    DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _month_dist(df: pd.DataFrame) -> list[dict]:
        total = len(df)
        if not total:
            return []
        counts = df["month"].value_counts().reindex(range(1, 13), fill_value=0)
        rows = []
        for m in range(1, 13):
            actual = float(counts[m] / total)
            expected = DAYS_IN_MONTH[m - 1] / 365.0
            rows.append(
                {
                    "month": m,
                    "month_name": MONTH_NAMES[m - 1],
                    "count": int(counts[m]),
                    "share": _safe(actual),
                    "expected_share": _safe(expected),
                    "deviation": _safe(actual - expected),
                    "relative_to_expected": _safe(actual / expected if expected else None),
                }
            )
        return rows

    overall_month = _month_dist(unique)
    top_sports_for_month = (
        unique.groupby("sport").size().sort_values(ascending=False).head(8).index.tolist()
    )
    per_sport_month = {sp: _month_dist(unique[unique["sport"] == sp]) for sp in top_sports_for_month}
    _write_json(
        OUT / "birth_month.json",
        {
            "overall": overall_month,
            "by_sport": per_sport_month,
            "total_unique_players": int(len(unique)),
        },
    )

    # -------- relative-age effect: birth quarter share by sport ----------------
    def _quarter_dist(df: pd.DataFrame) -> dict:
        total = len(df)
        if not total:
            return {}
        q = ((df["month"] - 1) // 3 + 1)
        counts = q.value_counts().reindex(range(1, 5), fill_value=0)
        # Expected share weighted by days in the quarter
        days_per_quarter = [
            sum(DAYS_IN_MONTH[0:3]),
            sum(DAYS_IN_MONTH[3:6]),
            sum(DAYS_IN_MONTH[6:9]),
            sum(DAYS_IN_MONTH[9:12]),
        ]
        return {
            "total": int(total),
            "Q1": _safe(counts[1] / total),
            "Q2": _safe(counts[2] / total),
            "Q3": _safe(counts[3] / total),
            "Q4": _safe(counts[4] / total),
            "expected": {
                "Q1": _safe(days_per_quarter[0] / 365),
                "Q2": _safe(days_per_quarter[1] / 365),
                "Q3": _safe(days_per_quarter[2] / 365),
                "Q4": _safe(days_per_quarter[3] / 365),
            },
        }

    rae_sports = (
        unique.groupby("sport").size().sort_values(ascending=False).head(12).index.tolist()
    )
    relative_age = {
        "overall": _quarter_dist(unique),
        "by_sport": [
            {"sport": sp, **_quarter_dist(unique[unique["sport"] == sp])}
            for sp in rae_sports
        ],
    }
    _write_json(OUT / "relative_age.json", relative_age)

    # -------- curiosities: quirky one-liners -----------------------------------
    # Most populous calendar dates overall (md only)
    top_dates = (
        unique["birth_md"].value_counts().head(10).reset_index()
    )
    top_dates.columns = ["birth_md", "n"]
    top_dates_payload = [
        {"date": r.birth_md, "count": int(r.n)} for r in top_dates.itertuples(index=False)
    ]

    leap_day_players = int((unique["birth_md"] == "02-29").sum())

    biggest_dup_cohort = (
        s.sort_values(["duplicate_pairs", "roster_size"], ascending=[False, False])
        .head(1)
        .iloc[0]
    )
    biggest_cohort_payload = {
        "sport": biggest_dup_cohort.sport,
        "team": biggest_dup_cohort.team,
        "season": str(biggest_dup_cohort.season) if biggest_dup_cohort.season else None,
        "country": biggest_dup_cohort.country,
        "roster_size": int(biggest_dup_cohort.roster_size),
        "duplicate_pairs": int(biggest_dup_cohort.duplicate_pairs),
        "source": biggest_dup_cohort.source_dataset,
    }

    # Average age of athletes by sport (current year minus birth year)
    this_year = datetime.now(timezone.utc).year
    unique_age = unique.dropna(subset=["year"]).copy()
    unique_age["age"] = this_year - unique_age["year"]
    unique_age = unique_age[(unique_age["age"] > 5) & (unique_age["age"] < 100)]
    age_by_sport = (
        unique_age.groupby("sport")
        .agg(players=("age", "count"), avg_age=("age", "mean"))
        .reset_index()
    )
    age_by_sport = age_by_sport[age_by_sport["players"] >= 50].sort_values("avg_age")
    youngest = age_by_sport.head(8)
    oldest = age_by_sport.tail(8).iloc[::-1]
    youngest_payload = [
        {"sport": r.sport, "players": int(r.players), "avg_age": _safe(r.avg_age)}
        for r in youngest.itertuples(index=False)
    ]
    oldest_payload = [
        {"sport": r.sport, "players": int(r.players), "avg_age": _safe(r.avg_age)}
        for r in oldest.itertuples(index=False)
    ]

    # Greatest deviation from theory: sports where reality most exceeds theory
    biggest_anomaly = per_sport[per_sport["cohorts"] >= 50].copy()
    biggest_anomaly["abs_dev"] = biggest_anomaly["deviation"].abs()
    biggest_anomaly = biggest_anomaly.sort_values("abs_dev", ascending=False).head(6)
    anomaly_payload = [
        {
            "sport": r.sport,
            "cohorts": int(r.cohorts),
            "observed_rate": _safe(r.observed_rate),
            "theoretical_rate": _safe(r.theoretical_rate),
            "deviation": _safe(r.deviation),
        }
        for r in biggest_anomaly.itertuples(index=False)
    ]

    # Calendar skew: Jan-Feb and Q1 are visibly overrepresented in the full
    # unique-player table.  Jan 1 is intentionally omitted from date extremes:
    # the ETL drops YYYY-01-01 records because several source datasets use that
    # exact date as a "known year, unknown month/day" placeholder.
    def _period_summary(label: str, months: list[int]) -> dict:
        count = int(unique[unique["month"].isin(months)].shape[0])
        share = count / len(unique) if len(unique) else 0.0
        expected = sum(DAYS_IN_MONTH[m - 1] for m in months) / 365.0
        return {
            "label": label,
            "months": months,
            "count": count,
            "share": _safe(share),
            "expected_share": _safe(expected),
            "deviation": _safe(share - expected),
            "relative_to_expected": _safe(share / expected if expected else None),
        }

    month_over = max(overall_month, key=lambda r: r["deviation"]) if overall_month else None
    month_under = min(overall_month, key=lambda r: r["deviation"]) if overall_month else None

    rankable_dates = []
    for m, days in enumerate(DAYS_IN_MONTH, start=1):
        for d in range(1, days + 1):
            md = f"{m:02d}-{d:02d}"
            if md != "01-01":
                rankable_dates.append(md)
    rankable_unique = unique[unique["birth_md"].isin(rankable_dates)]
    expected_per_rankable_date = (
        len(rankable_unique) / len(rankable_dates) if rankable_dates else 0.0
    )
    date_counts = unique["birth_md"].value_counts()
    date_extreme_rows = []
    for md in rankable_dates:
        count = int(date_counts.get(md, 0))
        deviation_count = count - expected_per_rankable_date
        date_extreme_rows.append(
            {
                "date": md,
                "count": count,
                "expected_count": _safe(expected_per_rankable_date),
                "deviation_count": _safe(deviation_count),
                "relative_to_expected": _safe(
                    count / expected_per_rankable_date
                    if expected_per_rankable_date
                    else None
                ),
            }
        )

    calendar_skew = {
        "jan_feb": _period_summary("Jan-Feb", [1, 2]),
        "q1": _period_summary("Q1", [1, 2, 3]),
        "q4": _period_summary("Q4", [10, 11, 12]),
        "most_overrepresented_month": month_over,
        "most_underrepresented_month": month_under,
        "expected_per_rankable_date": _safe(expected_per_rankable_date),
        "excluded_rankable_dates": ["01-01", "02-29"],
        "overrepresented_dates": sorted(
            date_extreme_rows,
            key=lambda r: r["deviation_count"],
            reverse=True,
        )[:8],
        "underrepresented_dates": sorted(
            date_extreme_rows,
            key=lambda r: r["deviation_count"],
        )[:8],
    }

    leap_expected = len(unique) / 1461.0 if len(unique) else 0.0
    leap_day = {
        "count": leap_day_players,
        "expected_count": _safe(leap_expected),
        "relative_to_expected": _safe(
            leap_day_players / leap_expected if leap_expected else None
        ),
    }

    # Sport-level relative-age extremes.  Minimum sample size keeps this from
    # becoming a leaderboard of tiny Olympic delegations.
    SPORT_RAE_MIN_PLAYERS = 500
    expected_q1 = sum(DAYS_IN_MONTH[0:3]) / 365.0
    expected_q4 = sum(DAYS_IN_MONTH[9:12]) / 365.0
    sport_quarter_rows = []
    for sport, g in unique.groupby("sport"):
        n = len(g)
        if n < SPORT_RAE_MIN_PLAYERS:
            continue
        q = ((g["month"] - 1) // 3 + 1)
        counts = q.value_counts().reindex(range(1, 5), fill_value=0)
        q1 = counts[1] / n
        q4 = counts[4] / n
        sport_quarter_rows.append(
            {
                "sport": sport,
                "players": int(n),
                "q1": _safe(q1),
                "q4": _safe(q4),
                "expected_q1": _safe(expected_q1),
                "expected_q4": _safe(expected_q4),
                "q1_deviation": _safe(q1 - expected_q1),
                "q4_deviation": _safe(q4 - expected_q4),
                "q1_q4_gap": _safe(q1 - q4),
            }
        )

    relative_age_extremes = {
        "min_players": SPORT_RAE_MIN_PLAYERS,
        "q1_heavy_sports": sorted(
            sport_quarter_rows,
            key=lambda r: r["q1_deviation"],
            reverse=True,
        )[:8],
        "late_year_sports": sorted(
            sport_quarter_rows,
            key=lambda r: r["q4_deviation"],
            reverse=True,
        )[:8],
    }

    # Cohorts with no shared birthdays despite a large theoretical chance of at
    # least one match: a more direct "how did that happen?" paradox view.
    clean = s[s["duplicate_pairs"] == 0].copy()
    clean["no_shared_probability"] = 1.0 - clean["theoretical_probability"]
    clean = clean.sort_values(
        ["roster_size", "theoretical_probability"],
        ascending=[False, False],
    ).head(8)
    clean_sheets_payload = [
        {
            "sport": r.sport,
            "team": r.team,
            "season": str(r.season) if r.season else None,
            "country": r.country,
            "roster_size": int(r.roster_size),
            "theoretical_probability": _safe(r.theoretical_probability),
            "no_shared_probability": _safe(r.no_shared_probability),
            "source": r.source_dataset,
        }
        for r in clean.itertuples(index=False)
    ]

    # Roster-level pileups.  We exclude historical Olympics here because a few
    # older bios appear to use mid-year placeholders such as July 1; the pro and
    # current-roster sources make better examples for this particular curiosity.
    trusted_cluster_sources = {
        "espn",
        "footballcsv",
        "mlb",
        "nfl",
        "nhl",
        "paris2024",
        "wwc2023",
    }

    def _player_list(values) -> list[str]:
        players = []
        seen = set()
        for value in values:
            if pd.isna(value):
                continue
            name = str(value)
            if not name or name in seen:
                continue
            players.append(name)
            seen.add(name)
            if len(players) >= 6:
                break
        return players

    roster_unique = ros.copy()
    roster_unique["_pid"] = roster_unique["player_external_id"].fillna("").astype(str)
    roster_unique.loc[roster_unique["_pid"] == "", "_pid"] = (
        roster_unique.loc[roster_unique["_pid"] == "", "player_name"]
        .fillna("")
        .astype(str)
    )
    roster_unique = roster_unique.drop_duplicates(subset=["group_id", "_pid"], keep="first")
    cluster_rosters = roster_unique[
        roster_unique["source_dataset"].isin(trusted_cluster_sources)
    ].copy()
    cohort_meta = s[
        ["group_id", "source_dataset", "sport", "team", "country", "season", "roster_size"]
    ]

    same_date_clusters = (
        cluster_rosters.groupby(["group_id", "birth_md"])
        .agg(
            same_date_players=("_pid", "count"),
            players=("player_name", _player_list),
        )
        .reset_index()
    )
    same_date_clusters = same_date_clusters[
        same_date_clusters["same_date_players"] >= 4
    ]
    same_date_clusters = same_date_clusters.merge(cohort_meta, on="group_id", how="left")
    same_date_clusters = same_date_clusters.sort_values(
        ["same_date_players", "roster_size"],
        ascending=[False, False],
    ).head(8)
    same_date_clusters_payload = [
        {
            "date": r.birth_md,
            "same_date_players": int(r.same_date_players),
            "players": r.players,
            "sport": r.sport,
            "team": r.team,
            "season": str(r.season) if r.season else None,
            "country": r.country,
            "roster_size": int(r.roster_size),
            "source": r.source_dataset,
        }
        for r in same_date_clusters.itertuples(index=False)
    ]

    exact_birthdate_rosters = cluster_rosters[
        cluster_rosters["birth_date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    ].copy()
    exact_birthdate_clusters = (
        exact_birthdate_rosters.groupby(["group_id", "birth_date"])
        .agg(
            same_birthdate_players=("_pid", "count"),
            players=("player_name", _player_list),
        )
        .reset_index()
    )
    exact_birthdate_clusters = exact_birthdate_clusters[
        exact_birthdate_clusters["same_birthdate_players"] >= 3
    ]
    exact_birthdate_clusters = exact_birthdate_clusters.merge(
        cohort_meta,
        on="group_id",
        how="left",
    )
    exact_birthdate_clusters = exact_birthdate_clusters.sort_values(
        ["same_birthdate_players", "roster_size"],
        ascending=[False, False],
    ).head(8)
    exact_birthdate_clusters_payload = [
        {
            "birth_date": r.birth_date,
            "same_birthdate_players": int(r.same_birthdate_players),
            "players": r.players,
            "sport": r.sport,
            "team": r.team,
            "season": str(r.season) if r.season else None,
            "country": r.country,
            "roster_size": int(r.roster_size),
            "source": r.source_dataset,
        }
        for r in exact_birthdate_clusters.itertuples(index=False)
    ]

    curiosities = {
        "total_unique_players": int(len(unique)),
        "leap_day_players": leap_day_players,
        "leap_day": leap_day,
        "calendar_skew": calendar_skew,
        "most_populous_dates": top_dates_payload,
        "biggest_birthday_cluster": biggest_cohort_payload,
        "youngest_sports": youngest_payload,
        "oldest_sports": oldest_payload,
        "biggest_anomalies": anomaly_payload,
        "relative_age_extremes": relative_age_extremes,
        "clean_sheets": clean_sheets_payload,
        "same_date_clusters": same_date_clusters_payload,
        "exact_birthdate_clusters": exact_birthdate_clusters_payload,
    }
    _write_json(OUT / "curiosities.json", curiosities)

    # =========================================================================
    # Meta
    # =========================================================================
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "roster_rows": int(rosters_n),
        "cohorts": n_cohorts,
        "min_cohort_size": 5,
        "sources_by_count": (
            s.groupby("source_dataset").size().rename("cohorts").reset_index().to_dict("records")
        ),
    }
    _write_json(OUT / "meta.json", meta)

    print(f"\nAll JSON written to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    run()
