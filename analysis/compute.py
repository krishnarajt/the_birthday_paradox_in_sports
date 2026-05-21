"""Compute per-cohort birthday-paradox stats and populate `group_stats`.

The theoretical probability that AT LEAST TWO out of `n` people share a
birthday (ignoring leap-day, assuming 365 equally-likely days) is:

    p(n) = 1 - prod_{k=0..n-1} (365 - k) / 365

Run with:  python -m analysis.compute
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from analysis.models import get_engine


def theoretical_probability(n: int) -> float:
    if n < 2:
        return 0.0
    if n >= 365:
        return 1.0
    prob_unique = 1.0
    for k in range(n):
        prob_unique *= (365 - k) / 365.0
    return 1.0 - prob_unique


def run(db_path: str = "analysis_db.sqlite") -> None:
    engine = get_engine(db_path)

    df = pd.read_sql(
        text(
            """
            SELECT group_id, source_dataset, sport, team, country, gender, season,
                   cohort_kind, player_external_id, player_name, birth_md
            FROM rosters
            WHERE birth_md IS NOT NULL
            """
        ),
        engine,
    )
    print(f"Loaded {len(df):,} roster rows")

    # Within a cohort, a player who appears twice (e.g. multiple events in
    # Olympics) should count once. Prefer external id when present; fall
    # back to player name. Critically: do NOT dedupe on birth_md, because
    # that would erase the very same-birthday duplicates we want to count.
    df["_pid"] = df["player_external_id"].fillna("").astype(str)
    df.loc[df["_pid"] == "", "_pid"] = df.loc[df["_pid"] == "", "player_name"].fillna("")
    df = df.drop_duplicates(subset=["group_id", "_pid"], keep="first").copy()
    df = df.drop(columns="_pid")

    grouped = df.groupby("group_id", dropna=False, sort=False)

    records = []
    for gid, g in grouped:
        n = len(g)
        unique = g["birth_md"].nunique()
        dup_pairs = n - unique
        first = g.iloc[0]
        records.append(
            {
                "group_id": gid,
                "source_dataset": first["source_dataset"],
                "sport": first["sport"],
                "team": first["team"],
                "country": first["country"],
                "gender": first["gender"],
                "season": first["season"],
                "cohort_kind": first["cohort_kind"],
                "roster_size": n,
                "unique_birthdays": unique,
                "duplicate_pairs": dup_pairs,
                "has_shared_birthday": dup_pairs > 0,
                "theoretical_probability": theoretical_probability(n),
            }
        )

    stats = pd.DataFrame(records)
    print(f"Computed stats for {len(stats):,} cohorts")

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM group_stats"))
    stats.to_sql("group_stats", engine, if_exists="append", index=False, chunksize=5000)
    print("Saved to group_stats")


if __name__ == "__main__":
    run()
