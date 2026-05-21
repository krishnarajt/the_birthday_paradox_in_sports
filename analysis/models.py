"""SQLAlchemy models for the unified birthday-paradox database.

Two tables:
- `rosters`: long-format roster table (one row per player-cohort membership)
- `group_stats`: pre-computed per-cohort birthday-paradox metrics

Every dataset (MLB, NFL, NHL, Olympics, ...) is normalized into `rosters`
using the same column set, so downstream analysis is uniform.
"""

from __future__ import annotations

from sqlalchemy import Boolean, Float, Index, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class Roster(Base):
    """One row per (player, cohort) pair, across every sport/source."""

    __tablename__ = "rosters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Provenance
    source_dataset: Mapped[str] = mapped_column(String(32), index=True)
    sport: Mapped[str] = mapped_column(String(64), index=True)

    # Cohort identifiers (which "team" the player belongs to in the analysis)
    group_id: Mapped[str] = mapped_column(String(255), index=True)
    team: Mapped[str] = mapped_column(String(255))
    country: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)
    gender: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)
    season: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    # What kind of cohort is this? team, delegation, squad, playing_xi
    cohort_kind: Mapped[str] = mapped_column(String(16), index=True, default="team")

    # Player info
    player_external_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    player_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    birth_date: Mapped[str | None] = mapped_column(String(10), nullable=True)
    birth_md: Mapped[str | None] = mapped_column(String(5), nullable=True, index=True)


class GroupStat(Base):
    """Pre-computed birthday-paradox metric for each cohort."""

    __tablename__ = "group_stats"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)

    source_dataset: Mapped[str] = mapped_column(String(32), index=True)
    sport: Mapped[str] = mapped_column(String(64), index=True)
    team: Mapped[str] = mapped_column(String(255))
    country: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)
    gender: Mapped[str | None] = mapped_column(String(8), nullable=True, index=True)
    season: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    cohort_kind: Mapped[str] = mapped_column(String(16), index=True, default="team")

    roster_size: Mapped[int] = mapped_column(Integer, index=True)
    unique_birthdays: Mapped[int] = mapped_column(Integer)
    duplicate_pairs: Mapped[int] = mapped_column(Integer)
    has_shared_birthday: Mapped[bool] = mapped_column(Boolean, index=True)
    theoretical_probability: Mapped[float] = mapped_column(Float)


Index("ix_rosters_sport_country", Roster.sport, Roster.country)
Index("ix_rosters_sport_gender", Roster.sport, Roster.gender)
Index("ix_group_stats_sport_size", GroupStat.sport, GroupStat.roster_size)


def get_engine(db_path: str = "analysis_db.sqlite"):
    return create_engine(f"sqlite:///{db_path}", future=True)


def get_session(db_path: str = "analysis_db.sqlite"):
    engine = get_engine(db_path)
    return sessionmaker(bind=engine, future=True)()


def init_db(db_path: str = "analysis_db.sqlite") -> None:
    engine = get_engine(db_path)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
