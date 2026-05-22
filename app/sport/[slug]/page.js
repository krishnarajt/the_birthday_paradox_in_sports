import Link from "next/link";
import KpiCard from "@/components/KpiCard";
import InsightNote from "@/components/InsightNote";
import GenderComparisonChart from "@/components/GenderComparisonChart";
import SportCountryChart from "@/components/SportCountryChart";
import { listSports, loadSport, fmtPct, fmtNum, fmtDelta } from "@/lib/data";

export function generateStaticParams() {
  return listSports().map((s) => ({ slug: s.slug }));
}

export default function SportPage({ params }) {
  const sport = loadSport(params.slug);
  const dev = sport.deviation ?? 0;
  const kind = sport.cohort_kind || "team";
  const KIND_META = {
    team:
      { label: "Team", title: "Actual squads — players who played together on one team." },
    delegation:
      { label: "Delegation", title: "National delegation of solo athletes — they didn't really form a team. Shared birthdays here are weaker evidence." },
    squad:
      { label: "Squad", title: "Tournament squads, not season-long teams. Player lists can shift match by match." },
  };
  const kindMeta = KIND_META[kind] || KIND_META.team;
  const kindBreakdown = sport.cohort_kind_breakdown || {};
  const direction = dev >= 0 ? "above" : "below";

  return (
    <div className="space-y-8">
      <div>
        <Link href="/sports/" className="text-sm text-accent">
          ← All sports
        </Link>
        <h1 className="text-3xl font-bold tracking-tight mt-2">
          {sport.sport}
        </h1>
        <div className="flex items-center gap-2 mt-2 flex-wrap">
          <span
            title={kindMeta.title}
            className="inline-flex items-center rounded-full border border-white/15 bg-white/5 px-2.5 py-0.5 text-[11px] uppercase tracking-wide text-white/70"
          >
            {kindMeta.label}
          </span>
          {Object.keys(kindBreakdown).length > 1 ? (
            <span className="text-[11px] text-white/40">
              ({Object.entries(kindBreakdown).map(([k, v]) => `${v} ${k}`).join(" · ")})
            </span>
          ) : null}
        </div>
        <p className="text-white/60 text-sm mt-2">
          {fmtNum(sport.cohorts)} team lists · {fmtNum(sport.total_players)} player entries ·
          avg team size {sport.avg_roster_size?.toFixed(1)}
        </p>
        <p className="text-white/70 text-sm mt-3 max-w-3xl">
          Read this sport as a comparison between raw coincidence and what
          team size alone predicts. Here, the real rate is{" "}
          <span className={dev >= 0 ? "text-good" : "text-bad"}>
            {fmtDelta(dev)}
          </span>{" "}
          {direction} the birthday-paradox baseline.
        </p>
      </div>

      <section className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Real team lists"
          value={fmtPct(sport.observed_rate)}
          sub="teams with shared birthdays"
          insight="This is the headline rate, but it should never be read without team size."
        />
        <KpiCard
          label="Expected from size"
          value={fmtPct(sport.theoretical_rate)}
          sub="from avg team size"
          insight="This is the fair baseline: what same-size random teams would do."
        />
        <KpiCard
          label="Gap from expected"
          value={<span className={dev >= 0 ? "text-good" : "text-bad"}>{fmtDelta(dev)}</span>}
          sub="real − expected"
          insight={dev >= 0 ? "Positive means the sport has more birthday matches than size alone predicts." : "Negative means this sport is quieter than team size alone predicts."}
        />
        <KpiCard
          label="Team lists"
          value={fmtNum(sport.cohorts)}
          sub="teams analysed"
          insight={sport.cohorts >= 50 ? "Enough team lists for a useful sport-level read." : "Small sample: treat the rate as a lead, not a verdict."}
        />
      </section>

      <InsightNote>
        The best question on a sport page is not "is the real rate high?"
        but "is it high after accounting for team size?" That is what the
        gap card is answering.
      </InsightNote>

      {sport.by_country.length || sport.by_gender.length ? (
        <section className="grid lg:grid-cols-2 gap-6">
          {sport.by_country.length ? (
            <SportCountryChart data={sport.by_country} sport={sport.sport} />
          ) : null}
          {sport.by_gender.length ? (
            <GenderComparisonChart
              data={sport.by_gender}
              title="Gender split inside this sport"
              description="Real and expected rates for labelled team lists in this sport."
            />
          ) : null}
        </section>
      ) : null}

      <section className="grid lg:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="text-lg font-semibold mb-1">By country</h3>
          <p className="text-xs text-white/50 mb-3">Top 40 countries by team-list count.</p>
          <div className="max-h-[420px] overflow-y-auto">
            <table className="data">
              <thead>
                <tr>
                  <th>Country</th>
                  <th>Team lists</th>
                  <th>Avg players</th>
                  <th>Real</th>
                </tr>
              </thead>
              <tbody>
                {sport.by_country.map((r) => (
                  <tr key={r.country}>
                    <td className="font-semibold">{r.country}</td>
                    <td>{fmtNum(r.cohorts)}</td>
                    <td>{r.avg_roster_size?.toFixed(1)}</td>
                    <td>{fmtPct(r.observed_rate)}</td>
                  </tr>
                ))}
                {sport.by_country.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="text-white/50 italic">
                      No per-country data available for this sport.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
          <InsightNote>
            Countries with larger average teams will naturally show more
            shared birthdays. The country list is most useful for finding which
            samples are driving this sport's overall rate.
          </InsightNote>
        </div>

        <div className="card">
          <h3 className="text-lg font-semibold mb-1">By gender</h3>
          <p className="text-xs text-white/50 mb-3">
            Where the dataset records it.
          </p>
          <table className="data">
            <thead>
              <tr>
                <th>Gender</th>
                <th>Team lists</th>
                <th>Real</th>
                <th>Expected</th>
                <th>Gap</th>
              </tr>
            </thead>
            <tbody>
              {sport.by_gender.map((r) => {
                const d = (r.observed_rate ?? 0) - (r.theoretical_rate ?? 0);
                return (
                  <tr key={r.gender}>
                    <td className="font-semibold">
                      {r.gender === "F" ? "Women" : r.gender === "M" ? "Men" : r.gender}
                    </td>
                    <td>{fmtNum(r.cohorts)}</td>
                    <td>{fmtPct(r.observed_rate)}</td>
                    <td className="text-white/60">{fmtPct(r.theoretical_rate)}</td>
                    <td className={d >= 0 ? "text-good" : "text-bad"}>{fmtDelta(d)}</td>
                  </tr>
                );
              })}
              {sport.by_gender.length === 0 ? (
                <tr>
                  <td colSpan={5} className="text-white/50 italic">
                    Gender not labelled in source data.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>

          <h3 className="text-lg font-semibold mt-8 mb-1">Sample team lists</h3>
          <p className="text-xs text-white/50 mb-3">
            Largest teams in the dataset.
          </p>
          <div className="max-h-[260px] overflow-y-auto">
            <table className="data">
              <thead>
                <tr>
                  <th>Team</th>
                  <th>Season</th>
                  <th>Players</th>
                  <th>Repeats</th>
                  <th>Expected chance</th>
                </tr>
              </thead>
              <tbody>
                {sport.example_cohorts.map((c, i) => (
                  <tr key={i}>
                    <td className="font-semibold">
                      {c.team}
                      {c.country ? <span className="text-white/40"> · {c.country}</span> : null}
                    </td>
                    <td className="text-white/70">{c.season ?? "—"}</td>
                    <td>{c.roster_size}</td>
                    <td>{c.duplicate_pairs}</td>
                    <td className="text-white/60">{fmtPct(c.theoretical_probability)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <InsightNote>
            The sample team lists show the mechanics: once the player count gets
            large, the expected chance climbs quickly, and each repeat is
            another player landing on a date already present.
          </InsightNote>
        </div>
      </section>
    </div>
  );
}
