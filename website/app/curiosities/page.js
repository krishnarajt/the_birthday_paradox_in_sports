import KpiCard from "@/components/KpiCard";
import BirthMonthChart from "@/components/BirthMonthChart";
import RelativeAgeChart from "@/components/RelativeAgeChart";
import InsightNote from "@/components/InsightNote";
import { loadJson, fmtNum, fmtPct, fmtDelta } from "@/lib/data";

export const metadata = {
  title: "Curiosities · Birthday Paradox in Sports",
  description:
    "Weird, quirky, and statistically interesting findings from athlete birthdays: relative-age effect, most populous calendar dates, leap-day athletes, and more.",
};

const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const prettyDate = (md) => {
  if (!md) return md;
  const [m, d] = md.split("-").map(Number);
  return `${MONTH_NAMES[m - 1]} ${d}`;
};
const prettyIsoDate = (iso) => {
  if (!iso) return iso;
  const [y, m, d] = iso.split("-").map(Number);
  return `${MONTH_NAMES[m - 1]} ${d}, ${y}`;
};
const prettySport = (sport = "") => sport.replace("Olympic ", "Olympic · ");
const fmtRatio = (r) => (r === null || r === undefined ? "—" : `${r.toFixed(2)}×`);
const fmtSignedNum = (n) => {
  if (n === null || n === undefined) return "—";
  const sign = n >= 0 ? "+" : "";
  return `${sign}${fmtNum(n)}`;
};
const sourceLabel = (source = "") =>
  ({
    espn: "ESPN",
    footballcsv: "footballcsv",
    mlb: "MLB",
    nfl: "NFL",
    nhl: "NHL",
    olympics: "Olympics",
    paris2024: "Paris 2024",
    wwc2023: "WWC 2023",
  }[source] ?? source);

export default function CuriositiesPage() {
  const months = loadJson("birth_month.json");
  const rae = loadJson("relative_age.json");
  const cu = loadJson("curiosities.json");

  const topDate = cu.most_populous_dates[0];
  const cluster = cu.biggest_birthday_cluster;
  const skew = cu.calendar_skew;
  const raeExtremes = cu.relative_age_extremes;
  const mostMonthByCount = [...months.overall].sort((a, b) => b.count - a.count)[0];

  return (
    <div className="space-y-8">
      <section>
        <h1 className="text-3xl md:text-4xl font-bold tracking-tight">
          Curiosities <span className="text-accent">·</span> weird, quirky,
          interesting
        </h1>
        <p className="text-white/70 mt-2 max-w-3xl">
          The Birthday Paradox is the headline. But once you have{" "}
          {fmtNum(cu.total_unique_players)} athlete birthdays in one place,
          you can ask much weirder questions. Which month produces the most
          athletes? Are sports really biased toward kids born in January?
          Which rosters somehow dodge the math entirely?
        </p>
      </section>

      <section className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Unique athletes analysed"
          value={fmtNum(cu.total_unique_players)}
          sub="deduplicated across all datasets"
          insight="This is large enough that small calendar skews become visible instead of anecdotal."
        />
        <KpiCard
          label="Most populous birthday"
          value={prettyDate(topDate.date)}
          sub={`${fmtNum(topDate.count)} athletes share this calendar date`}
          insight="The top dates cluster near the start of the year, which lines up with the relative-age pattern."
        />
        <KpiCard
          label="Leap-day athletes"
          value={fmtNum(cu.leap_day_players)}
          sub={`vs about ${fmtNum(cu.leap_day?.expected_count)} expected by calendar math`}
          insight="Leap day is almost exactly where calendar math predicts, so the dataset is not simply broken."
        />
        <KpiCard
          label="Q1 advantage"
          value={fmtPct(rae.overall.Q1)}
          sub={`vs ${fmtPct(rae.overall.expected.Q1)} expected (${fmtDelta(rae.overall.Q1 - rae.overall.expected.Q1)})`}
          insight="This is the clearest non-random signal: early-year births are overrepresented across athletes."
        />
      </section>

      <section>
        <BirthMonthChart data={months} />
      </section>

      <section>
        <RelativeAgeChart data={rae} />
      </section>

      <section className="grid xl:grid-cols-[1fr_2fr] gap-6">
        <div className="card">
          <h3 className="text-lg font-semibold mb-1">Calendar skew</h3>
          <p className="text-xs text-white/50 mb-4">
            January has the most athlete birthdays by raw count. February is
            slightly more surprising only after we account for its shorter month.
          </p>
          <div className="space-y-4">
            <div>
              <div className="kpi-label">Jan-Feb share</div>
              <div className="text-2xl font-semibold">
                {fmtPct(skew.jan_feb.share)}
              </div>
              <div className="text-sm text-good">
                {fmtDelta(skew.jan_feb.deviation)} vs calendar expectation
              </div>
            </div>
            <div className="grid sm:grid-cols-3 gap-4">
              <div>
                <div className="kpi-label">Most athletes</div>
                <div className="font-semibold">
                  {mostMonthByCount.month_name}
                </div>
                <div className="text-sm text-white/60">
                  {fmtNum(mostMonthByCount.count)} birthdays
                </div>
              </div>
              <div>
                <div className="kpi-label">Furthest above fair share</div>
                <div className="font-semibold">
                  {skew.most_overrepresented_month.month_name}
                </div>
                <div className="text-sm text-good">
                  {fmtRatio(skew.most_overrepresented_month.relative_to_expected)}
                </div>
              </div>
              <div>
                <div className="kpi-label">Lightest month</div>
                <div className="font-semibold">
                  {skew.most_underrepresented_month.month_name}
                </div>
                <div className="text-sm text-bad">
                  {fmtRatio(skew.most_underrepresented_month.relative_to_expected)}
                </div>
              </div>
            </div>
            <div className="text-sm text-white/65">
              Q1 comes in at <span className="font-semibold text-white">{fmtPct(skew.q1.share)}</span>,
              while Q4 lands at <span className="font-semibold text-white">{fmtPct(skew.q4.share)}</span>.
            </div>
            <InsightNote>
              The clean version is: January has the most athletes, February is
              furthest above its fair calendar share, and the whole first
              quarter is heavier than expected.
            </InsightNote>
          </div>
        </div>

        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Dates that overperform and underperform
          </h3>
          <p className="text-xs text-white/50 mb-3">
            Compared with an even spread across non-leap calendar dates. Jan 1
            is excluded because year-only DOB placeholders were removed during ETL.
          </p>
          <div className="grid md:grid-cols-2 gap-6">
            <table className="data">
              <thead>
                <tr>
                  <th>Hot date</th>
                  <th>Athletes</th>
                  <th>Difference</th>
                </tr>
              </thead>
              <tbody>
                {skew.overrepresented_dates.slice(0, 6).map((d) => (
                  <tr key={d.date}>
                    <td className="font-semibold">{prettyDate(d.date)}</td>
                    <td>{fmtNum(d.count)}</td>
                    <td className="text-good">{fmtSignedNum(d.deviation_count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <table className="data">
              <thead>
                <tr>
                  <th>Cold date</th>
                  <th>Athletes</th>
                  <th>Difference</th>
                </tr>
              </thead>
              <tbody>
                {skew.underrepresented_dates.slice(0, 6).map((d) => (
                  <tr key={d.date}>
                    <td className="font-semibold">{prettyDate(d.date)}</td>
                    <td>{fmtNum(d.count)}</td>
                    <td className="text-bad">{fmtSignedNum(d.deviation_count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <InsightNote>
            Hot and cold dates are less important individually than their
            shape: the hottest dates are mostly early-year, while the coldest
            lean late-year and holiday-adjacent.
          </InsightNote>
        </div>
      </section>

      <section className="grid lg:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Sports with the strongest Q1 tilt
          </h3>
          <p className="text-xs text-white/50 mb-3">
            Minimum {fmtNum(raeExtremes.min_players)} athletes per sport. The
            gap column compares Q1 births with Q4 births.
          </p>
          <table className="data">
            <thead>
              <tr>
                <th>Sport</th>
                <th>Players</th>
                <th>Q1</th>
                <th>Early-late gap</th>
              </tr>
            </thead>
            <tbody>
              {raeExtremes.q1_heavy_sports.map((r) => (
                <tr key={r.sport}>
                  <td>{prettySport(r.sport)}</td>
                  <td>{fmtNum(r.players)}</td>
                  <td className="font-semibold">{fmtPct(r.q1)}</td>
                  <td className="text-good">{fmtDelta(r.q1_q4_gap)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <InsightNote>
            Soccer and hockey are the loudest examples of relative age effect:
            many development systems group children by age cutoffs, giving
            early-year athletes a maturity edge.
          </InsightNote>
        </div>

        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Sports that tilt late instead
          </h3>
          <p className="text-xs text-white/50 mb-3">
            The rare cases where Q4 is heavier than the calendar would predict.
          </p>
          <table className="data">
            <thead>
              <tr>
                <th>Sport</th>
                <th>Players</th>
                <th>Q4</th>
                <th>Above fair share</th>
              </tr>
            </thead>
            <tbody>
              {raeExtremes.late_year_sports.map((r) => (
                <tr key={r.sport}>
                  <td>{prettySport(r.sport)}</td>
                  <td>{fmtNum(r.players)}</td>
                  <td className="font-semibold">{fmtPct(r.q4)}</td>
                  <td className={r.q4_deviation >= 0 ? "text-good" : "text-bad"}>
                    {fmtDelta(r.q4_deviation)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <InsightNote>
            Cricket going the other way is useful because it proves the
            early-year tilt is not universal. Different sports and countries
            can encode different age-cutoff calendars.
          </InsightNote>
        </div>
      </section>

      <section className="grid lg:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Most birthday repeats in one roster
          </h3>
          <p className="text-xs text-white/50 mb-4">
            The single roster with the most extra players landing on an
            already-used calendar birthday.
          </p>
          <div className="space-y-2">
            <div className="text-2xl font-semibold">{cluster.team}</div>
            <div className="text-sm text-white/70">
              {prettySport(cluster.sport)}
              {cluster.country ? ` · ${cluster.country}` : ""}
              {cluster.season ? ` · ${cluster.season}` : ""}
            </div>
            <div className="text-sm text-white/60">
              <span className="font-semibold text-white">
                {cluster.duplicate_pairs}
              </span>{" "}
              same-birthday repeats across{" "}
              <span className="font-semibold text-white">
                {cluster.roster_size}
              </span>{" "}
              players. Source: {sourceLabel(cluster.source)}.
            </div>
          </div>
          <InsightNote>
            This is not one birthday shared by 22 pairs; it means 22 roster
            spots landed on dates already occupied by someone else.
          </InsightNote>
        </div>

        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Most populous calendar dates
          </h3>
          <p className="text-xs text-white/50 mb-3">
            Top 10 days of the year by athlete count, after dropping
            unknown / year-only DOB placeholders.
          </p>
          <table className="data">
            <thead>
              <tr>
                <th>#</th>
                <th>Date</th>
                <th>Athletes</th>
              </tr>
            </thead>
            <tbody>
              {cu.most_populous_dates.map((d, i) => (
                <tr key={d.date}>
                  <td className="text-white/40">{i + 1}</td>
                  <td className="font-semibold">{prettyDate(d.date)}</td>
                  <td>{fmtNum(d.count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <InsightNote>
            Jan 1 is absent by design because year-only placeholder dates were
            removed. That makes Jan 2 and the surrounding dates more credible
            than they would be in a raw scrape.
          </InsightNote>
        </div>
      </section>

      <section className="card">
        <h3 className="text-lg font-semibold mb-1">
          Rosters that dodged the paradox
        </h3>
        <p className="text-xs text-white/50 mb-3">
          These large rosters had no shared birthdays at all, even though the
          theoretical chance of at least one match was already very high.
        </p>
        <table className="data">
          <thead>
            <tr>
              <th>Team</th>
              <th>Roster</th>
              <th>No-match odds</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody>
            {cu.clean_sheets.map((r) => (
              <tr key={`${r.team}-${r.season}-${r.sport}`}>
                <td>
                  <div className="font-semibold">{r.team}</div>
                  <div className="text-xs text-white/45">
                    {prettySport(r.sport)}
                    {r.country ? ` · ${r.country}` : ""}
                    {r.season ? ` · ${r.season}` : ""}
                  </div>
                </td>
                <td>{fmtNum(r.roster_size)}</td>
                <td className="text-bad">{fmtPct(r.no_shared_probability, 2)}</td>
                <td className="text-white/60">{sourceLabel(r.source)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <InsightNote>
          These are the anti-birthday-paradox rosters. Denver's 2002 NFL roster
          had only a 0.83% chance of avoiding every shared birthday under the
          simple model, yet it did.
        </InsightNote>
      </section>

      <section className="grid lg:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Four teammates, same calendar day
          </h3>
          <p className="text-xs text-white/50 mb-3">
            Largest same-date pileups from the cleaner roster sources.
          </p>
          <div className="space-y-4">
            {cu.same_date_clusters.slice(0, 5).map((r) => (
              <div key={`${r.team}-${r.season}-${r.date}`} className="border-b border-white/5 pb-3 last:border-0 last:pb-0">
                <div className="flex items-baseline justify-between gap-3">
                  <div className="font-semibold">
                    {fmtNum(r.same_date_players)} on {prettyDate(r.date)}
                  </div>
                  <div className="text-xs text-white/45">
                    {sourceLabel(r.source)}
                  </div>
                </div>
                <div className="text-sm text-white/60">
                  {prettySport(r.sport)} · {r.team}
                  {r.season ? ` · ${r.season}` : ""}
                </div>
                <div className="text-sm text-white/80 mt-1">
                  {r.players.join(", ")}
                </div>
              </div>
            ))}
          </div>
          <InsightNote>
            Four people on one calendar date is rare, but big football rosters
            make enough attempts that these pileups eventually appear.
          </InsightNote>
        </div>

        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Same birthday, same year
          </h3>
          <p className="text-xs text-white/50 mb-3">
            Triples where teammates shared the exact date of birth, not just
            month and day.
          </p>
          <div className="space-y-4">
            {cu.exact_birthdate_clusters.slice(0, 5).map((r) => (
              <div key={`${r.team}-${r.season}-${r.birth_date}`} className="border-b border-white/5 pb-3 last:border-0 last:pb-0">
                <div className="flex items-baseline justify-between gap-3">
                  <div className="font-semibold">
                    {fmtNum(r.same_birthdate_players)} born {prettyIsoDate(r.birth_date)}
                  </div>
                  <div className="text-xs text-white/45">
                    {sourceLabel(r.source)}
                  </div>
                </div>
                <div className="text-sm text-white/60">
                  {prettySport(r.sport)} · {r.team}
                  {r.season ? ` · ${r.season}` : ""}
                </div>
                <div className="text-sm text-white/80 mt-1">
                  {r.players.join(", ")}
                </div>
              </div>
            ))}
          </div>
          <InsightNote>
            Exact-date triples are stricter than the birthday paradox because
            the year must match too. These are tiny coincidences inside already
            large roster histories.
          </InsightNote>
        </div>
      </section>

      <section className="grid lg:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Youngest sports (by avg current age)
          </h3>
          <p className="text-xs text-white/50 mb-3">
            Average age of athletes still in our roster snapshots, as of
            today. Min 50 athletes per sport.
          </p>
          <table className="data">
            <thead>
              <tr>
                <th>Sport</th>
                <th>Players</th>
                <th>Avg age</th>
              </tr>
            </thead>
            <tbody>
              {cu.youngest_sports.map((r) => (
                <tr key={r.sport}>
                  <td>{prettySport(r.sport)}</td>
                  <td>{fmtNum(r.players)}</td>
                  <td>{r.avg_age.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <InsightNote>
            This table is really about source freshness. Current Olympic-style
            snapshots make youth-skewed sports look young because they include
            active or recent athletes.
          </InsightNote>
        </div>

        <div className="card">
          <h3 className="text-lg font-semibold mb-1">Oldest sports</h3>
          <p className="text-xs text-white/50 mb-3">
            Same calculation, the other tail. Equestrian wins for obvious
            reasons (athletes compete into their 60s+).
          </p>
          <table className="data">
            <thead>
              <tr>
                <th>Sport</th>
                <th>Players</th>
                <th>Avg age</th>
              </tr>
            </thead>
            <tbody>
              {cu.oldest_sports.map((r) => (
                <tr key={r.sport}>
                  <td>{prettySport(r.sport)}</td>
                  <td>{fmtNum(r.players)}</td>
                  <td>{r.avg_age.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <InsightNote>
            The oldest list is dominated by historical Olympic disciplines and
            lifetime datasets, so read it as dataset age plus sport longevity.
          </InsightNote>
        </div>
      </section>

      <section className="card">
        <h3 className="text-lg font-semibold mb-1">
          Where real rosters disagree with the math the most
        </h3>
        <p className="text-xs text-white/50 mb-3">
          Sports with the largest gap between real shared-birthday rates and
          the same-size random-roster baseline. Some gaps may be real selection
          effects; some may be data-quality artifacts.
        </p>
        <table className="data">
          <thead>
            <tr>
              <th>Sport</th>
              <th>Rosters</th>
              <th>Real</th>
              <th>Expected</th>
              <th>Gap</th>
            </tr>
          </thead>
          <tbody>
            {cu.biggest_anomalies.map((r) => (
              <tr key={r.sport}>
                <td>{prettySport(r.sport)}</td>
                <td>{fmtNum(r.cohorts)}</td>
                <td>{fmtPct(r.observed_rate)}</td>
                <td className="text-white/60">{fmtPct(r.theoretical_rate)}</td>
                <td className={r.deviation >= 0 ? "text-good" : "text-bad"}>
                  {fmtDelta(r.deviation)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <InsightNote>
          Positive gaps mean shared birthdays happen more often than the simple
          random-roster model expects. That can reflect real selection
          effects, repeated age groups, or data quirks.
        </InsightNote>
      </section>
    </div>
  );
}
