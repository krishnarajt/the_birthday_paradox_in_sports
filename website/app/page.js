import Link from "next/link";
import KpiCard from "@/components/KpiCard";
import ObservedVsTheoreticalChart from "@/components/ObservedVsTheoreticalChart";
import SportSizeCurveChart from "@/components/SportSizeCurveChart";
import PopularityScatter from "@/components/PopularityScatter";
import BirthMonthChart from "@/components/BirthMonthChart";
import DeviationExtremesChart from "@/components/DeviationExtremesChart";
import CountryComparisonChart from "@/components/CountryComparisonChart";
import GenderComparisonChart from "@/components/GenderComparisonChart";
import InsightNote from "@/components/InsightNote";
import { loadJson, listSports, fmtPct, fmtNum, fmtDelta } from "@/lib/data";

export default function HomePage() {
  const kpis = loadJson("kpis.json");
  const obs = loadJson("observed_vs_theoretical.json");
  const ranking = loadJson("sports_ranking.json");
  const byCountry = loadJson("by_country.json");
  const byGender = loadJson("by_gender.json");
  const popularity = loadJson("popularity.json");
  const curve = loadJson("sport_size_curve.json");
  const months = loadJson("birth_month.json");
  const sports = listSports();

  return (
    <div className="space-y-8">
      <section>
        <h1 className="text-3xl md:text-4xl font-bold tracking-tight">
          Does the birthday paradox{" "}
          <span className="text-accent">actually</span> hold in sports?
        </h1>
        <p className="text-white/70 mt-2 max-w-3xl">
          The math says: in a group of 23 people, there's a 50.73% chance two
          share a birthday. We checked against {fmtNum(kpis.total_cohorts)} real
          team lists across {kpis.total_sports} sports and {kpis.total_countries}{" "}
          countries.
        </p>
      </section>

      {/* KPI grid */}
      <section className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        <KpiCard
          label="Real team match rate"
          value={fmtPct(kpis.observed_shared_rate)}
          sub={`vs expected ${fmtPct(kpis.theoretical_average_rate)} (${fmtDelta(kpis.deviation_obs_minus_theo)})`}
          insight="The real team lists come in slightly above the clean model, which suggests birthdays are not perfectly random once selection systems and team-building enter the picture."
        />
        <KpiCard
          label="Team lists analysed"
          value={fmtNum(kpis.total_cohorts)}
          sub={`avg team size ${kpis.avg_roster_size?.toFixed(1)}`}
          insight="This is enough sample size for the headline result, but sports with only a few team lists still need caution."
        />
        <KpiCard
          label="Player entries"
          value={fmtNum(kpis.total_players_roster_rows)}
          sub={`from ${kpis.data_sources} datasets`}
          insight="Player entries are not unique people; repeated seasons intentionally show what real team lists looked like over time."
        />
        <KpiCard
          label="Sports covered"
          value={fmtNum(kpis.total_sports)}
          sub="leagues + Olympic disciplines"
          insight="The variety lets us separate universal birthday math from sport-specific selection effects."
        />
        <KpiCard
          label="Countries represented"
          value={fmtNum(kpis.total_countries)}
          sub="primarily via Olympics & football"
          insight="Country patterns mostly reflect team sizes and available datasets, not national biology."
        />
      </section>

      {/* Top question 1: does the paradox hold across sports? */}
      <section className="grid lg:grid-cols-2 gap-6">
        <ObservedVsTheoreticalChart data={obs} />
        <SportSizeCurveChart curve={curve} />
      </section>

      <section className="grid lg:grid-cols-2 gap-6">
        <DeviationExtremesChart sports={sports} />
        <PopularityScatter data={popularity} />
      </section>

      {/* Top question 2: which sport agrees with theory the most */}
      <section className="grid lg:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="text-lg font-semibold mb-1">
            Sports closest to clean birthday math
          </h3>
          <p className="text-xs text-white/50 mb-3">
            Sorted by gap from same-size random teams. Smaller is closer to the
            math. Minimum 20 team lists per sport.
          </p>
          <div className="max-h-[420px] overflow-y-auto">
            <table className="data">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Sport</th>
                  <th>Team lists</th>
                  <th>Avg players</th>
                  <th>Real</th>
                  <th>Expected</th>
                  <th>Gap</th>
                </tr>
              </thead>
              <tbody>
                {ranking.slice(0, 30).map((r) => (
                  <tr key={r.sport}>
                    <td className="text-white/40">{r.rank}</td>
                    <td>{r.sport.replace("Olympic ", "")}</td>
                    <td>{fmtNum(r.cohorts)}</td>
                    <td>{r.avg_roster_size?.toFixed(1)}</td>
                    <td>{fmtPct(r.observed_rate)}</td>
                    <td className="text-white/60">{fmtPct(r.theoretical_rate)}</td>
                    <td
                      className={
                        r.deviation >= 0 ? "text-good" : "text-bad"
                      }
                    >
                      {fmtDelta(r.deviation)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <InsightNote>
            The closer a sport gets to a zero gap, the more it behaves like
            simple birthday math. Road cycling and swimming are almost textbook
            cases here, which is surprisingly satisfying.
          </InsightNote>
        </div>

        <GenderComparisonChart data={byGender.overall} />
      </section>

      <section>
        <CountryComparisonChart data={byCountry} />
      </section>

      {/* Birth-month curiosity teaser */}
      <section className="grid lg:grid-cols-[2fr_1fr] gap-6">
        <BirthMonthChart data={months} />
        <div className="card flex flex-col justify-between">
          <div>
            <h3 className="text-lg font-semibold mb-1">Want more?</h3>
            <p className="text-sm text-white/70">
              The birthday paradox is just the headline. Once you have{" "}
              {fmtNum(months.total_unique_players)} athlete birthdays in one
              place, you can ask a lot of other questions: relative-age
              effect, leap-day athletes, the single luckiest team-up of all
              time, youngest and oldest sports, and more.
            </p>
            <InsightNote>
              The weirdest story is not a single birthday. It is the repeated
              early-year tilt: January and February keep showing up as heavier
              than a calendar-only model would predict.
            </InsightNote>
          </div>
          <Link
            href="/curiosities/"
            className="mt-6 inline-flex w-fit items-center gap-2 rounded-md bg-accent/10 border border-accent/30 px-3 py-2 text-sm text-accent hover:bg-accent/20 transition"
          >
            Explore curiosities →
          </Link>
        </div>
      </section>

      {/* Sports tab nav */}
      <section className="card">
        <div className="flex items-baseline justify-between flex-wrap gap-2 mb-3">
          <h3 className="text-lg font-semibold">Explore by sport</h3>
          <Link href="/sports/" className="text-sm text-accent">
            Full list →
          </Link>
        </div>
        <div className="flex gap-2 flex-wrap">
          {sports.slice(0, 24).map((s) => (
            <Link key={s.slug} href={`/sport/${s.slug}/`} className="tab">
              {s.sport.replace("Olympic ", "")}{" "}
              <span className="text-white/40 ml-1">
                {fmtPct(s.observed_rate, 0)}
              </span>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}
