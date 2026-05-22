import Link from "next/link";
import InsightNote from "@/components/InsightNote";
import { listSports, fmtPct, fmtNum, fmtDelta } from "@/lib/data";

export default function SportsIndexPage() {
  const sports = listSports();
  const olympics = sports.filter((s) => s.is_olympic);
  const leagues = sports.filter((s) => !s.is_olympic);

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">All sports</h1>
        <InsightNote>
          Use the gap more than the raw real-world rate. Big-team sports will
          naturally have high shared-birthday rates; the interesting cases are
          the ones that sit far above or below what team size predicts.
        </InsightNote>
      </div>

      <section>
        <h2 className="text-xl font-semibold mb-3">Leagues & tournaments</h2>
        <SportGrid sports={leagues} />
      </section>

      <section>
        <h2 className="text-xl font-semibold mb-3">Olympic disciplines</h2>
        <SportGrid sports={olympics} />
      </section>
    </div>
  );
}

function SportGrid({ sports }) {
  return (
    <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
      {sports.map((s) => {
        const dev = (s.observed_rate ?? 0) - (s.theoretical_rate ?? 0);
        return (
          <Link key={s.slug} href={`/sport/${s.slug}/`} className="card hover:ring-accent/40 transition">
            <div className="flex items-baseline justify-between gap-2">
              <div className="font-semibold">{s.sport.replace("Olympic ", "")}</div>
              <div className="text-xs text-white/40">{fmtNum(s.cohorts)} team lists</div>
            </div>
            <div className="mt-3 grid grid-cols-3 gap-2 text-sm">
              <div>
                <div className="kpi-label">Real</div>
                <div className="font-semibold">{fmtPct(s.observed_rate)}</div>
              </div>
              <div>
                <div className="kpi-label">Expected</div>
                <div className="font-semibold text-white/70">{fmtPct(s.theoretical_rate)}</div>
              </div>
              <div>
                <div className="kpi-label">Gap</div>
                <div className={`font-semibold ${dev >= 0 ? "text-good" : "text-bad"}`}>
                  {fmtDelta(dev)}
                </div>
              </div>
            </div>
            <div className="mt-3 border-t border-white/5 pt-3 text-xs text-white/55">
              {Math.abs(dev) < 0.01
                ? "Tracks the birthday math very closely."
                : dev > 0
                  ? "More birthday matches than team size alone predicts."
                  : "Fewer birthday matches than team size alone predicts."}
            </div>
          </Link>
        );
      })}
    </div>
  );
}
