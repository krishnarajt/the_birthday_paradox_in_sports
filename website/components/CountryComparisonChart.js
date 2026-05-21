"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend,
} from "recharts";
import InsightNote from "./InsightNote";

const fmtPct = (v) =>
  v === null || v === undefined ? "—" : `${(v * 100).toFixed(1)}%`;
const fmtDelta = (v) => {
  if (v === null || v === undefined) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(1)} points`;
};

export default function CountryComparisonChart({
  data,
  title = "Countries where teams share birthdays more than expected",
  description = "Countries with enough rosters, sorted by the largest positive gap from the same-size random-roster baseline.",
  minCohorts = 50,
}) {
  const rows = data
    .filter((r) => r.cohorts >= minCohorts && r.theoretical_rate !== undefined)
    .map((r) => ({
      country: r.country,
      cohorts: r.cohorts,
      "Real rosters": r.observed_rate,
      "Expected from size": r.theoretical_rate,
      deviation: r.observed_rate - r.theoretical_rate,
      avg_roster_size: r.avg_roster_size,
    }))
    .sort((a, b) => b.deviation - a.deviation)
    .slice(0, 12);
  const leader = rows[0];

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">{title}</h3>
      <p className="text-xs text-white/50 mb-4">
        {description} Minimum {minCohorts} team rosters.
      </p>
      <ResponsiveContainer width="100%" height={380}>
        <BarChart data={rows} margin={{ top: 10, right: 16, left: 0, bottom: 40 }}>
          <CartesianGrid stroke="#ffffff10" />
          <XAxis
            dataKey="country"
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            interval={0}
            angle={-30}
            textAnchor="end"
          />
          <YAxis
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            tickFormatter={fmtPct}
            domain={[0, 1]}
          />
          <Tooltip
            contentStyle={{
              background: "#121933",
              border: "1px solid #ffffff20",
              borderRadius: 12,
              color: "#e7ecff",
            }}
            formatter={(value, name) =>
              name === "Real rosters" || name === "Expected from size"
                ? fmtPct(value)
                : value
            }
            labelFormatter={(label) => `${label}`}
          />
          <Legend wrapperStyle={{ color: "#cdd6ff", fontSize: 12 }} />
          <Bar dataKey="Real rosters" fill="#7aa2ff" radius={[5, 5, 0, 0]} />
          <Bar dataKey="Expected from size" fill="#f7b955" radius={[5, 5, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
      {leader ? (
        <InsightNote label="Careful">
          Treat country rankings carefully: high rates often mean larger
          rosters, not a country-specific birthday effect. The useful signal is
          the gap from the roster-size baseline; here{" "}
          <span className="font-semibold text-white">{leader.country}</span>{" "}
          runs {fmtDelta(leader.deviation)} above expectation.
        </InsightNote>
      ) : null}
    </div>
  );
}
