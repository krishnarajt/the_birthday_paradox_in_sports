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

const fmtPct = (v) => (v === null || v === undefined ? "—" : `${(v * 100).toFixed(1)}%`);
const fmtDelta = (v) => {
  if (v === null || v === undefined) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(1)} points`;
};

export default function ObservedVsTheoreticalChart({ data }) {
  // sort by cohorts descending, take top 15 for legibility
  const rows = [...data]
    .sort((a, b) => b.cohorts - a.cohorts)
    .slice(0, 15)
    .map((d) => ({
      sport: d.sport.replace("Olympic ", ""),
      "Real rosters": d.observed_rate,
      "Expected from size": d.theoretical_rate,
      deviation: d.observed_rate - d.theoretical_rate,
      cohorts: d.cohorts,
    }));
  const biggestGap = [...rows].sort(
    (a, b) => Math.abs(b.deviation) - Math.abs(a.deviation)
  )[0];

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">
        Real rosters vs birthday math
      </h3>
      <p className="text-xs text-white/50 mb-4">
        Share of team rosters where at least two players share a birthday.
        Top 15 sports by number of real rosters in the dataset.
      </p>
      <ResponsiveContainer width="100%" height={420}>
        <BarChart
          data={rows}
          margin={{ top: 10, right: 10, left: 0, bottom: 80 }}
        >
          <CartesianGrid stroke="#ffffff10" />
          <XAxis
            dataKey="sport"
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            interval={0}
            angle={-35}
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
            formatter={(v) => fmtPct(v)}
          />
          <Legend wrapperStyle={{ color: "#cdd6ff" }} />
          <Bar dataKey="Real rosters" fill="#7aa2ff" radius={[6, 6, 0, 0]} />
          <Bar dataKey="Expected from size" fill="#f7b955" radius={[6, 6, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
      {biggestGap ? (
        <InsightNote label="Why this matters">
          The main pattern is reassuring: real rates usually track the math.
          The interesting exception among these high-sample
          sports is <span className="font-semibold text-white">{biggestGap.sport}</span>,
          which sits {fmtDelta(biggestGap.deviation)} from the same-size random
          roster baseline.
        </InsightNote>
      ) : null}
    </div>
  );
}
