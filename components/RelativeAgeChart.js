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
  ReferenceLine,
} from "recharts";
import InsightNote from "./InsightNote";

const fmtPct = (v) =>
  v === null || v === undefined ? "—" : `${(v * 100).toFixed(1)}%`;
const fmtPoints = (v) => {
  if (v === null || v === undefined) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(1)} points`;
};

export default function RelativeAgeChart({ data }) {
  const rows = data.by_sport.map((s) => ({
    sport: s.sport.replace("Olympic ", ""),
    Q1: s.Q1,
    Q2: s.Q2,
    Q3: s.Q3,
    Q4: s.Q4,
  }));
  const strongestQ1 = [...data.by_sport].sort(
    (a, b) => (b.Q1 - b.expected.Q1) - (a.Q1 - a.expected.Q1)
  )[0];

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">
        The relative-age effect
      </h3>
      <p className="text-xs text-white/50 mb-4">
        Share of athletes born in each calendar quarter, by sport. Under a flat
        distribution every quarter would be roughly 25%. A spike in Q1 means
        athletes born early in the year are over-represented — a well-known
        signature of youth-team age cutoffs.
      </p>
      <ResponsiveContainer width="100%" height={420}>
        <BarChart
          data={rows}
          layout="vertical"
          margin={{ top: 10, right: 20, left: 80, bottom: 10 }}
          stackOffset="expand"
        >
          <CartesianGrid stroke="#ffffff10" />
          <XAxis
            type="number"
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            tickFormatter={fmtPct}
            domain={[0, 1]}
          />
          <YAxis
            type="category"
            dataKey="sport"
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            width={150}
          />
          <Tooltip
            contentStyle={{
              background: "#121933",
              border: "1px solid #ffffff20",
              borderRadius: 12,
              color: "#e7ecff",
            }}
            formatter={(value) => fmtPct(value)}
          />
          <Legend wrapperStyle={{ color: "#cdd6ff", fontSize: 12 }} />
          <Bar dataKey="Q1" stackId="q" fill="#7c9cff" />
          <Bar dataKey="Q2" stackId="q" fill="#9d7cff" />
          <Bar dataKey="Q3" stackId="q" fill="#ffb86b" />
          <Bar dataKey="Q4" stackId="q" fill="#ff7c9c" />
          <ReferenceLine x={0.25} stroke="#ffffff60" strokeDasharray="3 3" />
          <ReferenceLine x={0.5} stroke="#ffffff60" strokeDasharray="3 3" />
          <ReferenceLine x={0.75} stroke="#ffffff60" strokeDasharray="3 3" />
        </BarChart>
      </ResponsiveContainer>
      <p className="text-[11px] text-white/40 mt-3">
        Dashed lines mark the 25/50/75% break-points of a perfectly uniform
        distribution. Overall across all sports: Q1 {fmtPct(data.overall.Q1)},
        Q2 {fmtPct(data.overall.Q2)}, Q3 {fmtPct(data.overall.Q3)}, Q4{" "}
        {fmtPct(data.overall.Q4)}.
      </p>
      {strongestQ1 ? (
        <InsightNote label="Surprise">
          The most revealing signal is not that Q1 is slightly high overall; it
          is that <span className="font-semibold text-white">{strongestQ1.sport.replace("Olympic ", "")}</span>{" "}
          is {fmtPoints(strongestQ1.Q1 - strongestQ1.expected.Q1)} above its
          calendar expectation. That looks like selection pressure, not random
          birthday noise.
        </InsightNote>
      ) : null}
    </div>
  );
}
