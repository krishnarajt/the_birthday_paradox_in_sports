"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from "recharts";
import InsightNote from "./InsightNote";

const fmtPct = (v) =>
  v === null || v === undefined ? "—" : `${(v * 100).toFixed(1)}%`;
const fmtDelta = (v) => {
  if (v === null || v === undefined) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(1)} points`;
};

export default function DeviationExtremesChart({ sports, minCohorts = 50 }) {
  const rows = sports
    .filter(
      (s) =>
        s.cohorts >= minCohorts &&
        s.observed_rate !== null &&
        s.theoretical_rate !== null
    )
    .map((s) => ({
      sport: s.sport.replace("Olympic ", ""),
      cohorts: s.cohorts,
      observed_rate: s.observed_rate,
      theoretical_rate: s.theoretical_rate,
      deviation: s.observed_rate - s.theoretical_rate,
    }))
    .sort((a, b) => Math.abs(b.deviation) - Math.abs(a.deviation))
    .slice(0, 12)
    .sort((a, b) => a.deviation - b.deviation);
  const top = [...rows].sort((a, b) => Math.abs(b.deviation) - Math.abs(a.deviation))[0];
  const maxAbs = Math.max(...rows.map((r) => Math.abs(r.deviation)), 0.01);

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">
        Where real teams beat the math
      </h3>
      <p className="text-xs text-white/50 mb-4">
        Sports where real rosters have the biggest gap from what random
        same-size rosters would predict. Minimum {minCohorts} team rosters to
        avoid tiny-sample noise.
      </p>
      <ResponsiveContainer width="100%" height={430}>
        <BarChart
          data={rows}
          layout="vertical"
          margin={{ top: 10, right: 20, left: 120, bottom: 10 }}
        >
          <CartesianGrid stroke="#ffffff10" />
          <XAxis
            type="number"
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            tickFormatter={fmtDelta}
            domain={[-maxAbs * 1.1, maxAbs * 1.1]}
          />
          <YAxis
            type="category"
            dataKey="sport"
            width={180}
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
          />
          <Tooltip
            cursor={{ fill: "#ffffff08" }}
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const p = payload[0].payload;
              return (
                <div className="rounded-xl bg-panel ring-1 ring-white/10 p-3 text-sm">
                  <div className="font-semibold">{p.sport}</div>
                  <div>Real rosters: {fmtPct(p.observed_rate)}</div>
                  <div>Expected from size: {fmtPct(p.theoretical_rate)}</div>
                  <div>Gap: {fmtDelta(p.deviation)}</div>
                  <div>Team rosters: {p.cohorts.toLocaleString()}</div>
                </div>
              );
            }}
          />
          <ReferenceLine x={0} stroke="#ffffff80" />
          <Bar dataKey="deviation" radius={[4, 4, 4, 4]}>
            {rows.map((entry) => (
              <Cell
                key={entry.sport}
                fill={entry.deviation >= 0 ? "#5ad48b" : "#ff7a90"}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      {top ? (
        <InsightNote label="Surprise">
          The big misses mostly go upward: real teams share birthdays more often
          than the clean birthday-problem model predicts.{" "}
          <span className="font-semibold text-white">{top.sport}</span> is the
          standout at {fmtDelta(top.deviation)}.
        </InsightNote>
      ) : null}
    </div>
  );
}
