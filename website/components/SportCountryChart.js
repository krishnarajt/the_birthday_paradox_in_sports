"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";
import InsightNote from "./InsightNote";

const fmtPct = (v) =>
  v === null || v === undefined ? "—" : `${(v * 100).toFixed(1)}%`;

export default function SportCountryChart({ data, sport }) {
  const rows = data
    .filter((r) => r.cohorts >= 5)
    .sort((a, b) => b.cohorts - a.cohorts)
    .slice(0, 12)
    .map((r) => ({
      country: r.country,
      cohorts: r.cohorts,
      observed_rate: r.observed_rate,
      avg_roster_size: r.avg_roster_size,
    }));
  const high = [...rows].sort((a, b) => b.observed_rate - a.observed_rate)[0];

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">Country comparison</h3>
      <p className="text-xs text-white/50 mb-4">
        Countries with the most rosters for {sport}, charted by the share of
        rosters with at least one shared birthday.
      </p>
      <ResponsiveContainer width="100%" height={340}>
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
              name === "observed_rate" ? fmtPct(value) : value
            }
          />
          <Bar dataKey="observed_rate" fill="#7aa2ff" radius={[6, 6, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
      {high ? (
        <InsightNote label="What to compare">
          This view is best for spotting sample-shape differences inside a
          sport. Among the highest-sample countries,{" "}
          <span className="font-semibold text-white">{high.country}</span>{" "}
          has the highest observed rate, with an average roster of{" "}
          <span className="font-semibold text-white">
            {high.avg_roster_size?.toFixed(1)}
          </span>.
        </InsightNote>
      ) : null}
    </div>
  );
}
