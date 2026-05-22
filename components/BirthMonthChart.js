"use client";

import { useState } from "react";
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend,
} from "recharts";
import InsightNote from "./InsightNote";

const fmtPct = (v) =>
  v === null || v === undefined ? "—" : `${(v * 100).toFixed(2)}%`;

export default function BirthMonthChart({ data }) {
  const sports = data.by_sport ? Object.keys(data.by_sport) : [];
  const [view, setView] = useState("overall");

  const rows = (view === "overall" ? data.overall : data.by_sport[view]).map(
    (r) => ({
      month: r.month_name,
      Athletes: r.share,
      "Expected (uniform)": r.expected_share,
      count: r.count,
      deviation: r.deviation,
    })
  );
  const mostOver = [...rows].sort((a, b) => b.deviation - a.deviation)[0];
  const mostUnder = [...rows].sort((a, b) => a.deviation - b.deviation)[0];
  const mostByCount = [...rows].sort((a, b) => b.count - a.count)[0];

  return (
    <div className="card">
      <div className="flex flex-wrap items-baseline justify-between gap-2 mb-1">
        <h3 className="text-lg font-semibold">
          When are athletes actually born?
        </h3>
        <select
          value={view}
          onChange={(e) => setView(e.target.value)}
          className="bg-white/5 border border-white/10 rounded-md text-xs px-2 py-1 text-white/80"
        >
          <option value="overall">All sports combined</option>
          {sports.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </div>
      <p className="text-xs text-white/50 mb-4">
        Share of athletes born in each calendar month, vs the share you'd expect
        if birthdays were evenly spread across calendar days. The orange line
        adjusts for month length, so February gets a lower fair-share baseline.
      </p>
      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={rows} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
          <CartesianGrid stroke="#ffffff10" />
          <XAxis dataKey="month" tick={{ fill: "#cdd6ff", fontSize: 12 }} />
          <YAxis
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            tickFormatter={fmtPct}
            domain={[0.06, 0.11]}
          />
          <Tooltip
            contentStyle={{
              background: "#121933",
              border: "1px solid #ffffff20",
              borderRadius: 12,
              color: "#e7ecff",
            }}
            formatter={(value, name) => [fmtPct(value), name]}
          />
          <Legend wrapperStyle={{ color: "#cdd6ff", fontSize: 12 }} />
          <Bar dataKey="Athletes" fill="#7c9cff" radius={[6, 6, 0, 0]} />
          <Line
            type="monotone"
            dataKey="Expected (uniform)"
            stroke="#ffb86b"
            strokeWidth={2}
            dot={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
      <p className="text-[11px] text-white/40 mt-3">
        Based on {data.total_unique_players.toLocaleString()} unique athletes
        across all loaded datasets. Records with missing or year-only DOBs
        are dropped at ETL time, so what you see here is real signal.
      </p>
      {mostOver && mostUnder && mostByCount ? (
        <InsightNote label="Surprise">
          <span className="font-semibold text-white">{mostByCount.month}</span>{" "}
          has the most athletes by raw count, but{" "}
          <span className="font-semibold text-white">{mostOver.month}</span>{" "}
          is furthest above its fair share after month length is accounted for.{" "}
          <span className="font-semibold text-white">{mostUnder.month}</span>{" "}
          is the lightest month relative to expectation.
        </InsightNote>
      ) : null}
    </div>
  );
}
