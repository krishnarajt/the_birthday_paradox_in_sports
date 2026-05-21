"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Legend,
} from "recharts";
import InsightNote from "./InsightNote";

const fmtPct = (v) => (v === null || v === undefined ? "—" : `${(v * 100).toFixed(1)}%`);

const COLORS = [
  "#7aa2ff", "#f7b955", "#5ad48b", "#ff7a90",
  "#a78bfa", "#22d3ee", "#facc15", "#fb923c",
];

export default function SportSizeCurveChart({ curve }) {
  const theory = curve.__theory_curve__;
  const universal = curve.__universal__;
  const halfPoint = theory.find((r) => r.p >= 0.5)?.n;
  const ninetyPoint = theory.find((r) => r.p >= 0.9)?.n;

  // Merge curves into a single dataset keyed by n.
  const sportNames = Object.keys(curve).filter(
    (k) => !k.startsWith("__")
  );

  const byN = new Map();
  for (const { n, p } of theory) byN.set(n, { n, "Expected from math": p });
  for (const r of universal) {
    const row = byN.get(r.n) || { n: r.n };
    row["All real team lists"] = r.observed_rate;
    byN.set(r.n, row);
  }
  for (const sport of sportNames) {
    for (const r of curve[sport]) {
      const row = byN.get(r.n) || { n: r.n };
      row[sport.replace("Olympic ", "")] = r.observed_rate;
      byN.set(r.n, row);
    }
  }
  const data = [...byN.values()].sort((a, b) => a.n - b.n);

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">
        Probability vs team size
      </h3>
      <p className="text-xs text-white/50 mb-4">
        Expected curve (yellow) vs real rates at each team size. Individual
        sports are plotted only where at least 3 real team lists exist at that size.
      </p>
      <ResponsiveContainer width="100%" height={420}>
        <LineChart data={data} margin={{ top: 10, right: 16, bottom: 10, left: 0 }}>
          <CartesianGrid stroke="#ffffff10" />
          <XAxis
            dataKey="n"
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            label={{ value: "Players on team list", position: "insideBottom", offset: -5, fill: "#cdd6ff" }}
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
            labelFormatter={(n) => `n = ${n}`}
          />
          <Legend wrapperStyle={{ color: "#cdd6ff" }} />
          <Line
            type="monotone"
            dataKey="Expected from math"
            stroke="#f7b955"
            strokeWidth={3}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="All real team lists"
            stroke="#ffffff"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          {sportNames.map((s, i) => (
            <Line
              key={s}
              type="monotone"
              dataKey={s.replace("Olympic ", "")}
              stroke={COLORS[i % COLORS.length]}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
      <InsightNote label="Why this matters">
        This is the paradox in one curve: by team size{" "}
        <span className="font-semibold text-white">{halfPoint}</span>, the
        theoretical chance has already crossed 50%; by{" "}
        <span className="font-semibold text-white">{ninetyPoint}</span>, it is
        over 90%. Most big rates on this site are team-size effects first,
        sport effects second.
      </InsightNote>
    </div>
  );
}
