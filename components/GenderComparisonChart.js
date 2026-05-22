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
const fmtNum = (n) =>
  n === null || n === undefined ? "—" : new Intl.NumberFormat("en-US").format(Math.round(n));

const genderLabel = (g) =>
  g === "F" ? "Women" : g === "M" ? "Men" : g || "Unknown";

export default function GenderComparisonChart({
  data,
  title = "Gender split: rate vs team size",
  description = "Real shared-birthday rates compared with same-size random teams, where the source records gender.",
}) {
  const rows = (Array.isArray(data) ? data : data?.overall || []).map((r) => ({
    gender: genderLabel(r.gender),
    cohorts: r.cohorts,
    "Real team lists": r.observed_rate,
    "Expected from size": r.theoretical_rate,
    avg_roster_size: r.avg_roster_size,
  }));
  const largestTeam = [...rows].sort(
    (a, b) => (b.avg_roster_size || 0) - (a.avg_roster_size || 0)
  )[0];

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">{title}</h3>
      <p className="text-xs text-white/50 mb-4">{description}</p>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={rows} margin={{ top: 10, right: 16, left: 0, bottom: 10 }}>
          <CartesianGrid stroke="#ffffff10" />
          <XAxis dataKey="gender" tick={{ fill: "#cdd6ff", fontSize: 12 }} />
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
            formatter={(value, name, item) => {
              if (name === "Real team lists" || name === "Expected from size") return fmtPct(value);
              return item?.payload?.avg_roster_size ? fmtNum(item.payload.avg_roster_size) : value;
            }}
          />
          <Legend wrapperStyle={{ color: "#cdd6ff", fontSize: 12 }} />
          <Bar dataKey="Real team lists" fill="#7aa2ff" radius={[6, 6, 0, 0]} />
          <Bar dataKey="Expected from size" fill="#f7b955" radius={[6, 6, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
      {largestTeam ? (
        <InsightNote label="Why it matters">
          The gender gap is mostly a team-size story.{" "}
          <span className="font-semibold text-white">{largestTeam.gender}</span>{" "}
          have the larger average team size here
          {largestTeam.avg_roster_size
            ? ` (${largestTeam.avg_roster_size.toFixed(1)} athletes), so their birthday-match rate naturally rises.`
            : ", so compare the real bars against the expected bars before reading too much into the raw rate."}
        </InsightNote>
      ) : null}
    </div>
  );
}
