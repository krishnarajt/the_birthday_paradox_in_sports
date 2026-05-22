"use client";

import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import InsightNote from "./InsightNote";

const fmtPct = (v) => (v === null || v === undefined ? "—" : `${(v * 100).toFixed(1)}%`);
const fmtNum = (n) => new Intl.NumberFormat("en-US").format(Math.round(n));

export default function PopularityScatter({ data }) {
  const rows = data.map((d) => ({
    sport: d.sport.replace("Olympic ", ""),
    players: d.total_players,
    observed_rate: d.observed_rate,
    theoretical_rate: d.theoretical_rate,
    cohorts: d.cohorts,
  }));
  const biggestSport = [...rows].sort((a, b) => b.players - a.players)[0];
  const highestRate = [...rows].sort((a, b) => b.observed_rate - a.observed_rate)[0];

  return (
    <div className="card">
      <h3 className="text-lg font-semibold mb-1">Popularity vs paradox rate</h3>
      <p className="text-xs text-white/50 mb-4">
        Each point is a sport. X = player entries in the dataset (a rough size
        proxy, log scale). Y = share of team lists with shared birthdays.
      </p>
      <ResponsiveContainer width="100%" height={420}>
        <ScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 0 }}>
          <CartesianGrid stroke="#ffffff10" />
          <XAxis
            type="number"
            dataKey="players"
            name="Players"
            scale="log"
            domain={["auto", "auto"]}
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            tickFormatter={fmtNum}
            label={{ value: "Player entries (log scale)", position: "insideBottom", offset: -10, fill: "#cdd6ff" }}
          />
          <YAxis
            type="number"
            dataKey="observed_rate"
            name="Real team lists"
            domain={[0, 1]}
            tick={{ fill: "#cdd6ff", fontSize: 11 }}
            tickFormatter={fmtPct}
          />
          <ZAxis type="number" dataKey="cohorts" range={[40, 400]} />
          <Tooltip
            cursor={{ stroke: "#ffffff30" }}
            contentStyle={{
              background: "#121933",
              border: "1px solid #ffffff20",
              borderRadius: 12,
              color: "#e7ecff",
            }}
            formatter={(v, key) => {
              if (key === "observed_rate" || key === "theoretical_rate") return fmtPct(v);
              if (key === "players" || key === "cohorts") return fmtNum(v);
              return v;
            }}
            labelFormatter={() => ""}
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const p = payload[0].payload;
              return (
                <div className="rounded-xl bg-panel ring-1 ring-white/10 p-3 text-sm">
                  <div className="font-semibold">{p.sport}</div>
                  <div>Real team lists: {fmtPct(p.observed_rate)}</div>
                  <div>Expected from size: {fmtPct(p.theoretical_rate)}</div>
                  <div>Player entries: {fmtNum(p.players)}</div>
                  <div>Team lists: {fmtNum(p.cohorts)}</div>
                </div>
              );
            }}
          />
          <Scatter data={rows} fill="#7aa2ff" />
        </ScatterChart>
      </ResponsiveContainer>
      {biggestSport && highestRate ? (
        <InsightNote label="Surprise">
          Popularity itself is not the magic ingredient.{" "}
          <span className="font-semibold text-white">{biggestSport.sport}</span>{" "}
          has the most player entries here, while{" "}
          <span className="font-semibold text-white">{highestRate.sport}</span>{" "}
          has the highest shared-birthday rate because its teams are much
          larger.
        </InsightNote>
      ) : null}
    </div>
  );
}
