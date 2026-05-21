import fs from "node:fs";
import path from "node:path";

const DATA_DIR = path.join(process.cwd(), "public", "data");

export function loadJson(relPath) {
  const full = path.join(DATA_DIR, relPath);
  return JSON.parse(fs.readFileSync(full, "utf8"));
}

export function listSports() {
  return loadJson("sports_index.json");
}

export function loadSport(slug) {
  return loadJson(`sports/${slug}.json`);
}

export function fmtPct(p, digits = 1) {
  if (p === null || p === undefined || Number.isNaN(p)) return "—";
  return `${(p * 100).toFixed(digits)}%`;
}

export function fmtNum(n) {
  if (n === null || n === undefined) return "—";
  return new Intl.NumberFormat("en-US").format(Math.round(n));
}

export function fmtDelta(d, digits = 1) {
  if (d === null || d === undefined) return "—";
  const sign = d >= 0 ? "+" : "";
  return `${sign}${(d * 100).toFixed(digits)} points`;
}
