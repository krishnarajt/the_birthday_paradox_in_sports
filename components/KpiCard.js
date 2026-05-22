export default function KpiCard({ label, value, sub, insight }) {
  return (
    <div className="card">
      <div className="kpi-label">{label}</div>
      <div className="kpi-value mt-2">{value}</div>
      {sub ? (
        <div className="text-xs text-white/50 mt-1">{sub}</div>
      ) : null}
      {insight ? (
        <div className="mt-3 border-t border-white/5 pt-3 text-xs leading-relaxed text-white/65">
          <span className="font-semibold text-accent">Why it matters: </span>
          {insight}
        </div>
      ) : null}
    </div>
  );
}
