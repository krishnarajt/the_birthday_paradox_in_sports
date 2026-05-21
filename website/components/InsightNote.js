export default function InsightNote({ children, label = "What to notice" }) {
  return (
    <div className="mt-4 rounded-lg border border-accent/20 bg-accent/10 px-3 py-2 text-sm text-white/75">
      <span className="font-semibold text-accent">{label}: </span>
      {children}
    </div>
  );
}
