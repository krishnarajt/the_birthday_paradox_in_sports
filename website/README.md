# Birthday Paradox in Sports — Website

Static dashboard powered by Next.js (App Router) + Recharts + Tailwind.
Reads the JSON files in `public/data/` produced by the Python pipeline in
`../analysis/`.

## Local dev

```bash
npm install
npm run dev          # http://localhost:3000
```

## Refresh the data

From the repository root:

```bash
python -m analysis.etl          # rebuild rosters table
python -m analysis.compute      # compute group_stats
python -m analysis.export_web   # write JSON files into website/public/data
```

## Deploy to Vercel

The project is configured for **static export** (`output: 'export'` in
`next.config.js`). On Vercel:

- Framework preset: **Next.js**
- Root directory: `website`
- Build command: `npm run build`
- Output directory: `out`

Or run locally:

```bash
npm run build        # writes static site to ./out
```

## Pages

- `/` — dashboard with the 5 headline questions
- `/sports/` — full grid of sports / Olympic disciplines
- `/sport/<slug>/` — per-sport detail (one static page per sport)
