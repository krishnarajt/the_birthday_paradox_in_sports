import "./globals.css";
import Link from "next/link";

export const metadata = {
  title: "Birthday Paradox in Sports",
  description:
    "Does the birthday paradox actually hold in real sports teams? An evidence-driven dashboard across MLB, NFL, NHL, the Olympics, world football and more.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <header className="sticky top-0 z-20 backdrop-blur bg-ink/70 border-b border-white/5">
          <div className="max-w-7xl mx-auto px-5 py-3 flex items-center gap-4">
            <Link href="/" className="font-semibold tracking-tight text-lg">
              <span className="text-accent">🎂</span> Birthday Paradox · Sports
            </Link>
            <nav className="ml-auto flex items-center gap-4 text-sm text-white/70">
              <Link href="/">Dashboard</Link>
              <Link href="/sports/">Sports</Link>
              <Link href="/curiosities/">Curiosities</Link>
              <a
                href="https://en.wikipedia.org/wiki/Birthday_problem"
                target="_blank"
                rel="noreferrer"
              >
                The math
              </a>
            </nav>
          </div>
        </header>
        <main className="max-w-7xl mx-auto px-5 py-8">{children}</main>
        <footer className="max-w-7xl mx-auto px-5 py-10 text-xs text-white/40">
          Data: MLB (Lahman), NFL (nflverse), NHL (current team lists), world
          football (footballcsv), Olympics (olympedia), Women's World Cup 2023,
          Paris 2024, NBA/WNBA/top European football leagues (ESPN), men's &
          women's cricket (Cricsheet + Wikidata DOBs).
        </footer>
      </body>
    </html>
  );
}
