"""
ESPN Cricinfo Complete Profile Scraper
Extracts all available player information

Installation:
    pip install playwright beautifulsoup4
    playwright install firefox

Usage:
    scraper = CricinfoProfileScraper("1365288")
    profile = scraper.get_profile()
"""

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional, Dict, List
import time
import random
import re


class CricinfoProfileScraper:
    """
    Complete Cricinfo player profile scraper
    """

    def __init__(self, slug_or_id: str, timeout: int = 30000):
        self.slug_or_id = self._normalize_slug(slug_or_id)
        self.timeout = timeout

    @staticmethod
    def _normalize_slug(slug: str) -> str:
        slug = str(slug).strip()
        if slug.isdigit():
            return slug
        if "-" in slug:
            parts = slug.split("-")
            player_id = parts[-1]
            if player_id.isdigit():
                return player_id
        raise ValueError(f"Invalid Cricinfo slug or ID: {slug}")

    def _build_url(self) -> str:
        return f"https://www.espncricinfo.com/cricketers/player-{self.slug_or_id}"

    def _fetch_page(self) -> BeautifulSoup:
        """Fetch page with anti-detection"""
        with sync_playwright() as p:
            print("Launching Firefox...")
            browser = p.firefox.launch(
                headless=False,
                firefox_user_prefs={
                    "dom.webdriver.enabled": False,
                    "useAutomationExtension": False,
                },
            )

            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            )

            page = context.new_page()

            try:
                url = self._build_url()

                print(f"Visiting homepage first...")
                try:
                    page.goto("https://www.espncricinfo.com/", timeout=15000)
                    page.wait_for_timeout(2000)
                    print("✓ Homepage loaded")
                except Exception as e:
                    print(f"⚠ Homepage load issue (continuing anyway): {e}")

                print(f"Navigating to: {url}")
                try:
                    # Use domcontentloaded instead of networkidle (faster)
                    page.goto(url, timeout=self.timeout, wait_until="domcontentloaded")
                    print("✓ Page DOM loaded, waiting for content...")
                    page.wait_for_timeout(3000)
                    print("✓ Content should be ready")
                except PlaywrightTimeout:
                    print(
                        "⚠ Timeout but page may have loaded, trying to get content..."
                    )

                html = page.content()

                # Save for debugging
                with open(f"debug_{self.slug_or_id}.html", "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"✓ HTML saved to debug_{self.slug_or_id}.html")

                return BeautifulSoup(html, "html.parser")

            except Exception as e:
                print(f"\n❌ Error during fetch: {e}")
                raise
            finally:
                print("Closing browser...")
                time.sleep(1)
                browser.close()
                print("✓ Browser closed")

    def _extract_basic_info(self, soup: BeautifulSoup) -> Dict:
        """Extract basic profile information"""
        info = {}

        # Find all label-value pairs
        for p in soup.find_all("p", class_="ds-text-tight-m"):
            label = p.get_text(strip=True).upper()
            parent = p.find_parent("div")
            if parent:
                value_span = parent.find("span", class_="ds-text-title-s")
                if value_span:
                    value_p = value_span.find("p")
                    if value_p:
                        value = value_p.get_text(strip=True)
                        info[label] = value

        return info

    def _parse_date(self, date_str: str) -> Optional[datetime.date]:
        """Parse date string to date object"""
        if not date_str:
            return None

        # Try common formats
        formats = [
            "%B %d, %Y",  # December 10, 2007
            "%B %d %Y",  # December 10 2007
            "%d %B %Y",  # 10 December 2007
            "%d/%m/%Y",  # 10/12/2007
            "%Y-%m-%d",  # 2007-12-10
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except:
                continue

        return None

    def _extract_teams(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract team information"""
        teams = []

        # Find the TEAMS section
        teams_section = None
        for p in soup.find_all("p", class_="ds-text-tight-m"):
            if p.get_text(strip=True).upper() == "TEAMS":
                teams_section = p.find_parent("div")
                break

        if teams_section:
            # Find all team links
            for link in teams_section.find_all("a", href=True):
                team_name_span = link.find("span", class_="ds-text-title-s")
                if team_name_span:
                    team_name = team_name_span.get_text(strip=True)
                    team_url = link.get("href", "")
                    teams.append(
                        {
                            "name": team_name,
                            "url": (
                                f"https://www.espncricinfo.com{team_url}"
                                if team_url.startswith("/")
                                else team_url
                            ),
                        }
                    )

        return teams

    def _extract_career_stats(self, soup: BeautifulSoup) -> Dict:
        """Extract career statistics tables"""
        stats = {"batting": [], "bowling": []}

        # Find all stat tables
        tables = soup.find_all("table", class_="ds-table")

        for table in tables:
            # Check if it's batting or bowling by looking at headers
            headers = [th.get_text(strip=True) for th in table.find_all("th")]

            is_batting = any(h in headers for h in ["Runs", "HS", "Ave", "SR", "BF"])
            is_bowling = any(h in headers for h in ["Wkts", "BBI", "BBM", "Econ"])

            # Extract rows
            rows = []
            for tr in table.find_all("tr")[1:]:  # Skip header
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if cells:
                    row_dict = dict(zip(headers, cells))
                    rows.append(row_dict)

            if is_batting and rows:
                stats["batting"] = rows
            elif is_bowling and rows:
                stats["bowling"] = rows

        return stats

    def _extract_debut_last(self, soup: BeautifulSoup) -> Dict:
        """Extract debut and last match information"""
        debut_last = {}

        # Look for debut/last section
        for header in soup.find_all(
            ["h2", "p"], class_=lambda x: x and "debut" in str(x).lower()
        ):
            section = header.find_parent("div")
            if section:
                # Find debut and last match links
                for link in section.find_all("a", href=True):
                    text = link.get_text(strip=True)
                    if "vs" in text.lower():
                        if (
                            "debut"
                            in link.get_previous("span", class_="ds-text-tight-m")
                            .get_text()
                            .lower()
                        ):
                            debut_last["debut"] = text
                        elif (
                            "last"
                            in link.get_previous("span", class_="ds-text-tight-m")
                            .get_text()
                            .lower()
                        ):
                            debut_last["last"] = text

        return debut_last

    def _extract_playing_role(self, soup: BeautifulSoup) -> Optional[str]:
        """Try to infer playing role from stats"""
        # Look in career stats section
        stats = self._extract_career_stats(soup)

        if stats["batting"] and stats["bowling"]:
            return "Allrounder"
        elif stats["batting"]:
            return "Batter"
        elif stats["bowling"]:
            return "Bowler"

        return None

    def get_profile(self) -> Dict:
        """Scrape complete player profile"""
        print("=" * 60)
        print("Starting complete profile scrape...")
        print("=" * 60)

        soup = self._fetch_page()

        # Check for access denied
        if "Access Denied" in soup.get_text():
            print("\n⚠️  WARNING: Still blocked by Akamai")
            return {"error": "Access Denied"}

        print("✓ Successfully fetched page!")

        # Extract all information
        basic_info = self._extract_basic_info(soup)

        # Parse date of birth
        dob = None
        birthplace = None
        if "BORN" in basic_info:
            dob = self._parse_date(basic_info["BORN"])
            # If date parsing failed, the whole string might be birthplace
            if not dob:
                birthplace = basic_info["BORN"]

        # Build complete profile
        profile = {
            # Basic Information
            "full_name": basic_info.get("FULL NAME"),
            "date_of_birth": dob,
            "birthplace": birthplace,
            "age": basic_info.get("AGE"),
            "batting_style": basic_info.get("BATTING STYLE"),
            "bowling_style": basic_info.get("BOWLING STYLE"),
            "playing_role": basic_info.get("PLAYING ROLE")
            or self._extract_playing_role(soup),
            # Teams
            "teams": self._extract_teams(soup),
            # Career Stats
            "career_stats": self._extract_career_stats(soup),
            # Debut/Last matches
            "debut_last": self._extract_debut_last(soup),
            # Additional fields from schema.org data
            "gender": None,
            "nationality": None,
        }

        # Try to extract from schema.org JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json

                data = json.loads(script.string)
                if data.get("@type") == "Person":
                    profile["gender"] = data.get("gender")
                    profile["nationality"] = data.get("nationality", {}).get("name")
                    if not profile["date_of_birth"] and "birthDate" in data:
                        profile["date_of_birth"] = self._parse_date(data["birthDate"])
            except:
                pass

        return profile


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ESPN CRICINFO COMPLETE PROFILE SCRAPER")
    print("=" * 60)

    test_player = "1365288"  # Adeshola Adekunle

    try:
        scraper = CricinfoProfileScraper(test_player)
        profile = scraper.get_profile()

        if "error" in profile:
            print(f"\n❌ Error: {profile['error']}")
        else:
            print("\n" + "=" * 60)
            print("COMPLETE PROFILE:")
            print("=" * 60)

            # Basic Info
            print("\n--- BASIC INFORMATION ---")
            print(f"Full Name: {profile['full_name']}")
            print(f"Date of Birth: {profile['date_of_birth']}")
            print(f"Age: {profile['age']}")
            print(f"Birthplace: {profile['birthplace']}")
            print(f"Nationality: {profile['nationality']}")
            print(f"Gender: {profile['gender']}")
            print(f"Batting Style: {profile['batting_style']}")
            print(f"Bowling Style: {profile['bowling_style']}")
            print(f"Playing Role: {profile['playing_role']}")

            # Teams
            print("\n--- TEAMS ---")
            for team in profile["teams"]:
                print(f"  • {team['name']}")

            # Career Stats
            print("\n--- CAREER STATS (BATTING) ---")
            for stat in profile["career_stats"]["batting"]:
                print(f"  {stat}")

            print("\n--- CAREER STATS (BOWLING) ---")
            for stat in profile["career_stats"]["bowling"]:
                print(f"  {stat}")

            # Debut/Last
            if profile["debut_last"]:
                print("\n--- DEBUT & LAST MATCHES ---")
                for key, value in profile["debut_last"].items():
                    print(f"  {key}: {value}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
