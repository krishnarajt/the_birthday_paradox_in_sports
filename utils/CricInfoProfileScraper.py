"""
ESPN Cricinfo Profile Scraper with Advanced Anti-Detection
Uses multiple techniques to bypass Akamai bot protection

Installation:
    pip install playwright-stealth playwright beautifulsoup4
    playwright install firefox

Usage:
    scraper = CricinfoProfileScraper("1365288")
    profile = scraper.get_profile()
"""

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional
import time
import random


class CricinfoProfileScraper:
    """
    Scrapes player metadata from ESPN Cricinfo using advanced anti-detection
    """

    def __init__(self, slug_or_id: str, timeout: int = 60000):
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
        """
        Fetch with maximum stealth - multiple strategies
        """
        with sync_playwright() as p:
            # Strategy 1: Use Firefox (often less detected than Chrome)
            print("Launching Firefox with stealth settings...")
            browser = p.firefox.launch(
                headless=False,  # Non-headless is less detectable
                firefox_user_prefs={
                    "dom.webdriver.enabled": False,
                    "useAutomationExtension": False,
                    "general.platform.override": "Win32",
                    "general.useragent.override": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
                },
            )

            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
                locale="en-US",
                timezone_id="America/New_York",
                permissions=["geolocation"],
                geolocation={"latitude": 40.7128, "longitude": -74.0060},  # New York
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                },
            )

            page = context.new_page()

            # Add random mouse movements and human-like behavior
            try:
                url = self._build_url()
                print(f"Navigating to: {url}")

                # First, visit the homepage to get cookies
                print("Visiting homepage first to establish session...")
                page.goto("https://www.espncricinfo.com/", timeout=self.timeout)

                # Simulate human behavior
                page.wait_for_timeout(random.randint(2000, 4000))

                # Scroll a bit
                page.evaluate("window.scrollBy(0, 300)")
                page.wait_for_timeout(random.randint(500, 1500))

                # Now visit the actual player page
                print(f"Now navigating to player page...")
                page.goto(url, timeout=self.timeout, wait_until="networkidle")

                # More human behavior
                page.wait_for_timeout(random.randint(3000, 5000))
                page.evaluate("window.scrollBy(0, 500)")
                page.wait_for_timeout(1000)

                html = page.content()

                # Check for access denied
                if "Access Denied" in html or "access denied" in html.lower():
                    print("\n❌ Still getting Access Denied!")
                    print("The page is using Akamai bot protection.")
                    print(
                        "\nTrying alternative approach - keeping browser open longer..."
                    )

                    # Keep browser open and wait
                    page.wait_for_timeout(10000)
                    html = page.content()

                soup = BeautifulSoup(html, "html.parser")

                # Save for debugging
                filename = f"cricinfo_debug_{self.slug_or_id}.html"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(soup.prettify())
                print(f"✓ HTML saved to {filename}")

                return soup

            except PlaywrightTimeout:
                raise RuntimeError(f"Timeout loading {url}")
            except Exception as e:
                raise RuntimeError(f"Failed to fetch: {str(e)}")
            finally:
                print("\nClosing browser in 3 seconds...")
                time.sleep(3)
                browser.close()

    @staticmethod
    def _extract_labeled_value(soup: BeautifulSoup, label: str) -> Optional[str]:
        """Extract value for a given label from Cricinfo's HTML structure"""
        # Look for ds-text-tight-m class (the label)
        for p in soup.find_all("p", class_="ds-text-tight-m"):
            text = p.get_text(strip=True)
            if text.upper() == label.upper():
                parent = p.find_parent("div")
                if parent:
                    value_span = parent.find("span", class_="ds-text-title-s")
                    if value_span:
                        value_p = value_span.find("p")
                        if value_p:
                            return value_p.get_text(strip=True)
        return None

    def _parse_born_field(
        self, born_raw: str
    ) -> tuple[Optional[datetime], Optional[str]]:
        """Parse the Born field for date and location"""
        if not born_raw:
            return None, None

        dob = None
        birthplace = None

        try:
            parts = [p.strip() for p in born_raw.split(",")]

            if len(parts) == 1:
                # Just date: "December 10, 2007"
                try:
                    dob = datetime.strptime(parts[0], "%B %d, %Y").date()
                except ValueError:
                    birthplace = parts[0]
            elif len(parts) >= 3:
                # Date + location: "December 10, 2007, Lagos, Nigeria"
                date_str = f"{parts[0]}, {parts[1]}"
                try:
                    dob = datetime.strptime(date_str, "%B %d, %Y").date()
                    birthplace = ", ".join(parts[2:])
                except ValueError:
                    birthplace = born_raw
            else:
                birthplace = born_raw
        except Exception:
            birthplace = born_raw

        return dob, birthplace

    def get_profile(self) -> dict:
        """Scrape and return player profile"""
        print("=" * 60)
        print("Starting enhanced scrape with anti-detection...")
        print("=" * 60)

        soup = self._fetch_page()

        # Check if we actually got content
        page_text = soup.get_text()
        if "Access Denied" in page_text:
            print("\n⚠️  WARNING: Still blocked by Akamai")
            print("Possible solutions:")
            print("1. Use a residential proxy service (Bright Data, Smartproxy)")
            print("2. Use a VPN")
            print("3. Try from a different IP address")
            print("4. Contact ESPN Cricinfo for API access")
            return {
                "full_name": None,
                "date_of_birth": None,
                "birthplace": None,
                "batting_style": None,
                "bowling_style": None,
                "playing_role": None,
                "error": "Access Denied by Akamai protection",
            }

        print("✓ Successfully bypassed protection!")

        born_raw = self._extract_labeled_value(soup, "Born")
        dob, birthplace = self._parse_born_field(born_raw)

        profile = {
            "full_name": self._extract_labeled_value(soup, "Full Name"),
            "date_of_birth": dob,
            "birthplace": birthplace,
            "batting_style": self._extract_labeled_value(soup, "Batting Style"),
            "bowling_style": self._extract_labeled_value(soup, "Bowling Style"),
            "playing_role": self._extract_labeled_value(soup, "Playing Role"),
        }

        return profile


# Test with proxy option
class CricinfoScraperWithProxy(CricinfoProfileScraper):
    """
    Version that supports proxies - useful if you have access to one
    """

    def __init__(
        self, slug_or_id: str, proxy: Optional[str] = None, timeout: int = 60000
    ):
        super().__init__(slug_or_id, timeout)
        self.proxy = proxy  # Format: "http://username:password@proxy:port"

    def _fetch_page(self) -> BeautifulSoup:
        """Fetch with proxy support"""
        with sync_playwright() as p:
            browser = p.firefox.launch(
                headless=False, proxy={"server": self.proxy} if self.proxy else None
            )

            # Rest same as parent class...
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            )

            page = context.new_page()

            try:
                # Visit homepage first
                page.goto("https://www.espncricinfo.com/", timeout=self.timeout)
                page.wait_for_timeout(3000)

                # Then player page
                url = self._build_url()
                page.goto(url, timeout=self.timeout)
                page.wait_for_timeout(5000)

                html = page.content()
                return BeautifulSoup(html, "html.parser")
            finally:
                browser.close()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("ESPN CRICINFO SCRAPER - ADVANCED MODE")
    print("=" * 60)
    print("\nNOTE: This will open a visible browser window.")
    print("This is more likely to work than headless mode.\n")

    test_player = "1365288"  # Adeshola Adekunle

    try:
        scraper = CricinfoProfileScraper(test_player)
        profile = scraper.get_profile()

        print("\n" + "=" * 60)
        print("PROFILE DATA:")
        print("=" * 60)
        for key, value in profile.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nIf still blocked, you'll need:")
        print("  • A residential proxy service")
        print("  • Or scrape from a different network/location")
