"""
Batch scraper for Cricinfo profiles from people.csv

Usage:
    from cricinfo_scraper import CricinfoProfileScraper
    cricinfo_batch_scraper()
"""

import pandas as pd
import time
from datetime import datetime
from typing import Set, Optional
import os
from pathlib import Path

from utils.CricInfoProfileScraper import CricinfoProfileScraper


def cricinfo_batch_scraper(
    input_csv: str = "people.csv",
    output_csv: str = "people_updated.csv",
    checkpoint_file: str = "scraper_checkpoint.txt",
    delay_between_requests: int = 5,
):
    """
    Scrape Cricinfo profiles for all players in CSV and update with results

    Args:
        input_csv: Path to input CSV with player data
        output_csv: Path to save updated CSV
        checkpoint_file: File to track progress (for resuming)
        delay_between_requests: Seconds to wait between requests
    """

    print("=" * 80)
    print("CRICINFO BATCH SCRAPER")
    print("=" * 80)

    # Load the CSV
    print(f"\n📂 Loading {input_csv}...")
    df = pd.read_csv(input_csv)
    total_rows = len(df)
    print(f"✓ Loaded {total_rows} rows")

    # Add new columns if they don't exist
    new_columns = [
        "scraped_full_name",
        "scraped_dob",
        "scraped_age",
        "scraped_birthplace",
        "scraped_nationality",
        "scraped_gender",
        "scraped_batting_style",
        "scraped_bowling_style",
        "scraped_playing_role",
        "scraped_teams",
        "scrape_status",
        "scrape_timestamp",
    ]

    for col in new_columns:
        if col not in df.columns:
            df[col] = None

    # Load visited set (for resuming interrupted runs)
    visited: Set[str] = set()
    if os.path.exists(checkpoint_file):
        print(f"\n📝 Loading checkpoint from {checkpoint_file}...")
        with open(checkpoint_file, "r") as f:
            visited = set(line.strip() for line in f if line.strip())
        print(f"✓ Found {len(visited)} already processed players")

    # Find players with cricinfo IDs that haven't been scraped yet
    cricinfo_cols = ["key_cricinfo", "key_cricinfo_2", "key_cricinfo_3"]

    players_to_scrape = []
    for idx, row in df.iterrows():
        # Check if already scraped
        if pd.notna(row.get("scrape_status")):
            continue

        # Get cricinfo ID from any of the cricinfo columns
        cricinfo_id = None
        for col in cricinfo_cols:
            if col in df.columns and pd.notna(row[col]):
                # Convert to string and remove .0 if it's a float
                raw_id = row[col]
                if isinstance(raw_id, float):
                    cricinfo_id = str(int(raw_id))
                else:
                    cricinfo_id = str(raw_id).strip()

                if cricinfo_id and cricinfo_id not in visited:
                    break

        if cricinfo_id:
            players_to_scrape.append(
                {
                    "index": idx,
                    "identifier": row["identifier"],
                    "name": row["name"],
                    "cricinfo_id": cricinfo_id,
                }
            )

    total_to_scrape = len(players_to_scrape)
    print(f"\n📊 Found {total_to_scrape} players to scrape")

    if total_to_scrape == 0:
        print("✓ All players already scraped!")
        return

    # Import the scraper

    # Process each player
    successful = 0
    failed = 0
    skipped = 0

    for i, player in enumerate(players_to_scrape, 1):
        idx = player["index"]
        identifier = player["identifier"]
        name = player["name"]
        cricinfo_id = player["cricinfo_id"]

        print(f"\n{'='*80}")
        print(f"[{i}/{total_to_scrape}] Processing: {name} (ID: {cricinfo_id})")
        print(f"{'='*80}")

        # Skip if already in visited set
        if cricinfo_id in visited:
            print(f"⏭️  Already processed, skipping...")
            skipped += 1
            continue

        try:
            # Scrape the profile
            scraper = CricinfoProfileScraper(cricinfo_id)
            profile = scraper.get_profile()

            # Check for errors
            if "error" in profile:
                print(f"❌ Scraping failed: {profile['error']}")
                df.at[idx, "scrape_status"] = f"error: {profile['error']}"
                df.at[idx, "scrape_timestamp"] = datetime.now().isoformat()
                failed += 1
            else:
                # Update the dataframe with scraped data
                df.at[idx, "scraped_full_name"] = profile.get("full_name")
                df.at[idx, "scraped_dob"] = (
                    str(profile.get("date_of_birth"))
                    if profile.get("date_of_birth")
                    else None
                )
                df.at[idx, "scraped_age"] = profile.get("age")
                df.at[idx, "scraped_birthplace"] = profile.get("birthplace")
                df.at[idx, "scraped_nationality"] = profile.get("nationality")
                df.at[idx, "scraped_gender"] = profile.get("gender")
                df.at[idx, "scraped_batting_style"] = profile.get("batting_style")
                df.at[idx, "scraped_bowling_style"] = profile.get("bowling_style")
                df.at[idx, "scraped_playing_role"] = profile.get("playing_role")

                # Store teams as comma-separated list
                if profile.get("teams"):
                    teams_str = ", ".join([t["name"] for t in profile["teams"]])
                    df.at[idx, "scraped_teams"] = teams_str

                df.at[idx, "scrape_status"] = "success"
                df.at[idx, "scrape_timestamp"] = datetime.now().isoformat()

                print(f"✓ Successfully scraped profile")
                print(f"  Name: {profile.get('full_name')}")
                print(f"  DOB: {profile.get('date_of_birth')}")
                print(f"  Role: {profile.get('playing_role')}")
                successful += 1

            # Mark as visited
            visited.add(cricinfo_id)

            # Save checkpoint
            with open(checkpoint_file, "a") as f:
                f.write(f"{cricinfo_id}\n")

            # Save after EACH scrape
            print(f"\n💾 Saving progress... ({i}/{total_to_scrape} processed)")
            df.to_csv(output_csv, index=False)
            print(f"✓ Saved to {output_csv}")

        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user. Saving progress...")
            df.to_csv(output_csv, index=False)
            print(f"✓ Progress saved to {output_csv}")
            print(f"\n📊 Stats so far:")
            print(f"  ✓ Successful: {successful}")
            print(f"  ❌ Failed: {failed}")
            print(f"  ⏭️  Skipped: {skipped}")
            return

        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            df.at[idx, "scrape_status"] = f"error: {str(e)}"
            df.at[idx, "scrape_timestamp"] = datetime.now().isoformat()
            failed += 1

            # Still mark as visited to avoid retrying immediately
            visited.add(cricinfo_id)
            with open(checkpoint_file, "a") as f:
                f.write(f"{cricinfo_id}\n")

        # Delay between requests (be nice to the server)
        if i < total_to_scrape:
            print(
                f"\n⏱️  Waiting {delay_between_requests} seconds before next request..."
            )
            time.sleep(delay_between_requests)

    # Final save
    print(f"\n💾 Saving final results...")
    df.to_csv(output_csv, index=False)
    print(f"✓ Saved to {output_csv}")

    # Summary
    print("\n" + "=" * 80)
    print("SCRAPING COMPLETE!")
    print("=" * 80)
    print(f"\n📊 Final Statistics:")
    print(f"  Total processed: {i}")
    print(f"  ✓ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")
    print(f"  ⏭️  Skipped: {skipped}")
    print(f"\n📁 Output file: {output_csv}")
    print(f"📝 Checkpoint file: {checkpoint_file}")

    # Show some sample results
    if successful > 0:
        print(f"\n🎉 Sample of scraped data:")
        sample_cols = [
            "name",
            "scraped_full_name",
            "scraped_dob",
            "scraped_playing_role",
        ]
        available_cols = [col for col in sample_cols if col in df.columns]
        sample = df[df["scrape_status"] == "success"][available_cols].head(5)
        print(sample.to_string(index=False))


if __name__ == "__main__":
    # Run the batch scraper
    cricinfo_batch_scraper(
        input_csv="dataset/people.csv",
        output_csv="dataset/people_updated.csv",
        checkpoint_file="dataset/scraper_checkpoint.txt",
        delay_between_requests=5,  # 5 seconds between requests
    )
