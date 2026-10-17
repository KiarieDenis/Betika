"""
predictz_scraper.py

Scraper for PredictZ football predictions & results:
- Today: https://www.predictz.com/predictions/
- Tomorrow: https://www.predictz.com/predictions/tomorrow/
- Future: https://www.predictz.com/predictions/{YYYYMMDD}/ (e.g. 2 days ahead)
- Yesterday: https://www.predictz.com/results/yesterday/

Extracts:
- League & Match Info
- Home / Away Teams & Recent Forms
- Prediction & Predicted Score
- Match Odds (1, X, 2)
- Match URL & Fixture ID
- Finished Scores & Actual Outcomes (for Yesterday results)
- Saves results as structured JSON files for ingestion into master CSV.
"""

import argparse
import json
import logging
import random
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from curl_cffi.requests import Session as CurlSession
except ImportError:
    CurlSession = None

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration & Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("predictz_scraper")

BASE_URL = "https://www.predictz.com"

DAY_URL_PATTERNS = {
    "yesterday": f"{BASE_URL}/results/yesterday/",
    "today": f"{BASE_URL}/predictions/",
    "tomorrow": f"{BASE_URL}/predictions/tomorrow/",
    "future": f"{BASE_URL}/predictions/{{date}}/",
}

DAY_OFFSETS = {
    "yesterday": -1,
    "today": 0,
    "tomorrow": 1,
    "future": 2,
}

REQUEST_TIMEOUT = 25
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 3
OUTPUT_DIR = Path(__file__).resolve().parent


def build_predictz_url(day_name: str) -> str:
    """Build URL for PredictZ given day."""
    template = DAY_URL_PATTERNS[day_name]
    target_date = datetime.now().date() + timedelta(days=DAY_OFFSETS[day_name])
    if "{date}" in template:
        return template.format(date=target_date.strftime("%Y%m%d"))
    return template


def create_session():
    """Create session with browser impersonation to bypass Cloudflare."""
    if CurlSession:
        return CurlSession(impersonate="chrome124")
    return cloudscraper.create_scraper()


def fetch_page(url: str, session) -> Optional[str]:
    """Fetch URL with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} got status {resp.status_code} for {url}")
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SECONDS)
    logger.error(f"Giving up on {url}")
    return None


# ---------------------------------------------------------------------------
# Parsing Logic
# ---------------------------------------------------------------------------

def parse_predictions_page(html: str, day_name: str, target_date) -> List[Dict[str, Any]]:
    """Parse PredictZ predictions page (today, tomorrow, future)."""
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    records = []
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    match_date = target_date.strftime("%Y-%m-%d")

    tables = soup.find_all(class_="pttable")
    for table in tables:
        # League name
        league_el = table.find(class_="ptlg")
        league = league_el.get_text(strip=True) if league_el else "General Predictions"

        # Rows
        rows = table.find_all(class_="ptcnt")
        for row in rows:
            game_el = row.find(class_="ptgame")
            link_el = game_el.find("a") if game_el else None
            match_str = link_el.get_text(strip=True) if link_el else (game_el.get_text(strip=True) if game_el else "")
            match_url = link_el.get("href", "") if link_el else ""

            # Extract Fixture ID
            fixture_id = ""
            if match_url:
                num_match = re.search(r"/(\d+)/?", match_url)
                if num_match:
                    fixture_id = num_match.group(1)

            home_el = row.find(class_="ptmobh")
            away_el = row.find(class_="ptmoba")
            home = home_el.get_text(strip=True) if home_el else ""
            away = away_el.get_text(strip=True) if away_el else ""

            if not home or not away:
                if " v " in match_str:
                    parts = match_str.split(" v ")
                    home, away = parts[0].strip(), parts[1].strip()

            if not home or not away:
                continue

            # Home & Away Form
            home_form_box = row.find(class_="ptlast5boxh")
            away_form_box = row.find(class_="ptlast5boxa")

            home_form = ""
            if home_form_box:
                home_form = "".join(f.get_text(strip=True) for f in home_form_box.find_all(class_=re.compile(r"neonboxsml2")))
            if not home_form:
                home_form = "N/A"

            away_form = ""
            if away_form_box:
                away_form = "".join(f.get_text(strip=True) for f in away_form_box.find_all(class_=re.compile(r"neonboxsml2")))
            if not away_form:
                away_form = "N/A"

            # Prediction & Predicted Score
            pred_el = row.find(class_="ptpredboxsml")
            pred_text = pred_el.get_text(strip=True) if pred_el else ""
            predicted_score = ""
            prediction = pred_text
            score_m = re.search(r"(\d+-\d+)", pred_text)
            if score_m:
                predicted_score = score_m.group(1)
                prediction = pred_text.replace(predicted_score, "").strip()

            # Odds: 1, X, 2
            odd_1, odd_x, odd_2 = "", "", ""
            odds_cells = row.find_all(class_="ptodds")
            if len(odds_cells) >= 3:
                odd_1 = odds_cells[0].get_text(strip=True)
                odd_x = odds_cells[1].get_text(strip=True)
                odd_2 = odds_cells[2].get_text(strip=True)
            else:
                odds_nums = re.findall(r"\d+\.\d+", row.get_text())
                if len(odds_nums) >= 3:
                    odd_1, odd_x, odd_2 = odds_nums[0], odds_nums[1], odds_nums[2]

            records.append({
                "Source": "predictz",
                "League": league,
                "Match": f"{home} v {away}",
                "Home_Team": home,
                "Away_Team": away,
                "Home_Form": home_form,
                "Away_Form": away_form,
                "Prediction": prediction,
                "Predicted_Score": predicted_score,
                "Odd_1": odd_1,
                "Odd_X": odd_x,
                "Odd_2": odd_2,
                "Fixture_ID": fixture_id,
                "Match_URL": match_url,
                "match_date": match_date,
                "Match_Category": day_name,
                "scraped_at": scraped_at,
            })

    return records


def parse_yesterday_results(html: str, target_date) -> List[Dict[str, Any]]:
    """Parse PredictZ results for finished matches on yesterday."""
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    records = []
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    match_date = target_date.strftime("%Y-%m-%d")

    rows = table.find_all("tr")
    for tr in rows:
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        match_time = cells[0].get_text(strip=True)
        league = cells[2].get_text(strip=True) if len(cells) > 2 else "Unknown"

        match_cell = cells[3]
        links = match_cell.find_all("a")
        if len(links) < 2:
            continue

        home = links[0].get_text(strip=True)
        away = links[1].get_text(strip=True)

        # Result score extraction
        match_text = match_cell.get_text(" ", strip=True)
        nums = re.findall(r"\b\d+\b", match_text)
        result_score = ""
        actual_outcome = ""
        if len(nums) >= 2:
            h_score, a_score = int(nums[0]), int(nums[1])
            result_score = f"{h_score}-{a_score}"
            if h_score > a_score:
                actual_outcome = "1"
            elif a_score > h_score:
                actual_outcome = "2"
            else:
                actual_outcome = "X"

        records.append({
            "Source": "predictz",
            "League": league,
            "Match": f"{home} v {away}",
            "Home_Team": home,
            "Away_Team": away,
            "match_time": match_time,
            "Result_Score": result_score,
            "Actual_Score": result_score,
            "Actual_Outcome": actual_outcome,
            "Result_Text": match_text,
            "match_date": match_date,
            "Match_Category": "yesterday",
            "scraped_at": scraped_at,
        })

    return records


def scrape_day(day_name: str, session) -> List[Dict[str, Any]]:
    """Scrape a specific day from PredictZ."""
    url = build_predictz_url(day_name)
    target_date = datetime.now().date() + timedelta(days=DAY_OFFSETS[day_name])
    logger.info(f"[{day_name.upper()}] Fetching {url} ...")

    html = fetch_page(url, session)
    if not html:
        logger.warning(f"Failed to fetch content for {day_name}")
        return []

    if day_name == "yesterday":
        records = parse_yesterday_results(html, target_date)
    else:
        records = parse_predictions_page(html, day_name, target_date)

    logger.info(f"[{day_name.upper()}] Parsed {len(records)} matches.")
    return records


def save_json(data: List[Dict[str, Any]], filename: str, output_dir: Path) -> Path:
    """Save data to JSON file with atomic write and previous file cleanup."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / filename

    prefix = filename.split("_")[0] + "_" + filename.split("_")[1]
    for old_file in output_dir.glob(f"{prefix}_*.json"):
        if old_file.name != filename:
            try:
                old_file.unlink()
            except Exception:
                pass

    tmp_path = out_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp_path.replace(out_path)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Scrape football predictions and results from PredictZ.")
    parser.add_argument(
        "--days",
        nargs="+",
        default=["yesterday", "today", "tomorrow", "future"],
        help="Days to scrape. Options: yesterday, today, tomorrow, future",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help="Directory to save scraped JSON files",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Also export to CSV",
    )

    args = parser.parse_args()
    out_dir = Path(args.output_dir)

    print("=" * 60)
    print(" PredictZ Predictions & Results Scraper")
    print(f" Targets: {', '.join(args.days)}")
    print(f" Output Directory: {out_dir}")
    print("=" * 60)

    session = create_session()
    all_results = {}
    total_matches = 0

    for day_name in args.days:
        if day_name not in DAY_URL_PATTERNS:
            logger.warning(f"Skipping unknown day: {day_name}")
            continue

        records = scrape_day(day_name, session)
        all_results[day_name] = records
        total_matches += len(records)

        target_date = datetime.now().date() + timedelta(days=DAY_OFFSETS[day_name])
        date_str = target_date.strftime("%d%m%Y")
        json_filename = f"predictz_{day_name}_{date_str}.json"

        save_json(records, json_filename, out_dir)
        print(f"  Saved {len(records)} matches -> {json_filename}")

        # Automatically append every scrape run into Supabase & CSV
        try:
            parent_dir = str(OUTPUT_DIR.parent)
            if parent_dir not in sys.path:
                sys.path.append(parent_dir)
            from store_to_supabase import append_records_to_supabase
            supa_added = append_records_to_supabase(records, default_source="predictz", default_category=day_name)
            if supa_added:
                print(f"  [Supabase] Uploaded to database: +{supa_added} records")
        except Exception as e:
            pass



        if args.csv and records:
            csv_filename = f"predictz_{day_name}_{date_str}.csv"
            pd.DataFrame(records).to_csv(out_dir / csv_filename, index=False, encoding="utf-8")
            print(f"  Saved CSV -> {csv_filename}")

        time.sleep(random.uniform(1.0, 2.0))

    print("\n" + "=" * 60)
    print(" PREDICTZ SCRAPING SUMMARY")
    print("=" * 60)
    for day_name, records in all_results.items():
        print(f"  {day_name.capitalize():12} : {len(records):3d} matches")
    print(f"\nTotal matches scraped: {total_matches}")
    print("=" * 60)


if __name__ == "__main__":
    main()
