"""
scraper.py

Data-ingestion front end: scrape WinDrawWin predictions/odds/results -> parse -> save as JSON.

Fetches yesterday / today / tomorrow / future prediction pages and writes
each day's data to a dated JSON file.

Extracts:
- League & Match Info
- Home / Away Teams & Recent Forms
- Match Odds (1, X, 2)
- Over / Under 2.5 Odds (O, U)
- Both Teams To Score (BTTS) Odds (Y, N)
- Stake, Prediction, Predicted Score
- Yesterday Results & Accuracy Evaluation
"""

import json
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from curl_cffi.requests import Session as CurlSession
except ImportError:
    CurlSession = None

import cloudscraper
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

BASE_URL = "https://www.windrawwin.com/predictions"

DAY_PATHS = {
    "yesterday": "yesterday/",
    "today": "today/",
    "tomorrow": "tomorrow/",
    # "future" is date-based: future/<YYYYMMDD>/ for today + 2 days
    "future": "future/{date}/",
}

DAY_OFFSETS = {
    "yesterday": -1,
    "today": 0,
    "tomorrow": 1,
    "future": 2,
}


def build_url(day_name: str) -> str:
    """Build the request URL for a given day, resolving date-based paths."""
    path_template = DAY_PATHS[day_name]
    if "{date}" in path_template:
        target_date = datetime.now().date() + timedelta(days=DAY_OFFSETS[day_name])
        path = path_template.format(date=target_date.strftime("%Y%m%d"))
    else:
        path = path_template
    return f"{BASE_URL}/{path}"


REQUEST_TIMEOUT = 25
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 3
OUTPUT_DIR = Path(__file__).resolve().parent


# ------------------------------------------------------------------
# Fetching
# ------------------------------------------------------------------

def create_session():
    """Create a high-fidelity scraping session using curl_cffi or cloudscraper fallback."""
    if CurlSession:
        return CurlSession(impersonate="chrome124")
    return cloudscraper.create_scraper()


def fetch_html(url: str, session) -> Optional[str]:
    """Fetch a page's HTML with retry handling and Cloudflare evasion."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            print(f"  [WARN] attempt {attempt}/{MAX_RETRIES} got status {resp.status_code} for {url}")
        except Exception as e:
            print(f"  [WARN] attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SECONDS)
    print(f"  [ERROR] giving up on {url}")
    return None


# ------------------------------------------------------------------
# Parsing
# ------------------------------------------------------------------

def _dedup_team_name(name: str) -> str:
    """Some team names appear doubled in the markup (e.g. 'ArsenalArsenal')."""
    mid = len(name) // 2
    return name[:mid] if len(name) > 4 and name[:mid] == name[mid:] else name


def parse_predictions_page(html: str) -> pd.DataFrame:
    """Parse today/tomorrow/future prediction pages with full Match Odds, O/U, and BTTS."""
    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    predictions = []
    current_league = None

    for el in soup.find_all(["div", "h2", "h3", "table"]):
        classes = el.get("class", [])

        if "ptleag" in classes or "wtgl" in classes:
            text = el.get_text(strip=True)
            if text and "prediction" not in text.lower() and "stake" not in text.lower():
                current_league = text
            continue

        if "wttr" not in classes:
            continue

        row = {
            "League": current_league or "General Predictions",
            "Source": "windrawwin",
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        teams = el.find_all(class_="wtteam")
        if len(teams) < 2:
            continue

        home = _dedup_team_name(next(teams[0].stripped_strings, "Unknown").strip())
        away = _dedup_team_name(next(teams[1].stripped_strings, "Unknown").strip())

        def form(team_el):
            forms = team_el.find_all(class_=re.compile(r"last5[wdl]"))
            return "".join(f.get_text(strip=True) for f in forms) or "N/A"

        row.update({
            "Home_Team": home,
            "Away_Team": away,
            "Home_Form": form(teams[0]),
            "Away_Form": form(teams[1]),
            "Match": f"{home} v {away}",
        })

        # 1. Match Odds (1, X, 2)
        mo_el = el.find(class_="wtmo")
        if mo_el:
            mo_cells = mo_el.find_all(class_="wtocell")
            if len(mo_cells) >= 3:
                row["Odd_1"] = mo_cells[0].get_text(strip=True)
                row["Odd_X"] = mo_cells[1].get_text(strip=True)
                row["Odd_2"] = mo_cells[2].get_text(strip=True)
            else:
                odds_nums = re.findall(r"\d+\.\d+", mo_el.get_text())
                if len(odds_nums) >= 3:
                    row["Odd_1"], row["Odd_X"], row["Odd_2"] = odds_nums[0], odds_nums[1], odds_nums[2]

        # 2. Over / Under 2.5 (Over, Under)
        ou_el = el.find(class_="wtou")
        if ou_el:
            ou_cells = ou_el.find_all(class_="wtocell")
            if len(ou_cells) >= 2:
                row["Odd_Over_25"] = ou_cells[0].get_text(strip=True)
                row["Odd_Under_25"] = ou_cells[1].get_text(strip=True)
            else:
                ou_nums = re.findall(r"\d+\.\d+", ou_el.get_text())
                if len(ou_nums) >= 2:
                    row["Odd_Over_25"], row["Odd_Under_25"] = ou_nums[0], ou_nums[1]

        # 3. Both Teams To Score (BTTS: Yes, No)
        bt_el = el.find(class_="wtbt")
        if bt_el:
            bt_cells = bt_el.find_all(class_="wtocell")
            if len(bt_cells) >= 2:
                row["Odd_BTTS_Yes"] = bt_cells[0].get_text(strip=True)
                row["Odd_BTTS_No"] = bt_cells[1].get_text(strip=True)
            else:
                bt_nums = re.findall(r"\d+\.\d+", bt_el.get_text())
                if len(bt_nums) >= 2:
                    row["Odd_BTTS_Yes"], row["Odd_BTTS_No"] = bt_nums[0], bt_nums[1]

        # 4. Predicted Score, Stake & Prediction
        score_el = el.find(class_=re.compile(r"wtsc|predscore"))
        if score_el:
            row["Predicted_Score"] = score_el.get_text(strip=True)

        pred_el = el.find(class_=re.compile(r"wtprd|predstake"))
        if pred_el:
            row["Prediction"] = pred_el.get_text(strip=True)

        stake_el = el.find(class_=re.compile(r"wtstk|stake"))
        if stake_el:
            row["Stake"] = stake_el.get_text(strip=True)

        predictions.append(row)

    return pd.DataFrame(predictions)


def parse_yesterday_page(html: str) -> pd.DataFrame:
    """Parse the results/accuracy table on the 'yesterday' page."""
    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    matches = []

    for row_el in soup.find_all("tr"):
        cells = row_el.find_all("td")
        if len(cells) < 5:
            continue

        link = cells[0].find("a")
        if not link:
            continue

        href_parts = link.get("href", "").split("/")
        teams_slug_idx = next((i for i, p in enumerate(href_parts) if "-v-" in p), -1)
        if teams_slug_idx != -1:
            teams_slug = href_parts[teams_slug_idx]
            t1_slug, t2_slug = teams_slug.split("-v-")
            home, away = t1_slug.title().replace("-", " "), t2_slug.title().replace("-", " ")
            league_slug = href_parts[teams_slug_idx - 1] if teams_slug_idx > 0 else "Unknown"
            league = league_slug.title().replace("-", " ")
        else:
            home, away, league = "Unknown", "Unknown", "Unknown"

        success_img = cells[4].find("img")
        is_correct = False
        if success_img:
            marker = (success_img.get("alt", "") + success_img.get("src", "")).lower()
            is_correct = "correct" in marker and "incorrect" not in marker

        # Extract match details
        match_text = link.get_text(strip=True)
        result_numbers = re.findall(r"\d+", match_text)
        result_score = "-".join(result_numbers[-2:]) if len(result_numbers) >= 2 else ""
        if not re.fullmatch(r"(?:0|[1-9]\d?)-(?:0|[1-9]\d?)", result_score):
            result_score = ""

        # Compute actual outcome from result_score if available
        actual_outcome = ""
        if result_score and "-" in result_score:
            try:
                h, a = [int(x) for x in result_score.split("-")]
                if h > a:
                    actual_outcome = "1"
                elif a > h:
                    actual_outcome = "2"
                else:
                    actual_outcome = "X"
            except Exception:
                pass

        matches.append({
            "League": league,
            "Match": f"{home} v {away}",
            "Home_Team": home,
            "Away_Team": away,
            "Stake": f"{cells[1].get_text(strip=True)} Stake",
            "Prediction": cells[2].get_text(strip=True),
            "Predicted_Score": cells[3].get_text(strip=True),
            "Result_Score": result_score,
            "Actual_Score": result_score,
            "Actual_Outcome": actual_outcome,
            "Is_Correct": is_correct,
            "Result_Text": match_text,
            "Source": "windrawwin",
            "match_date": yesterday_date,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(matches)


def compute_accuracy(df: pd.DataFrame) -> dict:
    """Calculate accuracy metrics and include details for high-stake matches."""
    if df.empty:
        return {"total": 0, "correct": 0, "accuracy": 0.0, "stakes": {}}

    metrics = {
        "total": len(df),
        "correct": int(df["Is_Correct"].sum()),
        "accuracy": float(df["Is_Correct"].mean()),
        "stakes": {}
    }

    for stake in ["Large", "Medium"]:
        subset = df[df["Stake"].str.contains(stake, case=False, na=False)]
        metrics["stakes"][stake] = {
            "count": len(subset),
            "correct": int(subset["Is_Correct"].sum()),
            "accuracy": float(subset["Is_Correct"].mean()) if not subset.empty else 0.0,
            "matches": subset.to_dict(orient="records")
        }
    return metrics


def parse_day(html: str, day_name: str) -> pd.DataFrame:
    target_date = datetime.now().date() + timedelta(days=DAY_OFFSETS[day_name])
    date_str = target_date.strftime("%Y-%m-%d")

    if day_name == "yesterday":
        df = parse_yesterday_page(html)
    else:
        df = parse_predictions_page(html)

    if not df.empty:
        df["match_date"] = date_str
        df["Source"] = "windrawwin"
        df["Match_Category"] = day_name

    return df


# ------------------------------------------------------------------
# Saving
# ------------------------------------------------------------------

def save_json(data, filename: str, overwrite_pattern: Optional[str] = None) -> Path:
    out_path = OUTPUT_DIR / filename
    if overwrite_pattern:
        for old_file in OUTPUT_DIR.glob(overwrite_pattern):
            if old_file.name != filename and old_file.is_file():
                try:
                    old_file.unlink()
                    print(f"  [OVERWRITE] Removed previous file: {old_file.name}")
                except Exception as e:
                    print(f"  [WARN] Could not remove {old_file.name}: {e}")

    tmp_path = out_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp_path.replace(out_path)
    return out_path


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    print("=" * 60)
    print("WinDrawWin Prediction & Odds Ingestion")
    print("=" * 60)

    session = create_session()
    results = {}
    accuracy_results = {}

    for day_name in DAY_PATHS:
        url = build_url(day_name)
        print(f"\n[{day_name.upper()}] Fetching {url} ...")

        html = fetch_html(url, session)
        df = parse_day(html, day_name)
        results[day_name] = len(df)

        target_date = datetime.now().date() + timedelta(days=DAY_OFFSETS[day_name])
        date_str = target_date.strftime("%d%m%Y")

        if html is None and df.empty:
            print(f"  [SKIP SAVE] Fetch failed for {day_name}, keeping existing data file if present.")
        else:
            records = df.to_dict(orient="records")
            save_json(
                records,
                f"{day_name}_{date_str}.json",
                overwrite_pattern=f"{day_name}_*.json",
            )
            print(f"  Saved {len(df)} games -> {day_name}_{date_str}.json")

            # Automatically append every scrape run into Supabase & CSV
            try:
                parent_dir = str(OUTPUT_DIR.parent)
                if parent_dir not in sys.path:
                    sys.path.append(parent_dir)
                from store_to_supabase import append_records_to_supabase
                supa_added = append_records_to_supabase(records, default_source="windrawwin", default_category=day_name)
                if supa_added:
                    print(f"  [Supabase] Uploaded to database: +{supa_added} records")
            except Exception as e:
                pass



        if day_name == "yesterday" and not df.empty:
            acc = compute_accuracy(df)
            accuracy_results["yesterday"] = acc
            save_json(acc, f"accuracy_{date_str}.json", overwrite_pattern="accuracy_*.json")

        time.sleep(random.uniform(1.0, 2.5))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for day_name, count in results.items():
        print(f"  {day_name.capitalize():12} {count:4d} games")
    if "yesterday" in accuracy_results:
        acc = accuracy_results["yesterday"]
        print(f"\nOverall Yesterday Accuracy: {acc['correct']}/{acc['total']} = {acc['accuracy']:.2%}")


if __name__ == "__main__":
    main()