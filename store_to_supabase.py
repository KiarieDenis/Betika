"""
store_to_supabase.py

Uploads and streams scraped match predictions, odds (1X2, O/U 2.5, BTTS), and results
directly into a Supabase (PostgreSQL) database.

Supports:
1. append_records_to_supabase(records): Direct streaming from scrapers after every scrape run.
2. migrate_csv_to_supabase(): Uploads existing historical_scrapes.csv into Supabase.
3. sync_all_json_to_supabase(): Scans all scrapers/*.json files and pushes any unrecorded data.
"""

import glob
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import pandas as pd

try:
    from supabase import create_client, Client
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "supabase"])
    from supabase import create_client, Client

# Load environment variables from .env
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("store_to_supabase")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = (os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

TABLE_NAME = "raw_scrapes"
SCRAPERS_DIR = BASE_DIR / "scrapers"
DATA_DIR = BASE_DIR / "data"
MASTER_CSV_PATH = DATA_DIR / "historical_scrapes.csv"


def get_client() -> Optional[Client]:
    """Initializes and returns Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY or "your-project-id" in SUPABASE_URL:
        logger.warning("Supabase credentials not configured. Please set SUPABASE_URL and SUPABASE_KEY in .env")
        return None
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return None


def clean_numeric(val: Any) -> Optional[float]:
    """Converts odds and numbers to float, or None if empty."""
    if val is None or pd.isna(val):
        return None
    val_str = str(val).strip()
    if not val_str or val_str.lower() in ["none", "nan", "n/a", ""]:
        return None
    try:
        return round(float(val_str), 2)
    except ValueError:
        return None


def clean_boolean(val: Any) -> Optional[bool]:
    """Converts boolean representations to True, False, or None."""
    if val is None or pd.isna(val):
        return None
    val_str = str(val).strip().lower()
    if val_str in ["true", "1", "yes"]:
        return True
    if val_str in ["false", "0", "no"]:
        return False
    return None


def normalize_for_supabase(item: Dict[str, Any], default_source: str = "unknown", default_category: str = "today") -> Dict[str, Any]:
    """Prepares a single record to match the public.raw_scrapes table columns."""
    actual_score = (
        item.get("Actual_Score") or 
        item.get("Result_Score") or 
        item.get("actual_score") or 
        ""
    )
    actual_score = str(actual_score).strip() if actual_score is not None and not pd.isna(actual_score) else ""

    actual_outcome = str(item.get("Actual_Outcome") or item.get("actual_outcome") or "").strip()
    if not actual_outcome and actual_score and "-" in actual_score:
        try:
            parts = actual_score.split("-")
            h, a = int(parts[0].strip()), int(parts[1].strip())
            if h > a:
                actual_outcome = "1"
            elif a > h:
                actual_outcome = "2"
            else:
                actual_outcome = "X"
        except Exception:
            pass

    scraped_at = item.get("scraped_at")
    if not scraped_at or str(scraped_at).lower() in ["none", "nan"]:
        scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    match_date = item.get("match_date") or item.get("Match_Date") or datetime.now().strftime("%Y-%m-%d")

    return {
        "scraped_at": str(scraped_at),
        "match_date": str(match_date),
        "match_category": str(item.get("Match_Category") or item.get("match_category") or default_category).lower(),
        "source": str(item.get("Source") or item.get("source") or default_source).lower(),
        "fixture_id": str(item.get("Fixture_ID") or item.get("fixture_id") or "") or None,
        "league": str(item.get("League") or item.get("league") or "") or None,
        "home_team": str(item.get("Home_Team") or item.get("home_team") or "Unknown").strip(),
        "away_team": str(item.get("Away_Team") or item.get("away_team") or "Unknown").strip(),
        "home_form": str(item.get("Home_Form") or item.get("home_form") or "") or None,
        "away_form": str(item.get("Away_Form") or item.get("away_form") or "") or None,
        "prediction": str(item.get("Prediction") or item.get("prediction") or "") or None,
        "predicted_score": str(item.get("Predicted_Score") or item.get("predicted_score") or "") or None,
        "stake": str(item.get("Stake") or item.get("stake") or "") or None,
        "odd_1": clean_numeric(item.get("Odd_1") or item.get("odd_1")),
        "odd_x": clean_numeric(item.get("Odd_X") or item.get("odd_x")),
        "odd_2": clean_numeric(item.get("Odd_2") or item.get("odd_2")),
        "odd_over_25": clean_numeric(item.get("Odd_Over_25") or item.get("odd_over_25")),
        "odd_under_25": clean_numeric(item.get("Odd_Under_25") or item.get("odd_under_25")),
        "odd_btts_yes": clean_numeric(item.get("Odd_BTTS_Yes") or item.get("odd_btts_yes")),
        "odd_btts_no": clean_numeric(item.get("Odd_BTTS_No") or item.get("odd_btts_no")),
        "match_time": str(item.get("Time") or item.get("match_time") or "") or None,
        "match_url": str(item.get("Match_URL") or item.get("match_url") or "") or None,
        "actual_score": actual_score or None,
        "actual_outcome": actual_outcome or None,
        "is_correct": clean_boolean(item.get("Is_Correct") or item.get("is_correct")),
        "result_text": str(item.get("Result_Text") or item.get("result_text") or "") or None,
    }


def append_records_to_supabase(
    raw_records: List[Dict[str, Any]], 
    default_source: str = "unknown", 
    default_category: str = "today",
    batch_size: int = 100
) -> int:
    """
    Appends raw scraped records directly to Supabase table.
    Uses on_conflict to prevent duplicate re-entry of the identical scrape event.
    """
    client = get_client()
    if not client:
        return 0

    if not raw_records:
        return 0

    cleaned_records = [
        normalize_for_supabase(r, default_source=default_source, default_category=default_category) 
        for r in raw_records
    ]

    total_inserted = 0
    for i in range(0, len(cleaned_records), batch_size):
        batch = cleaned_records[i:i + batch_size]
        try:
            res = client.table(TABLE_NAME).upsert(
                batch,
                on_conflict="scraped_at,source,match_date,home_team,away_team"
            ).execute()
            count = len(res.data) if res.data else len(batch)
            total_inserted += count
            logger.info(f"Uploaded batch {i // batch_size + 1} ({count} rows) to Supabase.")
        except Exception as e:
            logger.error(f"Error inserting batch {i // batch_size + 1} into Supabase: {e}")

    logger.info(f"✅ Supabase sync complete: {total_inserted} records processed into '{TABLE_NAME}'.")
    return total_inserted


def migrate_csv_to_supabase(csv_path: Optional[Path] = None) -> int:
    """Uploads the entire historical CSV into Supabase."""
    target_csv = csv_path or MASTER_CSV_PATH
    if not os.path.isfile(target_csv):
        logger.warning(f"CSV file not found: {target_csv}")
        return 0

    logger.info(f"Reading records from {target_csv} to migrate to Supabase...")
    df = pd.read_csv(target_csv)
    records = df.to_dict(orient="records")
    logger.info(f"Found {len(records)} records in CSV.")
    return append_records_to_supabase(records, batch_size=100)


def sync_all_json_to_supabase() -> int:
    """Scans all scrapers/*.json files and pushes all records to Supabase."""
    files = glob.glob(str(SCRAPERS_DIR / "*.json"))
    all_records = []

    for filepath in files:
        if "accuracy" in filepath:
            continue
        filename = Path(filepath).name
        cat = "today"
        if "yesterday" in filename:
            cat = "yesterday"
        elif "tomorrow" in filename:
            cat = "tomorrow"
        elif "future" in filename:
            cat = "future"

        source = "windrawwin"
        if "predictz" in filename:
            source = "predictz"

        with open(filepath, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        item.setdefault("Source", source)
                        item.setdefault("Match_Category", cat)
                        all_records.append(item)
            except Exception as e:
                logger.error(f"Error reading {filename}: {e}")

    logger.info(f"Collected {len(all_records)} records from JSON files.")
    return append_records_to_supabase(all_records, batch_size=100)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--sync-json":
        sync_all_json_to_supabase()
    else:
        migrate_csv_to_supabase()
