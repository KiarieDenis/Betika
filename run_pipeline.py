"""
run_pipeline.py

Unified execution script for Render Cron Job and automated scheduling:
1. Scrapes WinDrawWin (predictions, 1X2, O/U 2.5, BTTS odds, and yesterday results).
2. Scrapes PredictZ (predictions, 1X2 odds, and yesterday results).
3. Automatically appends raw records to Supabase (and local CSV backup).
"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def run():
    print("=" * 65)
    print(" STARTING BETIKA PREDICTIONS INGESTION PIPELINE")
    print("=" * 65)

    # 1. WinDrawWin
    print("\n>>> [1/2] Running WinDrawWin Scraper...")
    subprocess.run([sys.executable, str(BASE_DIR / "scrapers" / "scraper.py")], check=True)

    # 2. PredictZ
    print("\n>>> [2/2] Running PredictZ Scraper...")
    subprocess.run([sys.executable, str(BASE_DIR / "scrapers" / "predictz_scraper.py")], check=True)

    print("\n" + "=" * 65)
    print(" PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 65)

if __name__ == "__main__":
    run()
