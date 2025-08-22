import os
import requests
import pandas as pd
from pathlib import Path
import time

# -------------------------------
# Configuration
# -------------------------------
API_KEY = os.getenv("API_FOOTBALL_KEY")  # set this in your environment
HEADERS = {"X-RapidAPI-Host": "v3.football.api-sports.io",
           "X-RapidAPI-Key": API_KEY}

LEAGUE_ID = 39  # Premier League
START_SEASON = 2015
END_SEASON = 2025  # adjust as needed

RAW_DIR = Path("data/raw/api-football/PL")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Function to fetch one season
# -------------------------------
def fetch_season(season):
    print(f"Fetching season {season}...")
    url = f"https://v3.football.api-sports.io/fixtures?league={LEAGUE_ID}&season={season}"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Failed to fetch season {season}: {response.status_code}")
        return None
    
    data = response.json().get("response", [])
    if not data:
        print(f"No matches found for season {season}")
        return None
    
    df = pd.json_normalize(data)
    csv_file = RAW_DIR / f"{season}.csv"
    df.to_csv(csv_file, index=False)
    print(f"Saved {len(df)} matches to {csv_file}")
    return df

# -------------------------------
# Batch extraction
# -------------------------------
if __name__ == "__main__":
    for season in range(START_SEASON, END_SEASON + 1):
        fetch_season(season)
        time.sleep(6)  # avoid hitting API free-tier limits
