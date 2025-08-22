import os
import time
import json
import argparse
from datetime import datetime
from pathlib import Path
import requests
import pandas as pd

API_BASE = "https://api.football-data.org/v4"

def current_season_start_year(today=None):
    """Return the season's starting year for 'European' seasons (start ~July/Aug)."""
    today = today or datetime.utcnow()
    return today.year if today.month >= 7 else today.year - 1

def season_years(last_n_back = 5, include_current = True, today = None):
    cur = current_season_start_year(today)
    years = [cur] if include_current else []
    for i in range(1, last_n_back+1):
        years.append(cur-1)
    return years

def ensure_dirs(*paths):
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)  

def get_api_key():
    key = os.getenv("FOOTBALL_DATA_API_KEY")
    if not key:
        raise RuntimeError("FOOTBALL_DATA_API_KEY environment variable not set.")
    return key

def request_with_backoff(url, headers, params=None, max_retries=5):
    """Simple backoff for 429s; respects Retry-After if present."""
    attempt = 0
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code == 429:
            attempt += 1
            if attempt > max_retries:
                raise RuntimeError(f"Rate limit persisted after {max_retries} retries: {resp.text}")
            retry_after = resp.headers.get("Retry-After")
            sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else 60 * attempt
            print(f"[429] rate limited. sleeping {sleep_s}s...")
            time.sleep(sleep_s)
            continue
        resp.raise_for_status()
        return resp
    
def fetch_matches_for_season(competition_code: str, season_year: int, api_key: str):
    """
    Fetch all matches for a competition and season.
    football-data.org v4: /competitions/{code}/matches?season=YYYY
    """
    url = f"{API_BASE}/competitions/{competition_code}/matches"
    headers = {"X-Auth-Token": api_key}
    params = {"season": season_year}
    # Gentle pacing to avoid free-tier spikes (approx <=10 req/min)
    time.sleep(6)
    resp = request_with_backoff(url, headers=headers, params=params)
    data = resp.json()
    if "matches" not in data:
        raise RuntimeError(f"Unexpected response for {competition_code} {season_year}: {data}")
    return data

def flatten_matches_to_df(data: dict) -> pd.DataFrame:
    rows = []
    comp = data.get("competition", {})
    comp_id = comp.get("id")
    comp_code = comp.get("code")
    comp_name = comp.get("name")

    season_meta = data.get("filters", {})  # may or may not include season; we’ll also parse from matches

    for m in data.get("matches", []):
        score = m.get("score", {}) or {}
        full_time = score.get("fullTime", {}) or {}
        half_time = score.get("halfTime", {}) or {}
        home_team = m.get("homeTeam", {}) or {}
        away_team = m.get("awayTeam", {}) or {}
        season = m.get("season", {}) or {}

        rows.append({
            "match_id": m.get("id"),
            "utc_date": m.get("utcDate"),
            "status": m.get("status"),
            "matchday": m.get("matchday"),
            "stage": m.get("stage"),
            "group": m.get("group"),
            "last_updated": m.get("lastUpdated"),

            "competition_id": comp_id,
            "competition_code": comp_code,
            "competition_name": comp_name,

            "season_id": season.get("id"),
            "season_start_date": season.get("startDate"),
            "season_end_date": season.get("endDate"),
            "season_current_matchday": season.get("currentMatchday"),

            "home_team_id": home_team.get("id"),
            "home_team_name": home_team.get("name"),
            "away_team_id": away_team.get("id"),
            "away_team_name": away_team.get("name"),

            "full_time_home": full_time.get("home"),
            "full_time_away": full_time.get("away"),
            "half_time_home": half_time.get("home"),
            "half_time_away": half_time.get("away"),
            "winner": score.get("winner"),
            "duration": score.get("duration"),
        })
    df = pd.DataFrame(rows)
    # Normalize types
    if not df.empty and "utc_date" in df.columns:
        df["utc_date"] = pd.to_datetime(df["utc_date"], errors="coerce")
    return df

def save_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def main():
    parser = argparse.ArgumentParser(description="Extract football matches from football-data.org")
    parser.add_argument("--competition", default="PL", help="Competition code (e.g., PL, PD, SA, BL1, FL1)")
    parser.add_argument("--seasons-back", type=int, default=5, help="How many seasons back to include (besides current)")
    parser.add_argument("--include-current", type=lambda x: str(x).lower() in {"1","true","yes","y"}, default=True)
    parser.add_argument("--raw-dir", default="data/raw", help="Where to save raw JSON")
    parser.add_argument("--processed-dir", default="data/processed", help="Where to save processed CSV")
    args = parser.parse_args()

    ensure_dirs(args.raw_dir, args.processed_dir)
    api_key = get_api_key()

    years = season_years(last_n_back=args.seasons_back, include_current=args.include_current)
    print(f"Seasons to fetch for {args.competition}: {years}")

    total_matches = 0
    for yr in years:
        print(f"\n=== Fetching {args.competition} season {yr} ===")
        data = fetch_matches_for_season(args.competition, yr, api_key)

        # Save raw JSON
        raw_path = Path(args.raw_dir) / "football-data" / args.competition / str(yr) / "matches.json"
        save_json(data, raw_path)
        print(f"Saved raw JSON -> {raw_path}")

        # Flatten to CSV
        df = flatten_matches_to_df(data)
        csv_path = Path(args.processed_dir) / "football-data" / args.competition / f"matches_{yr}.csv"
        save_csv(df, csv_path)
        print(f"Saved CSV ({len(df)} rows) -> {csv_path}")
        total_matches += len(df)

    print(f"\nDone. Total rows across seasons: {total_matches}")

if __name__ == "__main__":
    main()