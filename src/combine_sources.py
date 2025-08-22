import pandas as pd
from pathlib import Path

RAW_DIR_API = Path("data/raw/api-football/PL")
RAW_DIR_FD = Path("data/processed/football-data/PL")  # football-data.org processed CSVs

PROCESSED_DIR = Path("data/processed/football-data/PL/combined")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# Combine API-Football CSVs
# -------------------------------
api_files = list(RAW_DIR_API.glob("*.csv"))
api_dfs = [pd.read_csv(f) for f in api_files]
if api_dfs:
    pd.concat(api_dfs, ignore_index=True).to_csv(PROCESSED_DIR / "api_football_matches.csv", index=False)
    print(f"API-Football combined CSV saved ({len(api_dfs)} files)")

# -------------------------------
# Combine Football-Data.org CSVs
# -------------------------------
fd_files = list(RAW_DIR_FD.glob("matches_*.csv"))
fd_dfs = [pd.read_csv(f) for f in fd_files]
if fd_dfs:
    pd.concat(fd_dfs, ignore_index=True).to_csv(PROCESSED_DIR / "football_data_org_matches.csv", index=False)
    print(f"Football-Data.org combined CSV saved ({len(fd_dfs)} files)")
