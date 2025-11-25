# /opt/airflow/data/scripts/fetch_data.py
import pandas as pd
import os
from datetime import datetime

RAW_DIR = "/opt/airflow/data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

URL_MESSAGES = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTacS4r6yCtXu4G1h2QYuU8602AhimGFmXPp65hhPwKStjEpnenLe64KcIaFImI5869bwiscjC4Jii1/pub?gid=1033608769&single=true&output=csv"
URL_STATUSES = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTacS4r6yCtXu4G1h2QYuU8602AhimGFmXPp65hhPwKStjEpnenLe64KcIaFImI5869bwiscjC4Jii1/pub?gid=966707183&single=true&output=csv"

def download_sheet(url: str, filename_prefix: str):
    print(f"Downloading {filename_prefix}...")
    df = pd.read_csv(url, dtype=str, low_memory=False)
    df = df.dropna(how='all').reset_index(drop=True)
    
    filename = f"{filename_prefix}_{datetime.now().strftime('%Y%m%d')}.tsv"
    path = os.path.join(RAW_DIR, filename)
    df.to_csv(path, sep='\t', index=False)
    print(f"SUCCESS: {len(df):,} rows → {path}")
    return path

def main():
    download_sheet(URL_MESSAGES, "whatsapp_messages")
    download_sheet(URL_STATUSES, "whatsapp_statuses")

if __name__ == "__main__":
    main()