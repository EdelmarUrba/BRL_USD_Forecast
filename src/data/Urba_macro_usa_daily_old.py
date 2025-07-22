#!/usr/bin/env python3
# coding: utf-8

"""
Urba_macro_usa_daily.py

Scientific script to download and consolidate key US macroeconomic indicators from FRED,
generating a daily frequency dataset by forward-filling monthly and quarterly data.

Indicators included:
- PAYEMS     - Total Nonfarm Employment (monthly)
- CPIAUCSL   - Consumer Price Index (monthly)
- GDP        - Gross Domestic Product (quarterly)
- GDPC1      - Real Gross Domestic Product (quarterly)
- FEDFUNDS   - Federal Funds Effective Rate (daily)
- UNRATE     - Unemployment Rate (monthly)
- UMCSENT    - Consumer Sentiment Index (monthly)

Features:
- Allows user to specify start and end dates (default last 5 years)
- Downloads and merges all series automatically
- Forward-fills lower frequency series to daily
- Exports clean, ISO-formatted daily CSV ready for research and forecasting pipelines
- Prints informative progress and sample outputs for human supervision

Usage:
    python Urba_macro_usa_daily.py
    python Urba_macro_usa_daily.py -start 2020-01-01 -end 2025-06-30
    python Urba_macro_usa_daily.py -h

Author: Edelmar Urba
Date: 2025-07-19
"""

import os
import sys
import pandas as pd
import requests
from datetime import datetime
from io import StringIO

# Define FRED series metadata: series ID and frequency
FRED_SERIES = {
    'PAYEMS':    {'name': 'Employment (Nonfarm Payroll)',    'freq': 'M'},
    'CPIAUCSL':  {'name': 'Consumer Price Index (CPI)',      'freq': 'M'},
    'GDP':       {'name': 'Gross Domestic Product',          'freq': 'Q'},
    'GDPC1':     {'name': 'Real Gross Domestic Product',     'freq': 'Q'},
    'FEDFUNDS':  {'name': 'Federal Funds Rate',              'freq': 'D'},
    'UNRATE':    {'name': 'Unemployment Rate',               'freq': 'M'},
    'UMCSENT':   {'name': 'Consumer Sentiment',              'freq': 'M'},
}

# Output directory and filename
OUTPUT_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa"
OUTPUT_FILE = "Urba_macro_usa_daily.csv"


def print_help():
    help_message = """
------------------------------------------------------------------------------
Script: Urba_macro_usa_daily.py

Automatically downloads and consolidates main US macroeconomic indicators from FRED.

Data Frequency:
    - PAYEMS, CPIAUCSL, UNRATE, UMCSENT: Monthly (forward-filled to daily)
    - GDP, GDPC1: Quarterly (forward-filled to daily)
    - FEDFUNDS: Daily

Parameters:
    -start YYYY-MM-DD : start date (default: 5 years ago)
    -end YYYY-MM-DD   : end date (default: today)
    -h or --help      : show this help message

Example:
    python Urba_macro_usa_daily.py -start 2020-01-01 -end 2025-07-01

Output:
    Daily consolidated CSV saved at:
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa/Urba_macro_usa_daily.csv

------------------------------------------------------------------------------
"""
    print(help_message)
    sys.exit(0)


def parse_args(argv):
    if '-h' in argv or '--help' in argv:
        print_help()
    today = datetime.today()
    start_date = (today.replace(year=today.year - 5)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    if '-start' in argv:
        try:
            start_date = argv[argv.index('-start') + 1]
        except Exception:
            print("Error: use -start YYYY-MM-DD")
            sys.exit(1)
    if '-end' in argv:
        try:
            end_date = argv[argv.index('-end') + 1]
        except Exception:
            print("Error: use -end YYYY-MM-DD")
            sys.exit(1)
    return start_date, end_date


def download_fred_series(series_id, start_date, end_date):
    print(f"  ↳ Downloading {series_id} - {FRED_SERIES[series_id]['name']}")
    url = (f"https://fred.stlouisfed.org/graph/fredgraph.csv"
           f"?id={series_id}&cosd={start_date}&coed={end_date}")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code} downloading {series_id} from FRED.")
    df = pd.read_csv(StringIO(response.content.decode('utf-8')))
    # Detect date column name and standardize
    date_cols = [c for c in ['DATE', 'observation_date'] if c in df.columns]
    if not date_cols:
        raise Exception(f"Date column not found in data for {series_id}")
    date_col = date_cols[0]
    df.rename(columns={date_col: 'date', df.columns[1]: series_id}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    return df[['date', series_id]]


def main():
    start_date, end_date = parse_args(sys.argv)
    print(f"\n[INFO] Starting download of FRED macro data from {start_date} to {end_date}...\n")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Download series individually
    series_dfs = {}
    for series_id in FRED_SERIES:
        df = download_fred_series(series_id, start_date, end_date)
        series_dfs[series_id] = df

    # Create full daily date index for the specified range
    daily_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    daily_df = pd.DataFrame({'date': daily_dates})

    # Merge the individual series with daily index and forward-fill less frequent data
    for series_id, df in series_dfs.items():
        daily_df = daily_df.merge(df, on='date', how='left')
        if FRED_SERIES[series_id]['freq'] != 'D':  # Forward fill for monthly & quarterly
            daily_df[series_id] = daily_df[series_id].ffill()
        else:
            # Even daily FEDFUNDS forward-fill missing days if any
            daily_df[series_id] = daily_df[series_id].ffill()

    # Sort by date descending and convert date to ISO string
    daily_df = daily_df.sort_values('date', ascending=False).reset_index(drop=True)
    daily_df['date'] = daily_df['date'].dt.strftime('%Y-%m-%d')

    # Save to CSV
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    daily_df.to_csv(output_path, index=False)

    print(f"\n✔ Successfully saved daily macro data to: {output_path}")
    print(f"✔ Dataset shape: {daily_df.shape}")
    print("\nSample preview:")
    print(daily_df.head(15).to_markdown(index=False))


if __name__ == "__main__":
    main()
