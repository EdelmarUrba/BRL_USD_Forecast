#!/usr/bin/env python3
# coding: utf-8
"""
Title: US Macroeconomic and ADP Employment Indicator Integration
Author: Edelmar Urba
Created: 2025-07-21

Description:
------------
This script integrates daily US macroeconomic indicators with historical ADP Employment data
(spanning at least 15 years) into a harmonized dataset. The output is sorted so that the
most recent date appears first (top of the file) and the oldest last (bottom of the file).
Logs, user interaction, and printed samples support best practices for international research
and didactic purposes.

Key Features:
-------------
- Full outer join by date, covering all available daily data.
- Forward fill ensures no missing dates, only leading nulls if initial values are absent.
- Sorted output: newest observations at the top, oldest at the bottom.
- Prevents accidental overwrite: user confirms or renames output file.
- Both head and tail of output shown both in console and log for human audit.
- All communication and documentation in English.

Usage:
------
1. Place the script in the project root or set BASE_DIR accordingly.
2. Ensure input files exist.
3. Run: `python Urba_macro_usa_adp_15y_fullfilled.py`
"""

import pandas as pd
from pathlib import Path
import logging
import sys
import datetime

# --- Paths and file names (modify BASE_DIR if needed) ---
BASE_DIR = Path("/home/edelmar-urba/Projetos/BRL_USD_Forecast")
RAW_ADP = BASE_DIR / "data/raw/adp_employments/Urba_adp_employment_history.csv"
RAW_MACRO = BASE_DIR / "data/raw/macro_usa/Urba_macro_usa_daily_15y.csv"
PROCESSED_DIR = BASE_DIR / "data/processed"
OUTFILE = PROCESSED_DIR / "Urba_macro_usa_adp_15y_fullfilled.csv"
LOGFILE = PROCESSED_DIR / f"integration_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Ensure the processed directory exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def explain_and_log_initial_context():
    logging.info("==== Integration Log: US Macroeconomic and ADP Employment Indicators ====")
    logging.info("Author: Edelmar Urba")
    logging.info(f"Execution Timestamp: {datetime.datetime.now().isoformat()}")
    logging.info(
        "Purpose: Integrate daily US macroeconomic time series and ADP Employment data into a single, "
        "fully synchronized and chronologically reverse-ordered dataset for advanced analysis and FRED Blog pairing."
    )
    logging.info(
        "Features: Full-outer-join by date, explanatory log, forward fill, "
        "prevent accidental overwrite, and sorted output (newest date first)."
    )
    logging.info(
        "Input files:\n"
        f"  - Macroeconomic: {RAW_MACRO}\n"
        f"  - ADP Employment: {RAW_ADP}\n"
        f"Output file:\n"
        f"  - {OUTFILE}\n"
        f"Log file:\n"
        f"  - {LOGFILE}\n"
    )

def read_and_prepare(filepath: Path, date_col: str = "date"):
    logging.info(f"Reading {filepath}...")
    df = pd.read_csv(filepath)
    logging.info(f"  Columns found: {list(df.columns)}")
    if date_col not in df.columns:
        logging.error(f"Column '{date_col}' not found in {filepath}.")
        raise ValueError(f"Missing '{date_col}' column.")
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col)
    logging.info(f"  Data range: {df[date_col].min().date()} to {df[date_col].max().date()} ({len(df)} rows).")
    return df

def integrate_and_fullfill(macro_df, adp_df):
    logging.info("Merging datasets via full outer join on 'date'.")
    merged = pd.merge(macro_df, adp_df, how='outer', on='date', suffixes=('_macro', '_adp'))
    min_date, max_date = merged['date'].min(), merged['date'].max()
    logging.info(f"Creating continuous daily time index: {min_date.date()} - {max_date.date()}")
    all_dates = pd.DataFrame({'date': pd.date_range(start=min_date, end=max_date, freq='D')})
    merged = pd.merge(all_dates, merged, how='left', on='date')
    fill_cols = [col for col in merged.columns if col != 'date']
    merged[fill_cols] = merged[fill_cols].fillna(method='ffill')
    logging.info("Forward fill complete; only leading missing values may remain (if any).")
    return merged

def document_column_meaning(merged_columns):
    logging.info("==== Final Dataset Columns ====")
    for col in merged_columns:
        if col == "date":
            explanation = "Daily calendar date (YYYY-MM-DD)."
        elif col.endswith("_macro"):
            explanation = f"Macroeconomic indicator from the US macro series — '{col}'."
        elif col.endswith("_adp"):
            explanation = f"ADP Employment indicator — '{col}'."
        else:
            explanation = f"Column to check origin: '{col}'."
        logging.info(f"- {col}: {explanation}")
    logging.info("All indicators are forward-filled. Data ready for time series and event studies.")

def print_and_log_sample(df):
    logging.info("==== First 7 rows of output data: ====")
    logging.info("\n" + df.head(7).to_string(index=False))
    print("\n--- First 7 rows of merged data ---")
    print(df.head(7).to_string(index=False))

def print_and_log_tail(df):
    logging.info("==== Last 7 rows of output data: ====")
    logging.info("\n" + df.tail(7).to_string(index=False))
    print("\n--- Last 7 rows of merged data ---")
    print(df.tail(7).to_string(index=False))

def main():
    explain_and_log_initial_context()
    macro_df = read_and_prepare(RAW_MACRO, date_col="date")
    adp_df = read_and_prepare(RAW_ADP, date_col="date")
    merged = integrate_and_fullfill(macro_df, adp_df)

    # Sort with the most recent date FIRST (descending order)
    merged = merged.sort_values('date', ascending=False).reset_index(drop=True)

    document_column_meaning(merged.columns)
    print_and_log_sample(merged)

    output_file_path = OUTFILE
    if output_file_path.exists():
        print(f"\nOutput file '{output_file_path}' already exists.")
        choice = input("Overwrite? (y = overwrite, n = choose a new filename): ").strip().lower()
        if choice != 'y':
            new_name = input("Enter new output filename (must end with .csv): ").strip()
            if not new_name.endswith('.csv'):
                new_name += '.csv'
            output_file_path = PROCESSED_DIR / new_name
            logging.info(f"User selected new output filename: {output_file_path}")
        else:
            logging.info("User confirmed overwrite of existing file.")

    logging.info(f"Saving merged dataset to: {output_file_path}")
    merged.to_csv(output_file_path, index=False)
    print(f"\nData saved to {output_file_path}")

    print_and_log_tail(merged)

    logging.info("Processing complete. For methodology and audit, refer to the log file and printouts.")
    logging.info("Archive this log as part of the research diary for transparency and traceability.")

if __name__ == "__main__":
    main()
