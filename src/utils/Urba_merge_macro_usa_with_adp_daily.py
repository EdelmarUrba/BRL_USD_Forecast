#!/usr/bin/env python
# coding: utf-8
"""
Script: Urba_merge_macro_usa_with_adp_daily.py

Description:
------------
Merges the ADP Employment indicator (monthly, 'us-private-employment_padronizado.csv') with the daily consolidated US macroeconomic dataset ('Urba_macro_usa_daily_consolidated.csv').
The ADP monthly values are forward-filled for each day of the corresponding month, producing a fully daily panel for advanced econometric forecasting and news studies.

Usage:
    python Urba_merge_macro_usa_with_adp_daily.py

Options:
    -h, --help    Show usage guide and column dictionary.

Author: Edelmar Urba (adapted for daily frequency and international publication)
Date: 2025-07-19
"""

import pandas as pd
import os
import sys
import textwrap

# === CONFIGURATION ===
DIR_INPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa'
FILE_MACRO = 'Urba_macro_usa_daily.csv'
FILE_ADP = 'us-private-employment_padronizado.csv'
FILE_OUTPUT = 'Urba_macro_usa_daily_consolidated_adp.csv'

PATH_MACRO = os.path.join(DIR_INPUT, FILE_MACRO)
PATH_ADP = os.path.join(DIR_INPUT, FILE_ADP)
PATH_OUTPUT = os.path.join(DIR_INPUT, FILE_OUTPUT)

DICT_COLUMNS = '''
Column dictionary:
- date: Date in ISO format (YYYY-MM-DD)
- PAYEMS: Nonfarm payrolls (BLS, daily via forward-fill)
- CPIAUCSL: Consumer Price Index (CPI-U, daily via forward-fill)
- GDP: Nominal GDP (quarterly, filled daily within quarter)
- GDPC1: Real GDP (quarterly, filled daily within quarter)
- FEDFUNDS: US Fed Funds Rate (daily, if available)
- UNRATE: Unemployment Rate (BLS, daily via forward-fill)
- UMCSENT: Consumer sentiment index (Univ. Michigan, daily via forward-fill)
- ADP_Employment_Change: Private employment change (ADP; monthly value forward-filled across days of the month)
'''

def print_help():
    print("\n" + "="*60)
    print("MACROECONOMIC & ADP INDICATORS DAILY INTEGRATION SCRIPT")
    print("="*60)
    print("\nPurpose:")
    print(textwrap.fill(
        "Integrates monthly ADP private employment indicator into the daily consolidated US macroeconomic dataset. The merger is performed via the daily ISO date field (YYYY-MM-DD), with the ADP value forward-filled for each month. This produces a daily panel for econometric analysis and news-driven forecasting.",
        width=80))
    print("\nUsage:")
    print("    python Urba_merge_macro_usa_with_adp_daily.py\n")
    print(DICT_COLUMNS)
    print("Input files expected:\n"
        f"  Macro: {PATH_MACRO}\n"
        f"  ADP: {PATH_ADP}\n"
        f"  Output : {PATH_OUTPUT}\n")
    print("Script prints head of files after processing for manual verification.")
    exit(0)

def main():
    if any(a in ['-h', '--help'] for a in sys.argv):
        print_help()

    print(f"\nLoading daily macroeconomic data:\n  {PATH_MACRO}")
    df_macro = pd.read_csv(PATH_MACRO, parse_dates=['date'])
    print("\nFirst rows of macro data:")
    print(df_macro.head(7).to_string(index=False))

    print(f"\nLoading ADP Employment data (monthly):\n  {PATH_ADP}")
    df_adp = pd.read_csv(PATH_ADP, parse_dates=['date'])
    print("\nFirst rows of ADP data:")
    print(df_adp.head(7).to_string(index=False))

    # Expand ADP monthly to daily with forward-fill within each month
    adp_daily = (df_adp
        .set_index('date')
        .reindex(pd.date_range(df_macro['date'].min(), df_macro['date'].max(), freq='D'))
        .fillna(method='ffill')
        .rename_axis('date').reset_index()
    )

    # Optionally keep only the ADP column (plus date)
    adp_indicator_col = [col for col in df_adp.columns if col != 'date'][0]
    adp_daily = adp_daily[['date', adp_indicator_col]]
    adp_daily.columns = ['date', 'ADP_Employment_Change']

    # Merge
    print("\nMerging on 'date' (YYYY-MM-DD)...")
    df_merged = pd.merge(df_macro, adp_daily, on='date', how='left')

    # Quality checks
    print(f"\nNumber of records pre-merge: {len(df_macro)}")
    print(f"Number of records post-merge: {len(df_merged)}")
    if df_merged['ADP_Employment_Change'].isnull().any():
        print("WARNING: Some daily dates are missing ADP data after forward-fill.")

    # Sort by date descending for readability
    df_merged = df_merged.sort_values('date', ascending=False)

    # Save
    df_merged.to_csv(PATH_OUTPUT, index=False)
    print(f"\nOutput file written:\n  {PATH_OUTPUT}")

    print("\nColumn dictionary available via --help.\n")
    print("First rows of merged output:")
    print(df_merged.head(7).to_string(index=False))
    print("\nProcess completed successfully.")

if __name__ == '__main__':
    main()
