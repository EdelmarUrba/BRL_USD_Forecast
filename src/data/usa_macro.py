#!/usr/bin/env python3
"""
usa_macro.py

Script for downloading and processing key US macroeconomic indicators from FRED and saving the output
both in the current working directory (for development) and in the project directory:
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa

Features:
- Downloads GDP, unemployment rate, and federal funds rate from FRED.
- Allows user to specify the period of interest (default: last 5 years).
- Saves output files in both the current directory and the specified project directory.
- Prints clear step-by-step messages for human supervision.
- Shows the first rows of each dataset for visual validation.
- Offers help and usage instructions via -h or --help.
- Prints an ABNT-style reference to the data source at the end.

Usage:
    python usa_macro.py --api_key FRED_API_KEY [--start_year YYYY --end_year YYYY]
    python usa_macro.py [--start_year YYYY --end_year YYYY]  # Uses registered API key

Options:
    --api_key     Your FRED API key (optional if set as environment variable or in .env).
    --start_year  First year of interest (default: last 5 years).
    --end_year    Last year of interest (default: current year).
    -h, --help    Show this help message and exit.

Output:
    CSV files for GDP, unemployment rate, and interest rate will be saved in both:
      - The current working directory
      - /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa

"""

import argparse
import os
from datetime import datetime
import pandas as pd

TARGET_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa"

def print_step(msg):
    print(f"\n{'='*60}\n{msg}\n{'='*60}")

def get_fred_api_key(cli_key=None):
    if cli_key:
        print("Using FRED API key provided via command line.")
        return cli_key
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    env_key = os.getenv("FRED_API_KEY")
    if env_key:
        print("Using FRED API key from environment variable or .env file.")
        return env_key
    raise ValueError("FRED API key not provided. Set it via --api_key, environment variable FRED_API_KEY, or in a .env file.")

def get_usa_macro_fred(start_date, end_date, fred_api_key):
    from fredapi import Fred
    fred = Fred(api_key=fred_api_key)
    print_step("Downloading US GDP (quarterly, billions of dollars, seasonally adjusted annual rate) from FRED")
    gdp = fred.get_series('GDP', observation_start=start_date, observation_end=end_date)
    gdp = gdp.resample('Q').ffill()
    gdp_df = pd.DataFrame({'Date': gdp.index.strftime('%Y-%m-%d'), 'USAGDP': gdp.values})
    print(gdp_df.head())

    print_step("Downloading US Unemployment Rate (monthly, %) from FRED")
    unemp = fred.get_series('UNRATE', observation_start=start_date, observation_end=end_date)
    unemp = unemp.resample('M').ffill()
    unemp_df = pd.DataFrame({'Date': unemp.index.strftime('%Y-%m-%d'), 'USAUN': unemp.values})
    print(unemp_df.head())

    print_step("Downloading US Federal Funds Rate (monthly, %) from FRED")
    ir = fred.get_series('FEDFUNDS', observation_start=start_date, observation_end=end_date)
    ir = ir.resample('M').ffill()
    ir_df = pd.DataFrame({'Date': ir.index.strftime('%Y-%m-%d'), 'USAIR': ir.values})
    print(ir_df.head())

    abnt = "FEDERAL RESERVE BANK OF ST. LOUIS. FRED Economic Data. Available at: <https://fred.stlouisfed.org/>. Accessed on: 14 Jul. 2025."
    return gdp_df, unemp_df, ir_df, abnt

def save_df_dual(df, name):
    # Save in current directory
    current_dir = os.getcwd()
    path_current = os.path.join(current_dir, f"{name}.csv")
    df.to_csv(path_current, index=False)
    print(f"Saved: {path_current}")

    # Save in project macro_usa directory
    os.makedirs(TARGET_DIR, exist_ok=True)
    path_target = os.path.join(TARGET_DIR, f"{name}.csv")
    df.to_csv(path_target, index=False)
    print(f"Saved: {path_target}")

def main():
    parser = argparse.ArgumentParser(
        description="Download and process US macroeconomic data (GDP, unemployment, interest rate) from FRED.\n"
                    "Output files are saved both in the current directory and in the project macro_usa directory.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    current_year = datetime.now().year
    default_start = current_year - 5 + 1
    parser.add_argument('--api_key', type=str, default=None, help='Your FRED API key (optional if set as environment variable or .env).')
    parser.add_argument('--start_year', type=int, default=default_start, help='First year of interest (default: last 5 years)')
    parser.add_argument('--end_year', type=int, default=current_year, help='Last year of interest (default: current year)')
    args = parser.parse_args()

    print_step("Starting US macroeconomic data download and processing")
    print("For help, run: python usa_macro.py -h\n")

    start_date = f"{args.start_year}-01-01"
    end_date = f"{args.end_year}-12-31"

    try:
        api_key = get_fred_api_key(args.api_key)
        gdp_df, unemp_df, ir_df, abnt_ref = get_usa_macro_fred(start_date, end_date, api_key)
        save_df_dual(gdp_df, "usa_gdp")
        save_df_dual(unemp_df, "usa_unemployment")
        save_df_dual(ir_df, "usa_interest_rate")
        print_step("Processing completed successfully!")
        print("Files saved in both the current directory and:")
        print(f"  {TARGET_DIR}")
        print("\nData Source for ABNT reference (include in your thesis):")
        print(abnt_ref)
    except Exception as e:
        print(f"\nError using FRED: {e}")
        print_step("If FRED is unavailable, please download US macroeconomic data manually from one of these sources:")
        sources = [
            "https://www.bea.gov/data/gdp/gross-domestic-product",
            "https://www.bls.gov/data/",
            "https://home.treasury.gov/data",
            "https://data.imf.org/",
            "https://data.worldbank.org/country/united-states",
            "https://data.oecd.org/united-states.htm",
            "https://tradingeconomics.com/united-states/indicators",
            "https://www.quandl.com/data/USA",
            "https://www.kaggle.com/datasets?search=usa+macroeconomic"
        ]
        for url in sources:
            print(url)
        print("\nAfter downloading, run this script with --input pointing to your local file.")

if __name__ == '__main__':
    main()
