#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_pareia_news_macro_jsonl_daily.py

Description:
------------
Pairs a daily macroeconomic panel (Urba_macro_usa_daily_consolidated_adp.csv) and news articles
(Urba_fred_blog_standard.jsonl), aligning each article to macroeconomic features valid for the article's date (ISO: YYYY-MM-DD).
Outputs a JSONL file where each news item is enriched with the corresponding macro indicators.
Designed for high scientific transparency and reproducibility.

Usage:
    python Urba_pareia_news_macro_jsonl_daily.py
        [--input-jsonl <news_file.jsonl>]
        [--macro-csv <macro_file.csv>]
        [--output-jsonl <output_file.jsonl>]
        [-h|--help]

If output-jsonl is not provided, it will be generated in DEFAULT_OUTDIR with the pattern
"paired_NEWSFILE_MACROFILE.jsonl".

Arguments:
    --input-jsonl   Input news JSONL file (must have 'date' field; default: DEFAULT_NEWS)
    --macro-csv     Daily macroeconomic CSV file (default: DEFAULT_MACRO)
    --output-jsonl  Output paired JSONL file (default: constructed in DEFAULT_OUTDIR)
    -h, --help      Print usage and column dictionary

Column Dictionary:
- date: (string, ISO YYYY-MM-DD) Date of news and macro alignment
- [macro features]: All columns from the macro CSV (e.g., PAYEMS, CPIAUCSL, GDP, GDPC1, FEDFUNDS, UNRATE, UMCSENT, ADP_Employment_Change)
- [news fields]: All original fields from JSONL news (title, body, source, tickers, etc.)
Each output record thus represents a news item accompanied by macro context on its publication date.

Author: Edelmar Urba (with updates for daily granularity and robust reproducibility)
Date: 2025-07-19
"""

import sys
import os
import json
import argparse
import pandas as pd
from tqdm import tqdm

# === DEFAULT PATHS ===
DEFAULT_NEWS = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_news_data/Urba_fred_blog_standard.jsonl'
DEFAULT_MACRO = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_time_series_data/Urba_macro_usa_daily_consolidated_adp.csv'
DEFAULT_OUTDIR = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/paired_time_series_news_training_data_daily'

DICT_COLUMNS = """
Column dictionary:
- date: (string, ISO YYYY-MM-DD) Date of news and macro alignment.
- [macro features]: All columns from the macro CSV (e.g., PAYEMS, CPIAUCSL, GDP, GDPC1, FEDFUNDS, UNRATE, UMCSENT, ADP_Employment_Change).
- [news fields]: All original fields from the JSONL news (headline, body, source, tickers, etc).

All output records pair daily macro context with each news item, enabling robust studies of macro-news interactions.
"""

def print_help():
    print("="*60)
    print("NEWS & DAILY MACROECONOMIC DATA PAIRING SCRIPT")
    print("="*60)
    print("\nPurpose:\nPairs a daily macroeconomic panel to a news JSONL collection, aligning by 'date' (ISO, YYYY-MM-DD). Output is a JSONL file, each line a JSON object with merged news and macro fields, suitable for downstream empirical analysis.\n")
    print("Usage:\n  python Urba_pareia_news_macro_jsonl_daily.py [--input-jsonl <news_file.jsonl>] [--macro-csv <macro_file.csv>] [--output-jsonl <output_file.jsonl>]\n")
    print("Defaults:\n  --input-jsonl:   {}".format(DEFAULT_NEWS))
    print("  --macro-csv:     {}".format(DEFAULT_MACRO))
    print("  --output-jsonl:  [AUTO: will be set below DEFAULT_OUTDIR]\n")
    print("  DEFAULT_OUTDIR:  {}".format(DEFAULT_OUTDIR))
    print(DICT_COLUMNS)
    print("\nRecommended validation: Review a sample of the paired data using pandas or jq for consistency.")
    sys.exit(0)

def infer_output_path(news_path, macro_path, outdir):
    news_base = os.path.splitext(os.path.basename(news_path))[0]
    macro_base = os.path.splitext(os.path.basename(macro_path))[0]
    outname = f"paired_{news_base}_{macro_base}.jsonl"
    return os.path.join(outdir, outname)

def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--input-jsonl', default=DEFAULT_NEWS, help="Input news JSONL file (default: DEFAULT_NEWS)")
    parser.add_argument('--macro-csv', default=DEFAULT_MACRO, help="Daily macroeconomic CSV file (default: DEFAULT_MACRO)")
    parser.add_argument('--output-jsonl', default=None, help="Output paired JSONL file (default: constructed in DEFAULT_OUTDIR)")
    parser.add_argument('-h', '--help', action='store_true', help="Show documentation and exit")
    args = parser.parse_args()
    if args.help:
        print_help()
    if args.output_jsonl is None:
        # Ensure output directory exists
        os.makedirs(DEFAULT_OUTDIR, exist_ok=True)
        args.output_jsonl = infer_output_path(args.input_jsonl, args.macro_csv, DEFAULT_OUTDIR)
    return args

def main():
    args = parse_args()

    print(f"\nLoading daily macroeconomic data:\n  {args.macro_csv}")
    macro_df = pd.read_csv(args.macro_csv, parse_dates=['date'])
    macro_df['date'] = macro_df['date'].dt.strftime('%Y-%m-%d')
    macro_df.set_index('date', inplace=True)
    macro_dict = macro_df.to_dict(orient='index')
    print(f"Loaded {len(macro_dict)} daily macro rows.")

    input_path = args.input_jsonl
    output_path = args.output_jsonl
    n_matched, n_missing = 0, 0

    print(f"\nPairing news ({input_path}) with daily macro data. Output: {output_path}")

    with open(input_path, 'r', encoding='utf-8') as fin, open(output_path, 'w', encoding='utf-8') as fout:
        for line in tqdm(fin, desc="Pairing"):
            news_item = json.loads(line)
            ndate = news_item.get('date')
            if ndate and ndate in macro_dict:
                macro_row = macro_dict[ndate]
                paired_row = dict(news_item)  # Copy all news fields
                for k, v in macro_row.items():
                    paired_row[k] = v
                json.dump(paired_row, fout, ensure_ascii=False)
                fout.write('\n')
                n_matched += 1
            else:
                n_missing += 1
                # Optionally, could append to a "missed" log for inspection

    print(f"\nProcess complete.")
    print(f"News items matched with macro data: {n_matched}")
    print(f"News items with no macro record for date: {n_missing}")
    if n_missing > 0:
        print("WARNING: Some news had no macro data for their dates. Review input consistency.")

    print(f"\nPaired news-macro data written to:\n  {output_path}")
    print("\nColumn dictionary and further details available with --help.")
    print("Script and output suitable for reproducible, international macro-news research.")

if __name__ == '__main__':
    main()
