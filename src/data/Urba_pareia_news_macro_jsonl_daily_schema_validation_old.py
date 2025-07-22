#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_pareia_news_macro_jsonl_daily_schema_validation.py

High-quality scientific software for reproducible research.
Pairs and validates daily macroeconomic data with news articles, using pydantic for schema validation.
Automatically detects and adapts schema for both pydantic v1.x (regex=) and v2.x (pattern=).
"""

import sys
import os
import json
import argparse
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# === Import and Version-detect pydantic ===
try:
    import pydantic
    from pydantic import BaseModel, ValidationError, constr
    PYD_VER_MAJOR = int(pydantic.__version__.split(".")[0])
except ImportError:
    print("[FATAL] The pydantic package is required. Install it using 'pip install pydantic'.")
    sys.exit(1)

# === DEFAULT PATHS ===
DEFAULT_NEWS = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_news_data/Urba_fred_blog_news_standard.jsonl'
DEFAULT_MACRO = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_time_series_data/Urba_macro_usa_daily_consolidated_adp.csv'
DEFAULT_OUTDIR = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/paired_time_series_news_training_data_daily'

DICT_COLUMNS = """
Column dictionary:
- date: (string, ISO YYYY-MM-DD) Date of news and macro alignment.
- [macro features]: All columns from the macro CSV (e.g., PAYEMS, CPIAUCSL, GDP, GDPC1, FEDFUNDS, UNRATE, UMCSENT, ADP_Employment_Change).
- [news fields]: All original fields from the JSONL news (headline, body, source, tickers, etc).
"""

#############
# DYNAMIC SCHEMA: Compatible for pydantic v1.x and v2.x
#############
def news_record_model_factory():
    """Return a NewsRecord class with correct parameter for constr(), by pydantic version."""
    FIELD_KWARG = {'pattern' if PYD_VER_MAJOR >=2 else 'regex': r'^\d{4}-\d{2}-\d{2}$'}
    class NewsRecord(BaseModel):
        date: constr(**FIELD_KWARG)  # Strict ISO format
        headline: str = None
        body: str = None
        source: str = None
        tickers: list = None
        class Config:
            extra = "allow"
    return NewsRecord

NewsRecord = news_record_model_factory()

# --- Macro schema validation (columns only) ---
REQUIRED_MACRO_COLS = [
    'date', 'PAYEMS', 'CPIAUCSL', 'GDP', 'GDPC1', 'FEDFUNDS', 'UNRATE', 'UMCSENT', 'ADP_Employment_Change'
]
def check_macro_schema(col_list):
    missing = [col for col in REQUIRED_MACRO_COLS if col not in col_list]
    if missing:
        raise ValueError(f"[Macro Schema Error] Missing required columns: {missing}")

############
# ARG-PARSING & HELP
############

def print_help():
    print("="*60)
    print("NEWS & DAILY MACROECONOMIC DATA PAIRING SCRIPT [Validated Schema, Pydantic v1+v2]")
    print("="*60)
    print("\nPurpose:\nPairs daily macroeconomic panel with news JSONL, enforcing minimal schema on both inputs.")
    print("News records must have 'date' (YYYY-MM-DD); macro CSV must have all required columns.")
    print("\nUsage:\n  python ... [--input-jsonl] [--macro-csv] [--output-jsonl]\n")
    print("Defaults:\n--input-jsonl:   {}\n--macro-csv:     {}\n--output-jsonl:  [auto in DEFAULT_OUTDIR]\nDEFAULT_OUTDIR:  {}".format(
        DEFAULT_NEWS, DEFAULT_MACRO, DEFAULT_OUTDIR))
    print(DICT_COLUMNS)
    print("\nSchema validation adapts to pydantic v1.x or v2.x automatically.")
    print("\nValidation: Skips each invalid news record; fails if macro schema unfit.")
    sys.exit(0)

def infer_output_path(news_path, macro_path, outdir):
    news_base = os.path.splitext(os.path.basename(news_path))[0]
    macro_base = os.path.splitext(os.path.basename(macro_path))[0]
    outname = f"paired_{news_base}_{macro_base}.jsonl"
    return os.path.join(outdir, outname)

def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--input-jsonl', default=DEFAULT_NEWS)
    parser.add_argument('--macro-csv', default=DEFAULT_MACRO)
    parser.add_argument('--output-jsonl', default=None)
    parser.add_argument('-h', '--help', action='store_true')
    args = parser.parse_args()
    if args.help:
        print_help()
    if args.output_jsonl is None:
        os.makedirs(DEFAULT_OUTDIR, exist_ok=True)
        args.output_jsonl = infer_output_path(args.input_jsonl, args.macro_csv, DEFAULT_OUTDIR)
    return args

###########
# MAIN
###########

def main():
    args = parse_args()
    print(f"\n[INFO] Detected pydantic major version: {PYD_VER_MAJOR}")
    print(f"[INFO] Validating and loading macroeconomic data:\n  {args.macro_csv}")
    macro_df = pd.read_csv(args.macro_csv, parse_dates=['date'])
    check_macro_schema(macro_df.columns)
    macro_df['date'] = macro_df['date'].dt.strftime('%Y-%m-%d')
    macro_df.set_index('date', inplace=True)
    macro_dict = macro_df.to_dict(orient='index')
    print(f"[INFO] Macro data: {len(macro_dict)} daily rows, schema OK.")

    input_path = args.input_jsonl
    output_path = args.output_jsonl
    n_total, n_matched, n_invalid, n_missing = 0, 0, 0, 0

    print(f"\n[INFO] Pairing news ({input_path}) with daily macro data. [Output: {output_path}]")
    with open(input_path, 'r', encoding='utf-8') as fin, open(output_path, 'w', encoding='utf-8') as fout:
        for line in tqdm(fin, desc="Schema Validating & Pairing"):
            n_total += 1
            try:
                news_item = json.loads(line)
                # Validate news schema (raise if any required field/formats wrong)
                rec = NewsRecord(**news_item)
                ndate = rec.date
                # Further validation: is date parseable?
                try:
                    datetime.strptime(ndate, "%Y-%m-%d")
                except Exception:
                    raise ValueError("Date field not in ISO format YYYY-MM-DD")

                if ndate in macro_dict:
                    macro_row = macro_dict[ndate]
                    paired_row = dict(news_item)  # Copy all fields
                    for k, v in macro_row.items():
                        paired_row[k] = v
                    json.dump(paired_row, fout, ensure_ascii=False)
                    fout.write('\n')
                    n_matched += 1
                else:
                    n_missing += 1
            except (ValidationError, ValueError) as e:
                print(f"[WARN] Skipping invalid news record (line {n_total}): {e}")
                n_invalid += 1
                continue

    print(f"\n[INFO] Processing complete.")
    print(f"  Total news processed:  {n_total}")
    print(f"  News matched with macro: {n_matched}")
    print(f"  News with no macro on date: {n_missing}")
    print(f"  INVALID news skipped (schema): {n_invalid}")
    if n_missing > 0:
        print("  WARNING: Some news had no macro data for their dates.")
    if n_invalid > 0:
        print("  WARNING: Some news were skipped due to schema violation or invalid date format.")

    print(f"\n[INFO] Paired output written to:\n  {output_path}")
    print("\nColumn dictionary and further details available with --help.")
    print("[END] Script enforces schema validation for reproducible macro-news studies.")

if __name__ == '__main__':
    main()
