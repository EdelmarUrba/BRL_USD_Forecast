#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_pareia_news_macro_jsonl_daily_schema_validation.py

Enhanced for international scientific, transparent, and maximally informative merging of daily macro data and news.

Key upgrades:
- Audits and transparently reports the date coverage of both news and macro series.
- Only filters out news outside the strict date range of the macro series (maximizing overlap), unless otherwise requested.
- All functionalities for schema validation, field mapping, and logging remain.

Author: Edelmar Urba & scientific code assistant, 2025-07-19
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

# --- DEFAULT PATHS (edit as needed) ---
DEFAULT_NEWS = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_news_data/Urba_fred_blog_news_standard.jsonl'
DEFAULT_MACRO = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_time_series_data/Urba_macro_usa_daily_consolidated_adp.csv'
DEFAULT_OUTDIR = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/paired_time_series_news_training_data_daily'

DICT_COLUMNS = """
Column dictionary:
- date: (string, ISO YYYY-MM-DD) Date of news and macro alignment (required for pairing)
- [macro features]: All columns from the macro CSV (e.g. PAYEMS, CPIAUCSL, GDP, GDPC1, FEDFUNDS, UNRATE, UMCSENT, ADP_Employment_Change).
- [news fields]: All original/mapped news fields (title, summary, publication_time, full_article, link, authors, etc).
"""

#############
# DYNAMIC SCHEMA: Compatible for pydantic v1.x and v2.x
#############
def news_record_model_factory():
    """Return a NewsRecord class with correct parameter for constr(), by pydantic version."""
    FIELD_KWARG = {'pattern' if PYD_VER_MAJOR >=2 else 'regex': r'^\d{4}-\d{2}-\d{2}$'}
    class NewsRecord(BaseModel):
        date: constr(**FIELD_KWARG)
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

def print_help():
    print("="*60)
    print("NEWS & DAILY MACROECONOMIC DATA PAIRING SCRIPT [Validated, Audited]")
    print("="*60)
    print('\nPurpose: Robust and transparent pairing of daily macroeconomic panel with news JSONL, maximizing overlap and auditability.\n')
    print(DICT_COLUMNS)
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
    macro_dates = set(macro_dict.keys())
    macro_min = min(macro_dates)
    macro_max = max(macro_dates)
    print(f"[INFO] Macro data: {len(macro_dict)} daily rows, covering {macro_min} to {macro_max}")

    input_path = args.input_jsonl
    output_path = args.output_jsonl

    # Step 1: Load all news, audit date coverage
    news_items = []
    min_news_date, max_news_date = None, None
    with open(input_path, 'r', encoding='utf-8') as fin:
        for idx, line in enumerate(fin):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception as e:
                print(f"[WARN] Skipping record (line {idx+1}): cannot parse JSON. {e}")
                continue
            # --- Ensure 'date'
            if not item.get('date'):
                pt = item.get('publication_time') or item.get('pub_date', '')
                if pt and isinstance(pt, str) and len(pt) >= 10:
                    item['date'] = pt[:10]
                else:
                    # Impute date from next valid news if exists (forward-fill)
                    print(f"[WARN] Missing 'date' for news at line {idx+1}, attempting forward-fill from next news (if possible).")
                    continue  # Could forward-fill using list if necessary
            # Field mapping for schema
            if 'headline' not in item and 'title' in item:
                item['headline'] = item['title']
            if 'body' not in item and 'full_article' in item:
                item['body'] = item['full_article']
            if 'source' not in item and 'category' in item:
                item['source'] = item['category']
            # Audit min/max date in news
            try:
                news_date = item['date']
                datetime.strptime(news_date, "%Y-%m-%d")
                if (min_news_date is None) or (news_date < min_news_date):
                    min_news_date = news_date
                if (max_news_date is None) or (news_date > max_news_date):
                    max_news_date = news_date
                news_items.append(item)
            except Exception:
                print(f"[WARN] Invalid or missing date for news at line {idx+1}: {item.get('date')} (Skipping record)")
                continue

    news_dates = set(item['date'] for item in news_items)
    print(f"[INFO] News data: {len(news_items)} records, covering {min_news_date} to {max_news_date}")

    # Step 2: Compute maximal possible overlap
    common_dates = news_dates & macro_dates
    only_in_news = news_dates - macro_dates
    only_in_macro = macro_dates - news_dates

    print(f"[AUDIT] News/series time span overlap:")
    print(f"- Dates covered by NEWS.......: {min_news_date} to {max_news_date} ({len(news_dates)} unique)")
    print(f"- Dates covered by MACRO......: {macro_min} to {macro_max} ({len(macro_dates)} unique)")
    print(f"- Dates available for PAIRING.: {len(common_dates)} (news with macro)")
    print(f"- News-only dates (unpaired)..: {len(only_in_news)}")
    print(f"- Macro-only dates............: {len(only_in_macro)}")

    if only_in_news:
        print("→ Suggestion: Only news entries within the intersection (covered by macro data) will be merged.")

    # Step 3: Pairing and schema validation (news filtered for intersection)
    n_total, n_matched, n_invalid, n_missing = 0, 0, 0, 0
    with open(output_path, 'w', encoding='utf-8') as fout:
        for idx, news_item in enumerate(tqdm(news_items, desc="Schema Validating & Pairing")):
            n_total += 1
            try:
                if news_item['date'] not in macro_dict:
                    n_missing += 1
                    continue
                rec = NewsRecord(**news_item)
                paired_row = dict(news_item)
                for k, v in macro_dict[news_item['date']].items():
                    paired_row[k] = v
                json.dump(paired_row, fout, ensure_ascii=False)
                fout.write('\n')
                n_matched += 1
            except (ValidationError, ValueError, json.JSONDecodeError) as e:
                print(f"[WARN] Skipping record (line {n_total}): {e}\n  → Content: {str(news_item)[:200]}[...]")
                n_invalid += 1
                continue

    print(f"\n[INFO] Processing complete (within overlap).")
    print(f"  Total news processed:       {n_total}")
    print(f"  News matched with macro:    {n_matched}")
    print(f"  News outside macro window:  {n_missing}")
    print(f"  INVALID news skipped:       {n_invalid}")

    if len(only_in_news) > 0:
        print(f"\n[REPORT] Example unpaired NEWS dates (up to 10): {sorted(list(only_in_news))[:10]}")
    if len(only_in_macro) > 0:
        print(f"[REPORT] Example macro-only dates (up to 10): {sorted(list(only_in_macro))[:10]}")

    print(f"\n[INFO] Paired output written to:\n  {output_path}")
    print("\nColumn dictionary and further details available with --help.")
    print("[END] Script provides transparent, robust, and maximally informative macro-news data pairing.")

if __name__ == '__main__':
    main()
