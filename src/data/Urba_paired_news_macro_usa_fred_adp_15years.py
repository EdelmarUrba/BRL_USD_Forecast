#!/usr/bin/env python3
# coding: utf-8
"""
Script: Urba_paired_news_macro_usa_fred_adp_15years.py

Description:
------------
Robust, transparent and publication-ready script to pair a daily time-series file of US macroeconomic & ADP indicators
with a corpus of economic news from the FRED Blog in JSON Lines format.
- Only dates present in both sources are matched (intersection).
- Full audit, schema validation, and documentation for reproducibility.
- Human-in-the-loop features: prints samples of input and output, logs every main stage.

Files:
------
Input News: 
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_fred_blog_news_raw_corpus_fullfilled.jsonl
Input Macro: 
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_macro_usa_adp_15y_fullfilled.csv
Output Paired: 
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_paired_news_macro_usa_fred_adp_15years.jsonl
Log file: 
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_paired_news_macro_usa_fred_adp_15years_log.txt

Author: Edelmar Urba & scientific code assistant
Date: 2025-07-21

References (ABNT style):
--------------------------
- LOPES, Alexandre; MELO, Tiago Nascimento. Big Data e Ciência de Dados: Conceitos, Tecnologias e Aplicações. 1. ed. Rio de Janeiro: Elsevier, 2020.
- SHUMWAY, Robert H.; STOFFER, David S. Time Series Analysis and Its Applications. 4. ed. New York: Springer, 2017.
- CAMPISTRE, Diego Aranha; OLIVEIRA, Rafael S.; CIOLFI, Cynthia. Mineração de Textos: Conceitos, Ferramentas e Aplicações. 1. ed. São Paulo: Senac, 2016.
- FRED Blog — Federal Reserve Bank of St. Louis. Research and Data Publications. Disponível em: <https://fredblog.stlouisfed.org/>. Acesso em: 21 jul. 2025.

"""

import sys
import os
import json
import logging
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# --- PATHS (fully explicit for documentation and reproducibility) ---
NEWS_PATH = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_fred_blog_news_raw_corpus_fullfilled.jsonl"
MACRO_PATH = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_macro_usa_adp_15y_fullfilled.csv"
OUT_PATH = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_paired_news_macro_usa_fred_adp_15years.jsonl"
LOG_PATH = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/Urba_paired_news_macro_usa_fred_adp_15years_log.txt"

############################################

def setup_logging():
    logger = logging.getLogger('pairing_logger')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    fh = logging.FileHandler(LOG_PATH, encoding='utf-8', mode='w')
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(fh)
        logger.addHandler(sh)
    else:
        # Prevent duplicate handlers on reruns in interactive sessions
        logger.handlers.clear()
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

logger = setup_logging()

def print_sample_jsonl(path, n=3):
    """Print first n records of JSONL file (truncated to 280 chars for safety)"""
    logger.info(f"First {n} lines of JSONL ({path}):")
    with open(path, 'r', encoding='utf-8') as fin:
        for i, line in enumerate(fin):
            if i >= n:
                break
            logger.info(line.strip()[:280])

def print_sample_csv(path, n=3):
    logger.info(f"First {n} lines of CSV ({path}):")
    df = pd.read_csv(path)
    logger.info("\n" + df.head(n).to_string(index=False))

# --- SCHEMA UTILITIES (light/robust for scientific docs) ---
REQUIRED_COLS_MACRO = [
    'date','DEXBZUS','PAYEMS','CPIAUCSL','GDP','GDPC1','FEDFUNDS','UNRATE','UMCSENT','NER','NER_SA'
]
def check_macro_schema(cols):
    missing = [c for c in REQUIRED_COLS_MACRO if c not in cols]
    if missing:
        logger.error(f"[SCHEMA ERROR] Missing columns in macro csv: {missing}")
        raise Exception(f"Missing: {missing}")

def parse_jsonl_news(path):
    items = []
    with open(path, 'r', encoding='utf-8') as fin:
        for idx, line in enumerate(fin, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                # Ensure 'date' (prefer publication_time > pub_date > try to extract from link)
                if 'date' not in record:
                    for cand in ['publication_time', 'pub_date']:
                        if cand in record and isinstance(record[cand], str) and len(record[cand]) >= 10:
                            record['date'] = record[cand][:10]
                            break
                if 'date' not in record and 'link' in record and isinstance(record['link'], str):
                    tokens = [t for t in record['link'].split('/') if t.isdigit()]
                    if len(tokens) >= 2:
                        year, month = tokens[:2]
                        record['date'] = f"{year}-{month}-01"
                if 'date' in record:
                    # Validation ISO date
                    try:
                        _ = datetime.strptime(record['date'], "%Y-%m-%d")
                    except Exception:
                        logger.warning(f"[NEWS SCHEMA] Invalid date at record {idx}: {record['date']}")
                        continue
                    items.append(record)
                else:
                    logger.warning(f"[NEWS SCHEMA] No date on news at record {idx}, skipping.")
            except Exception as ex:
                logger.warning(f"[JSONL] Failed to parse line {idx}: {ex}")
    return items

##############################
# MAIN PAIRING LOGIC
##############################

def main():
    logger.info("==== NEWS x MACRO PAIRED PANEL GENERATION ====")
    logger.info(f"Script name and location: {os.path.abspath(__file__)}")
    logger.info(f"  Input news file:    {NEWS_PATH}")
    logger.info(f"  Input macro file:   {MACRO_PATH}")
    logger.info(f"  Output paired file: {OUT_PATH}")
    logger.info(f"  Log file:           {LOG_PATH}")

    # Print and log first lines of input files (human-in-the-loop)
    print_sample_jsonl(NEWS_PATH, n=3)
    print_sample_csv(MACRO_PATH, n=3)

    # --- Load macro csv (with deduplication and log)
    logger.info("Loading and schema-checking macroeconomic CSV...")
    macro_df = pd.read_csv(MACRO_PATH, dtype=str)

    # Detect and handle duplicated dates
    dup_count = macro_df.duplicated(subset='date', keep=False).sum()
    if dup_count > 0:
        logger.warning(f"[DATA WARNING] {dup_count} duplicated date rows found in macro CSV. Removing duplicates and keeping the first occurrence per date for pairing consistency.")
        macro_df = macro_df.drop_duplicates(subset='date', keep='first')
    else:
        logger.info("No duplicated dates. Macro CSV index is unique.")

    check_macro_schema(macro_df.columns)
    macro_df['date'] = pd.to_datetime(macro_df['date']).dt.strftime('%Y-%m-%d')
    macro_df.set_index('date', inplace=True)
    macro_dict = macro_df.to_dict(orient='index')
    macro_dates = set(macro_dict.keys())
    logger.info(f"Macro panel: {len(macro_dict)} records, {macro_df.columns.size} columns.")
    macro_min, macro_max = min(macro_dates), max(macro_dates)
    logger.info(f"Macro period: {macro_min} to {macro_max}")

    # --- Load News JSONL
    logger.info("Loading news corpus JSONL...")
    news_items = parse_jsonl_news(NEWS_PATH)
    logger.info(f"Corpus: {len(news_items)} news items loaded.")
    news_dates = set(i['date'] for i in news_items)
    min_news, max_news = min(news_dates), max(news_dates)
    logger.info(f"News period: {min_news} to {max_news}")

    # --- Compute overlap
    paired_dates = sorted(macro_dates & news_dates)
    logger.info(f"> Paired dates available: {len(paired_dates)} (intersection of news and macro)")

    only_news_dates = sorted(news_dates - macro_dates)
    only_macro_dates = sorted(macro_dates - news_dates)
    if only_news_dates:
        logger.info(f"> Example news-only dates (not paired): {only_news_dates[:3]}")
    if only_macro_dates:
        logger.info(f"> Example macro-only dates (not paired): {only_macro_dates[:3]}")

    # --- Merge for output
    logger.info("Performing pairing & writing output JSONL...")
    n_total, n_written = 0, 0
    with open(OUT_PATH, 'w', encoding='utf-8') as fout:
        for record in news_items:
            if record['date'] in macro_dict:
                merged = dict(record)
                merged.update(macro_dict[record['date']])
                json.dump(merged, fout, ensure_ascii=False)
                fout.write('\n')
                n_written += 1
            n_total += 1
    logger.info(f"Total input news:   {n_total}")
    logger.info(f"Paired & written :  {n_written}")

    # --- Human control: show first lines of output
    logger.info(f"First 3 rows from paired output ({OUT_PATH}):")
    with open(OUT_PATH, 'r', encoding='utf-8') as fin:
        for i, line in enumerate(fin):
            if i >= 3:
                break
            logger.info(line.strip()[:280])
    logger.info("---- END OF PAIRING ----")
    logger.info("""
References:
- LOPES, Alexandre; MELO, Tiago Nascimento. Big Data e Ciência de Dados: Conceitos, Tecnologias e Aplicações. 1. ed. Rio de Janeiro: Elsevier, 2020.
- SHUMWAY, Robert H.; STOFFER, David S. Time Series Analysis and Its Applications. 4. ed. New York: Springer, 2017.
- CAMPISTRE, Diego Aranha; OLIVEIRA, Rafael S.; CIOLFI, Cynthia. Mineração de Textos: Conceitos, Ferramentas e Aplicações. 1. ed. São Paulo: Senac, 2016.
- FRED Blog — Federal Reserve Bank of St. Louis. Research and Data Publications. Disponível em: <https://fredblog.stlouisfed.org/>. Acesso em: 21 jul. 2025.
    """)

if __name__ == '__main__':
    main()
