#!/usr/bin/env python3
# coding: utf-8

"""
Urba_macro_usa_diarily_15_years.py

Script científico consolidado para download automático dos principais indicadores macroeconômicos dos EUA via FRED — incluindo a taxa de câmbio BRL-USD (DEXBZUS) —
com granularidade diária, cobrindo os últimos 15 anos (ou período definido pelo usuário).

Gera logs completos, imprime amostras do início e fim dos dados e garante ordem das colunas: date, DEXBZUS, demais indicadores.
"""

import os
import sys
import pandas as pd
import requests
from datetime import datetime, timedelta
from io import StringIO

FRED_SERIES = {
    'PAYEMS':   {'name': 'Employment (Nonfarm Payroll)',       'freq': 'M', 'url': 'https://fred.stlouisfed.org/series/PAYEMS'},
    'CPIAUCSL': {'name': 'Consumer Price Index (CPI)',         'freq': 'M', 'url': 'https://fred.stlouisfed.org/series/CPIAUCSL'},
    'GDP':      {'name': 'Gross Domestic Product',             'freq': 'Q', 'url': 'https://fred.stlouisfed.org/series/GDP'},
    'GDPC1':    {'name': 'Real Gross Domestic Product',        'freq': 'Q', 'url': 'https://fred.stlouisfed.org/series/GDPC1'},
    'FEDFUNDS': {'name': 'Federal Funds Rate',                 'freq': 'D', 'url': 'https://fred.stlouisfed.org/series/FEDFUNDS'},
    'UNRATE':   {'name': 'Unemployment Rate',                  'freq': 'M', 'url': 'https://fred.stlouisfed.org/series/UNRATE'},
    'UMCSENT':  {'name': 'Consumer Sentiment',                 'freq': 'M', 'url': 'https://fred.stlouisfed.org/series/UMCSENT'},
    'DEXBZUS':  {'name': 'BRL/USD Exchange Rate',              'freq': 'D', 'url': 'https://fred.stlouisfed.org/series/DEXBZUS'},
}

OUTPUT_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa"
OUTPUT_FILE = "Urba_macro_usa_daily_15y.csv"
LOG_FILE = "Urba_macro_usa_daily_15y_processing.log"

def print_help():
    help_message = f"""
------------------------------------------------------------------------------
Script: Urba_macro_usa_diarily_15_years.py

Download automatizado dos principais indicadores macroeconômicos dos EUA e
taxa de câmbio BRL-USD. Período padrão: últimos 15 anos até hoje.

Parâmetros:
    -start YYYY-MM-DD : data inicial opcional (default: 15 anos atrás)
    -end   YYYY-MM-DD : data final opcional   (default: hoje)
    -h ou --help      : exibe esta mensagem

Exemplo:
    python Urba_macro_usa_diarily_15_years.py -start 2010-01-01 -end 2025-07-20

Saídas:
    CSV diário: {OUTPUT_DIR}/{OUTPUT_FILE}
    Log:        {OUTPUT_DIR}/{LOG_FILE}
------------------------------------------------------------------------------
"""
    print(help_message)
    sys.exit(0)

def parse_args(argv):
    if '-h' in argv or '--help' in argv:
        print_help()
    today = datetime.today()
    start_date = (today - timedelta(days=int(365.25*15))).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    if '-start' in argv:
        try: start_date = argv[argv.index('-start') + 1]
        except Exception: print("Error: use -start YYYY-MM-DD"); sys.exit(1)
    if '-end' in argv:
        try: end_date = argv[argv.index('-end') + 1]
        except Exception: print("Error: use -end YYYY-MM-DD"); sys.exit(1)
    return start_date, end_date

def download_fred_series(series_id, start_date, end_date):
    meta = FRED_SERIES[series_id]
    print(f"  ↳ Downloading {series_id}: {meta['name']}")
    url = (f"https://fred.stlouisfed.org/graph/fredgraph.csv"
           f"?id={series_id}&cosd={start_date}&coed={end_date}")
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"[ERROR] Download {series_id}: HTTP {resp.status_code}")
    df = pd.read_csv(StringIO(resp.content.decode('utf-8')))
    date_cols = [c for c in ['DATE', 'observation_date'] if c in df.columns]
    if not date_cols: raise Exception(f"[ERROR] Date column not found for {series_id}")
    date_col = date_cols[0]
    df.rename(columns={date_col: 'date', df.columns[1]: series_id}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
    return df[['date', series_id]]

def main():
    start_date, end_date = parse_args(sys.argv)
    now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
    print(f"\n[INFO] Downloading FRED macro and FX data from {start_date} to {end_date} (executed: {now_str})\n")

    log = []
    log.append("==== LOG DO PROCESSAMENTO URBA_MACRO_USA_DAILY (15 anos) ====")
    log.append(f"Execução: {now_str}")
    log.append(f"Período solicitado: {start_date} a {end_date}")
    log.append("Indicadores baixados e URLs:")
    for sid, meta in FRED_SERIES.items():
        log.append(f"  - {sid:8} : {meta['name']} — {meta['url']}")
    log.append("----------------------------------------------------")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    series_dfs = {}
    for series_id in FRED_SERIES:
        try:
            df = download_fred_series(series_id, start_date, end_date)
            series_dfs[series_id] = df
            log.append(f"[OK] Série {series_id}: {len(df)} registros de {df['date'].min().date()} a {df['date'].max().date()}")
        except Exception as ex:
            log.append(f"[FAIL] Série {series_id}: {ex}")
            continue

    daily_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    daily_df = pd.DataFrame({'date': daily_dates})

    for series_id, df in series_dfs.items():
        daily_df = daily_df.merge(df, on='date', how='left')
        if FRED_SERIES[series_id]['freq'] != 'D':
            daily_df[series_id] = daily_df[series_id].ffill()
        else:
            daily_df[series_id] = daily_df[series_id].ffill()

    daily_df.sort_values('date', inplace=True)
    daily_df['date'] = daily_df['date'].dt.strftime('%Y-%m-%d')

    # ===== Ordenação das colunas: date (primeira), DEXBZUS (segunda), depois as demais =====
    cols = list(daily_df.columns)
    ordered_cols = []
    # Garante 'date' como primeira, 'DEXBZUS' como segunda
    if 'date' in cols:
        ordered_cols.append('date')
    if 'DEXBZUS' in cols:
        ordered_cols.append('DEXBZUS')
    for c in cols:
        if c not in ordered_cols:
            ordered_cols.append(c)
    daily_df = daily_df[ordered_cols]

    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    daily_df.to_csv(output_path, index=False)

    log.append("----------------------------------------------------")
    log.append(f"Arquivo de saída: {output_path}")
    log.append(f"Shape final: {daily_df.shape[0]} datas x {daily_df.shape[1]} colunas.")
    log.append(f"Primeira data: {daily_df['date'].iloc[0]} — Última data: {daily_df['date'].iloc[-1]}")
    log.append("")
    log.append("Primeiras linhas do CSV de saída:")
    log.append(daily_df.head(7).to_string(index=False))
    log.append("Últimas linhas do CSV de saída:")
    log.append(daily_df.tail(7).to_string(index=False))
    log.append("----------------------------------------------------")
    log.append("Referências fonte:")
    for sid, meta in FRED_SERIES.items():
        log.append(f"- {sid}: {meta['url']}")
    log.append("FRED base: https://fred.stlouisfed.org/")
    log.append(f"Script executado: {os.path.abspath(__file__)} — Data/Hora: {now_str}")
    log.append("==== FIM LOG ====")

    log_path = os.path.join(OUTPUT_DIR, LOG_FILE)
    with open(log_path, 'w', encoding='utf-8') as flog:
        flog.write('\n'.join(log))
    print(f"\n✔ Log científico detalhado salvo em: {log_path}")

    print("\nAmostra — Primeiras linhas do CSV de saída:")
    print(daily_df.head(10).to_markdown(index=False))
    print("\nAmostra — Últimas linhas do CSV de saída:")
    print(daily_df.tail(10).to_markdown(index=False))
    print("\nResumido: shape", daily_df.shape, "| Datas:", daily_df['date'].iloc[0], "a", daily_df['date'].iloc[-1])
    print(f"\n[✔] Execução científica finalizada. Dados diários prontos para integração ao pipeline macro-news.\n")

if __name__ == "__main__":
    main()
