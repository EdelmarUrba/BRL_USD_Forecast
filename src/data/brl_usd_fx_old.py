#!/usr/bin/env python3
"""
generate_brl_usd_dataset.py

Este script coleta e integra dados diários da taxa de câmbio USD/BRL (código FRED: DEXBZUS) dos últimos 5 anos,
além de indicadores macroeconômicos relevantes dos EUA (PIB, desemprego, juros) via FRED St. Louis.
Permite ao usuário informar a FRED API key via argumento ou utilizar automaticamente a chave registrada no arquivo .env
ou variável de ambiente FRED_API_KEY.

Os arquivos de saída são salvos no diretório corrente e também em:
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/brl_usd

Uso:
    python generate_brl_usd_dataset.py [--fred_api_key SUA_CHAVE_FRED]
        [--start_date AAAA-MM-DD] [--end_date AAAA-MM-DD] [--output_file nome.csv]
    Para mais informações, execute:
    python generate_brl_usd_dataset.py --help
"""

import argparse
import logging
import sys
import os
import pandas as pd
from datetime import datetime, date

# Diretório padrão do projeto para dados brutos BRL-USD
PROJECT_DATA_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/brl_usd"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_last_5_years_dates():
    end_date = date.today()
    start_date = date(end_date.year - 5, end_date.month, end_date.day)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def business_days(start_date, end_date):
    return pd.date_range(start=start_date, end=end_date, freq='B')

def get_fred_api_key(cli_key=None):
    if cli_key:
        logging.info("Usando FRED API key fornecida via linha de comando.")
        return cli_key
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    env_key = os.getenv("FRED_API_KEY")
    if env_key:
        logging.info("Usando FRED API key do arquivo .env ou variável de ambiente.")
        return env_key
    logging.error("FRED API key não fornecida. Use --fred_api_key ou defina FRED_API_KEY no ambiente ou .env.")
    sys.exit(1)

def get_fred_usdbrl(start_date, end_date, fred_api_key):
    from fredapi import Fred
    fred = Fred(api_key=fred_api_key)
    logging.info(f"Coletando série USD/BRL (DEXBZUS) do FRED de {start_date} até {end_date}")
    try:
        fx = fred.get_series('DEXBZUS', observation_start=start_date, observation_end=end_date)
        df_fx = pd.DataFrame({'Date': fx.index, 'BRL/USD': fx.values})
        df_fx = df_fx.dropna()
        df_fx['Time'] = pd.to_datetime(df_fx['Date']).dt.strftime('%Y-%m-%d')
        logging.info(f"Primeiras linhas da série FRED USD/BRL:\n{df_fx[['Time', 'BRL/USD']].head()}")
        return df_fx[['Time', 'BRL/USD']].drop_duplicates('Time')
    except Exception as e:
        logging.error(f"Erro ao coletar DEXBZUS do FRED: {e}")
        return pd.DataFrame(columns=['Time', 'BRL/USD'])

def get_usa_macro(start_date, end_date, fred_api_key):
    from fredapi import Fred
    fred = Fred(api_key=fred_api_key)
    logging.info(f"Coletando indicadores macroeconômicos dos EUA de {start_date} até {end_date} via FRED")
    try:
        gdp = fred.get_series('GDP', observation_start=start_date, observation_end=end_date).resample('Q').ffill()
        gdp_df = pd.DataFrame({'Data': gdp.index, 'USAGDP': gdp.values})
        unemp = fred.get_series('UNRATE', observation_start=start_date, observation_end=end_date).resample('M').ffill()
        unemp_df = pd.DataFrame({'Data': unemp.index, 'USAUN': unemp.values})
        ir = fred.get_series('FEDFUNDS', observation_start=start_date, observation_end=end_date).resample('M').ffill()
        ir_df = pd.DataFrame({'Data': ir.index, 'USAIR': ir.values})
        logging.info(f"Primeiras linhas PIB EUA:\n{gdp_df.head()}")
        return gdp_df, unemp_df, ir_df
    except Exception as e:
        logging.error(f"Erro ao coletar dados EUA via FRED: {e}")
        return pd.DataFrame(columns=['Data', 'USAGDP']), pd.DataFrame(columns=['Data', 'USAUN']), pd.DataFrame(columns=['Data', 'USAIR'])

def fill_daily(df_dates, df_macro, col_name):
    if df_macro.empty:
        logging.warning(f"DataFrame macroeconômico vazio para {col_name}. Preenchendo com NaN.")
        return pd.Series([float('nan')] * len(df_dates))
    df_macro = df_macro.set_index('Data').sort_index()
    return df_dates['Date'].apply(lambda d: df_macro[col_name].asof(d))

def save_dual(df, name):
    # Salva no diretório corrente
    path_current = os.path.join(os.getcwd(), name)
    df.to_csv(path_current, index=False)
    logging.info(f"Arquivo salvo: {path_current}")
    # Salva no diretório padrão do projeto
    os.makedirs(PROJECT_DATA_DIR, exist_ok=True)
    path_project = os.path.join(PROJECT_DATA_DIR, name)
    df.to_csv(path_project, index=False)
    logging.info(f"Arquivo salvo: {path_project}")

def generate_brl_usd_dataset(start_date, end_date, output_file, fred_api_key):
    logging.info(f"Iniciando geração do dataset BRL-USD (FRED) de {start_date} até {end_date}")
    business_dates = business_days(start_date, end_date)
    df_dates = pd.DataFrame({'Date': business_dates})
    df_dates['Time'] = df_dates['Date'].dt.strftime('%Y-%m-%d')

    df_fx = get_fred_usdbrl(start_date, end_date, fred_api_key)
    if df_fx.empty:
        logging.error("Não foi possível obter a série BRL-USD do FRED. Abortando geração do dataset.")
        return None

    gdp_usa_df, unemp_usa_df, ir_usa_df = get_usa_macro(start_date, end_date, fred_api_key)

    df = pd.merge(df_dates, df_fx, on='Time', how='left')
    df['USAGDP'] = fill_daily(df, gdp_usa_df, 'USAGDP')
    df['USAUN'] = fill_daily(df, unemp_usa_df, 'USAUN')
    df['USAIR'] = fill_daily(df, ir_usa_df, 'USAIR')

    df = df[['Time', 'BRL/USD', 'USAGDP', 'USAUN', 'USAIR']]
    save_dual(df, os.path.basename(output_file))
    logging.info(f"Primeiras linhas do arquivo salvo:\n{df.head()}")
    print("\nFonte dos dados para referência ABNT (inclua na monografia):")
    print("FEDERAL RESERVE BANK OF ST. LOUIS. U.S. Dollar to Brazilian Real Spot Exchange Rate (DEXBZUS). Available at: <https://fred.stlouisfed.org/series/DEXBZUS>. Accessed on: 14 Jul. 2025.")
    return df

def main():
    parser = argparse.ArgumentParser(
        description='''
        Script para coletar e integrar dados da taxa de câmbio USD/BRL (FRED St. Louis, DEXBZUS) e indicadores macroeconômicos dos EUA.
        A FRED API key pode ser fornecida via argumento --fred_api_key ou automaticamente do arquivo .env/variável de ambiente.
        Arquivo de saída salvo no diretório corrente e em /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/brl_usd.
        ''',
        formatter_class=argparse.RawTextHelpFormatter
    )
    start_5y, end_5y = get_last_5_years_dates()
    parser.add_argument('--start_date', type=str, default=start_5y, help='Data inicial no formato YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, default=end_5y, help='Data final no formato YYYY-MM-DD')
    parser.add_argument('--output_file', type=str, default=f'Urba_Exchage_all_data_{start_5y[:4]}_{end_5y[:4]}_FRED.csv', help='Arquivo de saída')
    parser.add_argument('--fred_api_key', type=str, default=None, help='Chave API do FRED (opcional, usa .env se não fornecida)')

    args = parser.parse_args()

    fred_api_key = get_fred_api_key(args.fred_api_key)

    generate_brl_usd_dataset(
        start_date=args.start_date,
        end_date=args.end_date,
        output_file=args.output_file,
        fred_api_key=fred_api_key
    )

if __name__ == '__main__':
    main()
