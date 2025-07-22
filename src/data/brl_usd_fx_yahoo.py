#!/usr/bin/env python3
"""
Script: generate_brl_usd_dataset.py

Descrição:
    Coleta e integra dados diários da taxa de câmbio BRL-USD dos últimos 5 anos, permitindo ao usuário escolher entre:
    - PTAX do dólar à vista (spot) e do dólar futuro (BACEN)
    - Yahoo Finance (padrão)
    Integra também indicadores macroeconômicos do Brasil (Ipeadata) e dos EUA (FRED).
    O script imprime logs detalhados, primeiras linhas dos dados e permite modo 'development' (reutiliza arquivo local) ou 'batch' (coleta online).

Arquivo de saída gerado:
    - data/processed/Urba_Exchage_all_data_YYYY_YYYY_D_raw.csv

Como usar:
    python generate_brl_usd_dataset.py [--source ptax_spot|ptax_fut|yahoo] [--fred_api_key SUA_CHAVE_FRED] [--mode development|batch]
        [--start_date AAAA-MM-DD] [--end_date AAAA-MM-DD] [--output_file caminho.csv]
    Para mais informações, execute:
    python generate_brl_usd_dataset.py --help
"""

import argparse
import logging
import sys
import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, date
from fredapi import Fred

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_last_5_years_dates():
    end_date = date.today()
    start_date = date(end_date.year - 5, end_date.month, end_date.day)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def get_ptax_spot(start_date, end_date):
    today = date.today()
    if pd.to_datetime(end_date).date() > today:
        logging.warning(f"Ajustando data final de {end_date} para {today} (última data possível)")
        end_date = today.strftime('%Y-%m-%d')
    url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinal=@dataFinal)?@dataInicial='{start_date}'&@dataFinal='{end_date}'&$format=json"
    logging.info(f"Coletando PTAX dólar à vista do BACEN de {start_date} até {end_date}")
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()['value']
    df = pd.DataFrame(data)
    if df.empty:
        logging.error("Nenhum dado PTAX spot retornado.")
        return pd.DataFrame(columns=['Time', 'BRL/USD'])
    df['Time'] = pd.to_datetime(df['dataHoraCotacao']).dt.strftime('%-m/%-d/%Y')
    df = df.sort_values('Time')
    df['BRL/USD'] = df['cotacaoVenda']
    logging.info(f"Primeiras linhas PTAX spot:\n{df[['Time','BRL/USD']].head()}")
    return df[['Time', 'BRL/USD']].drop_duplicates('Time')

def get_ptax_fut(start_date, end_date):
    today = date.today()
    if pd.to_datetime(end_date).date() > today:
        logging.warning(f"Ajustando data final de {end_date} para {today} (última data possível)")
        end_date = today.strftime('%Y-%m-%d')
    url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarFuturoPeriodo(dataInicial=@dataInicial,dataFinal=@dataFinal)?@dataInicial='{start_date}'&@dataFinal='{end_date}'&$format=json"
    logging.info(f"Coletando PTAX dólar futuro do BACEN de {start_date} até {end_date}")
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()['value']
    df = pd.DataFrame(data)
    if df.empty:
        logging.error("Nenhum dado PTAX futuro retornado.")
        return pd.DataFrame(columns=['Time', 'BRL/USD'])
    df['Time'] = pd.to_datetime(df['dataHoraCotacao']).dt.strftime('%-m/%-d/%Y')
    df = df.sort_values('Time')
    df['BRL/USD'] = df['cotacaoVenda']
    logging.info(f"Primeiras linhas PTAX futuro:\n{df[['Time','BRL/USD']].head()}")
    return df[['Time', 'BRL/USD']].drop_duplicates('Time')

def get_yahoo_fx(start_date, end_date):
    logging.info(f"Coletando taxa BRL-USD do Yahoo Finance de {start_date} até {end_date}")
    df_fx = yf.download('BRLUSD=X', start=start_date, end=end_date)
    if df_fx.empty:
        logging.warning("Ticker 'BRLUSD=X' não disponível. Tentando 'USDBRL=X' e invertendo valores.")
        df_fx = yf.download('USDBRL=X', start=start_date, end=end_date)
        if df_fx.empty:
            logging.error("Não foi possível obter dados de BRL-USD nem USDBRL=X no Yahoo Finance.")
            return pd.DataFrame(columns=['Time', 'BRL/USD'])
        df_fx['BRL/USD'] = 1 / df_fx['Close']
    else:
        df_fx['BRL/USD'] = df_fx['Close']
    df_fx = df_fx[['BRL/USD']].copy()
    df_fx.index = pd.to_datetime(df_fx.index)
    df_fx['Time'] = df_fx.index.strftime('%-m/%-d/%Y')
    df_fx.reset_index(drop=True, inplace=True)
    logging.info(f"Primeiras linhas da taxa BRL-USD (Yahoo):\n{df_fx.head()}")
    return df_fx[['Time', 'BRL/USD']]

def get_ipeadata_series(sercodigo, start_date, end_date):
    url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{sercodigo}')"
    logging.info(f"Coletando série Ipeadata {sercodigo} de {start_date} até {end_date}")
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()['value']
        df = pd.DataFrame(data)
        df['Data'] = pd.to_datetime(df['Data'])
        df = df[(df['Data'] >= pd.to_datetime(start_date)) & (df['Data'] <= pd.to_datetime(end_date))]
        df = df.sort_values('Data')
        df = df[['Data', 'Valor']]
        logging.info(f"Primeiras linhas da série {sercodigo}:\n{df.head()}")
        return df
    except Exception as e:
        logging.error(f"Erro ao coletar série {sercodigo}: {e}")
        return pd.DataFrame(columns=['Data', 'Valor'])

def get_brazil_macro(start_date, end_date):
    pib_df = get_ipeadata_series('SCN10_PIBVABR', start_date, end_date).rename(columns={'Valor': 'BRGDP'})
    unemp_df = get_ipeadata_series('PNADC12_TT', start_date, end_date).rename(columns={'Valor': 'BRUN'})
    selic_df = get_ipeadata_series('BM12_TJOVER', start_date, end_date).rename(columns={'Valor': 'BRIR'})
    return pib_df, unemp_df, selic_df

def get_usa_macro(start_date, end_date, fred_api_key):
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

def generate_brl_usd_dataset(start_date, end_date, output_file, fred_api_key, mode, source):
    logging.info(f"Iniciando geração do dataset BRL-USD no modo: {mode}, fonte: {source}")
    df_dates = pd.DataFrame({'Date': pd.date_range(start=start_date, end=end_date, freq='D')})
    df_dates['Time'] = df_dates['Date'].dt.strftime('%-m/%-d/%Y')

    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Criado diretório: {output_dir}")

    if mode == 'development':
        if os.path.exists(output_file):
            logging.info(f"Arquivo {output_file} encontrado. Carregando dados existentes para desenvolvimento.")
            df = pd.read_csv(output_file)
            logging.info(f"Primeiras linhas do arquivo carregado:\n{df.head()}")
            return df
        else:
            logging.warning(f"Arquivo {output_file} não encontrado. Coletando dados online.")

    if source == 'ptax_spot':
        df_fx = get_ptax_spot(start_date, end_date)
    elif source == 'ptax_fut':
        df_fx = get_ptax_fut(start_date, end_date)
    elif source == 'yahoo':
        df_fx = get_yahoo_fx(start_date, end_date)
    else:
        logging.error(f"Fonte de dados cambiais '{source}' não suportada.")
        return None

    if df_fx.empty:
        logging.error("Não foi possível obter a série BRL-USD. Abortando geração do dataset.")
        return None

    pib_df, unemp_df, selic_df = get_brazil_macro(start_date, end_date)
    gdp_usa_df, unemp_usa_df, ir_usa_df = get_usa_macro(start_date, end_date, fred_api_key)

    df = pd.merge(df_dates, df_fx, on='Time', how='left')
    df['BRGDP'] = fill_daily(df, pib_df, 'BRGDP')
    df['BRUN'] = fill_daily(df, unemp_df, 'BRUN')
    df['BRIR'] = fill_daily(df, selic_df, 'BRIR')
    df['USAGDP'] = fill_daily(df, gdp_usa_df, 'USAGDP')
    df['USAUN'] = fill_daily(df, unemp_usa_df, 'USAUN')
    df['USAIR'] = fill_daily(df, ir_usa_df, 'USAIR')

    df = df[['Time', 'BRL/USD', 'BRGDP', 'BRUN', 'BRIR', 'USAGDP', 'USAUN', 'USAIR']]
    df.to_csv(output_file, index=False)
    logging.info(f"Arquivo salvo com sucesso: {output_file}")
    logging.info(f"Primeiras linhas do arquivo salvo:\n{df.head()}")
    return df

def main():
    parser = argparse.ArgumentParser(
        description='''
        Script para coletar e integrar dados da taxa de câmbio BRL-USD (PTAX spot/futuro BACEN ou Yahoo Finance)
        e indicadores macroeconômicos do Brasil e EUA.

        Arquivo de saída:
        - data/processed/Urba_Exchage_all_data_YYYY_YYYY_D_raw.csv

        Modos de operação:
        - development: usa arquivo local se existir para desenvolvimento.
        - batch: coleta dados online e gera arquivo atualizado.

        Fontes de câmbio:
        - ptax_spot: PTAX do dólar à vista (BACEN)
        - ptax_fut: PTAX do dólar futuro (BACEN)
        - yahoo: Yahoo Finance (padrão)
        ''',
        formatter_class=argparse.RawTextHelpFormatter
    )
    start_5y, end_5y = get_last_5_years_dates()
    parser.add_argument('--start_date', type=str, default=start_5y, help='Data inicial no formato YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, default=end_5y, help='Data final no formato YYYY-MM-DD')
    parser.add_argument('--output_file', type=str, default=f'data/processed/Urba_Exchage_all_data_{start_5y[:4]}_{end_5y[:4]}_D_raw.csv', help='Arquivo CSV de saída')
    parser.add_argument('--fred_api_key', type=str, default='79e3af0a73ce7614b08c8f1c306a1d3e', help='Chave API do FRED para dados EUA')
    parser.add_argument('--mode', type=str, choices=['development', 'batch'], default='development', help='Modo de operação: development ou batch')
    parser.add_argument('--source', type=str, choices=['ptax_spot', 'ptax_fut', 'yahoo'], default='yahoo', help='Fonte dos dados cambiais: ptax_spot, ptax_fut ou yahoo')

    args = parser.parse_args()

    generate_brl_usd_dataset(
        start_date=args.start_date,
        end_date=args.end_date,
        output_file=args.output_file,
        fred_api_key=args.fred_api_key,
        mode=args.mode,
        source=args.source
    )

if __name__ == '__main__':
    main()
