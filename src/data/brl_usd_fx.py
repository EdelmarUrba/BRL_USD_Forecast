#!/usr/bin/env python3
"""
brl_usd_fx.py

Script para coletar e integrar dados diários da taxa de câmbio USD/BRL (FRED: DEXBZUS) e indicadores macroeconômicos do Brasil e dos EUA,
produzindo um arquivo de saída compatível com o formato utilizado no artigo From News to Forecast.

Colunas geradas:
- Time: data no formato YYYY-MM-DD (business days)
- BRL/USD: cotação USD/BRL (FRED DEXBZUS)
- BRGDP: PIB real do Brasil (trimestral, milhões BRL, FRED NGDPRSAXDCBRQ)
- BRUN: taxa de desemprego juvenil Brasil (anual, FRED SLUEM1524ZSBRA, % população jovem 15-24 anos)
- BRIR: taxa de juros interbancária Brasil (mensal, FRED IRSTCI01BRM156N, %)
- USAGDP: PIB dos EUA (quarterly, FRED GDP, USD)
- USAUN: taxa de desemprego EUA (monthly, FRED UNRATE, %)
- USAIR: fed funds rate EUA (monthly, FRED FEDFUNDS, %)

**Alerta importante:** A coluna BRUN representa a taxa de desemprego da população jovem (15-24 anos). Futuramente, poderemos integrar dados da taxa de desemprego total do Brasil quando disponíveis.

O arquivo é salvo no diretório corrente e também em
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/brl_usd

Uso:
    python brl_usd_fx.py [--fred_api_key SUA_CHAVE_FRED]
        [--start_date AAAA-MM-DD] [--end_date AAAA-MM-DD] [--output_file nome.csv]
    Para mais informações, execute:
    python brl_usd_fx.py --help
"""

import argparse
import logging
import sys
import os
import pandas as pd
from datetime import datetime, date

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
    fx = fred.get_series('DEXBZUS', observation_start=start_date, observation_end=end_date)
    fx.index = pd.to_datetime(fx.index)
    df_fx = pd.DataFrame({'Date': fx.index, 'BRL/USD': fx.values})
    df_fx = df_fx.dropna()
    df_fx['Time'] = df_fx['Date'].dt.strftime('%Y-%m-%d')
    logging.info(f"Primeiras linhas da série FRED USD/BRL:\n{df_fx[['Time', 'BRL/USD']].head()}")
    return df_fx[['Time', 'BRL/USD']].drop_duplicates('Time')

def get_brazil_macro(start_date, end_date, fred_api_key):
    from fredapi import Fred
    fred = Fred(api_key=fred_api_key)
    # PIB real trimestral em milhões BRL (NGDPRSAXDCBRQ)
    gdp = fred.get_series('NGDPRSAXDCBRQ', observation_start=start_date, observation_end=end_date)
    gdp.index = pd.to_datetime(gdp.index)
    gdp = gdp.resample('QE').ffill()
    gdp_df = pd.DataFrame({'Data': gdp.index, 'BRGDP': gdp.values})
    # Taxa de desemprego jovem anual (%) (SLUEM1524ZSBRA)
    unemp = fred.get_series('SLUEM1524ZSBRA', observation_start=start_date, observation_end=end_date)
    unemp.index = pd.to_datetime(unemp.index)
    unemp = unemp.resample('A').ffill()  # anual, preenchendo para cada ano
    unemp_df = pd.DataFrame({'Data': unemp.index, 'BRUN': unemp.values})
    # Taxa de juros interbancária (<24h), mensal (IRSTCI01BRM156N)
    selic = fred.get_series('IRSTCI01BRM156N', observation_start=start_date, observation_end=end_date)
    selic.index = pd.to_datetime(selic.index)
    selic = selic.resample('ME').ffill()
    selic_df = pd.DataFrame({'Data': selic.index, 'BRIR': selic.values})
    logging.info(f"Primeiras linhas PIB Brasil:\n{gdp_df.head()}")
    logging.info(f"Primeiras linhas desemprego jovem Brasil (15-24 anos):\n{unemp_df.head()}")
    logging.info(f"Primeiras linhas Selic Brasil:\n{selic_df.head()}")
    return gdp_df, unemp_df, selic_df

def get_usa_macro(start_date, end_date, fred_api_key):
    from fredapi import Fred
    fred = Fred(api_key=fred_api_key)
    # US GDP (quarterly)
    gdp = fred.get_series('GDP', observation_start=start_date, observation_end=end_date)
    gdp.index = pd.to_datetime(gdp.index)
    gdp = gdp.resample('QE').ffill()
    gdp_df = pd.DataFrame({'Data': gdp.index, 'USAGDP': gdp.values})
    # US Unemployment (monthly)
    unemp = fred.get_series('UNRATE', observation_start=start_date, observation_end=end_date)
    unemp.index = pd.to_datetime(unemp.index)
    unemp = unemp.resample('ME').ffill()
    unemp_df = pd.DataFrame({'Data': unemp.index, 'USAUN': unemp.values})
    # US Fed Funds Rate (monthly)
    ir = fred.get_series('FEDFUNDS', observation_start=start_date, observation_end=end_date)
    ir.index = pd.to_datetime(ir.index)
    ir = ir.resample('ME').ffill()
    ir_df = pd.DataFrame({'Data': ir.index, 'USAIR': ir.values})
    logging.info(f"Primeiras linhas PIB EUA:\n{gdp_df.head()}")
    return gdp_df, unemp_df, ir_df

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

    br_gdp_df, br_unemp_df, br_ir_df = get_brazil_macro(start_date, end_date, fred_api_key)
    us_gdp_df, us_unemp_df, us_ir_df = get_usa_macro(start_date, end_date, fred_api_key)

    df = pd.merge(df_dates, df_fx, on='Time', how='left')
    df['BRGDP'] = fill_daily(df, br_gdp_df, 'BRGDP')
    df['BRUN'] = fill_daily(df, br_unemp_df, 'BRUN')
    df['BRIR'] = fill_daily(df, br_ir_df, 'BRIR')
    df['USAGDP'] = fill_daily(df, us_gdp_df, 'USAGDP')
    df['USAUN'] = fill_daily(df, us_unemp_df, 'USAUN')
    df['USAIR'] = fill_daily(df, us_ir_df, 'USAIR')

    df = df[['Time', 'BRL/USD', 'BRGDP', 'BRUN', 'BRIR', 'USAGDP', 'USAUN', 'USAIR']]
    save_dual(df, os.path.basename(output_file))
    logging.info(f"Primeiras linhas do arquivo salvo:\n{df.head()}")
    print("\nAlerta: A coluna 'BRUN' representa a taxa de desemprego da população jovem (15-24 anos).")
    print("Futuramente poderemos integrar dados da taxa de desemprego total do Brasil quando disponíveis.\n")
    print("Fonte dos dados para referência ABNT (inclua na monografia):")
    print("FEDERAL RESERVE BANK OF ST. LOUIS. U.S. Dollar to Brazilian Real Spot Exchange Rate (DEXBZUS) and macroeconomic indicators. Available at: <https://fred.stlouisfed.org/>. Accessed on: 14 Jul. 2025.")
    return df

def main():
    parser = argparse.ArgumentParser(
        description='''
        Script para coletar e integrar dados da taxa de câmbio USD/BRL (FRED St. Louis, DEXBZUS)
        e indicadores macroeconômicos do Brasil e EUA, gerando arquivo compatível com o artigo From News to Forecast.
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
