#!/usr/bin/env python
# coding: utf-8

"""
Urba_macro_usa.py

Script para baixar e consolidar principais indicadores macroeconômicos dos EUA diretamente do FRED,
salvando em único arquivo CSV padronizado para integração à matriz de dados de previsão de volatilidade cambial.

Indicadores coletados:
- Payroll total (PAYEMS), mensal
- Índice de Preços ao Consumidor (CPIAUCSL), mensal
- PIB nominal (GDP), trimestral
- PIB real (GDPC1), trimestral
- Taxa de juros efetiva (FEDFUNDS), diária
- Taxa de desemprego (UNRATE), mensal
- Sentimento do consumidor (UMCSENT), mensal

Principais funcionalidades:
--------------------------
- Define o período a ser consultado (default: últimos 5 anos)
- Permite ao usuário especificar datas inicial e final
- Baixa automaticamente todas as séries do FRED
- Dados ordenados da data mais recente para a mais antiga
- Datas no formato ISO (YYYY-MM-DD)
- Salva arquivo "Urba_macro_usa.csv" em:
  /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa
- Imprime as primeiras linhas do arquivo final para controle

Execução:
---------
    python Urba_macro_usa.py

    Ou especificando datas:
    python Urba_macro_usa.py -start 2020-01-01 -end 2025-06-30

Ajuda:
------
    python Urba_macro_usa.py -h

Autor: Edelmar Urba
Data: 15/07/2025
"""

import os
import sys
import pandas as pd
import requests
from datetime import datetime
from io import StringIO

# Série FRED que serão baixadas
FRED_SERIES = {
    'PAYEMS':    {'nome': 'Employment (Nonfarm Payroll)',    'freq': 'M'},
    'CPIAUCSL':  {'nome': 'Consumer Price Index (CPI)',      'freq': 'M'},
    'GDP':       {'nome': 'Gross Domestic Product',          'freq': 'Q'},
    'GDPC1':     {'nome': 'Real Gross Domestic Product',     'freq': 'Q'},
    'FEDFUNDS':  {'nome': 'Federal Funds Rate',              'freq': 'D'},
    'UNRATE':    {'nome': 'Unemployment Rate',               'freq': 'M'},
    'UMCSENT':   {'nome': 'Consumer Sentiment',              'freq': 'M'},
}

# Diretório e nome do arquivo final
OUTPUT_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa"
OUTPUT_FILE = "Urba_macro_usa.csv"

def help_text():
    print("""
------------------------------------------------------------------------------
Script: Urba_macro_usa.py

Coleta e consolida automaticamente os principais indicadores macroeconômicos
dos EUA a partir do FRED para uso em modelagens econométricas e previsão de
volatilidade cambial com base em fatores macro.

Indicadores coletados:
    - PAYEMS     - Emprego não-agrícola (mensal)
    - CPIAUCSL   - Índice de Preços ao Consumidor (mensal)
    - GDP        - PIB nominal – US$ bilhões (trimestral)
    - GDPC1      - PIB real – US$ constantes (trimestral)
    - FEDFUNDS   - Taxa de juros Fed Funds efetiva (diária)
    - UNRATE     - Taxa de desemprego (mensal)
    - UMCSENT    - Sentimento do consumidor (mensal)

Parâmetros:
    -start YYYY-MM-DD : Data inicial da série (default: hoje menos 5 anos)
    -end   YYYY-MM-DD : Data final da série (default: hoje)
    -h or --help      : Exibir esta ajuda

Exemplo:
    python Urba_macro_usa.py -start 2020-01-01 -end 2025-07-01

Saída:
    Arquivo único / padronizado salvo em:
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa/Urba_macro_usa.csv

------------------------------------------------------------------------------
    """)
    sys.exit(0)

def get_dates_from_argv(argv):
    if '-h' in argv or '--help' in argv:
        help_text()
    today = datetime.today()
    start_date = (today.replace(year=today.year - 5)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    if '-start' in argv:
        try:
            start_date = argv[argv.index('-start') + 1]
        except:
            print("Erro: use -start YYYY-MM-DD"); sys.exit(1)
    if '-end' in argv:
        try:
            end_date = argv[argv.index('-end') + 1]
        except:
            print("Erro: use -end YYYY-MM-DD"); sys.exit(1)
    return start_date, end_date

def download_fred(series_id, start_date, end_date):
    print(f"  ↳ Baixando {series_id} - {FRED_SERIES[series_id]['nome']}")
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start_date}&coed={end_date}"
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f'Erro {resp.status_code} ao baixar "{series_id}" do FRED.')
    df = pd.read_csv(StringIO(resp.content.decode('utf-8')))
    date_col = [c for c in ['observation_date', 'DATE'] if c in df.columns][0]
    df.rename(columns={date_col: 'date', df.columns[1]: series_id}, inplace=True)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    return df[['date', series_id]]

def main():
    # Obtem datas
    start_date, end_date = get_dates_from_argv(sys.argv)
    print(f"\nIniciando coleta FRED de {start_date} até {end_date}\n")

    # Garante diretório de saída
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Baixa e mescla as séries
    df_final = None
    for series_id in FRED_SERIES.keys():
        df = download_fred(series_id, start_date, end_date)
        if df_final is None:
            df_final = df
        else:
            df_final = pd.merge(df_final, df, how='outer', on='date')

    # Ordena datas (mais recente primeiro)
    df_final['date'] = pd.to_datetime(df_final['date'])
    df_final = df_final.sort_values(by='date', ascending=False).reset_index(drop=True)
    df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d')

    # Salva arquivo
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df_final.to_csv(output_path, index=False)
    print(f"\n✔ Arquivo salvo com sucesso em: {output_path}\n")
    print("Primeiras linhas do arquivo consolidado:")
    print(df_final.head())

if __name__ == "__main__":
    main()
