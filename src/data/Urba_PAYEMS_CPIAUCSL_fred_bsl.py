#!/usr/bin/env python
# coding: utf-8

"""
Script para baixar as séries históricas dos EUA: Payroll (PAYEMS) e CPI (CPIAUCSL) via FRED.

Funcionalidades:
----------------
- Baixa os dados mensais de PAYEMS (emprego não-agrícola) e CPIAUCSL (índice de preços ao consumidor) nos últimos 5 anos até a data atual.
- Salva os arquivos no diretório especificado, com nomes padronizados e datas no formato ISO (YYYY-MM-DD).
- Ordena os dados em ordem decrescente de datas (mais recente primeiro), conforme padrão do projeto.
- Imprime as primeiras linhas de cada arquivo após o salvamento para conferência humana.
- Documentação completa para facilitar entendimento, manutenção e integração ao pipeline de dados.

Como usar:
----------
1. Verifique se as bibliotecas `pandas` e `requests` estão instaladas.
2. Execute o script. Os arquivos serão salvos em:
   /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa
3. As datas dos arquivos estarão no formato ISO.
4. Consulte a saída do terminal para verificação das primeiras linhas.
"""

import os
import pandas as pd
from datetime import datetime
import requests

def download_fred_series(series_id: str, start_date: str, end_date: str, output_dir: str, file_prefix: str):
    """
    Baixa uma série histórica do FRED, salva no diretório desejado em formato ISO e ordem decrescente de datas,
    e imprime as primeiras linhas para controle.

    Parâmetros:
        series_id   (str): Código da série no FRED (ex: 'PAYEMS', 'CPIAUCSL')
        start_date  (str): Data inicial, formato 'YYYY-MM-DD'
        end_date    (str): Data final, formato 'YYYY-MM-DD'
        output_dir  (str): Diretório de saída
        file_prefix (str): Prefixo e nome do arquivo de saída (.csv será acrescentado)
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    url = (f'https://fred.stlouisfed.org/graph/fredgraph.csv'
           f'?id={series_id}&cosd={start_date}&coed={end_date}')
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Erro {response.status_code} ao baixar a série {series_id} do FRED.')

    filename = f"{file_prefix}_{start_date}_to_{end_date}_fred_bsl.csv"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'wb') as f:
        f.write(response.content)

    # Leitura, normalização e ordenação dos dados em ordem decrescente (ISO)
    df = pd.read_csv(filepath)
    date_col = [c for c in ['observation_date', 'DATE'] if c in df.columns][0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')  # normaliza ISO
    df = df.sort_values(by=date_col, ascending=False).reset_index(drop=True)
    df.to_csv(filepath, index=False)

    print(f"\nSérie '{series_id}' salva em: {filepath}")
    print("Primeiras linhas do arquivo de saída:")
    print(df.head())

if __name__ == '__main__':
    # Determina período padrão dos últimos 5 anos até hoje
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today().replace(year=datetime.today().year - 5)).strftime('%Y-%m-%d')
    output_dir = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa'

    # Baixa e processa o Payroll (PAYEMS)
    download_fred_series(
        series_id='PAYEMS',
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        file_prefix='Urba_PAYEMS'
    )

    # Baixa e processa o CPI (CPIAUCSL)
    download_fred_series(
        series_id='CPIAUCSL',
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir,
        file_prefix='Urba_CPIAUCSL'
    )
