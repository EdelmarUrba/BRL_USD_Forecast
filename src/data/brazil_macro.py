#!/usr/bin/env python3
"""
brazil_macro.py

Script para baixar e processar dados macroeconômicos brasileiros para integração ao seu pipeline de dados.

FUNCIONALIDADES:
- Se um arquivo de entrada for fornecido, processa e padroniza o arquivo.
- Se não for fornecido arquivo de entrada, busca automaticamente dados macroeconômicos em até 10 fontes confiáveis da internet, na ordem de prioridade.
- Permite ao usuário especificar o período de interesse (padrão: últimos 5 anos).
- Salva o arquivo de saída no diretório corrente se não for especificado outro caminho.
- Exibe mensagens claras sobre cada etapa, mostra as primeiras linhas dos dados de entrada e saída, e orienta sobre as fontes pesquisadas.
- Mensagem de ajuda detalhada via -h ou --help, incluindo orientações de busca e referências.

USO:
    python brazil_macro.py --input INPUT_FILE --output OUTPUT_FILE [--start_year YYYY --end_year YYYY]
    python brazil_macro.py --output OUTPUT_FILE [--start_year YYYY --end_year YYYY]
    python brazil_macro.py [--start_year YYYY --end_year YYYY]  # Salva no diretório corrente

OPÇÕES:
    --input      Caminho para o arquivo de entrada (CSV, XLSX, ODS etc.). Se omitido, buscará dados online.
    --output     Caminho para o arquivo de saída processado (CSV). Se omitido, salva como 'brazil_macro_processed.csv' no diretório corrente.
    --start_year Primeiro ano de interesse (padrão: últimos 5 anos).
    --end_year   Último ano de interesse (padrão: ano atual).
    -h, --help   Exibe esta mensagem de ajuda e orientações detalhadas.

EXEMPLO:
    python brazil_macro.py --input data/raw/brazil_macro.ods --output data/processed/brazil_macro_processed.csv --start_year 2020 --end_year 2024
    python brazil_macro.py --start_year 2020 --end_year 2024  # Salva no diretório corrente

ORIENTAÇÕES PARA BUSCA DE DADOS MACROECONÔMICOS BRASILEIROS PELA INTERNET:
--------------------------------------------------------------------------
Se não houver arquivo local, o script tentará baixar automaticamente dados macroeconômicos do Brasil em até 10 fontes públicas e confiáveis, na seguinte ordem de prioridade:

1. Banco Mundial (World Bank): https://data.worldbank.org/country/brazil
2. Banco Central do Brasil (BCB): https://www.bcb.gov.br/en/statistics/selectedindicators
3. IBGE: https://www.ibge.gov.br/en/statistics/economic/national-accounts/17173-system-of-national-accounts-brazil.html?=&t=o-que-e
4. Trading Economics: https://tradingeconomics.com/brazil/indicators
5. IpeaData: https://www.ipeadata.gov.br/
6. FMI: https://www.imf.org/en/Countries/BRA
7. Portal de Dados Abertos do BCB: https://opendata.bcb.gov.br/
8. Kaggle: https://www.kaggle.com/datasets
9. CEIC Data: https://www.ceicdata.com/en/country/brazil
10. Zenodo: https://zenodo.org/

Se a primeira fonte não estiver disponível ou o arquivo baixado estiver vazio, o script tentará automaticamente as próximas fontes até obter dados válidos ou esgotar as 10 opções. Ao final, será exibida a fonte utilizada para referência ABNT.

DICAS PARA USO E PERSONALIZAÇÃO:
- Para buscar dados de indicadores específicos (PIB, inflação, emprego, etc.), consulte os portais do IBGE, BCB ou Trading Economics e ajuste a função de download no script.
- Para uso em monografias, dissertações ou artigos, utilize a referência ABNT exibida ao final do processamento.
- Se desejar integrar APIs (ex: BacenAPI, IBGE API), consulte a documentação oficial das instituições.

EXEMPLO DE REFERÊNCIA ABNT (será exibida ao final do processamento):
WORLD BANK. Economy and growth: Brazil. Disponível em: <https://data.worldbank.org/country/brazil>. Acesso em: 14 jul. 2025.

Principais fontes pesquisadas:
- World Bank
- Banco Central do Brasil
- IBGE
- Trading Economics
- IpeaData
- FMI
- CEIC Data
- Zenodo
- Kaggle

Se precisar de suporte para adaptar o script a uma fonte específica ou integrar APIs, consulte a documentação das instituições ou solicite exemplos de código.
"""

import argparse
import pandas as pd
import os
import requests
from datetime import datetime

# Lista de fontes (URL, nome para referência ABNT)
SOURCES = [
    {
        "url": "https://data.humdata.org/dataset/caac33be-6e4d-4d2f-a284-068ef29256a4/resource/84c388a0-d4f6-4056-beb1-9398beac2456/download/economy-and-growth_bra.csv",
        "abnt": "WORLD BANK. Economy and growth: Brazil. Disponível em: <https://data.worldbank.org/country/brazil>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4380/dados?formato=csv",
        "abnt": "BANCO CENTRAL DO BRASIL. Indicadores econômicos selecionados. Disponível em: <https://www.bcb.gov.br/en/statistics/selectedindicators>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://apisidra.ibge.gov.br/values/t/1846/n1/all/v/37/p/all/c315/7169",
        "abnt": "IBGE. Contas nacionais. Disponível em: <https://www.ibge.gov.br/en/statistics/economic/national-accounts/17173-system-of-national-accounts-brazil.html>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://www.ipeadata.gov.br/ExibeSerie.aspx?serid=38596&module=M",
        "abnt": "IPEADATA. Séries históricas econômicas. Disponível em: <https://www.ipeadata.gov.br/>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://www.imf.org/external/pubs/ft/weo/2024/01/weodata/WEOApr2024all.xls",
        "abnt": "FMI. World Economic Outlook. Disponível em: <https://www.imf.org/en/Countries/BRA>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://tradingeconomics.com/brazil/indicators",
        "abnt": "TRADING ECONOMICS. Brazil Economic Indicators. Disponível em: <https://tradingeconomics.com/brazil/indicators>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://opendata.bcb.gov.br/dataset?q=&sort=views_recent+desc",
        "abnt": "BANCO CENTRAL DO BRASIL. Portal de Dados Abertos. Disponível em: <https://opendata.bcb.gov.br/>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://www.kaggle.com/datasets/brunhs/kernel-dataset-brazil",
        "abnt": "KAGGLE. Brazil Macroeconomic Dataset. Disponível em: <https://www.kaggle.com/datasets>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://www.ceicdata.com/en/country/brazil",
        "abnt": "CEIC DATA. Brazil Data. Disponível em: <https://www.ceicdata.com/en/country/brazil>. Acesso em: 14 jul. 2025."
    },
    {
        "url": "https://zenodo.org/records/14742585/files/brazil_macro.csv?download=1",
        "abnt": "ZENODO. Brazil macroeconomic data. Disponível em: <https://zenodo.org/records/14742585>. Acesso em: 14 jul. 2025."
    },
]

def print_step(msg):
    print(f"\n{'='*60}\n{msg}\n{'='*60}")

def read_input_file(input_path):
    ext = os.path.splitext(input_path)[-1].lower()
    print_step(f"Lendo arquivo de entrada: {input_path}")
    if ext == ".csv":
        df = pd.read_csv(input_path)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(input_path)
    elif ext == ".ods":
        df = pd.read_excel(input_path, engine='odf')
    else:
        raise ValueError(f"Formato de arquivo não suportado: {ext}")
    print("Primeiras linhas do arquivo de entrada:")
    print(df.head())
    return df

def try_download_macro_data():
    print_step("Nenhum arquivo de entrada fornecido. Buscando dados macroeconômicos em até 10 fontes públicas e confiáveis...")
    for idx, source in enumerate(SOURCES, 1):
        print(f"Tentando fonte {idx}: {source['url']}")
        local_file = f"brazil_macro_online_{idx}.csv"
        try:
            response = requests.get(source['url'], timeout=30)
            response.raise_for_status()
            with open(local_file, "wb") as f:
                f.write(response.content)
            # Tenta ler como CSV
            try:
                df = pd.read_csv(local_file)
                if not df.empty and len(df.columns) > 1:
                    print(f"Fonte {idx} obtida com sucesso: {source['url']}")
                    print("Primeiras linhas dos dados baixados:")
                    print(df.head())
                    return df, source['abnt']
                else:
                    print(f"Arquivo baixado da fonte {idx} está vazio ou ilegível. Tentando próxima fonte...")
            except Exception as e:
                print(f"Não foi possível ler o arquivo baixado da fonte {idx}: {e}")
        except Exception as e:
            print(f"Erro ao baixar da fonte {idx}: {e}")
    print("Não foi possível obter dados macroeconômicos automaticamente nas fontes pesquisadas.")
    print("Considere baixar manualmente de uma das fontes listadas na mensagem de ajuda.")
    return None, None

def filter_by_period(df, start_year, end_year):
    print_step(f"Filtrando dados para o período: {start_year} a {end_year}")
    date_col = None
    for col in df.columns:
        if 'year' in col.lower() or 'date' in col.lower() or 'ano' in col.lower():
            date_col = col
            break
    if date_col is None:
        print("Aviso: Nenhuma coluna de ano ou data encontrada. Dados não serão filtrados por período.")
        return df
    try:
        df[date_col] = df[date_col].astype(str).str[:4].astype(int)
        df = df[(df[date_col] >= start_year) & (df[date_col] <= end_year)]
    except Exception as e:
        print(f"Não foi possível filtrar por ano devido a: {e}")
    print("Primeiras linhas após filtragem por período:")
    print(df.head())
    return df

def process_macro_data(df):
    print_step("Processando dados macroeconômicos")
    df = df.dropna(how='all')
    df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
    return df

def save_output_file(df, output_path):
    print_step(f"Salvando arquivo processado em: {output_path}")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print("Primeiras linhas do arquivo de saída:")
    print(df.head())

def main():
    parser = argparse.ArgumentParser(
        description="Baixa e processa dados macroeconômicos brasileiros para integração ao pipeline de dados.\n\n"
                    "Se não houver arquivo de entrada, serão buscados dados automaticamente em até 10 fontes públicas e confiáveis.\n"
                    "Para mais orientações e fontes, utilize -h ou --help.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    current_year = datetime.now().year
    default_start = current_year - 5 + 1
    parser.add_argument('--input', type=str, default=None, help='Caminho do arquivo de entrada (CSV, XLSX, ODS, etc.). Se omitido, buscará dados online.')
    parser.add_argument('--output', type=str, default=None, help='Caminho para o arquivo de saída processado (CSV). Se omitido, salva como brazil_macro_processed.csv no diretório corrente.')
    parser.add_argument('--start_year', type=int, default=default_start, help='Primeiro ano de interesse (padrão: últimos 5 anos)')
    parser.add_argument('--end_year', type=int, default=current_year, help='Último ano de interesse (padrão: ano atual)')
    args = parser.parse_args()

    print_step("Iniciando processamento de dados macroeconômicos brasileiros")
    print("Para ajuda, execute: python brazil_macro.py -h\n")

    if args.input:
        df = read_input_file(args.input)
        abnt_ref = "Fonte: arquivo local fornecido pelo usuário."
    else:
        df, abnt_ref = try_download_macro_data()
        if df is None:
            print("\nNão foi possível obter dados automaticamente. Consulte as fontes listadas no help e baixe manualmente se necessário.")
            return

    df = filter_by_period(df, args.start_year, args.end_year)
    df_proc = process_macro_data(df)

    # Determina caminho de saída
    if args.output:
        output_path = args.output
    else:
        output_path = os.path.join(os.getcwd(), "brazil_macro_processed.csv")
        print(f"Nenhum arquivo de saída especificado. Salvando em: {output_path}")

    save_output_file(df_proc, output_path)

    print_step("Processamento concluído com sucesso!")
    print(f"Arquivo final disponível em: {output_path}\n")
    print("Se precisar de dados de fonte ou indicador específico, edite o script para apontar para a base desejada (ex: IBGE, BCB, TradingEconomics, FMI, IpeaData, etc.).")
    print("\nFonte dos dados para referência ABNT (inclua na monografia):")
    print(abnt_ref)

if __name__ == '__main__':
    main()
