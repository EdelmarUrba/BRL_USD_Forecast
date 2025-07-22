#!/usr/bin/env python
# coding: utf-8

"""
Script: padroniza_adp_emprego.py

Descrição:
-----------
Padroniza o arquivo ADP 'us-private-employment.csv' para integração ao pipeline macroeconômico do projeto.
- Lê o arquivo de entrada diretamente em '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa'
- Salva a saída padronizada no mesmo diretório, com nome 'us-private-employment_padronizado.csv'
- Imprime as primeiras linhas dos arquivos para verificação

Utilização:
-----------
    python padroniza_adp_emprego.py
    python padroniza_adp_emprego.py --input us-private-employment.csv --output us-private-employment_padronizado.csv
    python padroniza_adp_emprego.py -h
"""

import pandas as pd
import argparse
import sys
import os

def show_help():
    print(__doc__)
    sys.exit(0)

def padroniza_adp(input_path, output_path):
    abs_input = os.path.abspath(input_path)
    abs_output = os.path.abspath(output_path)
    print(f"\nArquivo de entrada localizado em: {abs_input}")
    df = pd.read_csv(abs_input, sep=';')
    print("\nPRIMEIRAS LINHAS DO ARQUIVO DE ENTRADA:")
    print(df.head(), "\n")
    df.rename(columns={'DateTime': 'date', 'Private Employment': 'ADP_Employment_Change'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
    df.to_csv(abs_output, index=False)
    print(f"Arquivo de saída gerado e salvo em: {abs_output}")
    print("\nPRIMEIRAS LINHAS DO ARQUIVO PADRONIZADO:")
    print(df.head(), "\n")

def main():
    parser = argparse.ArgumentParser(add_help=False)
    default_dir = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa'
    parser.add_argument('--input', type=str, default=os.path.join(default_dir, 'us-private-employment.csv'), help="Arquivo de entrada")
    parser.add_argument('--output', type=str, default=os.path.join(default_dir, 'us-private-employment_padronizado.csv'), help="Arquivo de saída")
    parser.add_argument('-h', '--help', action='store_true', help='Exibe esta ajuda')
    args = parser.parse_args()

    if args.help:
        show_help()
    if not os.path.exists(args.input):
        print(f"Arquivo de entrada não encontrado: {os.path.abspath(args.input)}")
        sys.exit(1)
    padroniza_adp(args.input, args.output)

if __name__ == "__main__":
    main()
