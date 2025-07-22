#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_merge_macro_usa_with_adp.py

Descrição:
-----------
Integra o indicador ADP Employment ("us-private-employment_padronizado.csv")
ao dataset macroeconômico consolidado dos EUA ("Urba_macro_usa_monthly_consolidated.csv"),
usando a data ISO (YYYY-MM-01) como chave.
Gera o arquivo final "Urba_macro_usa_monthly_consolidated_adp.csv".

Modo de uso:
    python Urba_merge_macro_usa_with_adp.py

Opções:
    -h, --help      Exibe orientação e o dicionário de colunas do arquivo final.

Autor: Edelmar Urba
Data: 2025-07-16

"""

import pandas as pd
import os
import sys
import textwrap

# === CONFIGURAÇÕES ===
DIR_INPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/macro_usa'
ARQ_MACRO = 'Urba_macro_usa_monthly_consolidated.csv'
ARQ_ADP   = 'us-private-employment_padronizado.csv'
ARQ_SAIDA = 'Urba_macro_usa_monthly_consolidated_adp.csv'

CAMINHO_MACRO = os.path.join(DIR_INPUT, ARQ_MACRO)
CAMINHO_ADP   = os.path.join(DIR_INPUT, ARQ_ADP)
CAMINHO_SAIDA = os.path.join(DIR_INPUT, ARQ_SAIDA)

DICT_COLUNAS = """
Dicionário de Colunas:

- date: Data no padrão ISO (YYYY-MM-01)
- PAYEMS: Total de empregos não-agrícolas (BLS)
- CPIAUCSL: Índice de Preços ao Consumidor (CPI-U, mensal)
- GDP: Produto Interno Bruto nominal (trimestral; NaN nos outros meses)
- GDPC1: Produto Interno Bruto real (trimestral; NaN nos outros meses)
- FEDFUNDS: Federal Funds Rate (taxa de juros básica dos EUA)
- UNRATE: Taxa de desemprego (BLS)
- UMCSENT: Índice de sentimento do consumidor (Univ. Michigan)
- ADP_Employment_Change: Total de empregos privados estimado pela ADP (mensal, divulgado cerca de 2 dias antes do payroll)
"""

def print_help():
    print("\n"+"="*60)
    print("SCRIPT DE INTEGRAÇÃO DE INDICADORES MACROECONÔMICOS USA + ADP")
    print("="*60)
    print("\nFinalidade:")
    print(textwrap.fill(
        "Integra o indicador de empregos privados da ADP ao dataset mensal consolidado dos principais indicadores macroeconômicos dos EUA. "
        "A fusão (merge) é feita via campo de data ISO (YYYY-MM-01). O script gera um novo arquivo CSV consolidado, facilitando análises de regressão, séries temporais ou machine learning preditivo.",
        width=80))
    print("\nModo de uso:")
    print("    python Urba_merge_macro_usa_with_adp.py\n")
    print(DICT_COLUNAS)
    print("Arquivos esperados no diretório:\n"
          f"  Entrada 1: {CAMINHO_MACRO}\n"
          f"  Entrada 2: {CAMINHO_ADP}\n"
          f"  Saída   : {CAMINHO_SAIDA}\n")
    print("Saídas adicionais: o script imprime ao final as primeiras linhas dos arquivos envolvidos para conferência humana.\n")
    exit(0)

def main():
    # Ajuda customizada
    if any(a in ['-h', '--help'] for a in sys.argv):
        print_help()

    # ========== LEITURA ==========
    print(f"\nLendo arquivo macroeconômico consolidado:\n  {CAMINHO_MACRO}")
    df_macro = pd.read_csv(CAMINHO_MACRO, parse_dates=['date'])
    print("\nPRIMEIRAS LINHAS DO ARQUIVO MACROECONÔMICO:")
    print(df_macro.head(7).to_string(index=False))
    
    print(f"\nLendo ADP Employment (privado):\n  {CAMINHO_ADP}")
    df_adp = pd.read_csv(CAMINHO_ADP, parse_dates=['date'])
    print("\nPRIMEIRAS LINHAS DO ARQUIVO ADP:")
    print(df_adp.head(7).to_string(index=False))

    # ========== FUSÃO ==========
    print("\nRealizando merge pela coluna 'date' (ISO, YYYY-MM-01)...")
    df_merged = pd.merge(df_macro, df_adp, on='date', how='left')

    # Ordena por data decrescente para fácil leitura
    df_merged = df_merged.sort_values('date', ascending=False)

    # ========== SALVAR ==========
    df_merged.to_csv(CAMINHO_SAIDA, index=False)
    print(f"\nArquivo final salvo em:\n  {CAMINHO_SAIDA}")

    print("\nDicionário de colunas disponível via --help.\n")
    print("PRIMEIRAS LINHAS DO ARQUIVO DE SAÍDA:")
    print(df_merged.head(7).to_string(index=False))

    print("\nProcesso finalizado com sucesso!")

if __name__ == '__main__':
    main()
