#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_csv_to_news_standard_jsonl.py

Descrição:
-----------
Transfere um arquivo CSV de notícias (do FRED Blog, mas adaptável) para o padrão rigoroso de artigo usado no paper 'From News to Forecast':
campos = title, category, summary, link, publication_time, full_article.
Preserva toda informação original (não descarta campos extras!), apenas reorganiza.
Permite customizar input/output por linha de comando. Prints para controle humano.

Uso:
  python Urba_csv_to_news_standard_jsonl.py
  python Urba_csv_to_news_standard_jsonl.py --input /caminho/entrada.csv --output /caminho/saída.jsonl

Autor: Edelmar Urba
Data: 2025-07-17

Referências:
- DUNIS, C. L. et al. News will tell: Forecasting foreign exchange rates based on news story events in the economy calendar.
  Journal of International Financial Markets, Institutions and Money, 65, 2020.
- https://fredblog.stlouisfed.org/
"""

import pandas as pd
import json
import argparse
import os
from datetime import datetime
import sys
import textwrap

DEFAULT_INPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news/Urba_fred_blog_raw.csv'
DEFAULT_OUTPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_news_data/Urba_fred_blog_standard.jsonl'

HELP_TEXT = f"""
Este script transforma um CSV de notícias do FRED Blog para o padrão internacional usado no artigo From News to Forecast.

Campos gerados (por linha JSONL):
- title              : Título da notícia
- category           : Categoria (padrao: 'FRED Blog')
- summary            : Resumo breve (se houver)
- link               : Link para o artigo original
- publication_time   : Data/hora completa no padrão ISO (YYYY-MM-DDTHH:MM:SS)
- full_article       : Corpo/texto completo da notícia

Exemplo de uso:
    python Urba_csv_to_news_standard_jsonl.py
    python Urba_csv_to_news_standard_jsonl.py --input /path/in.csv --output /path/out.jsonl

Parâmetros restantes são os defaults deste projeto:

  Entrada:   {DEFAULT_INPUT}
  Saída:     {DEFAULT_OUTPUT}

Referências científicas e exemplos completos no script.
"""

def iso_dt(dtstr):
    """Normaliza datas para ISO completo (YYYY-MM-DDTHH:MM:SS)"""
    if not dtstr or pd.isna(dtstr): return ''
    s = str(dtstr).replace('/', '-').replace(' ', 'T')
    try:
        # Se já está em formato ISO "YYYY-MM-DDTHH:MM:SS"
        if "T" in s and len(s) >= 19:
            dt = datetime.fromisoformat(s[:19])
        elif len(s) == 10:  # só data
            dt = datetime.strptime(s, '%Y-%m-%d')
        else:
            dt = pd.to_datetime(s)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return s[:19].replace(' ', 'T')

def print_examples(orig_dict, out_dict):
    print("\nExemplo original (primeira linha):")
    print(json.dumps(orig_dict, indent=2, ensure_ascii=False))
    print("\nExemplo padronizado para o artigo:")
    print(json.dumps(out_dict, indent=2, ensure_ascii=False))

def main():
    parser = argparse.ArgumentParser(description="Padroniza CSV de notícias para o padrão do artigo From News to Forecast.")
    parser.add_argument('--input', '-i', type=str, default=DEFAULT_INPUT, help="Arquivo CSV de entrada.")
    parser.add_argument('--output', '-o', type=str, default=DEFAULT_OUTPUT, help="Arquivo JSONL de saída padronizada.")
    parser.add_argument('--helpfull', action='store_true', help="Mostra explicação detalhada e dicionário de campos.")
    parser.add_argument('--category', '-c', default='FRED Blog', help="Categoria default se ausente.")
    args = parser.parse_args()

    if args.helpfull:
        print(textwrap.dedent(HELP_TEXT))
        sys.exit(0)

    entrada_csv = args.input
    saida_jsonl = args.output
    categoria = args.category

    print(f"\nConvertendo arquivo CSV para padrão article (From News to Forecast):")
    print(f"  Entrada: {entrada_csv}")
    print(f"  Saída:   {saida_jsonl}")
    print(f"  Categoria default: '{categoria}'\n")

    # Leitura
    df = pd.read_csv(entrada_csv)
    if 'publication_time' not in df.columns and 'date' in df.columns:
        df['publication_time'] = df['date']

    n_rows = len(df)
    print(f"Lendo {n_rows} linhas do arquivo de entrada.")

    artigos = []
    for i, row in df.iterrows():
        obj = dict(row)
        out = {
            "title":      str(obj.get('title', '')),
            "category":   str(obj.get('category', categoria)),
            "summary":    str(obj.get('summary', '')),
            "link":       str(obj.get('link', '')),
            "publication_time": iso_dt(obj.get('publication_time', obj.get('date', ''))),
            "full_article": str(obj.get('full_article') or obj.get('content', '')),
        }
        # Preserva campos extras
        for k in obj:
            if k not in out and pd.notnull(obj[k]):
                out[k] = obj[k]
        artigos.append(out)
        # Guarda exemplos para print
        if i == 0: exemplo_orig, exemplo_out = obj, out

    print_examples(exemplo_orig, exemplo_out)

    print(f"\nSalvando {len(artigos)} registros no padrão JSONL em:")
    print(f"  {saida_jsonl}")
    os.makedirs(os.path.dirname(saida_jsonl), exist_ok=True)
    with open(saida_jsonl, 'w', encoding='utf-8') as f:
        for art in artigos:
            f.write(json.dumps(art, ensure_ascii=False) + "\n")

    print("\nTransformação finalizada com sucesso!")
    print("\nRecomenda-se conferir as primeiras linhas do arquivo gerado e documentar esta etapa no diário de bordo.")

if __name__ == "__main__":
    main()
