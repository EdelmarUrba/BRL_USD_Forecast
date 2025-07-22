#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_comparador_de_arquivos_jsonl.py

Descrição:
-----------
Compara dois arquivos JSONL padronizados de notícias (From News to Forecast) quanto a estrutura de campos, número de registros e conteúdos.
Gera um relatório de diferenças: campos exclusivos, títulos/links únicos em cada arquivo, totais, e amostras de divergências.
Salva o relatório em txt no mesmo diretório dos arquivos analisados.

Modo de uso:
  python Urba_comparador_de_arquivos_jsonl.py

Parâmetros:
  --file1: nome do primeiro arquivo (default: Urba_fred_blog_news_standard.jsonl)
  --file2: nome do segundo arquivo (default: Urba_fred_blog_news_standard_versao_1.jsonl)
  --out  : nome do arquivo txt de resultado (default: Urba_relatorio_comparacao_news_jsonl.txt)

Autor: Edelmar Urba
Data: 2025-07-17

Referências:
- DUNIS, C. L. et al. News will tell: Forecasting foreign exchange rates based on news story events in the economy calendar. JIFMIM, v. 65, 2020.
"""

import os
import sys
import json
import argparse
from collections import Counter

# Defaults
DIR = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/src/data/processed'
DEFAULT_FILE1 = os.path.join(DIR, 'Urba_fred_blog_news_standard.jsonl')
DEFAULT_FILE2 = os.path.join(DIR, 'Urba_fred_blog_news_standard_versao_1.jsonl')
DEFAULT_OUT = os.path.join(DIR, 'Urba_relatorio_comparacao_news_jsonl.txt')

def read_jsonl(path, key='title'):
    dados = []
    chaves = set()
    keys_counter = Counter()
    titulos = set()
    links = set()
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                item = json.loads(line)
                dados.append(item)
                chaves |= set(item.keys())
                keys_counter.update(item.keys())
                titulos.add(str(item.get('title', '')).strip())
                links.add(str(item.get('link', '')).strip())
            except Exception as e:
                print(f"[AVISO] Erro parsing linha em {path}: {e}")
    return dados, chaves, titulos, links, keys_counter

def gen_relatorio(file1, file2, out_path):
    # Lê os arquivos
    d1, c1, t1, l1, ctr1 = read_jsonl(file1)
    d2, c2, t2, l2, ctr2 = read_jsonl(file2)

    linhas = []
    linhas.append(f"Relatório de Comparação entre JSONLs de Notícias ({os.path.basename(file1)} vs {os.path.basename(file2)})\n")
    linhas.append("="*72)
    linhas.append(f"[1] {os.path.basename(file1)} - Registros: {len(d1)} - Campos únicos: {sorted(c1)}")
    linhas.append(f"[2] {os.path.basename(file2)} - Registros: {len(d2)} - Campos únicos: {sorted(c2)}")

    # Campos exclusivos/de interseção
    only1 = sorted(c1 - c2)
    only2 = sorted(c2 - c1)
    inters = sorted(c1 & c2)
    linhas.append(f"\nCampos SOMENTE em [1]: {only1 if only1 else '(nenhum)'}")
    linhas.append(f"Campos SOMENTE em [2]: {only2 if only2 else '(nenhum)'}")
    linhas.append(f"Campos em COMUM:        {inters}")
    linhas.append(f"\nOcorrência dos campos em [1]: {dict(ctr1)}")
    linhas.append(f"Ocorrência dos campos em [2]: {dict(ctr2)}")

    # Títulos e links exclusivos/de interseção
    tit_comum = t1 & t2
    tit_so_1 = t1 - t2
    tit_so_2 = t2 - t1
    linhas.append(f"\nTÍTULOS em comum: {len(tit_comum)}")
    linhas.append(f"TÍTULOS só em [1]: {len(tit_so_1)} exemplos: {list(tit_so_1)[:3]}")
    linhas.append(f"TÍTULOS só em [2]: {len(tit_so_2)} exemplos: {list(tit_so_2)[:3]}")
    link_comum = l1 & l2
    link_so_1 = l1 - l2
    link_so_2 = l2 - l1
    linhas.append(f"\nLINKS em comum: {len(link_comum)}")
    linhas.append(f"LINKS só em [1]: {len(link_so_1)} exemplos: {list(link_so_1)[:3]}")
    linhas.append(f"LINKS só em [2]: {len(link_so_2)} exemplos: {list(link_so_2)[:3]}")

    # Exemplos reais de divergência estrutural (primeiro diferente)
    exemplos1 = [d for d in d1 if str(d.get("title","")).strip() in tit_so_1]
    exemplos2 = [d for d in d2 if str(d.get("title","")).strip() in tit_so_2]
    if exemplos1:
        linhas.append("\nExemplo de notícia SÓ em [1]:\n" + json.dumps(exemplos1[0], indent=2, ensure_ascii=False))
    if exemplos2:
        linhas.append("\nExemplo de notícia SÓ em [2]:\n" + json.dumps(exemplos2[0], indent=2, ensure_ascii=False))

    # Amostra de diferenças de campos em registros de mesmo título
    exemplos_diff = []
    for d in d1:
        t = str(d.get('title','')).strip()
        for other in d2:
            if str(other.get('title','')).strip() == t and d != other:
                exemplos_diff.append((d, other))
                break
        if len(exemplos_diff) >= 2:
            break
    if exemplos_diff:
        linhas.append('\nExemplo de registro com mesmo título mas conteúdo diferente:')
        for a, b in exemplos_diff:
            linhas.append("No arquivo 1:")
            linhas.append(json.dumps(a, indent=2, ensure_ascii=False))
            linhas.append("No arquivo 2:")
            linhas.append(json.dumps(b, indent=2, ensure_ascii=False))
            linhas.append('-'*50)

    # Dicionário
    linhas.append("\n\nDicionário esperado de campos padrão:")
    linhas.append(json.dumps({
        "title": "Título da notícia",
        "category": "Categoria (ex: FRED Blog)",
        "summary": "Resumo ou lead (pode vir vazio)",
        "link": "URL completa",
        "publication_time": "Data e hora ISO",
        "full_article": "Conteúdo textual completo"
    }, indent=2, ensure_ascii=False))
    linhas.append("\nReferências principais:\n"
                  "- DUNIS, C. L. et al. News will tell: Forecasting foreign exchange rates... JIFMIM, 65, 2020.\n"
                  "- https://fredblog.stlouisfed.org/\n")
    # Salva
    with open(out_path, 'w', encoding='utf-8') as fout:
        fout.write('\n'.join(linhas))

    print(f"\nRelatório completo salvo em: {out_path}\n")
    print("--- Resumo ---")
    print('\n'.join(linhas[:18]))
    print(f"\nVerifique {out_path} para detalhes completos e exemplos reais.")

def main():
    parser = argparse.ArgumentParser(description="Compara estruturalmente dois arquivos JSONL de notícias e gera relatório txt.")
    parser.add_argument('--file1', '-f1', type=str, default=DEFAULT_FILE1, help="Primeiro arquivo JSONL (padrão: news_standard).")
    parser.add_argument('--file2', '-f2', type=str, default=DEFAULT_FILE2, help="Segundo arquivo JSONL (padrão: news_standard_versao_1).")
    parser.add_argument('--out', '-o', type=str, default=DEFAULT_OUT, help="Arquivo txt de saída.")
    args = parser.parse_args()

    gen_relatorio(args.file1, args.file2, args.out)

if __name__ == "__main__":
    main()
