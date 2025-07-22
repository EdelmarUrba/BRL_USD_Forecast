#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_compara_jsonl_news_struct.py

Descrição:
-----------
Compara dois arquivos de notícias em formato JSONL (linha a linha, cada linha um dicionário) – por exemplo, o arquivo antigo já existente e o novo padronizado para o projeto BRL_USD.
Relata:
- Campos presentes em cada arquivo (dicionário/estrutura);
- Diferenças de chaves;
- Número de registros;
- Duplicatas (opcional; pelo campo 'link' ou 'title');
- Amostras de divergências;
- Sintaxe/ajuda amigável e documentação científica.

Uso:
  python Urba_compara_jsonl_news_struct.py --input1 arquivo1.jsonl --input2 arquivo2.jsonl

- Se não passar parâmetros, usa os nomes recomendados do pipeline do projeto.

Autor: Edelmar Urba  
Data: 2025-07-17

Referências:
- DUNIS, C. L. et al. News will tell: Forecasting foreign exchange rates based on news story events in the economy calendar.
  Journal of International Financial Markets, Institutions and Money, 65, 2020.
- https://fredblog.stlouisfed.org/
"""
import argparse
import json
import os
import sys
import textwrap

DEFAULT_FILE1 = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news/Urba_fred_blog_raw.jsonl'
DEFAULT_FILE2 = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/news/Urba_fred_blog_standard.jsonl'

HELP_TEXT = f"""
Script de comparação rigorosa de dois JSONLs de notícias para análise de padronização de estrutura e conteúdo.

Parâmetros:
  --input1 / -i1 : Primeiro arquivo (ex: antigo, source)
  --input2 / -i2 : Segundo arquivo (ex: novo padronizado, target)

Exemplo de uso:
    python Urba_compara_jsonl_news_struct.py \\
        --input1 {DEFAULT_FILE1} --input2 {DEFAULT_FILE2}

O script reporta: número de registros, campos presentes, campos exclusivos, diferença de dicionário, registros duplicados e exemplos.

Referências e fundamentos científicos no cabeçalho do script.
"""

def read_jsonl(file_path, max_lines=None):
    """Lê um arquivo JSONL linha a linha. Retorna lista de dicts e lista de conjuntos de chaves por registro."""
    linhas = []
    chaves = set()
    titulos = set()
    links = set()
    n = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        for l in f:
            try:
                item = json.loads(l)
                linhas.append(item)
                chaves |= set(item.keys())
                titulos.add(item.get('title', '').strip())
                links.add(item.get('link', '').strip())
            except Exception as e:
                print(f"⚠️ Erro de parsing na linha {n+1} de {file_path}: {e}")
            n += 1
            if max_lines and n >= max_lines:
                break
    return linhas, chaves, titulos, links

def compara_dicts(keys1, keys2, label1, label2):
    only1 = keys1 - keys2
    only2 = keys2 - keys1
    both = keys1 & keys2
    print(f"\nCampos encontrados apenas em '{label1}': {sorted(only1)}")
    print(f"Campos encontrados apenas em '{label2}': {sorted(only2)}")
    print(f"Campos em comum ({len(both)}): {sorted(both)}")

def compara_jsonl_arquivos(arquivo1, arquivo2):
    print("\nLendo arquivos:")
    print("  1:", arquivo1)
    print("  2:", arquivo2)
    dados1, chaves1, titulos1, links1 = read_jsonl(arquivo1)
    dados2, chaves2, titulos2, links2 = read_jsonl(arquivo2)
    print(f"\nRegistros em '{arquivo1}': {len(dados1)}")
    print(f"Registros em '{arquivo2}': {len(dados2)}")

    print(f"\nCampos detectados no arquivo 1: {sorted(chaves1)}")
    print(f"Campos detectados no arquivo 2: {sorted(chaves2)}")
    compara_dicts(chaves1, chaves2, "arquivo1", "arquivo2")

    print("\nVerificando duplicatas pelo campo 'title'...")
    dups1 = len(titulos1) < len(dados1)
    dups2 = len(titulos2) < len(dados2)
    print(f"  Duplicatas no arquivo 1: {'SIM' if dups1 else 'NÃO'}")
    print(f"  Duplicatas no arquivo 2: {'SIM' if dups2 else 'NÃO'}")
    print("\nVerificando duplicatas pelo campo 'link'...")
    dlinks1 = len(links1) < len(dados1)
    dlinks2 = len(links2) < len(dados2)
    print(f"  Duplicatas de link no arquivo 1: {'SIM' if dlinks1 else 'NÃO'}")
    print(f"  Duplicatas de link no arquivo 2: {'SIM' if dlinks2 else 'NÃO'}")

    # Exemplos de divergências de campos
    print("\nExemplo de registro do arquivo 1:")
    print(json.dumps(dados1[0], indent=2, ensure_ascii=False) if dados1 else "  [VAZIO]")
    print("\nExemplo de registro do arquivo 2:")
    print(json.dumps(dados2[0], indent=2, ensure_ascii=False) if dados2 else "  [VAZIO]")

    # Exemplo de notícia só em um dos arquivos:
    titulos2_set = set(titulos2)
    exclusivos1 = [d for d in dados1 if d['title'].strip() not in titulos2_set]
    if exclusivos1:
        print("\nExemplo de notícia presente SÓ em arquivo 1:")
        print(json.dumps(exclusivos1[0], indent=2, ensure_ascii=False))
    titulos1_set = set(titulos1)
    exclusivos2 = [d for d in dados2 if d['title'].strip() not in titulos1_set]
    if exclusivos2:
        print("\nExemplo de notícia presente SÓ em arquivo 2:")
        print(json.dumps(exclusivos2[0], indent=2, ensure_ascii=False))

    print("\nResumo:")
    print(f"  - {len(dados1)} registros em '{arquivo1}'.")
    print(f"  - {len(dados2)} registros em '{arquivo2}'.")
    print(f"  - {len(chaves1)} campos únicos em arquivo 1.")
    print(f"  - {len(chaves2)} campos únicos em arquivo 2.")
    print(f"  - {len(titulos1 & titulos2)} títulos idênticos em ambos.")
    print(f"\nSe algum campo essencial (title, link, publication_time, category, summary, full_article) estiver ausente em qualquer base, recomenda-se rodar padronização.\n")

def main():
    parser = argparse.ArgumentParser(description="Compara estrutura e registros entre dois arquivos de notícias JSONL - pipeline BRL_USD", add_help=False)
    parser.add_argument('--input1', '-i1', type=str, default=DEFAULT_FILE1, help="Arquivo JSONL origem/antigo (default: Urba_fred_blog_raw.jsonl)")
    parser.add_argument('--input2', '-i2', type=str, default=DEFAULT_FILE2, help="Arquivo JSONL destino/padronizado (default: Urba_fred_blog_standard.jsonl)")
    parser.add_argument('--help', action='store_true', help="Mostra esta ajuda")
    args = parser.parse_args()

    if args.help:
        print(textwrap.dedent(HELP_TEXT))
        print("\nReferências implementadas, ABNT, ciência aberta, rastreabilidade total.")
        sys.exit(0)

    compara_jsonl_arquivos(args.input1, args.input2)

if __name__ == '__main__':
    main()
