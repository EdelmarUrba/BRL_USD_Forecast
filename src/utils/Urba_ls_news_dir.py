#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_ls_news_dir.py

Descrição:
-----------
Lista todos os arquivos presentes em um diretório de notícias do projeto, 
mostrando nome do arquivo, tamanho (KB), data e hora da última modificação.
Ferramenta de controle inicial antes de iniciar padronização, limpeza ou merge.

Uso:
    python Urba_ls_news_dir.py --dir /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news/

Autor: Edelmar Urba
Data: 2025-07-17
"""

import os
import argparse
import datetime

def list_files(diretorio):
    print(f"\nArquivos presentes em: {diretorio}\n")
    print("{:<40}{:<12}{:<25}".format("Arquivo", "Tamanho(KB)", "Última Modificação"))
    print("-" * 80)
    for fname in sorted(os.listdir(diretorio)):
        path = os.path.join(diretorio, fname)
        if os.path.isfile(path):
            stats = os.stat(path)
            tam_kb = stats.st_size // 1024
            dt_mod = datetime.datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            print("{:<40}{:<12}{:<25}".format(fname, tam_kb, dt_mod))
    print("-" * 80)

def main():
    parser = argparse.ArgumentParser(description="Lista arquivos em um diretório de notícias para controle humano.")
    parser.add_argument('--dir', '-d', required=True, help="Diretório alvo (ex: /caminho/para/news/)")
    args = parser.parse_args()

    if not os.path.isdir(args.dir):
        print(f"Diretório não existe: {args.dir}")
        return

    list_files(args.dir)

if __name__ == "__main__":
    main()
