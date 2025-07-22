#!/usr/bin/env python3
# coding: utf-8

"""
Urba_adp_press_release_bruteforce.py

Busca e baixa os press releases mensais do ADP National Employment Report (NER)
tentando o padrão de URL dos últimos anos, a partir da primeira quarta-feira
de cada mês. Baixa, extrai texto via pdfplumber e salva em JSONL. Log detalhado.

Autor: Edelmar Urba (2025-07-21)
"""

import os
import re
import json
import requests
import pdfplumber
from datetime import datetime, timedelta

SAVE_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/adp_press_releases"
os.makedirs(SAVE_DIR, exist_ok=True)
OUT_JSONL = os.path.join(SAVE_DIR, "Urba_adp_press_release_bruteforce.jsonl")
OUT_LOG = os.path.join(SAVE_DIR, "Urba_adp_press_release_bruteforce_log.txt")

# Defina o range de anos que deseja tentar (ajuste conforme precisão do padrão)
START_YEAR = 2022
END_YEAR = datetime.today().year

corpus = []
status_log = []
success_count = 0
fail_count = 0
tried_count = 0

for year in range(START_YEAR, END_YEAR+1):
    for month in range(1, 13):
        # Calcule a primeira quarta-feira do mês
        dt = datetime(year, month, 1)
        while dt.weekday() != 2:  # 0=segunda, 2=quarta
            dt += timedelta(days=1)
        yyyymmdd = dt.strftime('%Y%m%d')
        url = f"https://adp-ri-nrip-static.adp.com/artifacts/us_ner/{yyyymmdd}/ADP_NATIONAL_EMPLOYMENT_REPORT_Press_Release_{year}_{month:02d}%20FINAL.pdf"
        fname = os.path.join(SAVE_DIR, url.split("/")[-1])
        tried_count += 1

        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200 and b"%PDF" in resp.content[:10]:
                with open(fname, "wb") as f:
                    f.write(resp.content)
                # Extração de texto
                with pdfplumber.open(fname) as pdf:
                    pages = [page.extract_text() or "" for page in pdf.pages]
                full_text = "\n".join(pages).strip()
                clean_text = re.sub(r'\s+', ' ', full_text).strip()
                title = f"ADP NER Press Release {year}-{month:02d}"
                summary = clean_text[:400]
                doc = {
                    "date": dt.strftime('%Y-%m-%d'),
                    "source_url": url,
                    "title": title,
                    "full_text": clean_text,
                    "summary": summary,
                    "category": "ADP Press Release"
                }
                corpus.append(doc)
                status_log.append(f"[OK] {url}")
                success_count += 1
                print(f"[OK] {year}-{month:02d}")
            else:
                status_log.append(f"[NOT FOUND] {url}")
                print(f"[NOT FOUND] {year}-{month:02d}")
                fail_count += 1
        except Exception as ex:
            status_log.append(f"[ERROR] {url} | {ex}")
            print(f"[ERROR] {year}-{month:02d} | {ex}")
            fail_count += 1

# Salvar JSONL
with open(OUT_JSONL, "w", encoding="utf-8") as fjson:
    for doc in corpus:
        fjson.write(json.dumps(doc, ensure_ascii=False) + "\n")

# Salvar log
with open(OUT_LOG, "w", encoding="utf-8") as flog:
    flog.write(f"Execução: {datetime.now().isoformat(timespec='seconds')}\n")
    flog.write(f"Tentativas: {tried_count} | Sucessos: {success_count} | Falhas: {fail_count}\n\n")
    for entry in status_log:
        flog.write(entry + "\n")
    flog.write(f"\nArquivo JSONL corpus: {OUT_JSONL}\n")
    flog.write("Referência ABNT: ADP RESEARCH INSTITUTE. ADP National Employment Report – Press Releases. Disponível em: https://adpemploymentreport.com/. Acesso em: 21 jul. 2025.\n")

# Amostras para controle humano
print("\nPrimeiras 2 notícias extraídas:")
for doc in corpus[:2]:
    print("\n---")
    print(f"Data: {doc['date']}\nTítulo: {doc['title']}\nResumo: {doc['summary'][:140]}...\nFonte: {doc['source_url']}")
print("\nÚltimas 2 notícias extraídas:")
for doc in corpus[-2:]:
    print("\n---")
    print(f"Data: {doc['date']}\nTítulo: {doc['title']}\nResumo: {doc['summary'][:140]}...\nFonte: {doc['source_url']}")

print("\nResumo do processamento:")
print(f"Total de releases tentados: {tried_count}")
print(f"Sucesso na extração de conteúdo: {success_count}")
print(f"Falhas/arquivos não encontrados: {fail_count}")
print(f"Corpus salvo em: {OUT_JSONL}")
print(f"Log salvo em: {OUT_LOG}")
