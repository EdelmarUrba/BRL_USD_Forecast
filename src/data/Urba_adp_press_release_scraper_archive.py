#!/usr/bin/env python3
# coding: utf-8

"""
Urba_adp_press_release_scraper_archive.py

Baixa todos os PDFs de press releases históricos a partir da página de archive
da ADP, extrai texto integral e organiza corpus em JSONL, pronto para NLP.
Imprime amostras das primeiras e últimas notícias para controle manual.
Gera relatório sobre o sucesso do download e da extração.

Autor: Edelmar Urba — 2025-07-21
"""

import os
import re
import json
import requests
import pdfplumber
from bs4 import BeautifulSoup
from datetime import datetime

# CONFIGURAÇÃO DE DIRETÓRIOS E PATHS
SAVE_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/adp_press_releases"
os.makedirs(SAVE_DIR, exist_ok=True)
OUT_JSONL = os.path.join(SAVE_DIR, "Urba_adp_press_release_corpus.jsonl")
OUT_LOG = os.path.join(SAVE_DIR, "Urba_adp_press_release_log.txt")

ARCHIVE_URL = "https://adpemploymentreport.com/news-releases/"

# 1. RASPAGEM DOS LINKS DOS PDFs
r = requests.get(ARCHIVE_URL)
soup = BeautifulSoup(r.content, "html.parser")
pdf_links = []
for a in soup.find_all("a", href=True):
    href = a['href']
    if href.lower().endswith('.pdf'):
        url = href if href.startswith("http") else f"https://adpemploymentreport.com{href}"
        pdf_links.append(url)

status_log = []
corpus = []
success_count = 0
fail_count = 0

# 2. DOWNLOAD E PROCESSAMENTO DOS RELEASES
for idx, link in enumerate(pdf_links):
    fname = os.path.join(SAVE_DIR, link.split("/")[-1])
    try:
        resp = requests.get(link)
        if resp.status_code == 200 and b"%PDF" in resp.content[:10]:
            with open(fname, "wb") as f:
                f.write(resp.content)
            # Extração de texto do PDF
            with pdfplumber.open(fname) as pdf:
                pages = [page.extract_text() or "" for page in pdf.pages]
            full_text = "\n".join(pages).strip()
            clean_text = re.sub(r'\s+', ' ', full_text).strip()
            # Tenta extrair data e título do nome do arquivo
            name_match = re.search(r'(\d{4})[_\-](\d{2})', fname)
            if name_match:
                year, month = name_match.groups()
                date = f"{year}-{month}-01"
            else:
                date = ""
            title = f"ADP National Employment Report - {os.path.basename(fname)}"
            summary = clean_text[:400]  # Primeiro trecho como sumário
            doc = {
                "date": date,
                "source_url": link,
                "title": title,
                "full_text": clean_text,
                "summary": summary,
                "category": "ADP Press Release"
            }
            corpus.append(doc)
            status_log.append(f"[OK] {link}")
            success_count += 1
        else:
            status_log.append(f"[FAIL: NOT FOUND OR INVALID PDF] {link}")
            fail_count += 1
    except Exception as ex:
        status_log.append(f"[FAIL: ERROR] {link} | {ex}")
        fail_count += 1

# 3. SALVANDO CORPUS JSONL
with open(OUT_JSONL, "w", encoding="utf-8") as fjson:
    for doc in corpus:
        fjson.write(json.dumps(doc, ensure_ascii=False) + "\n")

# 4. RELATÓRIO E CONTROLE HUMANO: AMOSTRAS DE SAÍDA
print("\nPrimeiras 2 notícias extraídas:")
for doc in corpus[:2]:
    print("\n---")
    print(f"Data: {doc['date']}\nTítulo: {doc['title']}\nResumo: {doc['summary'][:140]}...\nFonte: {doc['source_url']}")
print("\nÚltimas 2 notícias extraídas:")
for doc in corpus[-2:]:
    print("\n---")
    print(f"Data: {doc['date']}\nTítulo: {doc['title']}\nResumo: {doc['summary'][:140]}...\nFonte: {doc['source_url']}")

# 5. RELATÓRIO LOG
with open(OUT_LOG, "w", encoding="utf-8") as flog:
    flog.write(f"Execução: {datetime.now().isoformat(timespec='seconds')}\n")
    flog.write(f"Página de releases: {ARCHIVE_URL}\n")
    flog.write(f"Total de links encontrados: {len(pdf_links)}\n")
    flog.write(f"Sucessos: {success_count} | Falhas: {fail_count}\n\n")
    for entry in status_log:
        flog.write(entry + "\n")
    flog.write(f"\nArquivo JSONL corpus: {OUT_JSONL}\n")
    flog.write("\nReferência ABNT:\n")
    flog.write("ADP RESEARCH INSTITUTE. ADP National Employment Report – Press Releases [press releases históricos]. Disponível em: https://adpemploymentreport.com/. Acesso em: 21 jul. 2025.\n")

print("\nResumo do processamento:")
print(f"Total de links encontrados: {len(pdf_links)}")
print(f"Sucesso na extração de conteúdo: {success_count}")
print(f"Falhas/arquivos não encontrados: {fail_count}")
print(f"Corpus salvo em: {OUT_JSONL}")
print(f"Log salvo em: {OUT_LOG}")
