#!/usr/bin/env python3
# coding: utf-8

"""
Urba_adp_press_release_scraper.py

Baixa, extrai e organiza o texto dos Press Releases mensais históricos do
ADP National Employment Report como corpus textual no padrão científico.
Salva resultados como JSONL ("notícia" = release), com campos:
- date (data do release)
- source_url (URL original)
- title (gerado automaticamente)
- full_text (texto integral extraído do PDF)
- summary (primeira linha ou parágrafo)
- category ("ADP Press Release")

Autor: Edelmar Urba — 2025-07-21
"""

import os
import re
import json
import requests
import pdfplumber
from datetime import datetime, timedelta

# CONFIGURAÇÃO
START_YEAR = 2010
END_YEAR = datetime.today().year
SAVE_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/adp_press_releases"
OUT_JSONL = os.path.join(SAVE_DIR, "Urba_adp_press_release_corpus.jsonl")
OUT_LOG = os.path.join(SAVE_DIR, "Urba_adp_press_release_log.txt")

def url_template(year, month):
    # Baseado no padrão das URLs públicas dos PDFs ADP
    # Exemplo (jun/2025): .../us_ner/20250702/ADP_NATIONAL_EMPLOYMENT_REPORT_Press_Release_2025_06%20FINAL.pdf
    month_str = f"{month:02d}"
    data_release = datetime(year, month, 1)
    # Estimar data de publicação (1a quarta-feira do mês): ajusta abaixo
    for delta in range(7):
        if (data_release + timedelta(days=delta)).weekday() == 2:  # 0=segunda, 2=quarta
            break
    pub_dt = data_release + timedelta(days=delta)
    # Ex: 20250702 para 02/jul/2025
    pub_dt_str = pub_dt.strftime("%Y%m%d")
    # URL final
    url = f"https://adp-ri-nrip-static.adp.com/artifacts/us_ner/{pub_dt_str}/ADP_NATIONAL_EMPLOYMENT_REPORT_Press_Release_{year}_{month_str}%20FINAL.pdf"
    return url, pub_dt.strftime("%Y-%m-%d")

os.makedirs(SAVE_DIR, exist_ok=True)
log = []
corpus = []

for year in range(START_YEAR, END_YEAR+1):
    for month in range(1, 13):
        url, pub_date = url_template(year, month)
        local_pdf = os.path.join(SAVE_DIR, f"ADP_NER_Press_Release_{year}_{month:02d}.pdf")
        try:
            # Download PDF
            r = requests.get(url)
            if r.status_code == 200 and b"%PDF" in r.content[:10]:
                with open(local_pdf, "wb") as f:
                    f.write(r.content)
                # Extrai texto
                with pdfplumber.open(local_pdf) as pdf:
                    raw_text = "\n".join(page.extract_text() or "" for page in pdf.pages if page.extract_text())
                clean_text = re.sub(r'\s+', ' ', raw_text).strip()
                title = f"ADP National Employment Report - {pub_date}"
                summary_candidate = clean_text.split('. ')[0]
                doc = {
                    "date": pub_date,
                    "source_url": url,
                    "title": title,
                    "full_text": clean_text,
                    "summary": summary_candidate[:400],
                    "category": "ADP Press Release"
                }
                corpus.append(doc)
                log.append(f"{year}-{month:02d}: [OK] {url}")
            else:
                log.append(f"{year}-{month:02d}: [NOT FOUND/INVALID PDF] {url}")
        except Exception as ex:
            log.append(f"{year}-{month:02d}: [FAIL] {url} | {ex}")

# Exporta JSONL corpus
with open(OUT_JSONL, "w", encoding="utf-8") as fjson:
    for doc in corpus:
        fjson.write(json.dumps(doc, ensure_ascii=False) + "\n")

# Exporta log
with open(OUT_LOG, "w", encoding="utf-8") as flog:
    flog.write(f"Execução: {datetime.now().isoformat(timespec='seconds')}\n")
    flog.write("\n".join(log))
    flog.write(f"\nTotal de releases encontrados: {len(corpus)}\n\n")
    flog.write(f"\nFonte dos Press Releases: https://adpemploymentreport.com/\n")
    flog.write("Referência ABNT: ADP RESEARCH INSTITUTE. ADP National Employment Report - Press Releases. Disponível em: https://adpemploymentreport.com/. Acesso em: 21 jul. 2025.\n")

print(f"Script finalizado. Corpus salvo em {OUT_JSONL}, log de execução em {OUT_LOG}")
