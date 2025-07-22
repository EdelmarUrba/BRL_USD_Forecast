#!/usr/bin/env python3
# coding: utf-8

"""
Urba_adp_employment_history.py

Baixa, extrai e processa a série histórica nacional ADP Employment diretamente da fonte oficial.
Gera CSV limpo, pronto para integração em painéis macroeconômicos, e log auditável.

Autor: Edelmar Urba (2025-07-21)
"""

import os
import requests
import zipfile
import pandas as pd
from io import StringIO
from datetime import datetime

# CONFIGURAÇÕES
ADP_URL = "https://adp-ri-nrip-static.adp.com/artifacts/us_ner/20250702/ADP_NER_history.zip"
OUT_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/adp_employments"
RAW_ZIP = os.path.join(OUT_DIR, "ADP_NER_history.zip")
CSV_TARGET = os.path.join(OUT_DIR, "Urba_adp_employment_history.csv")
LOG_PATH = os.path.join(OUT_DIR, "Urba_adp_employment_history_log.txt")

log = []
now_str = datetime.now().isoformat(sep=' ', timespec='seconds')

# Garantir diretório
os.makedirs(OUT_DIR, exist_ok=True)

# Download ZIP
log.append(f"Script: {os.path.abspath(__file__)}")
log.append(f"Data/hora execução: {now_str}")
log.append(f"Fonte oficial ADP: {ADP_URL}")
log.append(f"Baixando o arquivo ZIP para: {RAW_ZIP}")
resp = requests.get(ADP_URL, stream=True)
if resp.status_code == 200:
    with open(RAW_ZIP, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    log.append("[OK] Download completo.")
else:
    log.append(f"[ERRO] Falha no download. Status HTTP: {resp.status_code}")
    raise Exception("Falha no download do ZIP ADP.")
    
# Extrair e processar arquivo CSV
with zipfile.ZipFile(RAW_ZIP, 'r') as z:
    filelist = z.namelist()
    log.append(f"Arquivos no ZIP: {filelist}")
    csv_name = next((f for f in filelist if f.endswith('.csv')), None)
    if not csv_name:
        log.append("[ERRO] CSV não encontrado no ZIP.")
        raise Exception("CSV não encontrado no ZIP ADP.")
    z.extract(csv_name, OUT_DIR)

csv_path = os.path.join(OUT_DIR, csv_name)
log.append(f"Lendo CSV: {csv_path}")
df = pd.read_csv(csv_path)

# Filtrar nível nacional e colunas chave
summary = f"Tamanho inicial: {df.shape}"
df_nat = df[(df['agg_RIS'] == 'National') & (df['category'] == 'U.S.')]
df_out = df_nat[['date', 'NER', 'NER_SA']].copy()
df_out.to_csv(CSV_TARGET, index=False)
summary += f" | Filtrado: {df_out.shape}"

# Registro de amostras
log.append(f"Tamanho do dataframe filtrado: {df_out.shape}")
log.append("Amostra das 5 primeiras linhas:")
log.append(df_out.head().to_string(index=False))
log.append("Amostra das 5 últimas linhas:")
log.append(df_out.tail().to_string(index=False))
log.append(summary)
log.append(f"Arquivo consolidado final salvo em: {CSV_TARGET}")

# Bibliografia e metadados ABNT
log.append("\nReferência ABNT da fonte:")
log.append("ADP RESEARCH INSTITUTE. ADP National Employment Report [base de dados histórica]. Disponível em: https://adpemploymentreport.com/. Acesso em: 21 jul. 2025.")

with open(LOG_PATH, 'w', encoding='utf-8') as flog:
    flog.write('\n'.join(log))

print(f"Processo concluído, arquivo salvo em {CSV_TARGET}, log em {LOG_PATH}")
