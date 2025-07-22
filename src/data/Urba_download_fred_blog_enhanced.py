#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_download_fred_blog_enhanced.py

Descrição:
-----------
Raspagem dos posts do FRED Blog (https://fredblog.stlouisfed.org/) a partir de 01-01-2020,
com robustez contra bloqueios, logging aprimorado e exportação simultânea em CSV e JSONL.

- Filtro de data mínima configurável (--min_date; padrão: 2020-01-01)
- Retentativas automáticas com vários User-Agents para contornar bloqueios comuns
- Extração otimizada por seletores HTML e fallback encadeado
- Randomização de espera para evitar overload/bloqueio
- Exporta conteúdos em formato tabular (.csv) e legível para NLP (.jsonl)
- Caminho de saída fixo, conforme especificado

Uso:
----
    python Urba_download_fred_blog_enhanced.py
    python Urba_download_fred_blog_enhanced.py --max_posts 50 --min_date 2021-01-01

Dependências:
-------------
- requests
- pandas
- beautifulsoup4
- feedparser
- python-dotenv

Autor: Edelmar Urba
Data: 16/07/2025
"""

import os
import sys
import time
import random
import argparse
import requests
import pandas as pd
import feedparser
from bs4 import BeautifulSoup
from dotenv import dotenv_values
from datetime import datetime

def download_url_with_fallbacks(url, max_attempts=3, min_sleep=2, max_sleep=7):
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'curl/7.68.0',
        'Googlebot/2.1 (+http://www.google.com/bot.html)'
    ]
    last_exc = None
    for attempt in range(max_attempts):
        ua = random.choice(user_agents)
        try:
            resp = requests.get(url, headers={'User-Agent': ua}, timeout=20)
            if resp.status_code in (200, 201):
                return resp
            elif 400 <= resp.status_code < 500:
                print(f"⚠ HTTP {resp.status_code} para {url} (possível bloqueio ou 404)")
            elif 500 <= resp.status_code < 600:
                print(f"⚠ HTTP {resp.status_code} para {url} (erro no servidor)")
            time.sleep(random.uniform(min_sleep, max_sleep))
        except Exception as exc:
            print(f"⚠ Erro ao acessar {url}: {exc}")
            last_exc = exc
            time.sleep(random.uniform(min_sleep, max_sleep))
    print(f"❌ Falha definitiva ao baixar {url} após {max_attempts} tentativas")
    return None

def extract_text_fallback(soup):
    for sel in ['article', 'main', 'div.entry-content', 'div.post-single-content', 'div.content', 'body']:
        element = soup.select_one(sel) if sel != 'body' else soup.body
        if element:
            text = element.get_text(separator='\n', strip=True)
            if text and len(text) > 80:
                return text
    return ''

def get_fred_blog_feed(feed_url, max_posts=None, min_date=None):
    feed = feedparser.parse(feed_url)
    if not feed.entries:
        print("⚠ Nenhum post encontrado no feed.")
        return []

    entries = feed.entries[:max_posts] if max_posts else feed.entries
    blog_data = []
    min_date_dt = datetime.strptime(min_date, '%Y-%m-%d') if min_date else None

    for i, entry in enumerate(entries):
        url = getattr(entry, 'link', None)
        raw_date = getattr(entry, 'published', '') or getattr(entry, 'updated', '')
        title = getattr(entry, 'title', '')
        summary = getattr(entry, 'summary', '')
        content = ''
        date = None

        try:
            # Corrige comparações (remove timezone para garantir compatibilidade)
            date = pd.to_datetime(raw_date, utc=True).tz_localize(None)
        except Exception:
            date = None

        if min_date_dt and (date is None or date < min_date_dt):
            continue

        if url:
            resp = download_url_with_fallbacks(url)
            if resp and 200 <= resp.status_code < 300:
                try:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    content = extract_text_fallback(soup)
                except Exception as e:
                    print(f"⚠ Erro ao parsear HTML ({url}): {e}")
            else:
                print(f"⚠ Conteúdo não disponível em {url}")
        else:
            print(f"⚠ Entrada RSS sem URL válida.")

        blog_data.append({
            'date': date.isoformat() if date else '',
            'title': title,
            'url': url,
            'summary': summary,
            'content': content.strip()
        })

        print(f"[{i+1}/{len(entries)}] {title[:60]}... ({'OK' if content else 'Vazio'})")
        time.sleep(random.uniform(2, 5))
    return blog_data

def save_to_jsonl(data, path):
    import json
    with open(path, 'w', encoding='utf-8') as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write('\n')
    print(f"✔ JSONL salvo em: {path}")

def main():
    parser = argparse.ArgumentParser(description="Raspagem automatizada do FRED Blog com exportação estruturada")
    parser.add_argument('--feed_url', type=str, default='https://fredblog.stlouisfed.org/feed/')
    parser.add_argument('--max_posts', type=int, default=None)
    parser.add_argument('--min_date', type=str, default="2020-01-01", help="Data mínima (formato YYYY-MM-DD)")
    args = parser.parse_args()

    fixed_output_root = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news"
    csv_path = os.path.join(fixed_output_root, "Urba_fred_blog_raw.csv")
    jsonl_path = os.path.join(fixed_output_root, "Urba_fred_blog_raw.jsonl")
    os.makedirs(fixed_output_root, exist_ok=True)

    print(f"\nIniciando raspagem do FRED Blog (últimos 5 anos)...")
    blog_data = get_fred_blog_feed(args.feed_url, max_posts=args.max_posts, min_date=args.min_date)

    if not blog_data:
        print("⚠ Nenhum post coletado após filtragem.")
        return

    df_blog = pd.DataFrame(blog_data, columns=['date', 'title', 'url', 'summary', 'content'])
    df_blog = df_blog[df_blog['content'].str.len() > 80].copy()
    df_blog['date'] = pd.to_datetime(df_blog['date'], errors='coerce')
    df_blog = df_blog.sort_values(by='date', ascending=False).reset_index(drop=True)

    # Exportações
    df_blog.to_csv(csv_path, index=False)
    save_to_jsonl(blog_data, jsonl_path)

    print(f"\n✅ Coleta finalizada com {len(df_blog)} posts válidos.")
    print(f"📁 CSV: {csv_path}")
    print(f"📁 JSONL: {jsonl_path}")
    print(f"\nPrévia do conteúdo:\n")
    print(df_blog.head(3).to_markdown(index=False))

if __name__ == "__main__":
    main()
