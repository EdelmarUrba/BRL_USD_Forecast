#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_scrape_fred_blog_links.py

Descrição:
-----------
Scraper abrangente para o FRED Blog (https://fredblog.stlouisfed.org/), que:
- Navega por cada mês no formato /YYYY/MM/ (de 2020-01 ao presente),
- Descobre automaticamente os links de todas as postagens publicadas em cada página mensal e subpáginas adicionais (/YYYY/MM/page/N/),
- Realiza a raspagem detalhada de cada post individual: título, data, autores, resumo e corpo íntegro,
- Exporta todos os dados para arquivos CSV e JSONL consolidados, otimizados para NLP e ciência de dados,
- Possui robustez contra duplicidade, erros de conexão, e aplica user-agents e pausas aleatórias.

Compatível com todos os padrões e melhorias sugeridos anteriormente.

Autor: Edelmar Urba
Data: 16/07/2025
"""

import os
import re
import time
import json
import random
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date

# CONFIGURAÇÃO PRINCIPAL
OUTPUT_ROOT = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news"
CSV_PATH = os.path.join(OUTPUT_ROOT, "Urba_fred_blog_raw.csv")
JSONL_PATH = os.path.join(OUTPUT_ROOT, "Urba_fred_blog_raw.jsonl")
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "curl/7.64.1",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
]

def gerar_lista_arquivos(mes_inicio="2020-01"):
    hoje = date.today()
    ano, mes = map(int, mes_inicio.split("-"))
    inicio = date(ano, mes, 1)
    meses = []
    dt = inicio
    while dt <= hoje:
        meses.append(dt.strftime("%Y/%m/"))
        mes = dt.month + 1
        ano = dt.year
        if mes > 12:
            mes = 1
            ano += 1
        dt = date(ano, mes, 1)
    return meses

def fetch_soup(url, max_attempts=3):
    for attempt in range(max_attempts):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            resp = requests.get(url, headers=headers, timeout=18)
            if resp.status_code == 200:
                return BeautifulSoup(resp.content, "html.parser")
            time.sleep(random.uniform(1, 3))
        except Exception as e:
            print(f"⚠ Falha ao acessar {url}: {e}")
            time.sleep(random.uniform(2, 5))
    return None

def get_month_post_links(month_path):
    links = set()
    page = 1
    while True:
        if page == 1:
            url = f"https://fredblog.stlouisfed.org/{month_path}"
        else:
            url = f"https://fredblog.stlouisfed.org/{month_path}page/{page}/"
        print(f"🔎 Página {page} do mês: {url}")
        soup = fetch_soup(url)
        if not soup:
            print(f"⚠ Página não carregada ou inexistente: {url}")
            break
        # Tentar descobrir todos os links de posts na listagem
        articles = soup.find_all("h2", class_="entry-title")
        if not articles:
            print("⚠ Nenhum post encontrado nesta página")
            break
        new_links = 0
        for art in articles:
            a = art.find("a")
            if a and a.get('href'):
                if a['href'] not in links:
                    links.add(a['href'])
                    new_links += 1
        print(f"   ✔️ {new_links} novos links coletados nesta página.")
        page += 1
        time.sleep(random.uniform(1.0, 2.0))
    return links

def extrair_post_full(url):
    soup = fetch_soup(url)
    if not soup:
        return None
    # Título
    title_elem = soup.find("h1", class_="entry-title")
    title = title_elem.get_text(strip=True) if title_elem else ''
    # Data
    date_elem = soup.find("time")
    pub_date = date_elem["datetime"] if date_elem and date_elem.has_attr("datetime") else ''
    # Autores
    byline = ""
    byline_elem = soup.find("span", class_="byline")
    if byline_elem:
        byline = byline_elem.get_text(strip=True)
    # Resumo (primeiro parágrafo do conteúdo)
    summary = ""
    content_div = soup.find("div", class_="entry-content")
    if content_div:
        p = content_div.find("p")
        summary = p.get_text(strip=True) if p else ""
    # Texto completo
    text = ""
    if content_div:
        for tag in content_div(['script', 'style']):
            tag.decompose()
        text = content_div.get_text(separator="\n", strip=True)
        text = re.sub(r'\n{3,}', '\n\n', text)
    return {
        "title": title,
        "link": url,
        "publication_time": pub_date,
        "authors": byline,
        "summary": summary,
        "full_article": text
    }

def save_jsonl(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write("\n")
    print(f"✔ JSONL salvo: {filepath}")

def main():
    parser = argparse.ArgumentParser(description="Raspagem automática do acervo do FRED Blog por listagem mensal e links individuais de post.")
    parser.add_argument('--mes_inicio', type=str, default="2020-01", help="Mês inicial (YYYY-MM)")
    parser.add_argument('--max_posts', type=int, default=10000, help="Número máximo de posts totais")
    args = parser.parse_args()

    meses = gerar_lista_arquivos(mes_inicio=args.mes_inicio)
    print(f"⏳ {len(meses)} meses do FRED Blog (de {args.mes_inicio} a {date.today().strftime('%Y-%m')})")
    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    all_links = set()
    for mes_path in meses:
        print(f"\n🌐 Descobrindo links do mês: https://fredblog.stlouisfed.org/{mes_path}")
        month_links = get_month_post_links(mes_path)
        print(f"   ➡️ {len(month_links)} links únicos adicionados para {mes_path}")
        all_links.update(month_links)
        if len(all_links) >= args.max_posts:
            print("Limite global atingido durante descoberta dos links.")
            break
        time.sleep(random.uniform(0.8, 2.0))

    print(f"\n🔗 Total de links únicos de posts a processar: {len(all_links)}")
    posts_data = []
    for idx, link in enumerate(sorted(all_links)):
        post = extrair_post_full(link)
        if post and post["full_article"] and len(post["full_article"]) > 80:
            posts_data.append(post)
            print(f"   ✔️ [{idx+1}] {post['title'][:80]} | {post['publication_time'][:10]}")
        else:
            print(f"   ⭕ [{idx+1}] Falha ao extrair ou artigo vazio: {link}")
        if len(posts_data) >= args.max_posts:
            print("Limite máximo de posts processados.")
            break
        time.sleep(random.uniform(1.2, 2.4))

    if not posts_data:
        print("⚠ Nenhum post coletado. Reveja conectividade ou o parser.")
        return

    df = pd.DataFrame(posts_data)
    df = df.sort_values(by="publication_time", ascending=False).reset_index(drop=True)
    df.to_csv(CSV_PATH, index=False)
    save_jsonl(posts_data, JSONL_PATH)

    print(f"\n✅ Concluído! Total coletado: {len(df)} posts.")
    print(f"📄 CSV salvo: {CSV_PATH}")
    print(f"📄 JSONL salvo: {JSONL_PATH}")
    print("\n🔍 Amostra:")
    try:
        print(df[["publication_time", "title", "link"]].head(6).to_markdown(index=False))
    except Exception:
        print(df[["publication_time", "title", "link"]].head(6))

if __name__ == "__main__":
    main()
