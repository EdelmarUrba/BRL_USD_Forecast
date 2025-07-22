#!/usr/bin/env python3
"""
fred_news_rss_scraper.py

Coleta notícias/anúncios do FRED St. Louis via RSS (últimos 5 anos) e salva em JSON estruturado.
O arquivo de saída inclui o prefixo "Urba_" para padronização e fácil identificação no projeto.

Exemplo de nome de arquivo:
Urba_fred_announcements_last5y.json
"""

import argparse
import os
import json
import logging
from datetime import datetime, timedelta
import feedparser
from bs4 import BeautifulSoup

DEFAULT_OUTPUT_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news"
DEFAULT_OUTPUT_FILE = "Urba_fred_announcements_last5y.json"
FEED_URL = "https://news.research.stlouisfed.org/feed/"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_html(text):
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def parse_feed(feed_url, years=5):
    logging.info(f"Baixando feed RSS: {feed_url}")
    feed = feedparser.parse(feed_url)
    news = []
    cutoff = datetime.now() - timedelta(days=365*years)
    for entry in feed.entries:
        pub_time = entry.get("published", "") or entry.get("pubDate", "")
        try:
            dt = datetime(*entry.published_parsed[:6])
            pub_time_iso = dt.isoformat()
        except Exception:
            pub_time_iso = pub_time
            dt = None
        if dt and dt < cutoff:
            continue
        title = entry.title.strip()
        link = entry.link
        category = entry.get("category", "FRED Announcement")
        summary = clean_html(entry.get("summary", ""))
        full_article = ""
        if "content" in entry and entry.content:
            full_article = clean_html(entry.content[0].value)
        elif "summary_detail" in entry and entry.summary_detail:
            full_article = clean_html(entry.summary_detail.value)
        else:
            full_article = summary
        news.append({
            "title": title,
            "category": category,
            "summary": summary,
            "link": link,
            "publication_time": pub_time_iso,
            "full_article": full_article
        })
    logging.info(f"Total de notícias coletadas nos últimos {years} anos: {len(news)}")
    return news

def save_news(news, output_dir, output_file):
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, output_file)
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=4)
    logging.info(f"Arquivo salvo: {full_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Coleta notícias/anúncios do FRED St. Louis via RSS (últimos 5 anos) e salva em JSON estruturado com prefixo 'Urba_'.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Diretório para salvar o arquivo JSON (padrão: {DEFAULT_OUTPUT_DIR})"
    )
    parser.add_argument(
        "--output_file",
        type=str,
        default=DEFAULT_OUTPUT_FILE,
        help=f"Nome do arquivo JSON de saída (padrão: {DEFAULT_OUTPUT_FILE})"
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Quantidade de anos retroativos a considerar (padrão: 5)"
    )
    args = parser.parse_args()

    print("\nFRED News RSS Scraper - Coleta automatizada de notícias do FRED St. Louis (últimos 5 anos)")
    print("Este script baixa todas as notícias/anúncios do blog oficial via RSS e salva em JSON estruturado.")
    print("Formato compatível com o pipeline do projeto e com o artigo From News to Forecast.\n")
    print(f"Arquivo de saída: {os.path.join(args.output_dir, args.output_file)}\n")

    news = parse_feed(FEED_URL, years=args.years)
    save_news(news, args.output_dir, args.output_file)

    print("\nColeta concluída com sucesso.")
    print(f"Notícias salvas em: {os.path.join(args.output_dir, args.output_file)}")
    print("Você pode agora integrar essas notícias ao pipeline de séries temporais do projeto.\n")
    print("Para ajuda, execute: python fred_news_rss_scraper.py -h\n")

if __name__ == "__main__":
    main()

