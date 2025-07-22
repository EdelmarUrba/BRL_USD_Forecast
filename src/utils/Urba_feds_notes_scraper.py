#!/usr/bin/env python3
"""
Urba_feds_notes_scraper.py

Script para coletar automaticamente as FEDS Notes do Federal Reserve Board dos últimos 5 anos,
baixando metadados e textos completos, e salvando em JSON estruturado no padrão do projeto.

Características:
- Percorre os índices anuais das FEDS Notes (2021 a 2025).
- Extrai: título, autores, data de publicação (ISO 8601), link, resumo (se disponível), texto completo.
- Salva o arquivo como "Urba_feds_notes_last5y.json" no diretório:
    /home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news
- Mensagens claras de progresso, logs e instrução de uso via -h/--help.
- Compatível com o pipeline do projeto e com o padrão do artigo From News to Forecast.

Uso:
    python Urba_feds_notes_scraper.py [--output_dir PATH] [--years 5]
"""

import argparse
import os
import json
import logging
import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests

DEFAULT_OUTPUT_DIR = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news"
DEFAULT_OUTPUT_FILE = "Urba_feds_notes_last5y.json"
BASE_INDEX_URL = "https://www.federalreserve.gov/econres/notes/feds-notes/{}-index.htm"
BASE_DOMAIN = "https://www.federalreserve.gov"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_html(text):
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def get_years_list(years=5):
    current_year = datetime.now().year
    return [current_year - i for i in range(years-1, -1, -1)]

def parse_note_page(note_url):
    try:
        resp = requests.get(note_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # Título
        title_tag = soup.find("h1", class_="article-title")
        title = title_tag.get_text(strip=True) if title_tag else "No Title"
        # Autores
        authors_tag = soup.find("div", class_="author")
        authors = clean_html(authors_tag.get_text()) if authors_tag else ""
        # Data de publicação
        date_tag = soup.find("time")
        pub_date = date_tag.get("datetime", "") if date_tag else ""
        if not pub_date:
            # fallback: busca texto
            date_tag2 = soup.find("p", class_="article__time")
            pub_date = date_tag2.get_text(strip=True) if date_tag2 else ""
        # Resumo (primeiro parágrafo)
        summary = ""
        content_div = soup.find("div", class_="col-xs-12 col-sm-8 col-md-8")
        if content_div:
            paragraphs = content_div.find_all("p")
            if paragraphs:
                summary = paragraphs[0].get_text(strip=True)
            # Remove scripts, styles, etc.
            for tag in content_div(["script", "style", "aside", "footer"]):
                tag.decompose()
            full_text = content_div.get_text(separator=" ", strip=True)
        else:
            full_text = ""
        return {
            "title": title,
            "authors": authors,
            "publication_date": pub_date,
            "link": note_url,
            "summary": summary,
            "full_text": full_text
        }
    except Exception as e:
        logging.warning(f"Falha ao processar nota {note_url}: {e}")
        return None

def parse_year_index(year):
    url = BASE_INDEX_URL.format(year)
    logging.info(f"Lendo índice anual de FEDS Notes: {url}")
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        notes = []
        # Cada nota está em <div class="col-xs-12 col-sm-8 col-md-8"> com <a href=...>
        for item in soup.find_all("div", class_="col-xs-12 col-sm-8 col-md-8"):
            a_tag = item.find("a", href=True)
            if a_tag:
                note_url = a_tag["href"]
                # Garante URL absoluta
                if note_url.startswith("/"):
                    note_url = BASE_DOMAIN + note_url
                notes.append(note_url)
        return notes
    except Exception as e:
        logging.warning(f"Falha ao processar índice anual {url}: {e}")
        return []

def scrape_feds_notes(years=5):
    logging.info("Iniciando raspagem das FEDS Notes dos últimos 5 anos.")
    all_notes = []
    years_list = get_years_list(years)
    for year in years_list:
        note_urls = parse_year_index(year)
        for note_url in note_urls:
            note = parse_note_page(note_url)
            if note:
                # Filtro extra: só inclui se a data for do ano correto ou posterior
                try:
                    note_year = int(note["publication_date"][:4])
                    if note_year < year:
                        continue
                except Exception:
                    pass
                all_notes.append(note)
            time.sleep(1)  # delay educado
        time.sleep(2)
    logging.info(f"Total de notas coletadas: {len(all_notes)}")
    return all_notes

def save_notes(notes, output_dir, output_file):
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, output_file)
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(notes, f, ensure_ascii=False, indent=4)
    logging.info(f"Arquivo salvo: {full_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Coleta FEDS Notes dos últimos 5 anos (Federal Reserve Board), salva em JSON estruturado com prefixo 'Urba_'.",
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

    print("\nFEDS Notes Scraper - Coleta automatizada das notas do Federal Reserve Board (últimos 5 anos)")
    print("Este script percorre os índices anuais, extrai metadados e textos completos das FEDS Notes e salva em JSON estruturado.")
    print("Formato compatível com o pipeline do projeto e com o artigo From News to Forecast.\n")
    print(f"Arquivo de saída: {os.path.join(args.output_dir, args.output_file)}\n")

    notes = scrape_feds_notes(years=args.years)
    save_notes(notes, args.output_dir, args.output_file)

    print("\nColeta concluída com sucesso.")
    print(f"Notas salvas em: {os.path.join(args.output_dir, args.output_file)}")
    print("Você pode agora integrar essas notas ao pipeline de séries temporais e testes do projeto.\n")
    print("Para ajuda, execute: python Urba_feds_notes_scraper.py -h\n")

if __name__ == "__main__":
    main()
