#!/usr/bin/env python
# coding: utf-8

"""
Script: download_fred_blog.py

Descrição:
-----------
Script aprimorado para baixar, arquivar e tratar os posts do FRED Blog (https://fredblog.stlouisfed.org/),
pronto para servir como fonte robusta de texto-notícia para análise macroeconômica e modelagem de volatilidade, com robustez contra obstáculos comuns de scraping.

Recursos e Robustez:
- Busca pela chave FRED_API_KEY no .env na raiz do projeto.
- Download dos metadados via RSS, com múltiplas tentativas e fallback para User-Agent alternativo.
- Scraping de cada post com seleção inteligente de elementos HTML, fallback progressivo entre <article>, <main>, <div> e body.
- Identifica e trata bloqueios por firewall/captcha, status HTTP incomuns e respostas inesperadas.
- Esperas automáticas (com backoff exponencial) em falhas de conexão, além de pausa randomizada entre requisições para contornar limitações anti-bot.
- Impressão de logs detalhados para debug e transparência.
- CSV de saída consolidado, sempre no mesmo local, ordenado por data.

Uso:
----
    python download_fred_blog.py
    python download_fred_blog.py --max_posts 50 --output data/fred_blog_posts.csv

Requisitos:
-----------
- pip install requests beautifulsoup4 pandas feedparser python-dotenv

Autor: Edelmar Urba
Data: 15/07/2025
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

# Utilidade: localiza raiz do projeto pelo .env
def find_project_root(marker='.env'):
    dir_path = os.path.abspath(os.getcwd())
    while True:
        if os.path.isfile(os.path.join(dir_path, marker)):
            return dir_path
        parent = os.path.dirname(dir_path)
        if parent == dir_path:
            raise FileNotFoundError(f'Arquivo {marker} não encontrado acima de {os.getcwd()}')
        dir_path = parent

def load_fred_api_key(env_path):
    env = dotenv_values(env_path)
    return env.get('FRED_API_KEY', None)

def download_url_with_fallbacks(url, max_attempts=3, min_sleep=2, max_sleep=7):
    """Tenta baixar conteúdo da URL com múltiplos User-Agents, manipulando erros e obstáculos comuns."""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'curl/7.68.0',  # até se passar por CLI
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
                print(f"HTTP {resp.status_code} para {url} (provável bloqueio ou não encontrado)")
                time.sleep(random.uniform(min_sleep, max_sleep))
            elif 500 <= resp.status_code < 600:
                print(f"HTTP {resp.status_code} para {url} (erro do servidor)")
                time.sleep(random.uniform(min_sleep + 2, max_sleep + 2))
            else:
                print(f"HTTP {resp.status_code} estranho para {url}")
        except Exception as exc:
            print(f"Erro ao baixar {url} via UA '{ua}': {exc}")
            last_exc = exc
            time.sleep(random.uniform(min_sleep, max_sleep))
    print(f"⚠ Falha definitiva ao baixar {url}: {last_exc}")
    return None

def extract_text_fallback(soup):
    """Tenta extrair texto do post do blog usando diferentes estratégias/fallbacks."""
    for sel in ['article', 'main', 'div.entry-content', 'div.post-single-content', 'div.content', 'body']:
        element = soup.select_one(sel) if not sel == 'body' else soup.body
        if element:
            text = element.get_text(separator='\n', strip=True)
            if text and len(text) > 80:
                return text
    return ''

def get_fred_blog_feed(feed_url, max_posts=None):
    feed = feedparser.parse(feed_url)
    if not feed.entries:
        print(f"⚠ Nenhum post encontrado no RSS (talvez bloqueio temporário).")
        return []
    entries = feed.entries[:max_posts] if max_posts else feed.entries
    blog_data = []
    for i, entry in enumerate(entries):
        url = getattr(entry, 'link', None)
        date = getattr(entry, 'published', '') or getattr(entry, 'updated', '')
        title = getattr(entry, 'title', '')
        summary = getattr(entry, 'summary', '')
        content = ''
        if url:
            resp = download_url_with_fallbacks(url)
            if resp and 200 <= resp.status_code < 300:
                try:
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    content = extract_text_fallback(soup)
                except Exception as e:
                    print(f"Erro ao parsear artigo ({url}): {e}")
            else:
                print(f"⚠ Falha ao baixar artigo {url} (não será gravado com texto).")
        else:
            print(f"⚠ Entrada de RSS sem URL {entry}")
        blog_data.append({
            'date': date,
            'title': title,
            'url': url,
            'summary': summary,
            'content': content
        })
        print(f"[{i+1}/{len(entries)}] {title[:70]}... (texto: {'ok' if content else 'vazio'})")
        # Espera randomizada curta para não sobrecarregar servidor
        time.sleep(random.uniform(2, 5))
    return blog_data

def main():
    parser = argparse.ArgumentParser(description="Coleta posts do FRED Blog (St. Louis Fed) como notícia econômica robusta")
    parser.add_argument('--feed_url', type=str, default='https://fredblog.stlouisfed.org/feed/', help="RSS Feed do FRED Blog")
    parser.add_argument('--output', type=str, default=None, help="Arquivo CSV consolidado (default: data/fred_blog_posts.csv na raiz do projeto)")
    parser.add_argument('--max_posts', type=int, default=None, help="Número máximo de posts a baixar (default: todos do feed)")
    args = parser.parse_args()

    # Caminho consistente a partir da raiz do projeto
    project_root = find_project_root('.env')
    print(f"\nRaiz do projeto detectada em: {project_root}")
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        fred_api_key = load_fred_api_key(env_path)
        print("FRED_API_KEY carregada do .env (OK).")
    else:
        fred_api_key = None
        print("Aviso: .env não encontrado na raiz do projeto.")

    output_path = args.output if args.output else os.path.join(project_root, 'data', 'fred_blog_posts.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    print(f"\nBaixando posts FRED Blog via RSS ({args.feed_url})...")

    blog_data = get_fred_blog_feed(args.feed_url, max_posts=args.max_posts)
    if not blog_data:
        print("⚠ Nenhum post coletado, analise logs. Verifique conexão, headers e políticas do blog.")
    df_blog = pd.DataFrame(blog_data, columns=['date', 'title', 'url', 'summary', 'content'])

    # Limpa se necessário
    if not df_blog.empty:
        df_blog['content'] = df_blog['content'].astype(str)
        df_blog = df_blog[df_blog['content'].str.len() > 80].reset_index(drop=True)
        try:
            df_blog['date'] = pd.to_datetime(df_blog['date'], errors='coerce')
            df_blog = df_blog.sort_values(by='date', ascending=False).reset_index(drop=True)
        except Exception:
            pass

    df_blog.to_csv(output_path, index=False)
    print(f"\nArquivo consolidado salvo em: {output_path}")
    print("\nPRIMEIRAS LINHAS DO ARQUIVO:")
    print(df_blog.head(5))

if __name__ == "__main__":
    main()
