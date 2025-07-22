#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_transforma_news_to_article_standard.py

Descrição:
-----------
Transforma qualquer corpus de notícias do projeto (FRED Blog, Announcements etc.) 
para o formato-padrão de estrutura de notícia adotado no artigo “From News to Forecast”.
Salva o resultado como arquivo JSONL, prefixado por Urba_, pronto para NLP/modelagem compatíveis.

Funcionalidades:
  - Padroniza campos: title, category, summary, link, publication_time, full_article
  - Normaliza datas paraISO (YYYY-MM-DDTHH:MM:SS)
  - Permite especificar categoria default (ex: “FRED Blog”)
  - Loga exemplos humanos de entrada e saída
  - Pode gerar relatório básico de consistência entre bases (opcional future)
  - Inclui dicionário de campos (--help)
  - Referências de fontes e documentação como exige comunidade científica

Modo de uso:
    python Urba_transforma_news_to_article_standard.py --input Urba_fred_blog_raw.jsonl --output Urba_fred_blog_standard.jsonl --category "FRED Blog"
    python Urba_transforma_news_to_article_standard.py --help

Autor: Edelmar Urba
Data: 2025-07-17
"""

import argparse
import os
import json
from datetime import datetime
import sys
import textwrap

DIC_CAMPOS = """
Dicionário dos campos do arquivo de saída:

- title             : Título da notícia/artigo
- category          : Categoria (ex: "FRED Blog", "Fed Announcement", "Breaking News")
- summary           : Resumo breve (primeiro parágrafo ou description; opcional)
- link              : URL do artigo original
- publication_time  : Data/hora ISO de publicação (YYYY-MM-DDTHH:MM:SS)
- full_article      : Texto integral da notícia/postagem

Formato: JSONL (um objeto json por linha)
"""

REFERENCIAS = """
Referências principais:
- DUNIS, C. L. et al. News will tell: Forecasting foreign exchange rates based on news story events in the economy calendar. Journal of International Financial Markets, Institutions and Money, 65, 2020.
  Disponível em: https://www.sciencedirect.com/science/article/pii/S1042443120300693. Acesso em: 17 jul. 2025.
- FRED Blog. St. Louis Fed. https://fredblog.stlouisfed.org. Acesso em: 17 jul. 2025.
- ADP National Employment Report. https://adpemploymentreport.com/. Acesso em: 17 jul. 2025.
- Urba. Pipeline & código autoral: https://github.com/EdelmarUrba (repo privado). 2025.
"""

def print_help():
    print("\n" + "="*60)
    print("SCRIPT DE PADRONIZAÇÃO DE BASES DE NOTÍCIAS (NEWS/ARTICLE) - PROJETO BRL_USD")
    print("="*60)
    print("\nFinalidade:")
    print(textwrap.fill(
        "Transforma arquivos de notícias do projeto para o padrão de estrutura adotado no artigo From News to Forecast. "
        "Facilita merge automático, reuso de scripts de NLP/originais, e controle de compatibilidade científica.", width=80))
    print("\nModo de uso:")
    print(" python Urba_transforma_news_to_article_standard.py --input Urba_fred_blog_raw.jsonl --output Urba_fred_blog_standard.jsonl --category \"FRED Blog\"\n")
    print(DIC_CAMPOS)
    print("Referências e fontes utilizadas:")
    print(REFERENCIAS)
    print("\nExemplo de execução:\n"
          "  python Urba_transforma_news_to_article_standard.py --input Urba_fed_notes_raw.jsonl --output Urba_fed_notes_standard.jsonl --category \"Fed Notes\"")
    print("="*60)
    sys.exit(0)

def padroniza_noticia(item, categoria_default="FRED Blog"):
    """Transforma um dict de notícia para o padrão article original."""
    def _get(field, altlist=None, default=""):
        for key in ([field] + (altlist or [])):
            if key in item and item[key]:
                return str(item[key])
        return default

    # Normaliza data/hora (tenta padrões mais comuns)
    pub_time = _get('publication_time', ['date', 'publishedAt', 'published', 'data'])
    # Tenta modo ISO, se não der, tenta outros formatos
    try:
        pub_time = str(pub_time).replace('/', '-').replace(' ', 'T')
        if len(pub_time) == 10:
            dt = datetime.strptime(pub_time, "%Y-%m-%d")
            pub_time = dt.strftime("%Y-%m-%dT00:00:00")
        elif "T" in pub_time and len(pub_time) >= 19:
            dt = datetime.fromisoformat(pub_time[:19])
            pub_time = dt.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            dt = datetime.strptime(pub_time, "%Y-%m-%d %H:%M:%S")
            pub_time = dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        pub_time = str(pub_time)[:19].replace(' ', 'T') or ""

    # Resumo: tenta varios campos
    summary = _get('summary', ['descricao', 'description', 'resumo'], default="")

    return {
        "title": _get('title', ['titulo', 'headline']),
        "category": _get('category') or categoria_default,
        "summary": summary,
        "link": _get('link', ['url']),
        "publication_time": pub_time,
        "full_article": _get('full_article', ['content', 'texto', 'body', 'artigo'], default="")
    }

def main():
    parser = argparse.ArgumentParser(description="Padroniza corpus de notícias (JSONL) para o formato do artigo From News to Forecast.")
    parser.add_argument('--input', '-i', required=True, help="Arquivo JSONL de entrada (ex: Urba_fred_blog_raw.jsonl)")
    parser.add_argument('--output', '-o', required=True, help="Arquivo JSONL de saída (ex: Urba_fred_blog_standard.jsonl)")
    parser.add_argument('--category', '-c', default="FRED Blog", help="Categoria default, caso ausente (ex: 'FRED Blog')")
    parser.add_argument('--show_examples', action='store_true', help="Exibe exemplos de entrada/saída e comparação de campos")
    parser.add_argument('--helpfull', action='store_true', help="Ajuda detalhada e dicionário de campos")
    args = parser.parse_args()

    if args.helpfull:
        print_help()

    arq_entrada, arq_saida, categoria = args.input, args.output, args.category

    print(f"\nIniciando transformação e padronização do arquivo:\n  Origem : {arq_entrada}\n  Saída  : {arq_saida}\n  Categoria: {categoria}\n")

    n_total, n_ok, n_erro = 0, 0, 0
    exemplos_orig, exemplos_padron = [], []
    with open(arq_entrada, 'r', encoding='utf-8') as fin, open(arq_saida, 'w', encoding='utf-8') as fout:
        for linha in fin:
            n_total += 1
            try:
                raw = json.loads(linha)
                artigo = padroniza_noticia(raw, categoria)
                # Simples checagem de completude mínima (pelo menos título e body)
                if artigo["title"] and artigo["full_article"]:
                    n_ok += 1
                    fout.write(json.dumps(artigo, ensure_ascii=False) + "\n")
                else:
                    n_erro += 1
                if len(exemplos_orig) < 2:
                    exemplos_orig.append(raw)
                if len(exemplos_padron) < 2:
                    exemplos_padron.append(artigo)
            except Exception as e:
                n_erro += 1
                if n_erro < 3:
                    print(f"⚠️ Erro na linha {n_total}: {e}")

    print(f"\nLinhas processadas: {n_total}")
    print(f"Notícias padrão salvas: {n_ok}")
    print(f"Erros/descartes: {n_erro}")

    # Exemplo para controle humano
    print("\nExemplo de linha do arquivo de ENTRADA ORIGINAL:")
    print(json.dumps(exemplos_orig[0], indent=2, ensure_ascii=False))

    print("\nExemplo correspondente do arquivo de SAÍDA PADRONIZADO:")
    print(json.dumps(exemplos_padron[0], indent=2, ensure_ascii=False))

    print("\nDicionário de campos --help ou consulte o script.")
    print("\nReferências empregadas no padrão e pipeline:")
    print(REFERENCIAS)

    print("\nProcesso finalizado com sucesso! Arq. salvo:", arq_saida)

if __name__ == '__main__':
    main()
