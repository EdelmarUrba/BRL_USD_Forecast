#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_jsonlRaw_to_jsonStandard.py

Descrição:
-----------
Padroniza um arquivo JSONL de notícias/textos (formato variado) para o padrão internacional
do artigo From News to Forecast:
campos obrigatórios = title, category, summary, link, publication_time, full_article.

- Preenche com "" campos ausentes, converte datas para ISO.
- Campo category é inserido (default: "FRED Blog") se ausente.
- Gera arquivo de saída seguro e pronto para NLP e merge científico.
- Verifica sobrescrita do arquivo de saída; imprime amostra para validação humana.

Uso:
  python Urba_jsonlRaw_to_jsonStandard.py
  python Urba_jsonlRaw_to_jsonStandard.py --input ARQ_ENTRADA.jsonl --output ARQ_SAIDA.jsonl --category "FRED Blog"

Autor: Edelmar Urba
Data: 2025-07-17

Referências:
- DUNIS, C. L. et al. News will tell: Forecasting FX rates based on news story events. J. Int. Financial Markets... 65, 2020.
- FEDERAL RESERVE BANK OF ST. LOUIS. FRED Blog. https://fredblog.stlouisfed.org. Acesso em: 17 jul. 2025.
"""

import argparse
import json
import os
from datetime import datetime
import sys
import textwrap

# Defaults
DEFAULT_INPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news/Urba_fred_blog_raw.jsonl'
DEFAULT_OUTPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/src/data/processed/Urba_fred_blog_news_standard.jsonl'

HELP_TEXT = """
Transforma JSONL bruto variado em padrão internacional de notícias para NLP/modelagem quantitativa.

Campos garantidos por registro:
- title              : Título (string obrigatória)
- category           : Categoria (ex: 'FRED Blog')
- summary            : Resumo (string; vazio se não houver)
- link               : URL do texto original (string obrigatória)
- publication_time   : Data/hora em ISO, ou vazio se ausente/inválido
- full_article       : Texto completo da notícia/post (string; vazio se não houver)

Exemplo de uso:
    python Urba_jsonlRaw_to_jsonStandard.py
    python Urba_jsonlRaw_to_jsonStandard.py --input ARQ_ENTRADA.jsonl --output ARQ_SAIDA.jsonl --category "FRED Blog"

Campos extras de cada registro são preservados (mas não sobrescrevem os obrigatórios).

Referências em NLP, ciência de dados textuais e macroeconomia quantitativa:
- DUNIS, C. L. et al. News will tell... 2020.
- https://fredblog.stlouisfed.org/
"""

def limpa_nan(valor):
    if valor is None or (isinstance(valor, str) and valor.strip().lower() in ['nan', 'none', '']):
        return ""
    try:
        import math
        if isinstance(valor, float) and math.isnan(valor):
            return ""
    except:
        pass
    return str(valor).strip()

def iso_dt(val):
    val = limpa_nan(val)
    if not val:
        return ""
    s = val.replace('/', '-').replace(' ', 'T')
    try:
        if "T" in s and len(s) >= 19:
            dt = datetime.fromisoformat(s[:19])
        elif len(s) == 10:
            dt = datetime.strptime(s, '%Y-%m-%d')
        else:
            dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        # Tenta pandas (se disponível)
        try:
            import pandas as pd
            dt = pd.to_datetime(s)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except:
            pass
        return ""

def padroniza_registro(orig, categoria_default="FRED Blog"):
    """
    Produz dict padronizado para o artigo. Campos ausentes substituídos por string vazia ou default.
    Nunca remove campos extras, mas prioritiza os chaves do padrão.
    """
    out = {
        "title": limpa_nan(orig.get("title", "")),
        "category": limpa_nan(orig.get("category", categoria_default)) or categoria_default,
        "summary": limpa_nan(orig.get("summary", "")),
        "link": limpa_nan(orig.get("link", "")),
        "publication_time": iso_dt(orig.get("publication_time", orig.get("date", ""))),
        "full_article": limpa_nan(
            orig.get("full_article") or
            orig.get("content") or
            orig.get("texto") or
            orig.get("body") or
            ""
        ),
    }
    # Campos extras: preserva os que não estão em out
    for k in orig:
        if k not in out and (orig[k] is not None) and not (isinstance(orig[k], float) and str(orig[k]) == 'nan'):
            out[k] = limpa_nan(orig[k])
    return out

def print_examples(examples):
    print("\nPrimeiras linhas do arquivo de saída JSONL (padronizado):\n")
    for art in examples:
        print(json.dumps(art, indent=2, ensure_ascii=False), '\n' + '-'*40)

def main():
    parser = argparse.ArgumentParser(description="Padroniza JSONL de notícias para formato científico From News to Forecast.")
    parser.add_argument('--input', '-i', type=str, default=DEFAULT_INPUT, help="Arquivo JSONL de entrada.")
    parser.add_argument('--output', '-o', type=str, default=DEFAULT_OUTPUT, help="Arquivo JSONL de saída padronizada.")
    parser.add_argument('--category', '-c', type=str, default='FRED Blog', help="Categoria padrão se ausente.")
    parser.add_argument('--helpfull', action='store_true', help="Mostra explicação detalhada e dicionário de campos.")
    args = parser.parse_args()

    if args.helpfull:
        print(textwrap.dedent(HELP_TEXT))
        sys.exit(0)

    arq_in = args.input
    arq_out = args.output
    categoria = args.category

    print(f"\nConvertendo JSONL bruto para padrão internacional de notícias:")
    print(f"  Entrada: {arq_in}")
    print(f"  Saída:   {arq_out}")
    print(f"  Categoria default: '{categoria}'\n")

    # Controle de sobrescrita
    if os.path.exists(arq_out):
        print(f"⚠️ Atenção: arquivo de saída já existe: {arq_out}")
        while True:
            resp = input("Deseja sobrescrever (s) ou salvar como nova versão (n) com sufixo _versao_1? [s/n]: ").strip().lower()
            if resp == 's':
                break
            elif resp == 'n':
                root, ext = os.path.splitext(arq_out)
                arq_out = root + "_versao_1" + ext
                print(f"Novo arquivo de saída: {arq_out}")
                break

    artigos = []
    exemplos_out = []
    total, problemas = 0, 0
    with open(arq_in, 'r', encoding='utf-8') as fin:
        for linha in fin:
            total += 1
            try:
                raw = json.loads(linha)
                artigo = padroniza_registro(raw, categoria)
                artigos.append(artigo)
                if len(exemplos_out) < 5:
                    exemplos_out.append(artigo)
            except Exception as e:
                problemas += 1
                if problemas < 3:
                    print(f"⚠️ Problema ao normalizar linha {total}: {e}")

    print(f"Total de notícias processadas: {total}")
    print(f"Notícias válidas geradas:       {len(artigos)}")
    if problemas:
        print(f"Problemas encontrados em {problemas} registros. Verifique o arquivo/log.")

    print(f"\nSalvando o arquivo padronizado em:\n  {arq_out}")
    os.makedirs(os.path.dirname(arq_out), exist_ok=True)
    with open(arq_out, 'w', encoding='utf-8') as fout:
        for art in artigos:
            fout.write(json.dumps(art, ensure_ascii=False) + "\n")

    print_examples(exemplos_out)

    print("\nTransformação finalizada com sucesso!")
    print("\nRecomenda-se conferir as primeiras linhas do arquivo e registrar este passo no diário de bordo.\n")
    print(textwrap.dedent("""
    Referências:
    - DUNIS, C. L. et al. News will tell: Forecasting foreign exchange rates based on news story events in the economy calendar...
    - https://fredblog.stlouisfed.org/
    """))

if __name__ == "__main__":
    main()
