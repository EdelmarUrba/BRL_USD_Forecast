#!/usr/bin/env python3
# coding: utf-8

"""
Script: Urba_pareia_news_macro_jsonl.py

Descrição:
-----------
Pareia notícias econômicas (JSONL) com séries macroeconômicas (CSV), gerando arquivos para treinamento no padrão From News to Forecast.
* Agora com tratamento robusto de datas: ignora registros "nan"/vazios, informa total de descartes, só executa se houver datas válidas.

Autor: Edelmar Urba
Data: 2025-07-18
"""

import argparse
import pandas as pd
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

DEFAULT_NEWS = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_news_data/Urba_fred_blog_standard.jsonl'
DEFAULT_MACRO = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw_time_series_data/Urba_macro_usa_monthly_consolidated_adp.csv'
DEFAULT_OUTDIR = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/paired_time_series_news_training_data'

def parse_args():
    parser = argparse.ArgumentParser(description="Pareamento de notícias e macroeconômicos para modelagem científica.")
    parser.add_argument('--news', default=DEFAULT_NEWS, help='Arquivo JSONL de notícias')
    parser.add_argument('--macro', default=DEFAULT_MACRO, help='Arquivo CSV macroeconômico')
    parser.add_argument('--start', required=False, help='Data inicial (YYYY-MM-DD) [opcional]')
    parser.add_argument('--end', required=False, help='Data final (YYYY-MM-DD) [opcional]')
    parser.add_argument('--window_months', type=int, default=2, help='Meses antecedentes para janela de notícias')
    parser.add_argument('--target', type=str, default="ADP_Employment_Change", help='Campo alvo do macroeconômico')
    parser.add_argument('--input_fields', nargs='+', default=['summary','full_article'], help='Campos das notícias')
    parser.add_argument('--outdir', default=DEFAULT_OUTDIR, help='Diretório de saída')
    return parser.parse_args()

def read_news(news_path):
    news = []
    n_nan = 0
    with open(news_path, 'r', encoding='utf-8') as f:
        for line in f:
            raw = json.loads(line)
            dtstr = str(raw.get('publication_time','')).split('T')[0]
            if not dtstr or dtstr.lower() == 'nan':
                n_nan += 1
                continue
            try:
                dt = pd.to_datetime(dtstr)
                raw['date'] = dt
                news.append(raw)
            except Exception:
                n_nan += 1
                continue
    if n_nan:
        print(f"[AVISO] {n_nan} registros de notícias sem data válida foram ignorados.")
    if not news:
        print("[ERRO] Nenhum registro com data válida encontrado nas notícias.")
    df_news = pd.DataFrame(news)
    return df_news

def read_macro(macro_path):
    df = pd.read_csv(macro_path)
    df['date'] = pd.to_datetime(df['date'])
    return df

def get_window_start(date, window_months):
    return date - relativedelta(months=window_months)

def clean_text(s):
    if not s or str(s).strip().lower() in ['nan','none']:
        return ''
    return ' '.join(str(s).split())

def build_input_text(news_slice, input_fields):
    textos = []
    for _, row in news_slice.iterrows():
        texto = ' '.join([clean_text(row.get(f,'')) for f in input_fields])
        if texto.strip():
            textos.append(texto)
    return '\n'.join(textos)

def get_outfile_name(outdir, start, end):
    start_s = pd.to_datetime(start).date() if start else 'inicio'
    end_s = pd.to_datetime(end).date() if end else 'fim'
    filename = f"Urba_BRL_USD_train_data_{start_s}_{end_s}.jsonl"
    return os.path.join(outdir, filename)

def main():
    args = parse_args()
    print(f"\n[INFO] Lendo arquivos de entrada:")
    print(f" - Notícias:     {args.news}")
    print(f" - Macroeconômico: {args.macro}")

    news_df = read_news(args.news)
    macro_df = read_macro(args.macro)

    # === Impressão para controle humano ===
    if news_df.empty or news_df["date"].dropna().empty:
        print("\n[ERRO] Nenhuma data válida encontrada nas notícias. Ajuste o arquivo de entrada ou revise os campos 'publication_time'.")
        return
    non_null_dates = news_df['date'].dropna()
    news_min, news_max = non_null_dates.min(), non_null_dates.max()
    macro_min, macro_max = macro_df['date'].min(), macro_df['date'].max()

    print("\n>>> RESUMO DAS DATAS <<<")
    print(f"Notícias:         {news_min.date()} até {news_max.date()} ({(news_max - news_min).days} dias)")
    print(f"Macro:            {macro_min.date()} até {macro_max.date()} ({(macro_max - macro_min).days} dias)")
    print(f"Total notícias (com data):   {len(news_df)}")
    print(f"Total macro rows: {len(macro_df)}")
    print(f"Amostra datas Notícias: {[d.strftime('%Y-%m-%d') for d in sorted(non_null_dates.unique())[:5]]} ...")
    print(f"Amostra datas Macro:    {[d.strftime('%Y-%m-%d') for d in sorted(macro_df['date'].unique())[:5]]} ...\n")

    resp = input("Deseja continuar com o processamento destas datas? [s=sim/qualquer tecla=não]: ").strip().lower()
    if resp != 's':
        print("\n[INFO] Operação cancelada. Modifique os parâmetros (--start, --end, --news, --macro) conforme necessário.")
        return

    # Filtro de datas via argumentos
    macro_df_orig = macro_df.copy()
    if args.start:
        macro_df = macro_df[macro_df['date'] >= pd.to_datetime(args.start)]
    if args.end:
        macro_df = macro_df[macro_df['date'] <= pd.to_datetime(args.end)]
    if macro_df.empty:
        print("\n[ERRO] Macroeconômico sem registros no período definido.")
        return

    out_candidate = get_outfile_name(args.outdir, args.start or macro_df['date'].min(), args.end or macro_df['date'].max())
    print(f"\n[INFO] Padrão nome de saída: {out_candidate}")
    if os.path.exists(out_candidate):
        esc = input("Arquivo já existe. Sobrescrever (s), editar nome (e), ou cancelar (c)? [s/e/c]: ").strip().lower()
        if esc == "e":
            out_candidate = input("Informe novo caminho/arquivo de saída (.jsonl): ").strip()
        elif esc == "c":
            print("Operação cancelada.")
            return
    else:
        resp2 = input(f"Confirma arquivo de saída [{out_candidate}]? (s para confirmar/qualquer tecla para editar): ").strip().lower()
        if resp2 != "s":
            out_candidate = input("Informe novo caminho/arquivo de saída (.jsonl): ").strip()

    print("\n[INFO] Gerando exemplos pareados...")
    os.makedirs(os.path.dirname(out_candidate), exist_ok=True)
    n_pairs = 0
    sample_obj = None
    for idx, macro_row in macro_df.iterrows():
        dtmacro = macro_row['date']
        date_ini = get_window_start(dtmacro, args.window_months)
        news_slice = news_df[(news_df['date'] >= date_ini) & (news_df['date'] <= dtmacro)]
        input_text = build_input_text(news_slice, args.input_fields)
        if not input_text.strip():
            continue
        target = clean_text(macro_row.get(args.target,""))
        obj = {
            "instruction": f"Given all economic and financial news texts from {date_ini.date()} to {dtmacro.date()}, predict the value of {args.target} for {dtmacro.date()}.",
            "input": input_text,
            "output": target,
            "date": str(dtmacro.date()),
            "window_months": args.window_months,
            "target_field": args.target
        }
        with open(out_candidate, 'a', encoding='utf-8') as fout:
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
        n_pairs += 1
        if sample_obj is None:
            sample_obj = obj

    print(f"\n[OK] Total de pares salvos: {n_pairs}")
    print(f"[Arquivo de saída]: {out_candidate}")
    print(f"[Data/hora de execução]: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("\n[Amostra de exemplo pareado:]")
    if sample_obj:
        print(json.dumps(sample_obj, indent=2, ensure_ascii=False))
    else:
        print("[nenhum exemplo foi criado - revise os dados e parâmetros]")

if __name__ == "__main__":
    main()
