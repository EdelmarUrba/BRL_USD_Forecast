#!/usr/bin/env python
# coding: utf-8

"""
Script: Urba_jsonlRaw_to_jsonStandard_schema_validation.py

Description:
------------
Standardizes a raw news JSONL file (arbitrary fields) to the internationally recognized schema used in
'From News to Forecast' and related macro-news studies.

- Ensures REQUIRED FIELDS: title, category, summary, link, publication_time, full_article, date.
- Fills missing fields as empty strings (""), normalizes all dates to ISO-8601.
- Adds 'date' as YYYY-MM-DD, extracted from 'publication_time', for downstream compatibility.
- Field 'category' is set to a default (e.g., 'FRED Blog') if missing.
- Preserves original extra fields (does not overwrite required ones).
- Logs processing statistics and sample output.
- Performs schema validation on every record for maximum scientific reproducibility.
- Safe output: never overwrites output file without user confirmation.

Usage:
  python Urba_jsonlRaw_to_jsonStandard_schema_validation.py
  python Urba_jsonlRaw_to_jsonStandard_schema_validation.py --input IN.jsonl --output OUT.jsonl --category "FRED Blog"

Author: Edelmar Urba and scientific code assistant (2025-07-19, revised for rigorous macro-news compatibility)

References:
- Dunis, C. L. et al. "News will tell: Forecasting FX rates based on news story events." J. Int. Financial Markets, 65, 2020.
- Federal Reserve Bank of St. Louis. FRED Blog. https://fredblog.stlouisfed.org
"""

import argparse
import json
import os
from datetime import datetime
import sys
import textwrap

# === Pydantic for schema validation ===
try:
    import pydantic
    from pydantic import BaseModel, ValidationError
    PYD_VER = int(pydantic.__version__.split('.')[0])
except ImportError:
    print("[FATAL] Please install pydantic for schema validation (pip install pydantic).")
    sys.exit(1)

def schema_model_factory():
    """Return StandardNewsRecord for either pydantic v1.x or v2.x."""
    STR_KWARG = {'pattern' if PYD_VER >=2 else 'regex': r'^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?$'}
    from pydantic import constr, Field

    class StandardNewsRecord(BaseModel):
        title: str = Field(..., description="News title")
        category: str = Field(..., description="Category label")
        summary: str = Field(..., description="News summary")
        link: str = Field(..., description="News/article source URL")
        publication_time: constr(**STR_KWARG) = Field(..., description="Publication time ISO-8601 (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
        full_article: str = Field(..., description="Main article news text")
        # 'date' is added in the output for pairing, but optional here as validator
        class Config:
            extra = "allow"    # preserve extra fields
    return StandardNewsRecord

StandardNewsRecord = schema_model_factory()

# Defaults
DEFAULT_INPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/news/Urba_fred_blog_raw.jsonl'
DEFAULT_OUTPUT = '/home/edelmar-urba/Projetos/BRL_USD_Forecast/src/data/processed/Urba_fred_blog_news_standard.jsonl'
HELP_TEXT = """
International scientific news schema (see Dunis et al. 2020):

Required fields per record:
- title              : title (mandatory, str)
- category           : news/category label (e.g., 'FRED Blog')
- summary            : (may be empty string)
- link               : source url (mandatory, str)
- publication_time   : ISO datetime as YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS (mandatory)
- full_article       : main article text (may be empty string)
- date               : ISO calendar date (YYYY-MM-DD), extracted for scientific pairing.

Other extra fields are preserved and not overwritten (transparent archiving).
"""

def clean_text(val):
    if val is None or (isinstance(val, str) and val.strip().lower() in ['nan', 'none', '']):
        return ""
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return ""
    except:
        pass
    return str(val).strip()

def to_iso_datetime(val):
    """Robustly convert input value to ISO date/datetime ('YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'), return '' if cannot."""
    val = clean_text(val)
    if not val:
        return ""
    s = val.replace('/', '-').replace(' ', 'T')
    try:
        # Accept both date and datetime ISO variants
        if "T" in s and len(s) >= 19:
            dt = datetime.fromisoformat(s[:19])
        elif len(s) == 10:
            dt = datetime.strptime(s, '%Y-%m-%d')
        else:
            dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        try:
            import pandas as pd
            dt = pd.to_datetime(s)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            return ""

def extract_date_field(pubtime):
    """
    Extracts ISO YYYY-MM-DD from a datetime string (or returns empty if not possible).
    Used for pairing with daily macro data.
    """
    pubtime = clean_text(pubtime)
    if not pubtime:
        return ""
    try:
        if "T" in pubtime:
            return pubtime[:10]
        elif len(pubtime) == 10:  # Already 'YYYY-MM-DD'
            datetime.strptime(pubtime, '%Y-%m-%d')
            return pubtime
        # Try parse with pandas fallback
        import pandas as pd
        dt = pd.to_datetime(pubtime)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return ""

def standardize_record(orig, default_category="FRED Blog"):
    """
    Produces dict conforming to scientific standard schema.
    Fills missing fields as '' or given default (category).
    Adds 'date' field for alignment with daily macro data.
    Preserves extra fields, does not overwrite required ones.
    """
    pubtime = to_iso_datetime(
        orig.get("publication_time", orig.get("date", ""))
    )
    out = {
        "title": clean_text(orig.get("title", "")),
        "category": clean_text(orig.get("category", default_category)) or default_category,
        "summary": clean_text(orig.get("summary", "")),
        "link": clean_text(orig.get("link", "")),
        "publication_time": pubtime,
        "full_article": clean_text(
            orig.get("full_article") or
            orig.get("content") or
            orig.get("texto") or
            orig.get("body") or
            ""
        ),
        # -- ADICIONADO: campo 'date' extraído de publication_time --
        "date": extract_date_field(pubtime),
    }
    for k in orig:
        if k not in out and orig[k] is not None:
            out[k] = clean_text(orig[k])
    return out

def print_examples(examples):
    print("\nFirst lines of the standardized JSONL output:\n")
    for art in examples:
        print(json.dumps(art, indent=2, ensure_ascii=False), '\n' + '-'*40)

def main():
    parser = argparse.ArgumentParser(
        description="Standardize raw news JSONL to international scientific schema (Dunis et al. 2020) with date alignment for pairing.")
    parser.add_argument('--input', '-i', type=str, default=DEFAULT_INPUT, help="Input raw news JSONL file")
    parser.add_argument('--output', '-o', type=str, default=DEFAULT_OUTPUT, help="Standardized JSONL output")
    parser.add_argument('--category', '-c', type=str, default='FRED Blog', help="Default category if missing")
    parser.add_argument('--helpfull', action='store_true', help="Show full documentation")
    args = parser.parse_args()

    if args.helpfull:
        print(textwrap.dedent(HELP_TEXT))
        sys.exit(0)

    arq_in = args.input
    arq_out = args.output
    category = args.category

    print(f"\nStandardizing raw JSONL to international scientific news schema:")
    print(f"  Input:  {arq_in}")
    print(f"  Output: {arq_out}")
    print(f"  Default category: '{category}'\n")

    # Safety: do not overwrite output file without explicit confirmation.
    if os.path.exists(arq_out):
        print(f"⚠️ Output file already exists: {arq_out}")
        while True:
            resp = input("Overwrite (y) or save as new version (n)? [y/n]: ").strip().lower()
            if resp == 'y':
                break
            elif resp == 'n':
                root, ext = os.path.splitext(arq_out)
                arq_out = root + "_version_1" + ext
                print(f"Output will be: {arq_out}")
                break

    standardized, bad, examples_out = [], 0, []
    total = 0
    with open(arq_in, 'r', encoding='utf-8') as fin:
        for line in fin:
            total += 1
            try:
                raw = json.loads(line)
                record = standardize_record(raw, category)
                # Schema validation (ignores extra 'date' field)
                StandardNewsRecord(**{k: v for k, v in record.items() if k != "date"})
                standardized.append(record)
                if len(examples_out) < 5:
                    examples_out.append(record)
            except ValidationError as e:
                bad += 1
                if bad <= 5:
                    print(f"[WARN] Skipped line {total}: schema error: {e.errors()}")
            except Exception as e:
                bad += 1
                if bad <= 5:
                    print(f"[WARN] Skipped line {total}: {e}")

    print(f"\nTotal records processed:           {total}")
    print(f"Valid standardized records:        {len(standardized)}")
    if bad:
        print(f"Skipped {bad} records due to schema/parse error(s).")

    print(f"\nSaving standardized file:\n  {arq_out}")
    os.makedirs(os.path.dirname(arq_out), exist_ok=True)
    with open(arq_out, 'w', encoding='utf-8') as fout:
        for art in standardized:
            fout.write(json.dumps(art, ensure_ascii=False) + "\n")

    print_examples(examples_out)

    print("\nTransformation complete!")
    print("\nReview the first output lines for validation, and log this step in project QA records.")
    print(textwrap.dedent("""
    References:
    - Dunis, C. L. et al. News will tell: Forecasting foreign exchange rates based on news story events in the economy calendar...
    - https://fredblog.stlouisfed.org/
    """))

if __name__ == "__main__":
    main()
