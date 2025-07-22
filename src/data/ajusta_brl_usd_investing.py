#!/usr/bin/env python3
"""
Script to read and standardize the file Dados_Históricos_Índice_Dólar.ods
to the ISO 8601 date format (YYYY-MM-DD), as required for all future steps
of the BRL-USD-Forecast project.

- Reads the ODS file downloaded from Investing.com.
- Standardizes columns and dates to ISO 8601.
- Saves the result as a CSV ready for integration into the pipeline.

Place this script in: /home/edelmar-urba/Projetos/BRL_USD_Forecast/src/data/
"""

import pandas as pd
import os

# Input and output paths
input_path = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/raw/brl_usd/Dados_Históricos_Índice_Dólar.ods"
output_path = "/home/edelmar-urba/Projetos/BRL_USD_Forecast/data/processed/brl_usd_historico_iso.csv"

# Read the ODS file
df = pd.read_excel(input_path, engine='odf')

print("First rows of the original file:")
print(df.head())

# Rename columns to project standard (English, ISO)
df = df.rename(columns={'Data': 'Date', 'Último': 'USD_Index_Close'})

# Convert date column to ISO 8601 (YYYY-MM-DD), coercing errors
df['Date'] = pd.to_datetime(df['Date'], format="%d.%m.%Y", errors='coerce')

# Remove rows with invalid dates
df = df[df['Date'].notna()].copy()

# Format dates as ISO 8601 strings
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

# Select only relevant columns
df_out = df[['Date', 'USD_Index_Close']].copy()
df_out = df_out.dropna(subset=['Date', 'USD_Index_Close'])

# Create output directory if needed
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df_out.to_csv(output_path, index=False)

print(f"\nISO-standardized file saved at: {output_path}")
print("First rows of the ISO-standardized file:")
print(df_out.head())
print(f"Total valid rows: {len(df_out)}")
