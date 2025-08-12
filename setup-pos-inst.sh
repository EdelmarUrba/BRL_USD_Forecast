#!/bin/bash
#
# Pós-instalação Ubuntu 25.04: Ambiente Científico e Machine Learning
# Projeto: “From News to Forecast: Integrating Event Analysis in LLM-based Time Series Forecasting with Reflection”
# Autor: Edelmar Urba (adaptado com auxílio automatizado)
# Última atualização: 12/08/2025
#
# O objetivo deste script é automatizar a preparação do ambiente computacional, garantindo máxima reprodutibilidade, facilidade de auditoria e aderência às melhores práticas de engenharia de software conforme orientações internacionais e normas da ABNT (NBR 10719:2015).
#
# Instrução: execute com privilégios de superusuário após a instalação mínima do Ubuntu.
# Exemplo: sudo bash setup-pos-inst.sh
#
# 
# ALL INSTRUCTIONS ARE FULLY COMMENTED IN BOTH PORTUGUESE AND ENGLISH.
#

set -e
set -u

echo "------------------------------------------------"
echo "1. ATUALIZAÇÃO DO SISTEMA BÁSICO | SYSTEM UPDATE"
echo "------------------------------------------------"

sudo apt update && sudo apt upgrade -y

echo ""
echo "----------------------------------------------"
echo "2. INSTALAÇÃO DE FERRAMENTAS ESSENCIAIS | BASICS"
echo "----------------------------------------------"

sudo apt install -y \
  build-essential \
  git \
  unzip \
  curl \
  wget \
  htop \
  ntfs-3g \
  software-properties-common \
  snapd \
  ca-certificates \
  lsb-release

echo ""
echo "---------------------------------------------"
echo "3. INSTALAÇÃO DO PYTHON E DEPENDÊNCIAS | PYTHON SETUP"
echo "---------------------------------------------"

sudo apt install -y python3 python3-pip python3-venv

# [EN] Create and activate isolated Python environment for scientific computations
# [PT] Criação e ativação de ambiente Python isolado (virtualenv) para ciência de dados
python3 -m venv ~/venvs/news_forecast_env

echo ""
echo "Ativando o ambiente virtual Python (activate Python virtual environment)"
source ~/venvs/news_forecast_env/bin/activate

echo ""
echo "-----------------------------------------------"
echo "4. ATUALIZAÇÃO DO PIP E INSTALAÇÃO DE PACOTES PYTHON"
echo "-----------------------------------------------"

# [EN] Ensure up-to-date pip, setuptools, and wheel
# [PT] Atualiza pip, setuptools e wheel (boas práticas)
pip install --upgrade pip setuptools wheel

# [EN] Minimal ML/data science stack for LLM & time series (adjust for pipeline needs)
# [PT] Instalação de bibliotecas básicas para ML/ciência de dados (ajuste conforme requisitos do artigo)
pip install \
  numpy \
  pandas \
  scikit-learn \
  matplotlib \
  seaborn \
  scipy \
  jupyterlab \
  tqdm

# [EN] Common stacks for deep learning / Transformers / Huggingface
pip install \
  torch \
  torchvision \
  torchaudio \
  transformers \
  datasets

echo ""
echo "-----------------------------------------------"
echo "5. CLONE DO REPOSITÓRIO DO ARTIGO"
echo "-----------------------------------------------"

# [PT] Troque a URL abaixo caso haja novo endereço ou fork!
echo "Baixando repositório do artigo From News to Forecast..."
git clone https://github.com/EdelmarUrba/BRL_USD_Forecast.git ~/BRL_USD_Forecast

echo ""
echo "-----------------------------------------------"
echo "6. INSTALAÇÃO DE DEPENDÊNCIAS DO PROJETO (SE houver requirements.txt)"
echo "-----------------------------------------------"

if [ -f ~/BRL_USD_Forecast/requirements.txt ]; then
    echo "requirements.txt detectado, instalando bibliotecas do projeto..."
    pip install -r ~/BRL_USD_Forecast/requirements.txt
else
    echo "Não há requirements.txt detectado. Adapte este bloco conforme necessidades futuras."
fi

echo ""
echo "-----------------------------------------------"
echo "7. FINALIZAÇÃO | SUMMARY"
echo "-----------------------------------------------"

echo "Ambiente científico configurado com sucesso!
Para ativar o ambiente Python futuramente, execute:
  source ~/venvs/news_forecast_env/bin/activate

Para rodar Jupyter:
  jupyter lab

Referência do artigo: Edelmar Urba et al. From News to Forecast: Integrating Event Analysis in LLM-based Time Series Forecasting with Reflection. 2025.

Para citar, utilize a ABNT (NBR 6023:2018):

AUTOR(ES). Título do artigo. Título do Periódico, local de publicação, volume, número, página inicial-final, data.

Exemplo:
URBA, Edelmar; et al. From News to Forecast: Integrating Event Analysis in LLM-based Time Series Forecasting with Reflection. [Pré-print], 2025.

"

# Fim do Script
