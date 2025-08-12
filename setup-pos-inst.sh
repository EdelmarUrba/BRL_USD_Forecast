#!/bin/bash
#
# Script de Pós-Instalação: Ambiente Científico, Ferramentas de Desenvolvimento e Ajuste Fino de Teclado
# Projeto: From News to Forecast (Edelmar Urba)
# Compatível com Ubuntu 22.04+ e 25.04
#

set -e
set -u

echo "== 1. ATUALIZANDO SISTEMA =="
sudo apt update && sudo apt upgrade -y

echo "== 2. INSTALANDO PACOTES ESSENCIAIS =="
sudo apt install -y \
  build-essential git unzip curl wget htop \
  ntfs-3g software-properties-common snapd \
  ca-certificates lsb-release apt-transport-https gnupg \
  zsh locales

echo ""
echo "== 3. INSTALANDO E CONFIGURANDO PYTHON/CIÊNCIA DE DADOS =="
sudo apt install -y python3 python3-pip python3-venv
python3 -m venv ~/venvs/news_forecast_env
source ~/venvs/news_forecast_env/bin/activate
pip install --upgrade pip setuptools wheel
pip install numpy pandas scikit-learn matplotlib seaborn scipy jupyterlab tqdm torch torchvision torchaudio transformers datasets

echo ""
echo "== 4. CLONANDO REPOSITÓRIO DO PROJETO =="
git clone https://github.com/EdelmarUrba/BRL_USD_Forecast.git ~/BRL_USD_Forecast || true
if [ -f ~/BRL_USD_Forecast/requirements.txt ]; then
    pip install -r ~/BRL_USD_Forecast/requirements.txt
fi

echo ""
echo "== 5. INSTALANDO UTILITÁRIOS DE DESENVOLVIMENTO =="

echo "-- Instalando o Emacs --"
sudo apt install -y emacs

echo "-- Instalando o VSCode (via repositório oficial) --"
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > packages.microsoft.gpg
sudo install -o root -g root -m 644 packages.microsoft.gpg /usr/share/keyrings/
sudo sh -c 'echo "deb [arch=amd64 signed-by=/usr/share/keyrings/packages.microsoft.gpg] https://packages.microsoft.com/repos/vscode stable main" > /etc/apt/sources.list.d/vscode.list'
sudo apt update
sudo apt install -y code
rm -f packages.microsoft.gpg

echo ""
echo "== 6. INSTALANDO E CONFIGURANDO OH MY ZSH =="

# [PT] Instala o zsh e o Oh My Zsh para o usuário atual
# [EN] Install zsh and Oh My Zsh for the current user

sudo chsh -s $(which zsh) "$USER"
export RUNZSH=no
sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended

echo ""
echo "== 7. AJUSTE DEFINITIVO DO TECLADO ABNT2 (NO DEAD KEYS) =="

# [PT] Configura teclado para ABNT2 no sistema e na sessão gráfica, permanente
# [EN] Sets keyboard to ABNT2 (Brazil, no dead keys) system-wide and for Xorg sessions

# Console e configuração global
sudo sed -i 's/^XKBLAYOUT=.*/XKBLAYOUT="br"/g' /etc/default/keyboard
sudo sed -i 's/^XKBVARIANT=.*/XKBVARIANT="nodeadkeys"/g' /etc/default/keyboard
sudo sed -i 's/^XKBMODEL=.*/XKBMODEL="pc105"/g' /etc/default/keyboard
sudo sed -i 's/^XKBOPTIONS=.*/XKBOPTIONS=""/g' /etc/default/keyboard
sudo dpkg-reconfigure keyboard-configuration
sudo service keyboard-setup restart

# Sessão gráfica do usuário (inclui gnome, wayland e xorg)
if ! grep -q "setxkbmap br nodeadkeys" ~/.profile; then
    echo 'setxkbmap br nodeadkeys' >> ~/.profile
fi
# Sessão atual (funciona apenas em sessões X11, não em Wayland)
setxkbmap br nodeadkeys || true

echo ""
echo "== 8. RESUMO FINAL =="
echo "Ambiente de desenvolvimento, ciência de dados e teclado ABNT2 prontos!"
echo "Para ativar o ambiente Python: source ~/venvs/news_forecast_env/bin/activate"
echo "Para abrir o Visual Studio Code: code &"
echo "Para abrir o Emacs: emacs &"
echo "Para usar o Oh My Zsh, abra um novo terminal ou faça logout/login."
echo "Ajuste do teclado: se ainda faltar alguma tecla, revise Configurações > Região e Idioma > Layout do Teclado e confirme 'Portuguese (Brazil, no dead keys)' como ativo."
echo "Pronto para desenvolver e pesquisar!"

echo ""
echo echo ""
echo "== 9. INSTALANDO MINICONDA E CRIANDO AMBIENTE LLM_NEWS_CUDA =="

CONDA_DIR="$HOME/miniconda3"

# Instala Miniconda se ainda não estiver presente
if ! command -v conda &> /dev/null; then
    echo "-- Baixando instalador Miniconda --"
    wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p $CONDA_DIR
    eval "$($CONDA_DIR/bin/conda shell.bash hook)" || true
    $CONDA_DIR/bin/conda init bash
    $CONDA_DIR/bin/conda init zsh
else
    echo "-- Conda já detectado no sistema, pulando instalação --"
    eval "$(conda shell.bash hook)" || true
fi

# Cria o ambiente "llm_news_cuda" apenas se não existir
if ! conda info --envs | grep -q "llm_news_cuda"; then
    echo "-- Criando o ambiente conda: llm_news_cuda --"
    conda create -y -n llm_news_cuda python=3.11
    echo "-- Ambiente llm_news_cuda criado!"
else
    echo "-- Ambiente llm_news_cuda já existe, pulando criação --"
fi

# Ativa o ambiente e instala pacotes essenciais (ajuste conforme suas necessidades)
echo "-- Instalando bibliotecas básicas no ambiente llm_news_cuda (panda, numpy, scikit-learn, jupyterlab, cuda-toolkit se disponível) --"
conda activate llm_news_cuda
conda install -y numpy pandas scikit-learn matplotlib jupyterlab
conda install -y -c conda-forge pytorch torchvision torchaudio pytorch-cuda=12.1 transformers datasets tqdm

conda deactivate

echo "== Miniconda pronto e ambiente llm_news_cuda inicial criado! =="


# Fim
