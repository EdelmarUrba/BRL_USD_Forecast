#!/bin/bash
#
# Script de pós-instalação consolidado e idempotente
# Projeto: From News to Forecast
# Autor: Edelmar Urba
# Última atualização: 12/08/2025
#
# Pode ser executado diversas vezes; instala apenas o que estiver faltando.
#

set -e
set -u

### Funções utilitárias ###

install_if_missing() {
    for pkg in "$@"; do
        if dpkg -s "$pkg" &>/dev/null; then
            echo "[APT] Pacote '$pkg' já instalado, pulando."
        else
            echo "[APT] Instalando '$pkg'..."
            sudo apt install -y "$pkg"
        fi
    done
}

pip_install_if_missing() {
    for pkg in "$@"; do
        if python3 -m pip show "$pkg" &>/dev/null; then
            echo "[PIP] Pacote '$pkg' já instalado, pulando."
        else
            echo "[PIP] Instalando '$pkg'..."
            pip install "$pkg"
        fi
    done
}

conda_install_if_missing() {
    for pkg in "$@"; do
        if conda list | grep -q "^$pkg "; then
            echo "[CONDA] Pacote '$pkg' já instalado, pulando."
        else
            echo "[CONDA] Instalando '$pkg'..."
            conda install -y "$pkg"
        fi
    done
}

### 1. Atualizar sistema ###
echo "== Atualizando sistema =="
sudo apt update && sudo apt upgrade -y

### 2. Instalar pacotes essenciais ###
echo "== Instalando pacotes essenciais =="
install_if_missing build-essential git unzip curl wget htop \
                   ntfs-3g software-properties-common snapd \
                   ca-certificates lsb-release apt-transport-https gnupg \
                   zsh locales emacs

### 3. VSCode ###
if ! command -v code &>/dev/null; then
    echo "-- Instalando Visual Studio Code --"
    wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > packages.microsoft.gpg
    sudo install -o root -g root -m 644 packages.microsoft.gpg /usr/share/keyrings/
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/packages.microsoft.gpg] \
    https://packages.microsoft.com/repos/vscode stable main" | \
    sudo tee /etc/apt/sources.list.d/vscode.list
    sudo apt update
    sudo apt install -y code
    rm -f packages.microsoft.gpg
else
    echo "[APT] VSCode já instalado, pulando."
fi

### 4. Python e pacotes científicos ###
echo "== Configurando Python e venv =="
install_if_missing python3 python3-pip python3-venv
python3 -m venv ~/venvs/news_forecast_env || true
source ~/venvs/news_forecast_env/bin/activate
pip install --upgrade pip setuptools wheel
pip_install_if_missing numpy pandas scikit-learn matplotlib seaborn scipy jupyterlab tqdm torch torchvision torchaudio transformers datasets
deactivate

### 5. Clonar/atualizar repositório do projeto ###
if [ ! -d "$HOME/BRL_USD_Forecast" ]; then
    git clone https://github.com/EdelmarUrba/BRL_USD_Forecast.git ~/BRL_USD_Forecast
else
    echo "== Atualizando repositório BRL_USD_Forecast =="
    git -C ~/BRL_USD_Forecast pull
fi

### 6. Oh My Zsh ###
if [ ! -d "$HOME/.oh-my-zsh" ]; then
    echo "== Instalando Oh My Zsh =="
    sudo chsh -s "$(which zsh)" "$USER"
    export RUNZSH=no
    sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended
else
    echo "Oh My Zsh já instalado, pulando."
fi

### 7. Configuração do teclado ###
echo "== Configurando teclado ABNT2 no dead keys =="
sudo bash -c 'cat > /etc/default/keyboard <<EOF
XKBMODEL="pc105"
XKBLAYOUT="br"
XKBVARIANT="nodeadkeys"
XKBOPTIONS=""
EOF'
sudo dpkg-reconfigure keyboard-configuration
sudo service keyboard-setup restart
if ! grep -q "setxkbmap br nodeadkeys" ~/.profile; then
    echo 'setxkbmap br nodeadkeys' >> ~/.profile
fi

### 8. Miniconda + ambiente llm_news_cuda ###
CONDA_DIR="$HOME/miniconda3"
if ! command -v conda &>/dev/null; then
    echo "== Instalando Miniconda =="
    wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p "$CONDA_DIR"
    eval "$($CONDA_DIR/bin/conda shell.bash hook)"
    conda init bash
    conda init zsh
else
    echo "Miniconda já instalada, pulando."
    eval "$(conda shell.bash hook)"
fi

if ! conda env list | grep -q "llm_news_cuda"; then
    echo "-- Criando ambiente conda llm_news_cuda --"
    conda create -y -n llm_news_cuda python=3.11
    conda activate llm_news_cuda
    conda_install_if_missing numpy pandas scikit-learn matplotlib jupyterlab
    conda install -y -c pytorch pytorch torchvision torchaudio pytorch-cuda=12.1 -c nvidia
    conda_install_if_missing transformers datasets tqdm
    conda deactivate
else
    echo "Ambiente llm_news_cuda já existe, pulando criação."
fi

### Final ###
echo "== Setup concluído! =="
echo "Para ativar o ambiente Python (venv): source ~/venvs/news_forecast_env/bin/activate"
echo "Para ativar o ambiente Conda: conda activate llm_news_cuda"
